#!/usr/bin/env python3
"""VIXION 3.0 Increment 1 — offline Proposal Lab.

Reads already-observed VIXION 2.8 artefacts from LOCAL files, builds proposals for
ONE explicitly named policy, persists them to an append-only event log and prints a
summary. It never opens a network connection: there is no HTTP client in this
file or in vixion_v30_proposal.py.

    python scripts/v30_proposal_lab.py \
        --decisions data/entry_timing_decisions_v28.jsonl \
        --traces    data/entry_timing_traces_v28.jsonl \
        --policy    ask_le_90c \
        --output    ./data/v30

Optional replay / lifecycle helpers (all deterministic, all local):
    --as-of 2026-09-01T13:39:00+00:00   expire PROPOSED proposals whose TTL elapsed at that instant
    --reviews reviews.jsonl             apply explicit SIMULATED_APPROVED / SIMULATED_REJECTED events
                                        ({"proposal_id":..., "verdict":"SIMULATED_APPROVED", "at_utc":..., "reason":...})
    --replay-only                       rebuild the index from the log and print it; build nothing
    --json                              machine-readable summary
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import vixion_v30_proposal as v30  # noqa: E402


def read_jsonl(path):
    out = []
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"input not found: {p}")
    with open(p, "r", encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{p}:{n}: invalid JSON ({exc})")
    return out


def load_contexts(trace_records):
    """setup_id -> ctx from 2.8 `open` trace events; conflicting duplicates fail closed."""
    return v30.contexts_from_trace_events(trace_records)


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="VIXION 3.0 proposal-only lab (offline)")
    ap.add_argument("--decisions", help="entry_timing_decisions_v28.jsonl (or self-contained records with a `ctx` key)")
    ap.add_argument("--traces", help="entry_timing_traces_v28.jsonl (provides the frozen ctx per setup_id)")
    ap.add_argument("--policy", help="policy_id from the canonical 2.8 catalogue (required to build)")
    ap.add_argument("--output", default=str(ROOT / "data" / v30.DEFAULT_OUTPUT_SUBDIR))
    ap.add_argument("--as-of", help="ISO-8601 UTC instant used ONLY to evaluate TTL expiry deterministically")
    ap.add_argument("--reviews", help="JSONL of explicit simulated review events")
    ap.add_argument("--replay-only", action="store_true")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args(argv)


def main(argv=None, environ=None):
    args = parse_args(argv)
    env = os.environ if environ is None else environ
    out_dir = Path(args.output)
    log_path = out_dir / v30.EVENT_LOG_NAME
    specs = v30.load_canonical_policy_specs()
    summary = {"gateway_version": v30.GATEWAY_VERSION, "policy_catalog_size": len(specs),
               "policy_catalog_sha256": v30.catalog_fingerprint(specs), "event_log": str(log_path)}

    # All inputs are validated BEFORE the store is opened so a rejected run leaves no trace.
    decisions = contexts = config = None
    if not args.replay_only:
        if not args.policy:
            print("FAIL CLOSED: --policy is required (no automatic policy selection)", file=sys.stderr)
            return 2
        if args.policy not in specs:
            print(f"FAIL CLOSED: unknown policy_id {args.policy!r} (not in the 2.8 catalogue)", file=sys.stderr)
            return 2
        if not args.decisions:
            print("FAIL CLOSED: --decisions is required to build proposals", file=sys.stderr)
            return 2
        try:
            config = v30.config_from_env(args.policy, env)
        except ValueError as exc:
            print(f"FAIL CLOSED: {exc}", file=sys.stderr)
            return 2
        decisions = read_jsonl(args.decisions)
        try:
            contexts = load_contexts(read_jsonl(args.traces)) if args.traces else {}
        except (v30.ContextConflictError, v30.ContextInputError) as exc:
            print(f"FAIL CLOSED: {exc}", file=sys.stderr)
            return 2

    try:
        store = v30.ProposalStore(log_path, specs)
    except v30.ProposalError as exc:
        print(f"FAIL CLOSED: event log cannot be replayed: {exc}", file=sys.stderr)
        return 2

    report = None
    if not args.replay_only:
        recorded_at = v30.iso_utc(datetime.now(timezone.utc))
        report, _ = v30.process_decisions(decisions, contexts, config, specs, store, recorded_at_utc=recorded_at)
        summary["config"] = config.to_dict()

    if args.as_of:
        as_of = v30.parse_iso_utc(args.as_of)
        if as_of is None:
            print("FAIL CLOSED: --as-of must be ISO-8601 with timezone", file=sys.stderr)
            return 2
        try:
            summary["expired_now"] = store.expire_due(as_of)
        except v30.ProposalError as exc:
            print(f"FAIL CLOSED: expiry persistence failed: {exc}", file=sys.stderr)
            return 2

    if args.reviews:
        applied, skipped = [], []
        for rec in read_jsonl(args.reviews):
            verdict = rec.get("verdict")
            if verdict not in (v30.SIMULATED_APPROVED, v30.SIMULATED_REJECTED):
                skipped.append({"proposal_id": rec.get("proposal_id"), "why": "invalid_verdict"})
                continue
            try:
                ev = v30.make_event(verdict, rec.get("proposal_id"), rec.get("at_utc"), rec.get("reason", ""))
                outcome = store.record(ev)
                (applied if outcome == "applied" else skipped).append({"proposal_id": rec.get("proposal_id"), "why": outcome})
            except v30.PersistenceError as exc:
                print(f"FAIL CLOSED: review persistence failed: {exc}", file=sys.stderr)
                return 2
            except v30.ProposalError as exc:
                skipped.append({"proposal_id": rec.get("proposal_id"), "why": str(exc)})
        summary["reviews_applied"] = applied
        summary["reviews_skipped"] = skipped

    # Report is always derived from a fresh replay of the log (log = truth, index = cache).
    store.rebuild()
    summary["report"] = v30.summarize_store(store, report)
    summary["proposals"] = {pid: {"status": st["status"], "asset": st["proposal"]["asset"], "side": st["proposal"]["side"],
                                  "contract": st["proposal"]["contract_ticker"], "limit_price_cents": st["proposal"]["limit_price_cents"],
                                  "observed_ask_cents": st["proposal"]["observed_ask_cents"], "quantity": st["proposal"]["quantity"],
                                  "expires_at_utc": st["proposal"]["expires_at_utc"]}
                            for pid, st in sorted(store.index.items())}
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        r = summary["report"]
        print(f"VIXION 3.0 proposal lab · {v30.GATEWAY_VERSION} · policy catalogue {len(specs)} policies")
        print(f"event log: {log_path}")
        for k in ("decisions_seen", "eligible_decisions", "proposals_created", "duplicate_suppressed", "invalid_rejected",
                  "conflict_rejected", "persist_failed", "expired", "simulated_approved", "simulated_rejected", "proposals_in_store"):
            print(f"  {k:22s} {r.get(k, 0)}")
        for k in ("by_asset", "by_policy_id", "by_side", "rejection_reasons"):
            if r.get(k):
                print(f"  {k}: {json.dumps(r[k], sort_keys=True)}")
        for pid, p in summary["proposals"].items():
            print(f"  {pid} {p['status']:19s} {p['asset']:5s} {p['side']:4s} {p['contract']} ask {p['observed_ask_cents']} "
                  f"limit {p['limit_price_cents']}c x{p['quantity']} exp {p['expires_at_utc']}")
        print("NO ORDER SURFACE · proposal-only · nothing was sent anywhere")
    return 0


if __name__ == "__main__":
    sys.exit(main())
