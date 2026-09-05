#!/usr/bin/env python3
"""VIXION 3.0 Increment 2.1 — observe real V2.8 durable logs and produce proposals (offline).

    python scripts/v30_observe_v28.py --source-data-dir /data --policy ask_le_90c \
        --output /data/v30_observer --once [--as-of ISO] [--json]

    python scripts/v30_observe_v28.py --source-data-dir /data --policy ask_le_90c \
        --output /data/v30_observer --watch 2

Reads ONLY the local filesystem. No network, no exchange access, no ticks, no market results, no orders.
Exit codes: 0 ok · 2 fail-closed (bad args, unknown policy, corrupt/conflicting source,
persistence failure) · 3 missing trace context in --once · 130 Ctrl+C in --watch.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import vixion_v30_observer as obs  # noqa: E402
import vixion_v30_proposal as v30  # noqa: E402


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="VIXION 3.0 real V2.8 source observer (proposal-only, offline)")
    ap.add_argument("--source-data-dir", required=True, help="directory holding entry_timing_{traces,decisions}_v28.jsonl[.1]")
    ap.add_argument("--policy", required=True, help="explicit canonical V2.8 policy_id (no default, no auto-selection)")
    ap.add_argument("--output", help=f"output directory (default: <source>/{obs.DEFAULT_OUTPUT_SUBDIR})")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="single full scan")
    mode.add_argument("--watch", type=float, metavar="SECONDS", help="rescan the local files every N seconds (min 1)")
    ap.add_argument("--as-of", help="ISO-8601 UTC instant for expiry evaluation (--once only; default: now)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--max-scans", type=int, default=0, help=argparse.SUPPRESS)  # tests only: stop watch after N scans
    return ap.parse_args(argv)


def _emit(report, as_json):
    print(json.dumps(report, indent=2, sort_keys=True) if as_json else obs.human_summary(report))
    sys.stdout.flush()


def main(argv=None, environ=None, clock=None, sleep=None):
    args = parse_args(argv)
    env = os.environ if environ is None else environ
    clock = clock or obs.utc_now
    sleep = sleep or time.sleep
    specs = v30.load_canonical_policy_specs()
    if args.policy not in specs:
        print(f"FAIL CLOSED: unknown policy_id {args.policy!r} (not in the canonical V2.8 catalogue of {len(specs)})", file=sys.stderr)
        return 2
    try:
        config = v30.config_from_env(args.policy, env)
    except ValueError as exc:
        print(f"FAIL CLOSED: {exc}", file=sys.stderr)
        return 2
    source = Path(args.source_data_dir)
    if not source.is_dir():
        print(f"FAIL CLOSED: source data dir not found: {source}", file=sys.stderr)
        return 2
    output = Path(args.output) if args.output else source / obs.DEFAULT_OUTPUT_SUBDIR
    if args.watch is not None and args.as_of:
        print("FAIL CLOSED: --as-of is only valid with --once", file=sys.stderr)
        return 2
    if args.watch is not None and args.watch < 1.0:
        print("FAIL CLOSED: --watch interval must be >= 1 second", file=sys.stderr)
        return 2

    if args.once:
        as_of = clock()
        if args.as_of:
            as_of = v30.parse_iso_utc(args.as_of)
            if as_of is None:
                print("FAIL CLOSED: --as-of must be ISO-8601 with timezone", file=sys.stderr)
                return 2
        try:
            report, _ = obs.reconcile_once(source, output, config, specs, as_of, mode="once", scan_at_utc=clock())
        except obs.SourceMissingContextError as exc:
            print(f"FAIL CLOSED (missing context): {exc}", file=sys.stderr)
            return 3
        except (obs.SourceError, v30.ProposalError, ValueError, OSError) as exc:
            print(f"FAIL CLOSED: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 2
        _emit(report, args.json)
        if report["persist_failed"]:
            print(f"FAIL CLOSED: {report['persist_failed']} proposal event(s) could not be persisted", file=sys.stderr)
            return 2
        return 0

    scans = 0
    try:
        while True:
            now = clock()
            try:
                report, _ = obs.reconcile_once(source, output, config, specs, now, mode="watch", scan_at_utc=now)
            except (obs.SourceError, v30.ProposalError, ValueError, OSError) as exc:
                # Corruption/conflict/persistence: stop, non-zero, no infinite retry.
                print(f"FAIL CLOSED: {type(exc).__name__}: {exc}", file=sys.stderr)
                return 2
            _emit(report, args.json)
            if report["persist_failed"]:
                print(f"FAIL CLOSED: {report['persist_failed']} proposal event(s) could not be persisted", file=sys.stderr)
                return 2
            scans += 1
            if args.max_scans and scans >= args.max_scans:
                return 0
            sleep(args.watch)
    except KeyboardInterrupt:
        print("observer stopped (Ctrl+C)", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
