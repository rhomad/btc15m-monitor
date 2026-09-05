#!/usr/bin/env python3
"""VIXION 3.0 — Increment 2.1: Real V2.8 Source Observer / Reconciler.

    V2.8 durable logs (traces + decisions, rotated .1 then active)
        -> strict snapshot reader (binary, strict UTF-8, complete lines only)
        -> source reconciliation (trace-open index, decision identity, conflicts)
        -> existing vixion_v30_proposal.build_proposal / process_decisions (UNCHANGED core)
        -> existing ProposalStore -> <output>/proposal_events.jsonl
        -> existing ProposalStore.expire_due(as_of)

Hard boundaries of this module:
  * V2.8 artefacts are READ ONLY: opened with "rb" exclusively; never renamed, rotated,
    shortened, locked or written. Output lives in a separate directory.
  * No ticks, no market results, no exchange/HTTP access, no sockets, no
    credentials. Proposals derive from durable Decision + durable trace `open` ctx +
    explicit policy/config only (no hindsight).
  * Explicit policy only: the caller must name one canonical V2.8 policy_id.
  * Full rescan on every run; there is no persistent cursor. Correctness over
    efficiency: cost is O(size of the source logs) per scan and idempotency is
    guaranteed by the deterministic identity + duplicate suppression of ProposalStore.
"""
import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import vixion_v30_proposal as v30

OBSERVER_VERSION = "3.0-increment2.1-real-source-observer"
TRACES_NAME = "entry_timing_traces_v28.jsonl"
DECISIONS_NAME = "entry_timing_decisions_v28.jsonl"
ROTATED_SUFFIX = ".1"
DEFAULT_OUTPUT_SUBDIR = "v30_observer"
DECISION_IDENTITY_FIELDS = ("cohort", "setup_id", "policy", "decided_seq")
MISSING_ONCE = "MISSING_CONTEXT"
MISSING_WATCH = "DEFERRED_MISSING_CONTEXT"


class SourceError(Exception):
    """Base for fail-closed source problems. The scan that raised it writes nothing."""


class SourceCorruptError(SourceError):
    """Invalid UTF-8, invalid JSON on a complete line, non-object record, or a partial
    line inside a rotated (.1) file."""


class SourceConflictError(SourceError):
    """Two source records with the same identity but different canonical content."""


class SourceMissingContextError(SourceError):
    """A selected decision has no trace `open` ctx in the snapshot (fatal in --once)."""


class SourceIOError(SourceError):
    """An OS-level read failure other than transient absence (permissions, I/O error)."""


class OutputAliasError(SourceError):
    """The output event log aliases (symlink / hardlink / same path) a protected V2.8 artefact."""


PROTECTED_SOURCE_NAMES = (TRACES_NAME, TRACES_NAME + ROTATED_SUFFIX, DECISIONS_NAME, DECISIONS_NAME + ROTATED_SUFFIX)


# ---------------------------------------------------------------------------
# Strict snapshot reader
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceLine:
    file: str          # path as read
    line_no: int       # 1-based line number within that file
    raw_sha256: str    # sha256 over the raw line bytes with trailing CR/LF removed
    record: dict


@dataclass
class FileScan:
    path: str
    exists: bool
    records: list = field(default_factory=list)
    partial_tail_deferred: bool = False
    partial_tail_bytes: int = 0


def _reject_constant(name):
    raise ValueError(f"non-finite JSON constant {name} is not allowed in durable source")


def line_sha256(raw_line):
    """Provenance hash: sha256 over the raw line bytes after stripping exactly one
    trailing "\\n" and, if present before it, one trailing "\\r". Nothing else is altered."""
    if raw_line.endswith(b"\n"):
        raw_line = raw_line[:-1]
    if raw_line.endswith(b"\r"):
        raw_line = raw_line[:-1]
    return hashlib.sha256(raw_line).hexdigest()


def read_durable_file(path, active):
    """Read one V2.8 JSONL file in binary with snapshot semantics.

    * Lines complete at read time (terminated by "\\n") are visible.
    * The final unterminated segment of the ACTIVE file is a concurrent partial write:
      deferred (invisible for this scan), reported via partial_tail_deferred.
    * The same in a ROTATED (.1) file is corruption (a rotated file is never appended).
    * Invalid UTF-8, invalid JSON, non-finite constants, empty lines or non-object
      records on a complete line -> SourceCorruptError.
    No lock is taken; the producer is never blocked or written.
    """
    p = Path(path)
    scan = FileScan(path=str(p), exists=False)
    # Open directly: exists()+open is not atomic under V2.8 rotation. A file that is not
    # there at this instant is a transient snapshot state (absent), not an error; the next
    # scan sees whatever V2.8 has by then. Any other OS failure is fail-closed.
    try:
        with open(p, "rb") as fh:  # READ ONLY
            data = fh.read()
    except FileNotFoundError:
        return scan
    except OSError as exc:
        raise SourceIOError(f"{p}: cannot read source ({type(exc).__name__}: {exc})") from exc
    scan.exists = True
    parts = data.split(b"\n")
    tail = parts.pop()  # bytes after the last "\n" (b"" when the file is newline-terminated)
    if tail:
        if not active:
            raise SourceCorruptError(f"{p}: rotated file has an unterminated final line")
        scan.partial_tail_deferred = True
        scan.partial_tail_bytes = len(tail)
    for n, raw in enumerate(parts, 1):
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        if not raw.strip():
            raise SourceCorruptError(f"{p}:{n}: empty durable line")
        try:
            text = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise SourceCorruptError(f"{p}:{n}: invalid UTF-8 ({exc})") from exc
        try:
            rec = json.loads(text, parse_constant=_reject_constant)
        except ValueError as exc:
            raise SourceCorruptError(f"{p}:{n}: invalid JSON ({exc})") from exc
        if not isinstance(rec, dict):
            raise SourceCorruptError(f"{p}:{n}: record is not an object")
        scan.records.append(SourceLine(str(p), n, line_sha256(raw), rec))
    return scan


def read_rotated_then_active(base_path):
    """Chronological read: <file>.jsonl.1 (older) first, then <file>.jsonl (active)."""
    base = Path(base_path)
    rotated = read_durable_file(base.with_name(base.name + ROTATED_SUFFIX), active=False)
    activef = read_durable_file(base, active=True)
    return rotated, activef


@dataclass
class SourceSnapshot:
    traces_rotated: FileScan
    traces_active: FileScan
    decisions_rotated: FileScan
    decisions_active: FileScan

    @property
    def trace_lines(self):
        return self.traces_rotated.records + self.traces_active.records

    @property
    def decision_lines(self):
        return self.decisions_rotated.records + self.decisions_active.records

    def counts(self):
        return {"traces_rotated_records": len(self.traces_rotated.records),
                "traces_active_records": len(self.traces_active.records),
                "decisions_rotated_records": len(self.decisions_rotated.records),
                "decisions_active_records": len(self.decisions_active.records),
                "partial_tail_deferred": int(self.traces_active.partial_tail_deferred) + int(self.decisions_active.partial_tail_deferred),
                "files": {k: {"path": s.path, "exists": s.exists, "partial_tail_bytes": s.partial_tail_bytes}
                          for k, s in (("traces_rotated", self.traces_rotated), ("traces_active", self.traces_active),
                                       ("decisions_rotated", self.decisions_rotated), ("decisions_active", self.decisions_active))}}


def read_source_snapshot(source_dir):
    """Read and fully validate the whole V2.8 snapshot BEFORE anything else happens.
    Any corruption raises; nothing downstream runs."""
    src = Path(source_dir)
    tr, ta = read_rotated_then_active(src / TRACES_NAME)
    dr, da = read_rotated_then_active(src / DECISIONS_NAME)
    return SourceSnapshot(tr, ta, dr, da)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------
def build_trace_index(trace_lines):
    """setup_id -> {"ctx", "sha256", "file", "line_no"} using the gateway's strict open
    rules (identical duplicate = no-op, differing duplicate / malformed open = fail closed)."""
    # The core validates opens (malformed / conflicting) exactly as the gateway does.
    try:
        ctxs = v30.contexts_from_trace_events([sl.record for sl in trace_lines])
    except v30.ContextConflictError as exc:
        raise SourceConflictError(f"trace opens: {exc}") from exc
    except v30.ContextInputError as exc:
        raise SourceCorruptError(f"trace opens: {exc}") from exc
    index = {}
    for sl in trace_lines:
        rec = sl.record
        if rec.get("event") != "open":
            continue
        sid = rec["setup_id"]
        if sid in index:
            continue  # canonical-identical duplicate (the core already proved identity)
        index[sid] = {"ctx": ctxs[sid], "sha256": sl.raw_sha256, "file": sl.file, "line_no": sl.line_no}
    return index


def validate_output_log(output_dir, source_dir):
    """Fail closed before any write if the output event log could alias a V2.8 artefact.

    Rejects: output dir == source dir; output dir or the log path being a symlink; the
    log's real path equal to any protected artefact; the log being the same file
    (hardlink / bind) as any existing protected artefact; the log landing inside a
    directory that points at the source directory through a link.
    """
    output_dir, source_dir = Path(output_dir), Path(source_dir)
    log = output_dir / v30.EVENT_LOG_NAME
    src_res = source_dir.resolve()
    if output_dir.resolve() == src_res:
        raise OutputAliasError("output directory must be separate from the V2.8 source directory")
    for part in (output_dir, log):
        if part.is_symlink():
            raise OutputAliasError(f"{part} is a symlink; refusing to write through links")
    log_res = log.resolve()
    if log_res.parent == src_res:
        raise OutputAliasError(f"{log} resolves inside the V2.8 source directory")
    for name in PROTECTED_SOURCE_NAMES:
        protected = source_dir / name
        if log_res == protected.resolve():
            raise OutputAliasError(f"{log} resolves to protected V2.8 artefact {protected}")
        try:
            if log.exists() and protected.exists() and os.path.samefile(log, protected):
                raise OutputAliasError(f"{log} is the same file as protected V2.8 artefact {protected}")
        except OSError as exc:
            raise SourceIOError(f"cannot inspect {log}: {exc}") from exc
    if log.name in PROTECTED_SOURCE_NAMES:
        raise OutputAliasError("output log name collides with a V2.8 artefact")
    return log


def validate_source_decision(rec, fields, policy_id, where):
    """Structural validation of a source Decision record BEFORE it is used as a dict key or
    classified. Raises SourceCorruptError. The core re-validates everything else later."""
    if not isinstance(rec, dict):
        raise SourceCorruptError(f"{where}: decision record is not an object")
    unexpected = sorted(set(rec) - set(fields))
    missing = sorted(set(fields) - set(rec))
    if unexpected or missing:
        raise SourceCorruptError(f"{where}: decision field set differs from canonical V2.8 schema "
                                 f"(unexpected={unexpected[:5]} missing={missing[:5]})")
    if rec.get("schema_version") != v30.V28_REQUIRED_SCHEMA_VERSION:
        raise SourceCorruptError(f"{where}: decision schema_version != {v30.V28_REQUIRED_SCHEMA_VERSION}")
    if not isinstance(rec.get("cohort"), str) or not rec["cohort"]:
        raise SourceCorruptError(f"{where}: decision cohort is not a string")
    if not isinstance(rec.get("setup_id"), str) or not rec["setup_id"]:
        raise SourceCorruptError(f"{where}: decision setup_id is not a non-empty string")
    if not isinstance(rec.get("policy"), str) or rec["policy"] != policy_id:
        raise SourceCorruptError(f"{where}: decision policy is not the selected policy")
    seq = rec.get("decided_seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise SourceCorruptError(f"{where}: decision decided_seq is not an int")
    if seq < -1:
        raise SourceCorruptError(f"{where}: decision decided_seq {seq} < -1")
    if seq == -1:
        # V2.8 no-tick close: TraceRuntime.last_seq starts at -1 and TraceRuntime.close()
        # finalises WAITING policies with a pseudo tick carrying that seq. This is a
        # legitimate durable record ONLY as EXPIRED / fill=False with no entry tick. It
        # never represents a fill and never becomes a proposal source (the core reports
        # it as not_eligible: decision_not_filled).
        if rec.get("state") != "EXPIRED" or rec.get("fill") is not False:
            raise SourceCorruptError(f"{where}: decided_seq -1 is only valid for EXPIRED/fill=False (no-tick close)")
        for k in ("entry_seq", "entry_tick_at", "entry_quote_fetched_at"):
            if rec.get(k) is not None:
                raise SourceCorruptError(f"{where}: decided_seq -1 (no-tick close) but {k} is not null")
    if (rec.get("fill") is True or rec.get("state") == v30.V28_FILLED) and seq < 0:
        raise SourceCorruptError(f"{where}: FILLED decision with decided_seq {seq}")
    if not v30.json_finite(rec):
        raise SourceCorruptError(f"{where}: decision contains non-finite values")


def decision_identity(rec):
    return tuple(rec.get(k) for k in DECISION_IDENTITY_FIELDS)


def select_decisions(decision_lines, policy_id):
    """Keep decisions for the explicitly selected policy; detect duplicate / conflicting
    source records by (cohort, setup_id, policy, decided_seq). Returns (selected, stats)."""
    seen = {}
    terminal = {}  # (cohort, setup_id, policy) -> decided_seq: V2.8 emits exactly ONE terminal decision per PolicyState
    ordered = []
    fields = v30.load_canonical_decision_fields()
    stats = {"decisions_seen": 0, "selected_policy_decisions": 0, "selected_filled_decisions": 0,
             "source_duplicate_records": 0, "source_conflicts": 0}
    for sl in decision_lines:
        stats["decisions_seen"] += 1
        rec = sl.record
        if not isinstance(rec, dict) or rec.get("policy") != policy_id:
            continue
        stats["selected_policy_decisions"] += 1
        where = f"{sl.file}:{sl.line_no}"
        validate_source_decision(rec, fields, policy_id, where)  # before any hashing / lookup
        ident = decision_identity(rec)
        try:
            cj = v30.canonical_json(rec)
        except (TypeError, ValueError) as exc:
            raise SourceCorruptError(f"{where}: decision is not finite JSON ({exc})") from exc
        tkey = ident[:3]
        if tkey in terminal and terminal[tkey] != ident[3]:
            stats["source_conflicts"] += 1
            raise SourceConflictError(f"multiple_terminal_decisions: {tkey} has decided_seq {terminal[tkey]} and {ident[3]} "
                                      f"({where}); V2.8 emits exactly one terminal decision per setup/policy")
        terminal[tkey] = ident[3]
        if ident in seen:
            if seen[ident]["canonical"] != cj:
                stats["source_conflicts"] += 1
                raise SourceConflictError(f"conflicting decision records for identity {ident} "
                                          f"({seen[ident]['line'].file}:{seen[ident]['line'].line_no} vs {sl.file}:{sl.line_no})")
            stats["source_duplicate_records"] += 1
            continue
        seen[ident] = {"canonical": cj, "line": sl}
        ordered.append(sl)
        if rec.get("state") == v30.V28_FILLED and rec.get("fill") is True:
            stats["selected_filled_decisions"] += 1
    return ordered, stats


# ---------------------------------------------------------------------------
# Source provenance verification
# ---------------------------------------------------------------------------
def frozen_ctx_subset(ctx, frozen_trace_ctx):
    """Project a source ctx onto exactly the keys the core froze (the core's own subset)."""
    return {k: ctx.get(k) for k in frozen_trace_ctx}


def verify_proposal_against_source(proposal, decision_index, trace_index, policy_specs):
    """Prove a persisted proposal is exactly what the durable source implies.

    Returns a dict with status in {"source_verified", "source_missing", "source_mismatch"}
    plus decision_source_sha256 / trace_open_source_sha256 (raw durable line hashes).
    Nothing is written; the proposal schema is untouched.
    """
    fc = proposal.get("frozen_context") or {}
    fdec = fc.get("decision") or {}
    ident = decision_identity(fdec)
    src = decision_index.get(ident)
    out = {"proposal_id": proposal.get("proposal_id"), "identity": list(ident), "status": None, "detail": "",
           "decision_source_sha256": None, "trace_open_source_sha256": None}
    if src is None:
        out["status"] = "source_missing"
        out["detail"] = "no durable decision with this identity"
        return out
    out["decision_source_sha256"] = src.raw_sha256
    tr = trace_index.get(proposal.get("setup_id"))
    if tr is None:
        out["status"] = "source_missing"
        out["detail"] = "no durable trace open for setup_id"
        return out
    out["trace_open_source_sha256"] = tr["sha256"]
    problems = []
    if v30.canonical_json(fdec) != v30.canonical_json(src.record):
        problems.append("frozen decision != source decision")
    if v30.canonical_json(fc.get("trace_ctx")) != v30.canonical_json(frozen_ctx_subset(tr["ctx"], fc.get("trace_ctx") or {})):
        problems.append("frozen trace_ctx != source ctx subset")
    if proposal.get("policy_id") != src.record.get("policy"):
        problems.append("policy_id != source policy")
    if proposal.get("setup_id") != src.record.get("setup_id"):
        problems.append("setup_id != source setup_id")
    if proposal.get("source_decision_seq") != src.record.get("decided_seq"):
        problems.append("source_decision_seq != source decided_seq")
    # Strongest check: rebuild from the SOURCE (not from frozen_context) with the frozen config.
    cfgd = fc.get("config") or {}
    try:
        config = v30.GatewayConfig(policy_id=cfgd.get("policy_id"), quantity=cfgd.get("quantity"),
                                   max_quantity=cfgd.get("max_quantity"), ttl_s=cfgd.get("ttl_s"),
                                   allowed_cohorts=tuple(cfgd.get("allowed_cohorts") or ()))
        rebuilt = v30.build_proposal(src.record, tr["ctx"], config, policy_specs)
        if not rebuilt.ok:
            problems.append(f"source does not rebuild: {rebuilt.reason}")
        elif rebuilt.proposal["content_hash"] != proposal.get("content_hash"):
            problems.append("rebuilt-from-source content_hash differs")
    except (TypeError, ValueError) as exc:
        problems.append(f"frozen config invalid: {exc}")
    out["status"] = "source_mismatch" if problems else "source_verified"
    out["detail"] = "; ".join(problems)
    return out


# ---------------------------------------------------------------------------
# Reconciliation run (one scan)
# ---------------------------------------------------------------------------
def empty_report(policy_id):
    return {"observer_version": OBSERVER_VERSION, "gateway_version": v30.GATEWAY_VERSION, "policy_id": policy_id,
            "source": {}, "decisions_seen": 0, "selected_policy_decisions": 0, "selected_filled_decisions": 0,
            "proposals_created": 0, "duplicate_suppressed": 0, "not_eligible": 0, "invalid_rejected": 0,
            "conflict_rejected": 0, "persist_failed": 0, "expired_now": 0, "deferred_missing_context": 0,
            "source_verified": 0, "source_missing": 0, "source_mismatch": 0, "source_conflicts": 0,
            "by_asset": {}, "by_side": {}, "rejection_reasons": {}, "last_source_decided_at": None,
            "last_scan_at": None, "proposals": {}, "provenance": {}, "missing_context_setups": []}


def reconcile_once(source_dir, output_dir, config, policy_specs, as_of_utc, mode="once", recorded_at_utc=None, scan_at_utc=None):
    """One full scan. Returns (report, store). Raises SourceError before ANY write when the
    snapshot is corrupt/conflicting, the output aliases a source artefact, or (in --once)
    a selected decision lacks its ctx.
    ``as_of_utc`` is used ONLY for expiry evaluation (never for build_proposal).
    ``scan_at_utc`` is the real orchestration instant (report.last_scan_at, event
    recorded_at_utc); it defaults to the wall clock and is injectable for tests."""
    source_dir, output_dir = Path(source_dir), Path(output_dir)
    scan_at_utc = scan_at_utc or utc_now()
    if recorded_at_utc is None:
        recorded_at_utc = v30.iso_utc(scan_at_utc)
    log_path = validate_output_log(output_dir, source_dir)  # fail closed before any write
    report = empty_report(config.policy_id)

    # 1) full snapshot, fully validated before anything else (fail closed on corruption)
    snap = read_source_snapshot(source_dir)
    report["source"] = snap.counts()
    trace_index = build_trace_index(snap.trace_lines)
    selected, dstats = select_decisions(snap.decision_lines, config.policy_id)
    report.update({k: dstats[k] for k in ("decisions_seen", "selected_policy_decisions", "selected_filled_decisions", "source_conflicts")})
    report["source"]["decision_duplicate_records"] = dstats["source_duplicate_records"]
    decision_index = {decision_identity(sl.record): sl for sl in selected}

    # 2) context availability
    ready, missing = [], []
    for sl in selected:
        sid = sl.record.get("setup_id")
        if sid in trace_index:
            ready.append(sl)
        else:
            missing.append(sid)
    report["missing_context_setups"] = sorted(set(str(x) for x in missing))
    if missing and mode == "once":
        raise SourceMissingContextError(f"{len(missing)} selected decision(s) without a trace open ctx: "
                                        f"{report['missing_context_setups'][:5]}")
    report["deferred_missing_context"] = len(missing)

    # 3) store opens only after a fully valid snapshot (re-check the alias guard: the
    #    filesystem may have changed while the snapshot was read)
    validate_output_log(output_dir, source_dir)
    try:
        store = v30.ProposalStore(log_path, policy_specs)
    except OSError as exc:
        raise SourceIOError(f"cannot open output log {log_path}: {exc}") from exc
    contexts = {sid: e["ctx"] for sid, e in trace_index.items()}
    decisions = [sl.record for sl in ready]
    prep, results = v30.process_decisions(decisions, contexts, config, policy_specs, store, recorded_at_utc=recorded_at_utc)
    for k in ("proposals_created", "duplicate_suppressed", "invalid_rejected", "conflict_rejected", "persist_failed",
              "by_asset", "by_side", "rejection_reasons"):
        report[k] = prep[k]
    report["not_eligible"] = sum(1 for _, r in results if not r.ok and r.category == "not_eligible")

    # 4) expiry (wall clock enters here only)
    report["expired_now"] = len(store.expire_due(as_of_utc, recorded_at_utc=recorded_at_utc))

    # 5) provenance of every stored proposal for this policy against the source snapshot
    store.rebuild()
    for pid, st in sorted(store.index.items()):
        p = st["proposal"]
        if p.get("policy_id") != config.policy_id:
            continue
        v = verify_proposal_against_source(p, decision_index, trace_index, policy_specs)
        report[v["status"]] += 1
        report["provenance"][pid] = v
        report["proposals"][pid] = {"status": st["status"], "asset": p["asset"], "side": p["side"], "contract": p["contract_ticker"],
                                    "observed_ask_cents": p["observed_ask_cents"], "limit_price_cents": p["limit_price_cents"],
                                    "quantity": p["quantity"], "expires_at_utc": p["expires_at_utc"]}
    decided = [sl.record.get("decided_at") for sl in selected if isinstance(sl.record.get("decided_at"), str)]
    report["last_source_decided_at"] = max(decided) if decided else None
    report["as_of_utc"] = v30.iso_utc(as_of_utc)      # expiry evaluation instant only
    report["last_scan_at"] = v30.iso_utc(scan_at_utc)  # real orchestration instant
    return report, store


def human_summary(report):
    lines = [f"VIXION 3.0 observer · {report['observer_version']} · gateway {report['gateway_version']} · policy {report['policy_id']}"]
    s = report.get("source", {})
    lines.append(f"source: traces {s.get('traces_rotated_records', 0)}+{s.get('traces_active_records', 0)} · decisions "
                 f"{s.get('decisions_rotated_records', 0)}+{s.get('decisions_active_records', 0)} · partial tail deferred {s.get('partial_tail_deferred', 0)}")
    for k in ("decisions_seen", "selected_policy_decisions", "selected_filled_decisions", "proposals_created", "duplicate_suppressed",
              "not_eligible", "invalid_rejected", "conflict_rejected", "persist_failed", "expired_now", "deferred_missing_context",
              "source_verified", "source_missing", "source_mismatch", "source_conflicts"):
        lines.append(f"  {k:26s} {report.get(k, 0)}")
    for k in ("by_asset", "by_side", "rejection_reasons"):
        if report.get(k):
            lines.append(f"  {k}: {json.dumps(report[k], sort_keys=True)}")
    lines.append(f"  last_source_decided_at     {report.get('last_source_decided_at')}")
    lines.append(f"  as_of_utc (expiry)         {report.get('as_of_utc')}")
    lines.append(f"  last_scan_at               {report.get('last_scan_at')}")
    for pid, p in report.get("proposals", {}).items():
        prov = report.get("provenance", {}).get(pid, {})
        lines.append(f"  {pid} {p['status']:19s} {p['asset']:5s} {p['side']:4s} {p['contract']} ask {p['observed_ask_cents']} "
                     f"limit {p['limit_price_cents']}c x{p['quantity']} · {prov.get('status')}")
    lines.append("NO ORDER SURFACE · local V2.8 observer only · nothing sent anywhere")
    return "\n".join(lines)


def utc_now():
    return datetime.now(timezone.utc)
