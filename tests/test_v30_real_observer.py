"""VIXION 3.0 Increment 2.1 — real V2.8 source observer tests (O01..O40 + extra invariants).

Fixtures are SYNTHETIC but produced by the real V2.8 engine (PolicyState.on_tick) and
written in the exact durable format of vixion_v27.DecisionTrace (json + "\\n"). No
production data is used or claimed.
"""
import hashlib
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from datetime import timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="v30-obs-data-"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))

import vixion_v28 as v28  # noqa: E402
import vixion_v30_proposal as v30  # noqa: E402
import vixion_v30_observer as obs  # noqa: E402
import v30_observe_v28 as cli  # noqa: E402
from test_v30_proposal_gateway import make_ctx, run_policy, filled_decision, iso, WS_MS, DET_MS, ELIG_END, cfg  # noqa: E402

SPECS = v30.load_canonical_policy_specs()
POLICY = "ask_le_90c"
V30_OBS_FILES = ("vixion_v30_observer.py", "scripts/v30_observe_v28.py")
LATER = v30.parse_iso_utc(iso(ELIG_END + 3_600_000))  # well after everything expired
EARLY = v30.parse_iso_utc(iso(DET_MS + 3000))        # before any TTL elapses


def open_rec(ctx):
    return {"schema_version": 2, "event": "open", "setup_id": ctx["setup_id"], "opened_at": iso(DET_MS), "ctx": ctx}


def close_rec(ctx):
    return {"schema_version": 2, "event": "close", "setup_id": ctx["setup_id"], "end_reason": "eligibility_ended",
            "closed_at": iso(ELIG_END), "seq_last": 7}


def setup(asset="ETH", side="UP", ticker="KXETH15M-26SEP011345-T4500", asks=(92.0, 91.0, 89.0), policy=POLICY):
    sid = f"{asset}|{iso(WS_MS)}"
    ctx = make_ctx(setup_id=sid, asset=asset, side=side, market_ticker=ticker)
    return ctx, run_policy(policy, asks, ctx)


def jsonl_bytes(records):
    return b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in records)


class SourceDir:
    """Temp dir with V2.8-shaped durable files written exactly like vixion_v27.DecisionTrace."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="v30-src-")
        self.path = Path(self.tmp.name)
        self.out = self.path / obs.DEFAULT_OUTPUT_SUBDIR

    def write(self, name, records=(), raw=b"", rotated=False):
        p = self.path / (name + (obs.ROTATED_SUFFIX if rotated else ""))
        p.write_bytes(jsonl_bytes(records) + raw)
        return p

    def traces(self, records=(), raw=b"", rotated=False):
        return self.write(obs.TRACES_NAME, records, raw, rotated)

    def decisions(self, records=(), raw=b"", rotated=False):
        return self.write(obs.DECISIONS_NAME, records, raw, rotated)

    def append(self, name, records=(), raw=b""):
        with open(self.path / name, "ab") as fh:
            fh.write(jsonl_bytes(records) + raw)

    def hashes(self):
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(self.path.iterdir()) if p.is_file()}

    def cleanup(self):
        self.tmp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.cleanup()


def run(src, as_of=LATER, mode="once", policy=POLICY, output=None, **cfgover):
    return obs.reconcile_once(src.path, output or src.out, cfg(policy, **cfgover), SPECS, as_of, mode=mode)


def run_cli(argv, environ=None, clock=None, sleep=None):
    out, err = io.StringIO(), io.StringIO()
    so, se = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = out, err
    try:
        rc = cli.main(argv, environ={} if environ is None else environ, clock=clock, sleep=sleep)
    finally:
        sys.stdout, sys.stderr = so, se
    return rc, out.getvalue(), err.getvalue()


def events_in(src):
    p = src.out / v30.EVENT_LOG_NAME
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.exists() else []


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------
class TestSourceReader(unittest.TestCase):
    def test_O01_rotated_read_before_active(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)], rotated=True)
            src.traces([close_rec(ctx)])
            src.decisions([dict(d, decided_seq=0, entry_seq=0)], rotated=True)  # older record
            src.decisions([d])
            snap = obs.read_source_snapshot(src.path)
            self.assertEqual([sl.record["event"] for sl in snap.trace_lines], ["open", "close"])
            self.assertEqual([sl.record["decided_seq"] for sl in snap.decision_lines], [0, 2])
            self.assertTrue(snap.trace_lines[0].file.endswith(".jsonl.1"))
            self.assertEqual(snap.counts()["traces_rotated_records"], 1)
            self.assertEqual(snap.counts()["decisions_active_records"], 1)

    def test_O02_active_partial_final_line_deferred(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d], raw=b'{"schema_version": 2, "cohort": "forward_v28", "policy": "ask_le_9')
            snap = obs.read_source_snapshot(src.path)
            self.assertEqual(len(snap.decision_lines), 1)
            self.assertTrue(snap.decisions_active.partial_tail_deferred)
            self.assertEqual(snap.counts()["partial_tail_deferred"], 1)
            rep, _ = run(src)
            self.assertEqual(rep["proposals_created"], 1)
            self.assertEqual(rep["source"]["partial_tail_deferred"], 1)

    def test_O03_partial_rotated_line_fails(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d], raw=b'{"partial": tr', rotated=True)
            src.decisions([])
            with self.assertRaises(obs.SourceCorruptError):
                obs.read_source_snapshot(src.path)
            with self.assertRaises(obs.SourceCorruptError):
                run(src)
            self.assertFalse(src.out.exists())

    def test_O04_complete_malformed_json_fails(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            for raw in (b'{"broken": \n', b'[1,2,3]\n', b'"string"\n', b'\n', b'{"x": NaN}\n', b'{"x": Infinity}\n'):
                src.decisions([d], raw=raw)
                with self.assertRaises(obs.SourceCorruptError, msg=raw):
                    obs.read_source_snapshot(src.path)
            self.assertFalse(src.out.exists())

    def test_O05_invalid_utf8_fails(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d], raw=b'{"setup_id": "\xff\xfe"}\n')
            with self.assertRaises(obs.SourceCorruptError) as cm:
                obs.read_source_snapshot(src.path)
            self.assertIn("UTF-8", str(cm.exception))
            src.traces([], raw=b'\xc3\x28\n')  # invalid 2-byte sequence in traces
            with self.assertRaises(obs.SourceCorruptError):
                obs.read_source_snapshot(src.path)

    def test_crlf_tolerated_and_hash_excludes_only_trailing_crlf(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            body = json.dumps(d).encode("utf-8")
            src.decisions([], raw=body + b"\r\n")
            snap = obs.read_source_snapshot(src.path)
            self.assertEqual(snap.decision_lines[0].raw_sha256, hashlib.sha256(body).hexdigest())
            self.assertEqual(obs.line_sha256(body + b"\n"), obs.line_sha256(body + b"\r\n"))
            self.assertEqual(obs.line_sha256(body), hashlib.sha256(body).hexdigest())
            self.assertNotEqual(obs.line_sha256(body + b" \n"), obs.line_sha256(body))  # inner bytes untouched

    def test_missing_files_are_empty_not_errors(self):
        with SourceDir() as src:
            snap = obs.read_source_snapshot(src.path)
            self.assertEqual((snap.trace_lines, snap.decision_lines), ([], []))
            rep, _ = run(src)
            self.assertEqual(rep["proposals_created"], 0)


# ---------------------------------------------------------------------------
# Read-only + policy
# ---------------------------------------------------------------------------
class TestReadOnlyAndPolicy(unittest.TestCase):
    def test_O06_source_files_remain_byte_identical(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)], rotated=True)
            src.traces([close_rec(ctx)])
            src.decisions([d], raw=b"partial-tail")
            src.decisions([], rotated=True)
            before = src.hashes()
            for _ in range(3):
                run(src)
            run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            after = src.hashes()
            self.assertEqual({k: v for k, v in after.items() if k in before}, before)
            self.assertEqual(sorted(p.name for p in src.path.iterdir()), sorted(set(before) | {obs.DEFAULT_OUTPUT_SUBDIR}))
            self.assertEqual([p.name for p in src.out.iterdir()], [v30.EVENT_LOG_NAME])
        src_text = (ROOT / "vixion_v30_observer.py").read_text(encoding="utf-8")
        for m in re.findall(r'open\([^)]*\)', src_text):
            self.assertIn('"rb"', m, f"observer opens files only for binary reading: {m}")
        for w in ("os.replace", "os.rename", "unlink", "truncate", "rmtree", ".write_text", ".write_bytes", "shutil"):
            self.assertNotIn(w, src_text, w)

    def test_O07_explicit_policy_required(self):
        with SourceDir() as src:
            with self.assertRaises(SystemExit) as cm:  # argparse: --policy is required (no default)
                run_cli(["--source-data-dir", str(src.path), "--once"])
            self.assertEqual(cm.exception.code, 2)
            self.assertFalse(src.out.exists())

    def test_O08_unknown_policy_fails(self):
        with SourceDir() as src:
            for bad in ("best", "ask_le_89c", ""):
                rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", bad, "--once"])
                self.assertEqual(rc, 2, bad)
                self.assertIn("unknown policy_id", err)
            self.assertFalse(src.out.exists())


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------
class TestReconciliation(unittest.TestCase):
    def test_O09_selected_filled_creates_proposal(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx), close_rec(ctx)])
            src.decisions([d])
            rep, store = run(src, as_of=EARLY)
            self.assertEqual((rep["selected_policy_decisions"], rep["selected_filled_decisions"], rep["proposals_created"]), (1, 1, 1))
            pid = next(iter(store.index))
            self.assertEqual(store.index[pid]["proposal"], v30.build_proposal(d, ctx, cfg(), SPECS).proposal)
            self.assertEqual(rep["by_asset"], {"ETH": 1})
            self.assertEqual(rep["by_side"], {"UP": 1})
            self.assertEqual(rep["source_verified"], 1)

    def test_O10_nonselected_policy_ignored(self):
        ctx, d = setup()
        other = run_policy("delay_5s", (92.0,) * 7, ctx)
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([other, d, run_policy("entry_at_detection", (92.0,), ctx)])
            rep, _ = run(src)
            self.assertEqual((rep["decisions_seen"], rep["selected_policy_decisions"], rep["proposals_created"]), (3, 1, 1))
            rep2, _ = run(src, policy="delay_5s", output=src.path / "o2")
            self.assertEqual((rep2["selected_policy_decisions"], rep2["proposals_created"]), (1, 1))

    def test_O11_selected_non_filled_creates_no_proposal(self):
        ctx, _ = setup()
        spec = next(s for s in v28.POLICY_SPECS if s["policy"] == POLICY)
        ps = v28.PolicyState(spec)
        c = dict(ctx, detection_ask=92.0)
        expired = ps.on_tick(v28.build_tick(c, 3, ELIG_END + 1000, ELIG_END + 1000, 80.0, bid=79.0, ask_size=5.0), c).to_dict()
        self.assertEqual(expired["state"], "EXPIRED")
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([expired])
            rep, _ = run(src)
            self.assertEqual((rep["selected_policy_decisions"], rep["selected_filled_decisions"], rep["proposals_created"], rep["not_eligible"]), (1, 0, 0, 1))
            self.assertEqual(events_in(src), [])

    def test_O12_missing_trace_open_fails_closed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([close_rec(ctx)])  # no open
            src.decisions([d])
            with self.assertRaises(obs.SourceMissingContextError):
                run(src, mode="once")
            self.assertFalse(src.out.exists())
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            self.assertEqual(rc, 3)
            self.assertIn("missing context", err)
            # watch: deferred, nothing written, other setups still processed
            rep, _ = run(src, mode="watch")
            self.assertEqual((rep["deferred_missing_context"], rep["proposals_created"]), (1, 0))
            self.assertEqual(rep["missing_context_setups"], [ctx["setup_id"]])
            self.assertEqual(events_in(src), [])

    def test_O13_conflicting_trace_opens_fail(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx), open_rec(dict(ctx, cons_pct=95.5))])
            src.decisions([d])
            with self.assertRaises(obs.SourceConflictError):
                run(src)
            src.traces([open_rec(ctx), {"schema_version": 2, "event": "open", "setup_id": ctx["setup_id"], "ctx": None}])
            with self.assertRaises(obs.SourceCorruptError):
                run(src)
            self.assertFalse(src.out.exists())

    def test_O14_identical_duplicate_trace_open_allowed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)], rotated=True)
            src.traces([open_rec(json.loads(json.dumps(ctx))), close_rec(ctx)])
            src.decisions([d])
            rep, _ = run(src)
            self.assertEqual(rep["proposals_created"], 1)
            self.assertEqual(rep["source_verified"], 1)

    def test_O15_identical_duplicate_decision_allowed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d], rotated=True)
            src.decisions([json.loads(json.dumps(d)), d])
            rep, _ = run(src)
            self.assertEqual((rep["decisions_seen"], rep["proposals_created"], rep["duplicate_suppressed"]), (3, 1, 0))
            self.assertEqual(rep["source"]["decision_duplicate_records"], 2)
            self.assertEqual(len(events_in(src)), 2)  # PROPOSED + EXPIRED (as_of LATER)

    def test_O16_conflicting_duplicate_decision_fails(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d, dict(d, entry_ask_cents=88.0)])
            with self.assertRaises(obs.SourceConflictError):
                run(src)
            self.assertFalse(src.out.exists())
            # conflict across rotation boundary too
            src.decisions([d], rotated=True)
            src.decisions([dict(d, trigger_rule="tampered")])
            with self.assertRaises(obs.SourceConflictError):
                run(src)
            # a second terminal decision for the same setup/policy (another seq) is ALSO a conflict
            src.decisions([d], rotated=True)
            src.decisions([dict(d, decided_seq=3, entry_seq=3)])
            with self.assertRaises(obs.SourceConflictError) as cm:
                run(src)
            self.assertIn("multiple_terminal_decisions", str(cm.exception))


class TestIdempotencyAcrossScans(unittest.TestCase):
    def _src(self):
        ctx, d = setup()
        ctx2, d2 = setup("SOL", "DOWN", "KXSOL15M-26SEP011345-T200", (93.0, 90.0))
        src = SourceDir()
        src.traces([open_rec(ctx), open_rec(ctx2)])
        src.decisions([d, d2])
        return src, (ctx, d), (ctx2, d2)

    def test_O17_restart_full_rescan_zero_duplicates(self):
        src, _, _ = self._src()
        with src:
            rep1, _ = run(src)
            self.assertEqual(rep1["proposals_created"], 2)
            n = len(events_in(src))
            for _ in range(3):
                rep, store = run(src)  # fresh ProposalStore each time == restart
                self.assertEqual((rep["proposals_created"], rep["duplicate_suppressed"]), (0, 2))
                self.assertEqual(len(events_in(src)), n)
            self.assertEqual(len(store.index), 2)

    def test_O18_repeated_watch_scan_zero_duplicates(self):
        src, _, _ = self._src()
        with src:
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "4", "--json"],
                                   clock=lambda: LATER, sleep=lambda s: None)
            self.assertEqual(rc, 0, err)
            self.assertEqual(len(events_in(src)), 4)  # 2 PROPOSED + 2 EXPIRED, once
            self.assertEqual(out.count('"proposals_created": 2'), 1)
            self.assertEqual(out.count('"duplicate_suppressed": 2'), 3)

    def test_O19_new_decision_between_scans_creates_one_proposal(self):
        src, (ctx, d), (ctx2, d2) = self._src()
        with src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep1, _ = run(src)
            self.assertEqual(rep1["proposals_created"], 1)
            src.append(obs.TRACES_NAME, [open_rec(ctx2)])
            src.append(obs.DECISIONS_NAME, [d2])
            rep2, store = run(src)
            self.assertEqual((rep2["proposals_created"], rep2["duplicate_suppressed"]), (1, 1))
            self.assertEqual(len(store.index), 2)
            rep3, _ = run(src)
            self.assertEqual(rep3["proposals_created"], 0)

    def test_O20_partial_decision_completed_between_scans_processed_once(self):
        src, (ctx, d), (ctx2, d2) = self._src()
        with src:
            line2 = json.dumps(d2).encode("utf-8")
            src.traces([open_rec(ctx), open_rec(ctx2)])
            src.decisions([d], raw=line2[:40])  # concurrent partial write
            rep1, _ = run(src)
            self.assertEqual((rep1["proposals_created"], rep1["source"]["partial_tail_deferred"]), (1, 1))
            src.append(obs.DECISIONS_NAME, raw=line2[40:] + b"\n")  # producer finishes the line
            rep2, _ = run(src)
            self.assertEqual((rep2["proposals_created"], rep2["source"]["partial_tail_deferred"]), (1, 0))
            rep3, store = run(src)
            self.assertEqual((rep3["proposals_created"], rep3["duplicate_suppressed"]), (0, 2))
            self.assertEqual(len(store.index), 2)

    def test_O21_rotation_between_scans_no_duplicate(self):
        src, (ctx, d), (ctx2, d2) = self._src()
        with src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            run(src)
            # V2.8 rotation: active -> .1, new active gets the next records
            os.replace(src.path / obs.TRACES_NAME, src.path / (obs.TRACES_NAME + ".1"))
            os.replace(src.path / obs.DECISIONS_NAME, src.path / (obs.DECISIONS_NAME + ".1"))
            src.traces([open_rec(ctx2)])
            src.decisions([d2])
            rep, store = run(src)
            self.assertEqual((rep["proposals_created"], rep["duplicate_suppressed"]), (1, 1))
            self.assertEqual(rep["source"]["decisions_rotated_records"], 1)
            # second rotation drops the oldest data (V2.8 unlinks the old .1); the stored
            # proposal for the dropped decision is then reported source_missing, never duplicated
            os.replace(src.path / obs.DECISIONS_NAME, src.path / (obs.DECISIONS_NAME + ".1"))
            src.decisions([])
            rep2, store2 = run(src)
            self.assertEqual((rep2["proposals_created"], len(store2.index), rep2["source_missing"]), (0, 2, 1))


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------
class TestProvenance(unittest.TestCase):
    def test_O22_source_verification_passes_for_valid_proposal(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = run(src, as_of=EARLY)
            pid = next(iter(store.index))
            prov = rep["provenance"][pid]
            self.assertEqual(prov["status"], "source_verified")
            snap = obs.read_source_snapshot(src.path)
            self.assertEqual(prov["decision_source_sha256"], snap.decision_lines[0].raw_sha256)
            self.assertEqual(prov["trace_open_source_sha256"], snap.trace_lines[0].raw_sha256)
            self.assertEqual(prov["decision_source_sha256"], hashlib.sha256(json.dumps(d).encode()).hexdigest())
            # hashes are report-only: never in the durable proposal (Increment 1 schema untouched)
            dumped = json.dumps(store.index[pid]["proposal"])
            self.assertNotIn(prov["decision_source_sha256"], dumped)
            self.assertNotIn("source_sha256", dumped)

    def test_O23_tampered_source_decision_produces_mismatch(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            run(src)
            src.decisions([dict(d, entry_ask_cents=88.0, improvement_cents=4.0,
                                effective_cost_cents=round(88.0 + d["fee_effective_cents"], 4))])
            rep, store = run(src)
            pid = next(iter(store.index))
            self.assertEqual(rep["source_mismatch"], 1)
            self.assertIn("frozen decision != source decision", rep["provenance"][pid]["detail"])
            self.assertEqual(rep["proposals_created"], 0)  # the tampered decision has the same identity: a different
            # proposal content -> conflict, never a second durable proposal
            self.assertEqual(rep["conflict_rejected"], 1)

    def test_O24_tampered_source_trace_ctx_produces_mismatch(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            run(src)
            src.traces([open_rec(dict(ctx, cons_pct=94.0))])
            rep, store = run(src)
            pid = next(iter(store.index))
            self.assertEqual(rep["source_mismatch"], 1)
            self.assertIn("frozen trace_ctx != source ctx subset", rep["provenance"][pid]["detail"])
            # ctx keys the core does not freeze do not affect verification
            src.traces([open_rec(dict(ctx, quote_max_age_s=9.0))])
            rep, _ = run(src)
            self.assertEqual(rep["source_verified"], 1)

    def test_O25_identity_links_to_exact_source_seq(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = run(src)
            pid = next(iter(store.index))
            p = store.index[pid]["proposal"]
            self.assertEqual(p["source_decision_seq"], d["decided_seq"])
            self.assertEqual(rep["provenance"][pid]["identity"], [d["cohort"], d["setup_id"], POLICY, d["decided_seq"]])
            self.assertEqual(p["idempotency_key"], v30.identity_key(d["cohort"], d["setup_id"], POLICY, ctx["market_ticker"], "UP", d["decided_seq"]))
            # a proposal whose identity points at a seq that is not in the source -> source_missing
            forged = json.loads(json.dumps(p))
            forged["frozen_context"]["decision"]["decided_seq"] = 9
            v = obs.verify_proposal_against_source(forged, {obs.decision_identity(d): obs.read_source_snapshot(src.path).decision_lines[0]},
                                                   obs.build_trace_index(obs.read_source_snapshot(src.path).trace_lines), SPECS)
            self.assertEqual(v["status"], "source_missing")


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------
class TestExpiry(unittest.TestCase):
    def test_O26_historical_proposal_immediately_expires(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = run(src, as_of=LATER)
            pid = next(iter(store.index))
            self.assertEqual((rep["proposals_created"], rep["expired_now"], store.index[pid]["status"]), (1, 1, "EXPIRED"))
            ev = events_in(src)
            self.assertEqual([e["event_type"] for e in ev], ["PROPOSED", "EXPIRED"])
            self.assertEqual(ev[1]["at_utc"], v30.iso_utc(LATER))
            self.assertEqual(ev[0]["proposal"]["expires_at_utc"], iso(DET_MS + 2000 + 30_000))  # TTL from frozen decided_at

    def test_O27_non_expired_remains_proposed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = run(src, as_of=EARLY)
            pid = next(iter(store.index))
            self.assertEqual((rep["expired_now"], store.index[pid]["status"]), (0, "PROPOSED"))
            rep2, store2 = run(src, as_of=LATER)
            self.assertEqual((rep2["proposals_created"], rep2["expired_now"], store2.index[pid]["status"]), (0, 1, "EXPIRED"))

    def test_as_of_never_reaches_build(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            _, s1 = run(src, as_of=EARLY, output=src.path / "a")
            _, s2 = run(src, as_of=LATER, output=src.path / "b")
            p1, p2 = next(iter(s1.index.values()))["proposal"], next(iter(s2.index.values()))["proposal"]
            self.assertEqual(dict(p1, status=None), dict(p2, status=None))  # only the lifecycle status differs
            self.assertEqual(p1["content_hash"], p2["content_hash"])


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------
class TestFailClosed(unittest.TestCase):
    def test_O28_output_persistence_failure_fails_closed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            blocked = src.path / "blocked"
            blocked.write_text("not a directory")
            rep, store = run(src, output=blocked / "sub")  # real OS failure: parent is a file
            self.assertEqual((rep["persist_failed"], rep["proposals_created"], store.index), (1, 0, {}))
            self.assertEqual(rep["rejection_reasons"], {"persist_failed": 1})
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--output", str(blocked / "sub")])
            self.assertEqual(rc, 2)
            self.assertIn("could not be persisted", err)
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "3",
                                    "--output", str(blocked / "sub")], clock=lambda: LATER, sleep=lambda s: None)
            self.assertEqual(rc, 2)  # watch stops on the first persistence failure
            # simulated append failure inside the store: counted, nothing created
            original = v30.ProposalEventLog.append
            v30.ProposalEventLog.append = lambda self, ev: (_ for _ in ()).throw(v30.PersistenceError("disk full"))
            try:
                rep, store = run(src, output=src.path / "o2")
            finally:
                v30.ProposalEventLog.append = original
            self.assertEqual((rep["persist_failed"], rep["proposals_created"], store.index), (1, 0, {}))

    def test_O29_malformed_source_scan_writes_zero_new_events(self):
        ctx, d = setup()
        ctx2, d2 = setup("SOL", "DOWN", "KXSOL15M-26SEP011345-T200", (93.0, 90.0))
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            run(src)
            before = (src.out / v30.EVENT_LOG_NAME).read_bytes()
            src.append(obs.TRACES_NAME, [open_rec(ctx2)])
            src.append(obs.DECISIONS_NAME, [d2], raw=b"{corrupt}\n")
            with self.assertRaises(obs.SourceCorruptError):
                run(src)
            self.assertEqual((src.out / v30.EVENT_LOG_NAME).read_bytes(), before)
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "5"],
                                 clock=lambda: LATER, sleep=lambda s: None)
            self.assertEqual(rc, 2)  # watch stops, no infinite retry
            self.assertEqual((src.out / v30.EVENT_LOG_NAME).read_bytes(), before)

    def test_watch_ctrl_c_clean_exit_and_min_interval(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])

            def interrupt(s):
                raise KeyboardInterrupt
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1"], clock=lambda: LATER, sleep=interrupt)
            self.assertEqual(rc, 130)
            self.assertEqual(len(events_in(src)), 2)
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "0.5"])
            self.assertEqual(rc, 2)
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--as-of", iso(DET_MS)])
            self.assertEqual(rc, 2)


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------
class TestSafety(unittest.TestCase):
    def test_O30_no_networking_with_socket_blocked(self):
        real_socket, real_create = socket.socket, socket.create_connection

        def deny(*a, **k):
            raise AssertionError("network blocked by test")
        socket.socket = socket.create_connection = deny
        try:
            ctx, d = setup()
            with SourceDir() as src:
                src.traces([open_rec(ctx)])
                src.decisions([d])
                rep, _ = run(src)
                self.assertEqual(rep["proposals_created"], 1)
                rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--json"])
                self.assertEqual(rc, 0, err)
                self.assertEqual(json.loads(out)["duplicate_suppressed"], 1)
                rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "2"],
                                       clock=lambda: LATER, sleep=lambda s: None)
                self.assertEqual(rc, 0, err)
        finally:
            socket.socket, socket.create_connection = real_socket, real_create

    def test_O31_no_credential_access(self):
        for f in V30_OBS_FILES:
            src = (ROOT / f).read_text(encoding="utf-8")
            for name in ("DASHBOARD_PASSWORD", "KALSHI", "TELEGRAM", "BINANCE", "RAILWAY", "PRIVATE_KEY", "API_KEY", "SECRET",
                         "secret", "signing", "api_key"):
                self.assertNotIn(name, src, f"{f} references {name}")
            self.assertNotIn("os.environ[", src)
            self.assertNotIn("os.getenv", src)
            for m in re.findall(r'(?:environ|env)\.get\("([A-Z0-9_]+)"', src):
                self.assertTrue(m.startswith("V30_"), f"{f} reads non-V30 env var {m}")
        # config is only ever obtained through the approved core helper, with the explicit mapping
        self.assertIn("v30.config_from_env(args.policy, env)", (ROOT / "scripts/v30_observe_v28.py").read_text())
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            hostile = {"KALSHI_API_KEY": "x", "KALSHI_PRIVATE_KEY": "y", "DASHBOARD_PASSWORD": "z", "TELEGRAM_BOT_TOKEN": "t",
                       "V30_MAX_PROPOSAL_QTY": "1"}
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--json"], environ=hostile)
            self.assertEqual(rc, 0, err)
            for v in ("x", "y", "z", "t"):
                self.assertNotIn(f'"{v}"', out)

    def test_O32_static_no_live_order_surface(self):
        needles = ["/portfolio/orders", "/portfolio/", "/orders", "place" + "_order", "submit" + "_order", "create" + "_order",
                   "cancel" + "_order", "amend" + "_order", "requests.post", "requests.put", "requests.patch", "requests.delete",
                   "requests.", "httpx", "aiohttp", "urllib.request", "urlopen", "http.client", "import socket", "websocket",
                   "execute(", "def execute", "broker", "Broker", "trading_api", "trade-api", "KalshiClient", "SUBMITTED",
                   "LIVE_FILLED", "LIVE_ORDER", "ORDER_SENT", 'method="POST"', "method='POST'"]
        for f in V30_OBS_FILES:
            src = (ROOT / f).read_text(encoding="utf-8")
            for n in needles:
                self.assertNotIn(n, src, f"{f} contains {n!r}")
            self.assertNotRegex(src, r"^\s*(import|from)\s+(requests|urllib|http|socket|ssl|httpx|aiohttp|asyncio|threading)\b")
        for name in ("urlopen", "Request", "requests", "socket", "http", "threading"):
            self.assertFalse(hasattr(obs, name), name)
            self.assertFalse(hasattr(cli, name), name)

    def test_O33_no_automatic_policy_ranking_or_selection(self):
        src_text = (ROOT / "vixion_v30_observer.py").read_text() + (ROOT / "scripts/v30_observe_v28.py").read_text()
        for w in ("best_policy", "rank", "sorted(POLICY", "POLICY_NAMES", "argmax", "promote", "winner", "pnl", "hit_rate"):
            self.assertNotIn(w, src_text, w)
        self.assertIn('required=True', (ROOT / "scripts/v30_observe_v28.py").read_text().split('"--policy"')[1].split("\n")[0])
        # a source with 3 policies for the same setup yields exactly the selected one
        ctx, d = setup()
        others = [run_policy(p, (92.0,) * 7 + (89.0,), ctx) for p in ("delay_5s", "entry_at_detection", "ask_le_95c")]
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d] + others)
            rep, store = run(src)
            self.assertEqual(rep["selected_policy_decisions"], 1)
            self.assertEqual({st["proposal"]["policy_id"] for st in store.index.values()}, {POLICY})

    def test_O34_no_tick_log_dependency(self):
        src_text = (ROOT / "vixion_v30_observer.py").read_text() + (ROOT / "scripts/v30_observe_v28.py").read_text()
        for w in ("entry_timing_ticks", "ticks_v28", "ticks_log", "build_tick", "on_tick", "ask_cents_now", "best_ask", "later_ask"):
            self.assertNotIn(w, src_text, w)
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            # a ticks log with cheaper later asks is present but must be irrelevant
            src.write("entry_timing_ticks_v28.jsonl", [{"setup_id": ctx["setup_id"], "seq": 5, "ask_cents": 60.0}])
            rep, store = run(src)
            self.assertEqual(next(iter(store.index.values()))["proposal"]["limit_price_cents"], 89)
            self.assertEqual(src.hashes()["entry_timing_ticks_v28.jsonl"], hashlib.sha256((src.path / "entry_timing_ticks_v28.jsonl").read_bytes()).hexdigest())

    def test_O35_no_outcome_settlement_dependency(self):
        src_text = (ROOT / "vixion_v30_observer.py").read_text() + (ROOT / "scripts/v30_observe_v28.py").read_text()
        for w in ("settlement", "kalshi_result", "kalshi_win", "final_close", "outcome", "resolved", "pnl", "ledger"):
            self.assertNotRegex(src_text, r"\b" + w + r"\b", w)
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx), close_rec(ctx)])
            src.decisions([d])
            src.write("multiasset_shadow_ledger_v23.json", [{"kalshi_result": "no"}])
            _, s1 = run(src, output=src.path / "a")
            src.write("multiasset_shadow_ledger_v23.json", [{"kalshi_result": "yes"}])
            _, s2 = run(src, output=src.path / "b")
            self.assertEqual(next(iter(s1.index.values()))["proposal"], next(iter(s2.index.values()))["proposal"])


def _git_show(ref, path):
    try:
        return subprocess.run(["git", "-C", str(ROOT), "show", f"{ref}:{path}"], capture_output=True, check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _main_bytes(path):
    for ref in ("origin/main", "main"):
        b = _git_show(ref, path)
        if b is not None:
            return ref, b
    return None, None


class TestByteIdentical(unittest.TestCase):
    def _same(self, *files):
        for f in files:
            ref, b = _main_bytes(f)
            self.assertIsNotNone(b, f"cannot read {f} from main")
            self.assertEqual(hashlib.sha256(b).hexdigest(), hashlib.sha256((ROOT / f).read_bytes()).hexdigest(), f"{f} differs from {ref}")

    def test_O36_v27_runtime_byte_identical(self):
        self._same("app.py", "vixion_v27.py", "launcher_v27.py", ".github/workflows/v27-regression.yml")

    def test_O37_v28_runtime_byte_identical(self):
        self._same("vixion_v28.py", "launcher_v28.py", "scripts/verify_v28_post_deploy.py", ".github/workflows/v28-regression.yml")

    def test_O38_v3_increment1_core_byte_identical(self):
        self._same("vixion_v30_proposal.py", "scripts/v30_proposal_lab.py", ".github/workflows/v30-proposal-regression.yml")
        self.assertEqual(hashlib.sha256((ROOT / "vixion_v30_proposal.py").read_bytes()).hexdigest(),
                         "89745d776989db256568bc426dcb1d36262b8a2aa662df013e0706540dced9af")  # approved Increment 1.2 core

    def test_O39_dockerfile_byte_identical(self):
        self._same("Dockerfile")
        self.assertNotIn("v30", (ROOT / "Dockerfile").read_text())
        for f in ("launcher_v28.py", "launcher_v27.py", "app.py"):
            self.assertNotIn("v30_observ", (ROOT / f).read_text())

    def test_O40_full_existing_regression_discoverable(self):
        names = sorted(p.name for p in (ROOT / "tests").glob("test_*.py"))
        for n in ("test_v27_launcher.py", "test_v27_observability.py", "test_v28_entry_timing.py", "test_v28_verifier.py",
                  "test_v30_proposal_gateway.py", "test_v30_real_observer.py"):
            self.assertIn(n, names)


# ---------------------------------------------------------------------------
# Increment 2.1.1: output aliasing, source decision validation, terminal uniqueness,
# rotation TOCTOU, last_scan_at semantics
# ---------------------------------------------------------------------------
def _setup_rotated_decision(src):
    ctx, d = setup()
    src.traces([open_rec(ctx)])
    src.decisions([d], rotated=True)
    src.decisions([])  # empty active
    return ctx, d


class TestOutputAliasing(unittest.TestCase):
    def _assert_rejected_unchanged(self, src, out_dir, exc=obs.OutputAliasError):
        before = src.hashes()
        with self.assertRaises(exc):
            run(src, output=out_dir)
        self.assertEqual(src.hashes(), before)
        rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--output", str(out_dir)])
        self.assertEqual(rc, 2)
        self.assertIn("FAIL CLOSED", err)
        self.assertEqual(src.hashes(), before)

    def test_O41_output_symlink_to_active_decisions_rejected(self):
        with SourceDir() as src:
            _setup_rotated_decision(src)
            out = src.path / "out"
            out.mkdir()
            (out / v30.EVENT_LOG_NAME).symlink_to(src.path / obs.DECISIONS_NAME)
            self._assert_rejected_unchanged(src, out)
            self.assertEqual((src.path / obs.DECISIONS_NAME).read_bytes(), b"")

    def test_O42_output_symlink_to_trace_source_rejected(self):
        with SourceDir() as src:
            _setup_rotated_decision(src)
            for target in (obs.TRACES_NAME, obs.TRACES_NAME + ".1", obs.DECISIONS_NAME + ".1"):
                out = src.path / ("out_" + target.replace(".", "_"))
                out.mkdir()
                (out / v30.EVENT_LOG_NAME).symlink_to(src.path / target)
                self._assert_rejected_unchanged(src, out)
            # symlinked output DIRECTORY pointing at the source dir
            out = src.path / "outlink"
            out.symlink_to(src.path)
            self._assert_rejected_unchanged(src, out)
            # output dir is a real dir but via a symlinked parent that resolves into the source
            link_parent = src.path.parent / (src.path.name + "-alias")
            link_parent.symlink_to(src.path)
            try:
                self._assert_rejected_unchanged(src, link_parent)
            finally:
                link_parent.unlink()

    def test_O43_hardlink_output_log_to_source_rejected(self):
        with SourceDir() as src:
            _setup_rotated_decision(src)
            for target in (obs.DECISIONS_NAME, obs.DECISIONS_NAME + ".1", obs.TRACES_NAME):
                out = src.path / ("hl_" + target.replace(".", "_"))
                out.mkdir()
                os.link(src.path / target, out / v30.EVENT_LOG_NAME)
                self._assert_rejected_unchanged(src, out)

    def test_O44_normal_separate_output_still_works(self):
        with SourceDir() as src:
            _setup_rotated_decision(src)
            before = src.hashes()
            rep, store = run(src)  # default <source>/v30_observer
            self.assertEqual(rep["proposals_created"], 1)
            self.assertEqual({k: v for k, v in src.hashes().items() if k in before}, before)
            with tempfile.TemporaryDirectory() as td:  # fully external output dir
                rep2, _ = run(src, output=Path(td) / "elsewhere")
                self.assertEqual(rep2["proposals_created"], 1)


class TestSourceDecisionValidation(unittest.TestCase):
    def _corrupt(self, mutate, mode="once"):
        ctx, d = setup()
        bad = dict(d)
        mutate(bad)
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([bad])
            with self.assertRaises(obs.SourceCorruptError) as cm:
                run(src, mode=mode)
            self.assertFalse(src.out.exists())
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            self.assertEqual(rc, 2)
            self.assertIn("SourceCorruptError", err)
            return str(cm.exception)

    def test_O45_setup_id_list_is_source_corrupt_not_typeerror(self):
        self.assertIn("setup_id", self._corrupt(lambda r: r.update(setup_id=[])))
        self._corrupt(lambda r: r.update(setup_id=""))
        self._corrupt(lambda r: r.update(setup_id={"a": 1}))

    def test_O46_decided_seq_dict_is_source_corrupt(self):
        self.assertIn("decided_seq", self._corrupt(lambda r: r.update(decided_seq={})))
        for v in ([], "2", True, -2, -10, 2.0, None):
            self._corrupt(lambda r, v=v: r.update(decided_seq=v))
        # -1 is NOT universally invalid (V2.8 no-tick close, see O55) but it is invalid on a FILLED record
        self._corrupt(lambda r: r.update(decided_seq=-1))

    def test_O47_cohort_list_is_source_corrupt(self):
        self.assertIn("cohort", self._corrupt(lambda r: r.update(cohort=[])))
        self._corrupt(lambda r: r.update(cohort=None))
        self._corrupt(lambda r: r.update(schema_version=3))
        self._corrupt(lambda r: r.__delitem__("ticks_seen"))
        self._corrupt(lambda r: r.update(kalshi_result="yes"))
        self._corrupt(lambda r: r.update(ask_size=float("inf")))

    def test_O48_incomplete_selected_decision_in_watch_is_not_deferred(self):
        ctx, d = setup()
        bad = dict(d)
        del bad["entry_ask_cents"]
        with SourceDir() as src:
            src.traces([])  # no open at all: a valid record would be DEFERRED in watch...
            src.decisions([bad])
            with self.assertRaises(obs.SourceCorruptError):  # ...but a structurally corrupt one is corruption
                run(src, mode="watch")
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "3"],
                                   clock=lambda: LATER, sleep=lambda s: None)
            self.assertEqual(rc, 2)
            self.assertNotIn("deferred", err.lower())
            self.assertFalse(src.out.exists())
            # unselected policies are not validated here (they never reach identity/lookup either)
            src.decisions([dict(bad, policy="delay_5s")])
            rep, _ = run(src, mode="watch")
            self.assertEqual((rep["selected_policy_decisions"], rep["deferred_missing_context"]), (0, 0))


class TestTerminalUniqueness(unittest.TestCase):
    def test_O49_same_setup_policy_different_seq_is_conflict(self):
        ctx, d = setup()
        d3 = run_policy(POLICY, (92.0, 91.0, 92.0, 89.0), ctx)  # a genuinely valid FILLED at seq 3
        self.assertEqual((d["decided_seq"], d3["decided_seq"]), (2, 3))
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d, d3])
            with self.assertRaises(obs.SourceConflictError) as cm:
                run(src)
            self.assertIn("multiple_terminal_decisions", str(cm.exception))
            self.assertFalse(src.out.exists())
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            self.assertEqual(rc, 2)
            # different policy for the same setup is fine (one terminal per setup/POLICY)
            src.decisions([d, run_policy("ask_le_95c", (92.0,), ctx)])
            rep, _ = run(src)
            self.assertEqual(rep["proposals_created"], 1)
            # different cohort is a different terminal key
            src.decisions([d, dict(d, cohort=v28.COHORT_BACKFILL, decided_seq=3, entry_seq=3)], rotated=False)
            rep, _ = run(src, output=src.path / "o2")
            self.assertEqual((rep["selected_policy_decisions"], rep["proposals_created"], rep["not_eligible"]), (2, 1, 1))

    def test_O50_conflict_across_rotation_same_result(self):
        ctx, d = setup()
        d3 = run_policy(POLICY, (92.0, 91.0, 92.0, 89.0), ctx)
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d], rotated=True)
            src.decisions([d3])
            with self.assertRaises(obs.SourceConflictError) as cm:
                run(src)
            self.assertIn("multiple_terminal_decisions", str(cm.exception))
            src.decisions([d3], rotated=True)
            src.decisions([d])
            with self.assertRaises(obs.SourceConflictError):
                run(src)
            self.assertFalse(src.out.exists())

    def test_O51_exact_duplicate_across_rotation_allowed(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)], rotated=True)
            src.traces([open_rec(ctx)])
            src.decisions([d], rotated=True)
            src.decisions([json.loads(json.dumps(d))])
            rep, store = run(src)
            self.assertEqual((rep["proposals_created"], rep["source_conflicts"], rep["source"]["decision_duplicate_records"]), (1, 0, 1))
            self.assertEqual(len(store.index), 1)


class TestRotationRace(unittest.TestCase):
    def test_O52_disappearance_between_lookup_and_open(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            real_open = obs.open if hasattr(obs, "open") else open
            import builtins
            active = src.path / obs.DECISIONS_NAME
            calls = {"n": 0}

            def vanishing_open(path, *a, **k):
                # the file is present at lookup time and unlinked right before open()
                if Path(str(path)) == active and calls["n"] == 0:
                    calls["n"] += 1
                    self.assertTrue(active.exists())
                    active.unlink()
                return real_open(path, *a, **k)
            builtins_open = builtins.open
            builtins.open = vanishing_open
            try:
                snap = obs.read_source_snapshot(src.path)
            finally:
                builtins.open = builtins_open
            self.assertFalse(snap.decisions_active.exists)
            self.assertEqual(snap.decision_lines, [])
            self.assertEqual(calls["n"], 1)
            rep, _ = run(src)  # transient absence == empty, controlled
            self.assertEqual(rep["proposals_created"], 0)

    def test_O53_permission_error_is_controlled(self):
        if os.geteuid() == 0:
            self.skipTest("running as root: chmod cannot produce PermissionError")
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            p = src.decisions([d])
            p.chmod(0)
            try:
                with self.assertRaises(obs.SourceIOError):
                    obs.read_source_snapshot(src.path)
                with self.assertRaises(obs.SourceIOError):
                    run(src)
                rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
                self.assertEqual(rc, 2)
                self.assertIn("SourceIOError", err)
                rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--watch", "1", "--max-scans", "3"],
                                     clock=lambda: LATER, sleep=lambda s: None)
                self.assertEqual(rc, 2)
            finally:
                p.chmod(0o644)
            self.assertFalse(src.out.exists())

    def test_O53b_read_oserror_wrapped_regardless_of_privilege(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            import builtins
            real = builtins.open

            def denied(path, *a, **k):
                if str(path).endswith(obs.DECISIONS_NAME):
                    raise PermissionError(13, "Permission denied", str(path))
                return real(path, *a, **k)
            builtins.open = denied
            try:
                with self.assertRaises(obs.SourceIOError):
                    obs.read_source_snapshot(src.path)
                rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            finally:
                builtins.open = real
            self.assertEqual(rc, 2)
            self.assertIn("SourceIOError", err)

    def test_O54_next_scan_after_rotation_race_processes_exactly_once(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            import builtins
            real = builtins.open
            active = src.path / obs.DECISIONS_NAME
            rotated = src.path / (obs.DECISIONS_NAME + ".1")
            state = {"raced": False}

            def racing_open(path, *a, **k):
                # V2.8 rotates exactly between the .1 read and the active read of scan 1
                if Path(str(path)) == active and not state["raced"]:
                    state["raced"] = True
                    os.replace(active, rotated)
                    active.write_bytes(b"")
                return real(path, *a, **k)
            builtins.open = racing_open
            try:
                rep1, _ = run(src)
            finally:
                builtins.open = real
            self.assertEqual(rep1["proposals_created"], 0)  # scan 1 saw the record in neither part
            rep2, store = run(src)  # next scan sees it in .1 exactly once
            self.assertEqual((rep2["proposals_created"], rep2["source"]["decisions_rotated_records"]), (1, 1))
            rep3, _ = run(src)
            self.assertEqual((rep3["proposals_created"], rep3["duplicate_suppressed"]), (0, 1))
            self.assertEqual(len(store.index), 1)


class TestLastScanAt(unittest.TestCase):
    def test_last_scan_at_is_orchestration_time_not_as_of(self):
        ctx, d = setup()
        clock_now = v30.parse_iso_utc("2026-09-05T10:00:00+00:00")
        as_of = v30.parse_iso_utc("2026-09-01T13:38:04+00:00")  # historical, before the TTL elapses (decided 13:38:04 + 30 s)
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = obs.reconcile_once(src.path, src.out, cfg(), SPECS, as_of, mode="once", scan_at_utc=clock_now)
            self.assertEqual(rep["last_scan_at"], v30.iso_utc(clock_now))
            self.assertEqual(rep["as_of_utc"], v30.iso_utc(as_of))
            self.assertEqual(rep["expired_now"], 0)  # evaluated against 2026-09-01, not 2026-09-05
            self.assertEqual(next(iter(store.index.values()))["status"], "PROPOSED")
            ev = events_in(src)[0]
            self.assertEqual(ev["recorded_at_utc"], v30.iso_utc(clock_now))
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--json",
                                    "--as-of", "2026-09-01T13:38:04+00:00"], clock=lambda: clock_now)
            js = json.loads(out)
            self.assertEqual((js["last_scan_at"], js["as_of_utc"], js["expired_now"]), (v30.iso_utc(clock_now), v30.iso_utc(as_of), 0))


class TestAuditorReproductions(unittest.TestCase):
    """Increment 2.1.1 auditor findings A-H, reproduced verbatim."""

    def test_A_symlinked_output_to_empty_active_decisions(self):
        with SourceDir() as src:
            _setup_rotated_decision(src)
            out = src.path / "output"
            out.mkdir()
            (out / v30.EVENT_LOG_NAME).symlink_to(src.path / obs.DECISIONS_NAME)
            before = src.hashes()
            rc, o, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--output", str(out)])
            self.assertEqual(rc, 2)
            self.assertEqual(src.hashes(), before)
            self.assertEqual((src.path / obs.DECISIONS_NAME).stat().st_size, 0)
            self.assertNotIn("proposals_created", o)

    def test_B_malformed_setup_id_list_no_typeerror(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([dict(d, setup_id=[])])
            try:
                run(src, mode="watch")
            except obs.SourceCorruptError:
                pass
            else:
                self.fail("expected SourceCorruptError")

    def test_C_malformed_decided_seq_dict_no_typeerror(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([dict(d, decided_seq={})])
            with self.assertRaises(obs.SourceCorruptError):
                run(src, mode="watch")

    def test_D_valid_decision_missing_trace_in_watch_deferred(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([])
            src.decisions([d])
            rep, _ = run(src, mode="watch")
            self.assertEqual((rep["deferred_missing_context"], rep["proposals_created"]), (1, 0))
            src.append(obs.TRACES_NAME, [open_rec(ctx)])
            rep, _ = run(src, mode="watch")
            self.assertEqual((rep["deferred_missing_context"], rep["proposals_created"]), (0, 1))

    def test_E_malformed_decision_missing_trace_in_watch_not_deferred(self):
        ctx, d = setup()
        with SourceDir() as src:
            src.traces([])
            src.decisions([dict(d, cohort=[])])
            with self.assertRaises(obs.SourceCorruptError):
                run(src, mode="watch")

    def test_F_two_valid_decisions_seq2_and_seq3_source_conflict(self):
        ctx, d = setup()
        d3 = run_policy(POLICY, (92.0, 91.0, 92.0, 89.0), ctx)
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            run(src)
            n = len(events_in(src))
            src.decisions([d, d3])
            with self.assertRaises(obs.SourceConflictError):
                run(src)
            self.assertEqual(len(events_in(src)), n)  # zero new events
            rc, _, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once"])
            self.assertEqual(rc, 2)
            self.assertIn("multiple_terminal_decisions", err)
            self.assertEqual(len(events_in(src)), n)

    def test_G_file_disappears_before_open(self):
        with SourceDir() as src:
            ghost = src.path / obs.TRACES_NAME
            ghost.write_bytes(b"")
            import builtins
            real = builtins.open

            def vanish(path, *a, **k):
                if Path(str(path)) == ghost and ghost.exists():
                    ghost.unlink()
                return real(path, *a, **k)
            builtins.open = vanish
            try:
                snap = obs.read_source_snapshot(src.path)
            finally:
                builtins.open = real
            self.assertFalse(snap.traces_active.exists)

    def test_H_historical_as_of_does_not_falsify_last_scan_at(self):
        ctx, d = setup()
        now = v30.parse_iso_utc("2026-09-05T12:34:56+00:00")
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rc, out, _ = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--json",
                                  "--as-of", "2026-09-01T00:00:00+00:00"], clock=lambda: now)
            js = json.loads(out)
            self.assertTrue(js["last_scan_at"].startswith("2026-09-05T12:34:56"))
            self.assertTrue(js["as_of_utc"].startswith("2026-09-01T00:00:00"))



# ---------------------------------------------------------------------------
# Increment 2.1.2: V2.8 no-tick close (decided_seq == -1) is a legitimate EXPIRED record
# ---------------------------------------------------------------------------
def real_no_tick_expired(ctx, policy=POLICY, close_ms=ELIG_END):
    """Produced by the REAL engine: TraceRuntime.close() before any tick was persisted."""
    rt = v28.TraceRuntime(dict(ctx))
    out = rt.close("restart_expired", close_ms)
    d = next(x for x in out if x.policy == policy)
    return d.to_dict()


class TestNoTickExpiredDecision(unittest.TestCase):
    def test_O55_real_trace_runtime_close_before_any_tick(self):
        ctx = make_ctx()
        d = real_no_tick_expired(ctx)
        self.assertEqual((d["state"], d["fill"], d["decided_seq"], d["entry_seq"]), ("EXPIRED", False, -1, None))
        self.assertIsNone(d["entry_tick_at"])
        self.assertIsNone(d["entry_quote_fetched_at"])
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([d])
            rep, store = run(src)  # no SourceCorruptError
            self.assertEqual((rep["selected_policy_decisions"], rep["selected_filled_decisions"], rep["proposals_created"],
                              rep["not_eligible"], rep["invalid_rejected"]), (1, 0, 0, 1, 0))
            self.assertEqual(store.index, {})
            self.assertEqual(events_in(src), [])
            rc, out, err = run_cli(["--source-data-dir", str(src.path), "--policy", POLICY, "--once", "--json"])
            self.assertEqual(rc, 0, err)
            js = json.loads(out)
            self.assertEqual((js["not_eligible"], js["proposals_created"]), (1, 0))
            self.assertEqual(events_in(src), [])
            # the whole 37-policy close batch is accepted the same way for its own policy
            rt = v28.TraceRuntime(dict(ctx))
            batch = [x.to_dict() for x in rt.close("restart_expired", ELIG_END)]
            self.assertEqual(len(batch), 37)
            src.decisions(batch)
            rep, _ = run(src, policy="delay_5s", output=src.path / "o2")
            self.assertEqual((rep["selected_policy_decisions"], rep["not_eligible"], rep["proposals_created"]), (1, 1, 0))

    def _corrupt(self, rec, needle=None):
        ctx = make_ctx()
        with SourceDir() as src:
            src.traces([open_rec(ctx)])
            src.decisions([rec])
            with self.assertRaises(obs.SourceCorruptError) as cm:
                run(src)
            if needle:
                self.assertIn(needle, str(cm.exception))
            self.assertFalse(src.out.exists())

    def test_O56_filled_with_seq_minus_one_rejected(self):
        ctx, d = setup()
        self._corrupt(dict(d, decided_seq=-1), "decided_seq -1")
        self._corrupt(dict(d, decided_seq=-1, entry_seq=None, entry_tick_at=None, entry_quote_fetched_at=None), "decided_seq -1")

    def test_O57_timeout_with_seq_minus_one_rejected(self):
        ctx = make_ctx()
        d = real_no_tick_expired(ctx)
        self._corrupt(dict(d, state="TIMEOUT"), "only valid for EXPIRED")
        # a REAL timeout is born from a real tick (seq >= 0); the engine never emits TIMEOUT at -1
        spec = next(s for s in v28.POLICY_SPECS if s["policy"] == "max10s_93c")
        ps = v28.PolicyState(spec)
        c = dict(ctx, detection_ask=95.0)
        t = ps.on_tick(v28.build_tick(c, 11, DET_MS + 11_000, DET_MS + 11_000, 89.0, bid=88.0, ask_size=5.0), c).to_dict()
        self.assertEqual((t["state"], t["decided_seq"]), ("TIMEOUT", 11))

    def test_O58_rejected_with_seq_minus_one_rejected(self):
        ctx = make_ctx()
        d = real_no_tick_expired(ctx)
        self._corrupt(dict(d, state="REJECTED"), "only valid for EXPIRED")
        self._corrupt(dict(d, fill=True), "only valid for EXPIRED")
        self._corrupt(dict(d, fill=None), "only valid for EXPIRED")

    def test_O59_expired_seq_minus_one_with_entry_fields_rejected(self):
        ctx = make_ctx()
        d = real_no_tick_expired(ctx)
        self._corrupt(dict(d, entry_seq=0), "entry_seq is not null")
        self._corrupt(dict(d, entry_tick_at=iso(DET_MS)), "entry_tick_at is not null")
        self._corrupt(dict(d, entry_quote_fetched_at=iso(DET_MS)), "entry_quote_fetched_at is not null")
        self._corrupt(dict(d, decided_seq=-2), "< -1")

    def test_O60_expired_seq_minus_one_missing_ctx_watch_deferred_then_not_eligible(self):
        ctx = make_ctx()
        d = real_no_tick_expired(ctx)
        with SourceDir() as src:
            src.traces([])
            src.decisions([d])
            rep, _ = run(src, mode="watch")
            self.assertEqual((rep["deferred_missing_context"], rep["proposals_created"], rep["not_eligible"]), (1, 0, 0))
            self.assertEqual(rep["missing_context_setups"], [ctx["setup_id"]])
            with self.assertRaises(obs.SourceMissingContextError):
                run(src, mode="once")
            src.append(obs.TRACES_NAME, [open_rec(ctx)])
            rep, store = run(src, mode="watch")
            self.assertEqual((rep["deferred_missing_context"], rep["proposals_created"], rep["not_eligible"]), (0, 0, 1))
            self.assertEqual((store.index, events_in(src)), ({}, []))
            # the same terminal key can never also carry a FILLED at another seq (terminal uniqueness still applies)
            _, filled = setup()
            src.decisions([d, filled])
            with self.assertRaises(obs.SourceConflictError):
                run(src, mode="watch")



if __name__ == "__main__":
    unittest.main()
