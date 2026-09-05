"""VIXION 3.0 Increment 1 — proposal gateway tests (T01..T30 + extra invariants).

Fixtures are produced by the REAL 2.8 engine (PolicyState.on_tick over build_tick
ticks) so the gateway is tested against the genuine Decision contract.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="v30-data-"))

import vixion_v28 as v28  # noqa: E402  (catalogue + fixture engine only)
import vixion_v30_proposal as v30  # noqa: E402
sys.path.insert(0, str(ROOT / "scripts"))
import v30_proposal_lab as lab  # noqa: E402

WS_MS = 1_788_269_400_000  # 2026-09-01T13:30:00Z (15m boundary)
MINUTE = 8
DET_MS = WS_MS + MINUTE * 60000 + 2000
ELIG_END = WS_MS + (MINUTE + 1) * 60000
SPECS = v30.load_canonical_policy_specs()
V30_FILES = ("vixion_v30_proposal.py", "scripts/v30_proposal_lab.py")
PRODUCTION_FILES = ("app.py", "vixion_v27.py", "launcher_v27.py", "vixion_v28.py", "launcher_v28.py", "Dockerfile")


def iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def make_ctx(**over):
    ctx = {"cohort": v28.COHORT_FORWARD, "setup_id": "ETH|" + iso(WS_MS), "asset": "ETH", "side": "UP",
           "window_start_utc": iso(WS_MS), "window_start_ms": WS_MS, "setup_minute": MINUTE,
           "detection_at": iso(DET_MS), "detection_ms": DET_MS, "eligibility_end_ms": ELIG_END,
           "market_ticker": "KXETH15M-26SEP011345-T4500", "cons_pct": 95.0, "threshold_bp": 30.0, "distance_bp": 34.2,
           "target_gap_bp": 1.1, "basis_ok": True, "edge_min_points": 10.0, "fee_buffer_cents": 1.0,
           "production_max_buy_cents": 83.0, "signal_status": "SHADOW PRICE HIGH", "quote_max_age_s": 5.0,
           "tick_resolution": "1s", "production_detection_ask_cents": 92.0, "production_detection_bid_cents": 90.0}
    ctx.update(over)
    return ctx


def run_policy(policy, asks, ctx=None):
    """Feed 1 Hz ticks with the given asks through ONE 2.8 policy; return its terminal decision dict."""
    ctx = dict(ctx or make_ctx())
    spec = next(s for s in v28.POLICY_SPECS if s["policy"] == policy)
    ps = v28.PolicyState(spec)
    for seq, ask in enumerate(asks):
        t_ms = DET_MS + seq * 1000
        tick = v28.build_tick(ctx, seq, t_ms, t_ms, ask, bid=ask - 1.0, ask_size=5.0)
        if ctx.get("detection_ask") is None and tick["quote_valid"]:
            ctx["detection_ask"] = tick["ask_cents"]
        ps.ticks_seen += 1
        d = ps.on_tick(tick, ctx)
        if d is not None:
            return d.to_dict()
    raise AssertionError("policy never reached a terminal state")


def filled_decision(policy="ask_le_90c", asks=(92.0, 91.0, 89.0)):
    d = run_policy(policy, asks)
    assert d["state"] == "FILLED", d
    return d


def cfg(policy="ask_le_90c", **over):
    return v30.GatewayConfig(policy_id=policy, **over)


class TmpStore:
    def __enter__(self):
        self.dir = tempfile.TemporaryDirectory(prefix="v30-")
        self.path = Path(self.dir.name) / "v30" / v30.EVENT_LOG_NAME
        self.store = v30.ProposalStore(self.path, SPECS)
        return self

    def __exit__(self, *a):
        self.dir.cleanup()


# ---------------------------------------------------------------------------
# Build / validation
# ---------------------------------------------------------------------------
class TestBuild(unittest.TestCase):
    def test_T01_valid_proposal(self):
        d = filled_decision()
        r = v30.build_proposal(d, make_ctx(), cfg(), SPECS)
        self.assertTrue(r.ok, r)
        p = r.proposal
        self.assertEqual(p["status"], "PROPOSED")
        self.assertEqual(p["observed_ask_cents"], 89.0)
        self.assertEqual(p["limit_price_cents"], 89)
        self.assertEqual(p["quantity"], 1)
        self.assertEqual(p["side"], "UP")
        self.assertEqual(p["contract_ticker"], "KXETH15M-26SEP011345-T4500")
        self.assertEqual(p["source_decision_seq"], 2)
        self.assertEqual(p["source_tick_seq"], 2)
        self.assertEqual(p["created_at_utc"], p["source_decided_at_utc"])
        self.assertEqual(p["frozen_context"]["policy_cap_cents"], 90.0)
        self.assertAlmostEqual(p["edge_cents"], 95.0 - 89 - d["fee_effective_cents"], places=4)
        for k in ("schema_version", "proposal_id", "idempotency_key", "created_at_utc", "expires_at_utc", "setup_id",
                  "trace_id", "asset", "policy_id", "source_decision_seq", "source_tick_seq", "source_decided_at_utc",
                  "contract_ticker", "side", "observed_ask_cents", "limit_price_cents", "quantity", "fair_pct",
                  "conservative_fair_pct", "edge_cents", "status", "reason", "source", "frozen_context"):
            self.assertIn(k, p)
        self.assertIsNone(v30.validate_proposal(p, SPECS, cfg()))

    def test_T02_missing_policy_rejected(self):
        r = v30.build_proposal(filled_decision(), make_ctx(), cfg(policy=""), SPECS)
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "missing_policy_id")

    def test_T03_unknown_policy_rejected(self):
        r = v30.build_proposal(filled_decision(), make_ctx(), cfg(policy="ask_le_89c"), SPECS)
        self.assertEqual(r.reason, "unknown_policy_id")
        self.assertEqual(len(SPECS), 37, "canonical 2.8 catalogue must be the 37 policies")

    def test_T04_missing_setup_id_rejected(self):
        d = filled_decision()
        d["setup_id"] = ""
        r = v30.build_proposal(d, make_ctx(setup_id=""), cfg(), SPECS)
        self.assertEqual(r.reason, "missing_setup_id")
        r = v30.build_proposal(filled_decision(), None, cfg(), SPECS)
        self.assertEqual(r.reason, "missing_or_mismatched_context")

    def test_T05_missing_contract_rejected(self):
        r = v30.build_proposal(filled_decision(), make_ctx(market_ticker=""), cfg(), SPECS)
        self.assertEqual(r.reason, "missing_contract")
        r = v30.build_proposal(filled_decision(), make_ctx(market_ticker=None), cfg(), SPECS)
        self.assertEqual(r.reason, "missing_contract")

    def test_T06_invalid_side_rejected(self):
        for bad in ("YES", "up", "", None, 1):
            r = v30.build_proposal(filled_decision(), make_ctx(side=bad), cfg(), SPECS)
            self.assertEqual(r.reason, "invalid_side", bad)

    def test_T07_invalid_price_rejected(self):
        for bad in (0.0, 100.0, -5.0, float("nan"), float("inf"), None, "89"):
            d = filled_decision()
            d["entry_ask_cents"] = bad
            r = v30.build_proposal(d, make_ctx(), cfg(), SPECS)
            self.assertEqual(r.reason, "invalid_observed_ask", bad)
        d = filled_decision()
        d["entry_ask_cents"] = 0.4  # in-range for floats but floors to 0 cents
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_observed_ask")
        d = filled_decision()
        d["fee_effective_cents"] = -1.0
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_fee")

    def test_T08_invalid_quantity_rejected(self):
        for q in (0, -1):
            r = v30.build_proposal(filled_decision(), make_ctx(), cfg(quantity=q), SPECS)
            self.assertEqual(r.reason, "invalid_quantity", q)
        r = v30.build_proposal(filled_decision(), make_ctx(), cfg(quantity=True), SPECS)
        self.assertEqual(r.reason, "invalid_quantity")

    def test_T09_quantity_above_max_rejected(self):
        r = v30.build_proposal(filled_decision(), make_ctx(), cfg(quantity=2, max_quantity=1), SPECS)
        self.assertEqual(r.reason, "quantity_above_max")
        r = v30.build_proposal(filled_decision(), make_ctx(), cfg(quantity=1, max_quantity=0), SPECS)
        self.assertEqual(r.reason, "invalid_max_quantity")
        with self.assertRaises(ValueError):
            v30.config_from_env("ask_le_90c", {"V30_MAX_PROPOSAL_QTY": "abc"})
        c = v30.config_from_env("ask_le_90c", {"V30_MAX_PROPOSAL_QTY": "3", "V30_PROPOSAL_QTY": "2"})
        self.assertEqual((c.quantity, c.max_quantity), (2, 3))

    def test_price_cap_by_family(self):
        # threshold: ask 89 with cap 90 -> 89
        self.assertEqual(v30.build_proposal(filled_decision(), make_ctx(), cfg(), SPECS).proposal["limit_price_cents"], 89)
        # pullback_2c: detection 92 -> cap 90; fill at 89.5 -> floor 89
        d = filled_decision("pullback_2c", (92.0, 91.0, 89.5))
        p = v30.build_proposal(d, make_ctx(), cfg("pullback_2c"), SPECS).proposal
        self.assertEqual(p["frozen_context"]["policy_cap_cents"], 90.0)
        self.assertEqual(p["limit_price_cents"], 89)
        # production_baseline: cap = production_max_buy (83); fill at 83.0 (edge 95-83-1=11 >= 10)
        d = filled_decision("production_baseline", (92.0, 83.0))
        p = v30.build_proposal(d, make_ctx(), cfg("production_baseline"), SPECS).proposal
        self.assertEqual(p["frozen_context"]["policy_cap_cents"], 83.0)
        self.assertEqual(p["limit_price_cents"], 83)
        # detection: no cap; fractional ask floors down (never pays above observed)
        d = filled_decision("entry_at_detection", (91.7,))
        p = v30.build_proposal(d, make_ctx(), cfg("entry_at_detection"), SPECS).proposal
        self.assertIsNone(p["frozen_context"]["policy_cap_cents"])
        self.assertEqual(p["limit_price_cents"], 91)
        # inconsistent input: FILLED above the policy cap -> fail closed
        d = filled_decision()
        d["entry_ask_cents"] = 93.0
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "observed_ask_above_policy_cap")

    def test_not_eligible_decisions_produce_no_proposal(self):
        ctx = make_ctx()
        # ask_le_85c never matches at 92/91/89; an ineligible tick then forces EXPIRED (not FILLED)
        spec = next(s for s in v28.POLICY_SPECS if s["policy"] == "ask_le_85c")
        ps = v28.PolicyState(spec)
        t = v28.build_tick(ctx, 0, ELIG_END + 1000, ELIG_END + 1000, 80.0, bid=79.0, ask_size=5.0)
        d = ps.on_tick(t, ctx).to_dict()
        self.assertEqual(d["state"], "EXPIRED")
        r = v30.build_proposal(d, ctx, cfg("ask_le_85c"), SPECS)
        self.assertEqual((r.category, r.reason), ("not_eligible", "decision_not_filled"))
        d = filled_decision()
        r = v30.build_proposal(d, make_ctx(), cfg("delay_5s"), SPECS)
        self.assertEqual(r.reason, "policy_not_selected")
        d["cohort"] = v28.COHORT_BACKFILL
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "cohort_not_allowed")
        d = filled_decision()
        self.assertEqual(v30.build_proposal(d, make_ctx(basis_ok=False), cfg(), SPECS).reason, "basis_not_ok")
        d = filled_decision()
        d["entry_seq"] = d["decided_seq"] + 1
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "decided_seq_ne_entry_seq")


# ---------------------------------------------------------------------------
# Identity / idempotency / replay
# ---------------------------------------------------------------------------
class TestIdentityAndReplay(unittest.TestCase):
    def test_T10_deterministic_identity(self):
        d = filled_decision()
        a = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
        b = v30.build_proposal(json.loads(json.dumps(d)), make_ctx(), cfg(), SPECS).proposal
        self.assertEqual(a, b)
        self.assertEqual(a["proposal_id"], b["proposal_id"])
        self.assertEqual(a["idempotency_key"], b["idempotency_key"])
        self.assertEqual(a["content_hash"], b["content_hash"])
        self.assertEqual(a["idempotency_key"], v30.identity_key(d["cohort"], d["setup_id"], "ask_le_90c",
                                                                 a["contract_ticker"], "UP", d["decided_seq"]))
        self.assertEqual(a["proposal_id"], "v30p_" + a["idempotency_key"][:32])
        # identity changes when any identity field changes
        d2 = dict(d, decided_seq=d["decided_seq"] + 1, entry_seq=d["entry_seq"] + 1)
        self.assertNotEqual(v30.build_proposal(d2, make_ctx(), cfg(), SPECS).proposal["proposal_id"], a["proposal_id"])
        self.assertNotEqual(v30.build_proposal(d, make_ctx(market_ticker="X-1"), cfg(), SPECS).proposal["proposal_id"], a["proposal_id"])

    def test_T11_duplicate_decision_suppressed(self):
        d = filled_decision()
        with TmpStore() as ts:
            rep, res = v30.process_decisions([d, dict(d), json.loads(json.dumps(d))], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            self.assertEqual(rep["proposals_created"], 1)
            self.assertEqual(rep["duplicate_suppressed"], 2)
            self.assertEqual(len(ts.store.log.read()), 1)
            self.assertEqual(len(ts.store.index), 1)

    def test_same_identity_different_content_is_conflict_not_merge(self):
        d = filled_decision()
        with TmpStore() as ts:
            v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            rep, res = v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(ttl_s=5.0), SPECS, ts.store)
            self.assertEqual(rep["conflict_rejected"], 1)
            self.assertEqual(rep["proposals_created"], 0)
            self.assertEqual(len(ts.store.log.read()), 1, "conflicting content must never be appended")

    def test_T12_replay_deterministic(self):
        d1, d2 = filled_decision(), filled_decision("ask_le_90c", (91.0, 90.0))
        d2 = dict(d2, setup_id="SOL|" + iso(WS_MS))
        ctxs = {d1["setup_id"]: make_ctx(), d2["setup_id"]: make_ctx(setup_id=d2["setup_id"], asset="SOL", side="DOWN",
                                                                        market_ticker="KXSOL15M-26SEP011345-T200")}
        with TmpStore() as ts:
            rep1, _ = v30.process_decisions([d1, d2], ctxs, cfg(), SPECS, ts.store)
            events = ts.store.log.read()
            a = v30.replay_proposals(events, SPECS)
            b = v30.replay_proposals(json.loads(json.dumps(events)), SPECS)
            self.assertEqual(a, b)
            self.assertEqual(json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True))
            # a second full run over the same inputs changes nothing
            ts2 = v30.ProposalStore(ts.path, SPECS)
            rep2, _ = v30.process_decisions([d1, d2], ctxs, cfg(), SPECS, ts2)
            self.assertEqual(rep2["duplicate_suppressed"], 2)
            self.assertEqual(v30.replay_proposals(ts2.log.read(), SPECS), a)

    def test_T13_append_only_event_persistence(self):
        d = filled_decision()
        with TmpStore() as ts:
            v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            first = ts.path.read_bytes()
            pid = next(iter(ts.store.index))
            ts.store.record(v30.make_event(v30.SIMULATED_APPROVED, pid, iso(DET_MS + 5000), "review"))
            second = ts.path.read_bytes()
            self.assertTrue(second.startswith(first), "existing bytes must never be rewritten")
            self.assertEqual(second.count(b"\n"), 2)
            # public API of the log has no rewrite/delete capability
            for name in ("truncate", "delete", "rewrite", "remove", "update", "overwrite"):
                self.assertFalse(hasattr(v30.ProposalEventLog, name), name)

    def test_T14_index_rebuild_from_log(self):
        d = filled_decision()
        with TmpStore() as ts:
            v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            pid = next(iter(ts.store.index))
            ts.store.record(v30.make_event(v30.SIMULATED_REJECTED, pid, iso(DET_MS + 5000), "review"))
            snapshot = json.dumps(ts.store.index, sort_keys=True)
            fresh = v30.ProposalStore(ts.path, SPECS)  # no in-memory state carried over
            self.assertEqual(json.dumps(fresh.index, sort_keys=True), snapshot)
            self.assertEqual(fresh.index[pid]["status"], v30.SIMULATED_REJECTED)
            # a mutable index file is never the source of truth: none is written
            self.assertEqual(sorted(p.name for p in ts.path.parent.iterdir()), [v30.EVENT_LOG_NAME])

    def test_T15_write_failure_fails_closed(self):
        d = filled_decision()
        with TmpStore() as ts:
            store = ts.store
            original = store.log.append

            def broken(event):
                raise v30.PersistenceError("disk full (simulated)")
            store.log.append = broken
            rep, res = v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, store)
            self.assertEqual(rep["persist_failed"], 1)
            self.assertEqual(rep["proposals_created"], 0)
            self.assertEqual(res[0][1].category, "persist")
            self.assertEqual(store.index, {}, "a proposal whose durable write failed must not be in the cache")
            store.log.append = original
            self.assertEqual(store.log.read(), [])
            # real OS failure: log path is a directory
            bad = v30.ProposalEventLog(Path(ts.dir.name))  # an existing directory, not a file
            p = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
            with self.assertRaises(v30.PersistenceError):
                bad.append(v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], "proposed", proposal=p))
            with self.assertRaises(v30.ProposalError):  # invalid envelope never reaches disk
                bad.append({"x": 1})

    def test_T16_terminal_state_immutable(self):
        d = filled_decision()
        with TmpStore() as ts:
            v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            pid = next(iter(ts.store.index))
            original_event = ts.store.log.read()[0]
            ts.store.record(v30.make_event(v30.EXPIRED, pid, iso(ELIG_END), "ttl"))
            for et in (v30.SIMULATED_APPROVED, v30.SIMULATED_REJECTED):
                with self.assertRaises(v30.ProposalError):
                    ts.store.record(v30.make_event(et, pid, iso(ELIG_END + 1000), "late"))
            # exact replay of the original PROPOSED event is an idempotent no-op even after terminal
            self.assertEqual(ts.store.record(dict(original_event)), "duplicate")
            # same proposal content under a different envelope is a conflict, not a duplicate
            p0 = original_event["proposal"]
            with self.assertRaises(v30.ProposalError):
                ts.store.record(v30.make_event(v30.PROPOSED, pid, p0["created_at_utc"], "re-proposed", proposal=p0))
            self.assertEqual(ts.store.index[pid]["status"], v30.EXPIRED)
            self.assertEqual(len(ts.store.log.read()), 2)

    def test_T17_invalid_transition_rejected(self):
        d = filled_decision()
        p = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
        # event for unknown proposal
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(None, v30.make_event(v30.EXPIRED, p["proposal_id"], iso(ELIG_END)))
        st, out = v30.apply_proposal_event(None, v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], proposal=p))
        self.assertEqual(out, "applied")
        st2, _ = v30.apply_proposal_event(st, v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 3000)))
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st2, v30.make_event(v30.SIMULATED_REJECTED, p["proposal_id"], iso(DET_MS + 4000)))
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st2, v30.make_event(v30.EXPIRED, p["proposal_id"], iso(DET_MS + 4000)))
        # duplicate terminal event is idempotent (no double transition)
        ev = v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 3000))
        st3, out = v30.apply_proposal_event(st2, ev)
        self.assertEqual((out, len(st3["history"])), ("duplicate", 2))
        with self.assertRaises(v30.ProposalError):
            v30.make_event("SUBMITTED", p["proposal_id"], iso(DET_MS))
        # corrupt payload in the log fails replay closed
        bad = v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], proposal=dict(p, limit_price_cents=95))
        with self.assertRaises(v30.ProposalError):
            v30.replay_proposals([bad], SPECS)
        self.assertEqual(sorted(v30.STATES), sorted(["PROPOSED", "EXPIRED", "SIMULATED_APPROVED", "SIMULATED_REJECTED"]))

    def test_T18_expiration_deterministic(self):
        d = filled_decision()
        p = v30.build_proposal(d, make_ctx(), cfg(ttl_s=30.0), SPECS).proposal
        decided = v30.parse_iso_utc(d["decided_at"])
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]), decided + timedelta(seconds=30))
        self.assertFalse(v30.evaluate_expiry(p, decided + timedelta(seconds=29.999)))
        self.assertTrue(v30.evaluate_expiry(p, decided + timedelta(seconds=30)))
        # TTL never extends beyond the 2.8 eligibility end
        p2 = v30.build_proposal(d, make_ctx(), cfg(ttl_s=3600.0), SPECS).proposal
        self.assertEqual(v30.parse_iso_utc(p2["expires_at_utc"]).timestamp() * 1000, ELIG_END)
        with TmpStore() as ts:
            ts.store.propose(p)
            self.assertEqual(ts.store.expire_due(decided + timedelta(seconds=10)), [])
            self.assertEqual(ts.store.expire_due(decided + timedelta(seconds=30)), [p["proposal_id"]])
            self.assertEqual(ts.store.expire_due(decided + timedelta(seconds=30)), [])  # idempotent
            self.assertEqual(ts.store.index[p["proposal_id"]]["status"], v30.EXPIRED)


# ---------------------------------------------------------------------------
# No hindsight
# ---------------------------------------------------------------------------
class TestNoHindsight(unittest.TestCase):
    def _original(self):
        return v30.build_proposal(filled_decision(), make_ctx(), cfg(), SPECS).proposal

    def test_T19_future_cheaper_tick_does_not_modify_proposal(self):
        original = self._original()
        # The 2.8 policy already decided at seq 2 (ask 89). Later cheaper ticks exist in the trace.
        d_future = run_policy("ask_le_90c", (92.0, 91.0, 89.0, 80.0, 75.0, 60.0))
        self.assertEqual(d_future["decided_seq"], 2)  # 2.8 froze on its own tick
        replayed = v30.build_proposal(d_future, make_ctx(), cfg(), SPECS).proposal
        self.assertEqual(replayed, original)
        self.assertEqual(replayed["limit_price_cents"], 89)
        with TmpStore() as ts:
            v30.process_decisions([filled_decision()], {original["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            rep, _ = v30.process_decisions([d_future], {original["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            self.assertEqual(rep["duplicate_suppressed"], 1)
            self.assertEqual(ts.store.index[original["proposal_id"]]["proposal"], original)

    def test_T20_future_more_expensive_tick_does_not_modify_proposal(self):
        original = self._original()
        d_future = run_policy("ask_le_90c", (92.0, 91.0, 89.0, 96.0, 99.0))
        self.assertEqual(v30.build_proposal(d_future, make_ctx(), cfg(), SPECS).proposal, original)

    def test_T21_proposal_does_not_depend_on_future_resolution(self):
        """Strict schema: any future/outcome field is rejected deterministically; the
        canonical-only decision yields the FULL proposal (frozen_context + content_hash)."""
        original = self._original()
        base = filled_decision()
        future = {"kalshi_result": "yes", "kalshi_win": True, "settlement": {"ts": iso(ELIG_END + 900_000)},
                  "underlying_final_close": 4512.3, "pnl": 10.0, "resolved": True}
        for k, v in future.items():
            r = v30.build_proposal(dict(base, **{k: v}), make_ctx(), cfg(), SPECS)
            self.assertFalse(r.ok, k)
            self.assertEqual(r.reason, f"unexpected_decision_fields:{k}")
        r = v30.build_proposal(dict(base, **future), make_ctx(), cfg(), SPECS)
        self.assertEqual(r.reason, "unexpected_decision_fields:" + ",".join(sorted(future)))
        # the same decision without the extra keys is byte-identical to the original, everything included
        again = v30.build_proposal(json.loads(json.dumps(base)), make_ctx(), cfg(), SPECS).proposal
        self.assertEqual(again, original)
        self.assertEqual(again["frozen_context"], original["frozen_context"])
        self.assertEqual(again["content_hash"], original["content_hash"])
        # ctx extras are not frozen either (only the declared subset is copied)
        p = v30.build_proposal(base, make_ctx(kalshi_result="no", settlement="x"), cfg(), SPECS).proposal
        self.assertEqual(p, original)
        # frozen decision is exactly the canonical field set
        self.assertEqual(set(original["frozen_context"]["decision"]), set(v30.load_canonical_decision_fields()))
        self.assertEqual(len(v30.load_canonical_decision_fields()), 31)
        src = (ROOT / "vixion_v30_proposal.py").read_text(encoding="utf-8")
        for word in ("kalshi_win", "kalshi_result", "settlement_ts", "settlement", "pnl", "resolved", "final_close"):
            self.assertNotRegex(src, r'\b' + word + r'\b', f"core must not reference {word}")

    def test_missing_canonical_field_rejected(self):
        d = filled_decision()
        del d["improvement_cents"]
        r = v30.build_proposal(d, make_ctx(), cfg(), SPECS)
        self.assertEqual(r.reason, "missing_decision_fields:improvement_cents")

    def test_build_is_pure_no_clock_no_io(self):
        import inspect
        src = inspect.getsource(v30.build_proposal)
        for forbidden in ("datetime.now", "time.time", "open(", "os.environ", "urlopen", "socket"):
            self.assertNotIn(forbidden, src, forbidden)


# ---------------------------------------------------------------------------
# Safety: offline, no credentials, no order surface
# ---------------------------------------------------------------------------
class TestSafety(unittest.TestCase):
    def test_T22_offline_operation_with_network_blocked(self):
        real_socket, real_create = socket.socket, socket.create_connection

        class Blocked(Exception):
            pass

        def deny(*a, **k):
            raise Blocked("network blocked by test")
        socket.socket = deny
        socket.create_connection = deny
        try:
            d = filled_decision()
            with TmpStore() as ts:
                rep, _ = v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
                self.assertEqual(rep["proposals_created"], 1)
                pid = next(iter(ts.store.index))
                ts.store.expire_due(v30.parse_iso_utc(d["decided_at"]) + timedelta(seconds=60))
                self.assertEqual(v30.ProposalStore(ts.path, SPECS).index[pid]["status"], v30.EXPIRED)
                # the CLI lab end-to-end, still with sockets denied
                dec_path = Path(ts.dir.name) / "decisions.jsonl"
                tr_path = Path(ts.dir.name) / "traces.jsonl"
                dec_path.write_text(json.dumps(d) + "\n", encoding="utf-8")
                tr_path.write_text(json.dumps({"schema_version": 2, "event": "open", "setup_id": d["setup_id"],
                                               "opened_at": iso(DET_MS), "ctx": make_ctx()}) + "\n", encoding="utf-8")
                out = io.StringIO()
                sys.stdout, saved = out, sys.stdout
                try:
                    rc = lab.main(["--decisions", str(dec_path), "--traces", str(tr_path), "--policy", "ask_le_90c",
                                   "--output", str(Path(ts.dir.name) / "lab"), "--json"], environ={})
                finally:
                    sys.stdout = saved
                self.assertEqual(rc, 0)
                js = json.loads(out.getvalue())
                self.assertEqual(js["report"]["proposals_created"], 1)
                self.assertEqual(js["policy_catalog_size"], 37)
        finally:
            socket.socket, socket.create_connection = real_socket, real_create

    def test_T23_no_trading_credentials_required(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("KALSHI")}
        for k in ("KALSHI_PRIVATE_KEY", "KALSHI_API_KEY", "KALSHI_SECRET", "KALSHI_KEY_ID"):
            env.pop(k, None)
        c = v30.config_from_env("ask_le_90c", env)
        self.assertTrue(v30.build_proposal(filled_decision(), make_ctx(), c, SPECS).ok)
        for f in V30_FILES:
            src = (ROOT / f).read_text(encoding="utf-8")
            for name in ("KALSHI_PRIVATE_KEY", "KALSHI_API_KEY", "KALSHI_SECRET", "KALSHI_KEY_ID", "PRIVATE_KEY", "api_key", "API_KEY"):
                self.assertNotIn(name, src, f"{f} references {name}")
            self.assertNotIn("os.environ[", src)
            for m in re.findall(r'environ\.get\("([A-Z0-9_]+)"', src) + re.findall(r'env\.get\("([A-Z0-9_]+)"', src):
                self.assertTrue(m.startswith("V30_"), f"{f} reads non-V30 env var {m}")

    def test_T24_no_live_order_endpoint_strings(self):
        needles = ["/portfolio/orders", "/portfolio/", "/orders", "place" + "_order", "submit" + "_order", "create" + "_order",
                   "cancel" + "_order", "amend" + "_order", "SUBMITTED", "LIVE_FILLED", "LIVE_ORDER", "ORDER_SENT",
                   "execute(", "def execute", "KalshiClient", "kalshi_python", "trading_api", "trade-api"]
        for f in V30_FILES:
            src = (ROOT / f).read_text(encoding="utf-8")
            for n in needles:
                self.assertNotIn(n, src, f"{f} contains {n!r}")
        # the state machine has no submit/fill/live states and no external transitions
        for s in v30.STATES + v30.EVENT_TYPES:
            self.assertNotRegex(s, r"SUBMIT|FILL|LIVE|ORDER|SENT")

    def test_T25_no_outbound_trading_http_methods(self):
        needles = ["requests.post", "requests.put", "requests.patch", "requests.delete", "requests.", 'method="POST"',
                   "method='POST'", 'method="PUT"', 'method="PATCH"', 'method="DELETE"', "urlopen", "urllib.request",
                   "http.client", "httpx", "aiohttp", "websocket", "import socket", "Request("]
        for f in V30_FILES:
            src = (ROOT / f).read_text(encoding="utf-8")
            for n in needles:
                self.assertNotIn(n, src, f"{f} contains {n!r}")
            self.assertNotRegex(src, r"^\s*(import|from)\s+(requests|urllib|http|socket|ssl|httpx|aiohttp)\b", )
        # runtime view: module namespace has no network objects
        for name in ("urlopen", "Request", "requests", "socket", "http"):
            self.assertFalse(hasattr(v30, name), name)

    def test_all_event_types_are_proposal_only(self):
        self.assertEqual(set(v30.EVENT_TYPES), {"PROPOSED", "EXPIRED", "SIMULATED_APPROVED", "SIMULATED_REJECTED"})
        self.assertEqual(v30.TRANSITIONS[v30.PROPOSED], {"EXPIRED", "SIMULATED_APPROVED", "SIMULATED_REJECTED"})
        for t in v30.TERMINAL_STATES:
            self.assertEqual(v30.TRANSITIONS[t], set())


# ---------------------------------------------------------------------------
# Production untouched
# ---------------------------------------------------------------------------
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


class TestProductionUntouched(unittest.TestCase):
    def _assert_same_as_main(self, *files):
        for f in files:
            ref, b = _main_bytes(f)
            self.assertIsNotNone(b, f"cannot read {f} from main (need origin/main or main ref)")
            self.assertEqual(hashlib.sha256(b).hexdigest(), hashlib.sha256((ROOT / f).read_bytes()).hexdigest(),
                             f"{f} differs from {ref}")

    def test_T26_v27_files_unchanged(self):
        self._assert_same_as_main("app.py", "vixion_v27.py", "launcher_v27.py")

    def test_T27_v28_files_unchanged(self):
        self._assert_same_as_main("vixion_v28.py", "launcher_v28.py", "scripts/verify_v28_post_deploy.py",
                                  ".github/workflows/v28-regression.yml", ".github/workflows/v27-regression.yml")

    def test_T28_dockerfile_unchanged(self):
        self._assert_same_as_main("Dockerfile")
        self.assertNotIn("v30", (ROOT / "Dockerfile").read_text(encoding="utf-8"))

    def test_T29_existing_regression_still_discoverable(self):
        # The full suite is executed by `unittest discover -s tests`; here we assert nothing v3.0 shadows it.
        names = sorted(p.name for p in (ROOT / "tests").glob("test_*.py"))
        for n in ("test_v27_launcher.py", "test_v27_observability.py", "test_v28_entry_timing.py", "test_v28_verifier.py"):
            self.assertIn(n, names)
        self.assertFalse(hasattr(v28, "v30"))
        self.assertNotIn("vixion_v30", (ROOT / "vixion_v28.py").read_text(encoding="utf-8"))
        self.assertNotIn("vixion_v30", (ROOT / "launcher_v28.py").read_text(encoding="utf-8"))

    def test_T30_path_safe_persistence(self):
        self.assertNotIn(os.sep, v30.EVENT_LOG_NAME)
        self.assertNotIn("/", v30.EVENT_LOG_NAME)
        self.assertNotIn("\\", v30.EVENT_LOG_NAME)
        d = filled_decision()
        with tempfile.TemporaryDirectory(prefix="v30 path-") as td:
            path = Path(td) / "nested dir" / "v30" / v30.EVENT_LOG_NAME  # spaces + missing parents
            store = v30.ProposalStore(path, SPECS)
            rep, _ = v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, store)
            self.assertEqual(rep["proposals_created"], 1)
            raw = path.read_bytes()
            self.assertNotIn(b"\r\n", raw)  # newline="\n" pinned on every platform
            self.assertEqual(v30.ProposalStore(path, SPECS).index, store.index)
            # proposal ids are filename-safe should anyone ever key files by them
            pid = next(iter(store.index))
            self.assertRegex(pid, r"^v30p_[0-9a-f]{32}$")


# ---------------------------------------------------------------------------
# CLI lab
# ---------------------------------------------------------------------------
class TestLab(unittest.TestCase):
    def _run(self, argv, environ=None):
        out, err = io.StringIO(), io.StringIO()
        so, se = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = out, err
        try:
            rc = lab.main(argv, environ=environ if environ is not None else {})
        finally:
            sys.stdout, sys.stderr = so, se
        return rc, out.getvalue(), err.getvalue()

    def test_lab_fail_closed_without_policy_or_unknown_policy(self):
        with tempfile.TemporaryDirectory() as td:
            dec = Path(td) / "d.jsonl"
            dec.write_text(json.dumps(filled_decision()) + "\n")
            rc, _, err = self._run(["--decisions", str(dec), "--output", td])
            self.assertEqual(rc, 2)
            self.assertIn("--policy is required", err)
            rc, _, err = self._run(["--decisions", str(dec), "--policy", "best_policy", "--output", td])
            self.assertEqual(rc, 2)
            self.assertIn("unknown policy_id", err)
            self.assertFalse((Path(td) / v30.EVENT_LOG_NAME).exists())

    def test_lab_full_lifecycle_and_report(self):
        d = filled_decision()
        with tempfile.TemporaryDirectory() as td:
            dec = Path(td) / "d.jsonl"
            tr = Path(td) / "t.jsonl"
            dec.write_text("\n".join(json.dumps(x) for x in [d, dict(d), dict(d, policy="delay_5s")]) + "\n")
            tr.write_text(json.dumps({"event": "open", "setup_id": d["setup_id"], "ctx": make_ctx()}) + "\n")
            out_dir = str(Path(td) / "out")
            rc, out, err = self._run(["--decisions", str(dec), "--traces", str(tr), "--policy", "ask_le_90c",
                                      "--output", out_dir, "--json"], environ={"V30_MAX_PROPOSAL_QTY": "1"})
            self.assertEqual(rc, 0, err)
            js = json.loads(out)
            r = js["report"]
            self.assertEqual((r["decisions_seen"], r["eligible_decisions"], r["proposals_created"], r["duplicate_suppressed"]),
                             (3, 2, 1, 1))
            self.assertEqual(r["by_asset"], {"ETH": 1})
            self.assertEqual(r["by_policy_id"], {"ask_le_90c": 1})
            self.assertEqual(r["by_side"], {"UP": 1})
            pid = next(iter(js["proposals"]))
            # reviews + as-of expiry via the CLI, then replay-only must show the same state
            rev = Path(td) / "r.jsonl"
            rev.write_text(json.dumps({"proposal_id": pid, "verdict": "SIMULATED_APPROVED", "at_utc": iso(DET_MS + 4000)}) + "\n"
                           + json.dumps({"proposal_id": pid, "verdict": "SUBMITTED", "at_utc": iso(DET_MS + 4000)}) + "\n")
            rc, out, err = self._run(["--replay-only", "--reviews", str(rev), "--output", out_dir, "--json"])
            self.assertEqual(rc, 0, err)
            js = json.loads(out)
            self.assertEqual(js["report"]["simulated_approved"], 1)
            self.assertEqual(js["reviews_skipped"][0]["why"], "invalid_verdict")
            rc, out, _ = self._run(["--replay-only", "--as-of", iso(ELIG_END + 1000), "--output", out_dir, "--json"])
            js = json.loads(out)
            self.assertEqual(js["expired_now"], [])  # already terminal: no double transition
            self.assertEqual(js["proposals"][pid]["status"], "SIMULATED_APPROVED")
            self.assertEqual(len((Path(out_dir) / v30.EVENT_LOG_NAME).read_text().splitlines()), 2)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Increment 1.1: decision <-> canonical policy consistency, policy TTL, contexts, causality
# ---------------------------------------------------------------------------
class TestDecisionConsistency(unittest.TestCase):
    def _rej(self, mutate, ctx=None, policy="ask_le_90c", asks=(92.0, 91.0, 89.0)):
        d = filled_decision(policy, asks)
        mutate(d)
        r = v30.build_proposal(d, ctx or make_ctx(), cfg(policy), SPECS)
        self.assertFalse(r.ok)
        return r.reason

    def test_family_mismatch(self):
        self.assertEqual(self._rej(lambda d: d.update(family="delay")), "policy_family_mismatch")

    def test_params_mismatch(self):
        self.assertEqual(self._rej(lambda d: d.update(params={"ask_le": 91.0})), "policy_params_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(params={})), "policy_params_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(params={"ask_le": 90.0, "x": 1})), "policy_params_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(params=None)), "policy_params_mismatch")
        # numeric normalisation: 90 (int) vs 90.0 (float) and tuple-of-pairs form are canonical-equal
        d = filled_decision()
        d["params"] = {"ask_le": 90}
        self.assertTrue(v30.build_proposal(d, make_ctx(), cfg(), SPECS).ok)
        d["params"] = [("ask_le", 90)]
        self.assertTrue(v30.build_proposal(d, make_ctx(), cfg(), SPECS).ok)

    def test_not_fillable_1_contract(self):
        self.assertEqual(self._rej(lambda d: d.update(fillable_1_contract=False)), "not_fillable_1_contract")
        self.assertEqual(self._rej(lambda d: d.update(fillable_1_contract=None)), "not_fillable_1_contract")

    def test_liquidity_unverified(self):
        self.assertEqual(self._rej(lambda d: d.update(liquidity="unverified")), "liquidity_unverified")

    def test_not_economically_valid(self):
        self.assertEqual(self._rej(lambda d: d.update(economically_valid=False)), "not_economically_valid")

    def test_fee_model_invalid(self):
        for bad in (-0.1, None, float("nan"), "1"):
            self.assertEqual(self._rej(lambda d: d.update(fee_model_cents=bad)), "invalid_fee_model", bad)

    def test_fee_effective_below_model(self):
        d = filled_decision()
        self.assertEqual(self._rej(lambda d: d.update(fee_effective_cents=d["fee_model_cents"] - 0.01,
                                                      effective_cost_cents=d["entry_ask_cents"] + d["fee_model_cents"] - 0.01)),
                         "fee_effective_below_model")

    def test_effective_cost_mismatch(self):
        self.assertEqual(self._rej(lambda d: d.update(effective_cost_cents=d["effective_cost_cents"] + 0.5)), "effective_cost_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(effective_cost_cents=None)), "effective_cost_mismatch")

    def test_no_fill_reason_on_filled(self):
        self.assertEqual(self._rej(lambda d: d.update(no_fill_reason="timeout")), "filled_with_no_fill_reason")

    def test_crossed_entry_quote(self):
        self.assertEqual(self._rej(lambda d: d.update(entry_bid_cents=95.0)), "crossed_entry_quote")

    def test_improvement_mismatch(self):
        self.assertEqual(self._rej(lambda d: d.update(improvement_cents=0.0)), "improvement_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(detection_ask_cents=None)), "missing_detection_ask")

    def test_elapsed_and_delay_consistency(self):
        self.assertEqual(self._rej(lambda d: d.update(elapsed_at_decision_s=7.0)), "elapsed_mismatch")
        self.assertEqual(self._rej(lambda d: d.update(delay_s=d["delay_s"] + 1.0)), "delay_mismatch")
        self.assertEqual(self._rej(lambda d: None, ctx=make_ctx(detection_ms=DET_MS + 10_000)), "invalid_detection_ms")

    def test_ctx_cohort_mismatch(self):
        self.assertEqual(self._rej(lambda d: None, ctx=make_ctx(cohort=v28.COHORT_V27)), "context_cohort_mismatch")
        self.assertEqual(self._rej(lambda d: None, ctx=make_ctx(cohort=None)), "context_cohort_mismatch")

    def test_ctx_setup_id_mismatch(self):
        self.assertEqual(self._rej(lambda d: None, ctx=make_ctx(setup_id="ETH|other")), "missing_or_mismatched_context")
        self.assertEqual(self._rej(lambda d: None, ctx=make_ctx(asset="SOL")), "setup_id_asset_mismatch")


class TestPolicyTTL(unittest.TestCase):
    def _hybrid(self, decided_offset_s, policy="max10s_93c"):
        # max10s_93c: matches ask<=90 within 10 s. Ticks at 1 Hz; fill at seq == decided_offset_s.
        asks = [95.0] * decided_offset_s + [89.0]
        return filled_decision(policy, asks)

    def test_max10s_decision_at_7s_expires_exactly_at_10s(self):
        d = self._hybrid(7)
        self.assertEqual(d["decided_seq"], 7)
        p = v30.build_proposal(d, make_ctx(), cfg("max10s_93c", ttl_s=30.0), SPECS).proposal
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, DET_MS + 10_000)
        self.assertEqual(v30.parse_iso_utc(p["frozen_context"]["policy_deadline_utc"]).timestamp() * 1000, DET_MS + 10_000)
        # ttl shorter than the policy deadline wins
        p2 = v30.build_proposal(d, make_ctx(), cfg("max10s_93c", ttl_s=2.0), SPECS).proposal
        self.assertEqual(v30.parse_iso_utc(p2["expires_at_utc"]).timestamp() * 1000, DET_MS + 9_000)

    def test_max10s_decision_after_10s_rejected(self):
        d = self._hybrid(7)
        for off in (10_000, 11_000):  # at or after the policy deadline
            late = dict(d, decided_at=iso(DET_MS + off), entry_tick_at=iso(DET_MS + off),
                        entry_quote_fetched_at=iso(DET_MS + off), elapsed_at_decision_s=off / 1000.0, delay_s=off / 1000.0)
            r = v30.build_proposal(late, make_ctx(), cfg("max10s_93c", ttl_s=30.0), SPECS)
            self.assertEqual(r.reason, "decided_after_policy_deadline", off)
        # the 2.8 engine itself never FILLs there: it times out
        spec = next(s for s in v28.POLICY_SPECS if s["policy"] == "max10s_93c")
        ps = v28.PolicyState(spec)
        ctx = dict(make_ctx(), detection_ask=92.0)
        t = v28.build_tick(ctx, 11, DET_MS + 11_000, DET_MS + 11_000, 89.0, bid=88.0, ask_size=5.0)
        self.assertEqual(ps.on_tick(t, ctx).state, "TIMEOUT")

    def test_hybrid_pullback_deadline(self):
        d = filled_decision("max15s_pullback1c", (92.0, 92.0, 92.0, 91.0))
        p = v30.build_proposal(d, make_ctx(), cfg("max15s_pullback1c", ttl_s=60.0), SPECS).proposal
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, DET_MS + 15_000)

    def test_non_hybrid_uses_ttl_and_eligibility_end(self):
        d = filled_decision()
        p = v30.build_proposal(d, make_ctx(), cfg(ttl_s=30.0), SPECS).proposal
        self.assertIsNone(p["frozen_context"]["policy_deadline_utc"])
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, DET_MS + 2000 + 30_000)
        p = v30.build_proposal(d, make_ctx(), cfg(ttl_s=3600.0), SPECS).proposal
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, ELIG_END)


class TestContexts(unittest.TestCase):
    def _open(self, ctx, **extra):
        return dict({"schema_version": 2, "event": "open", "setup_id": ctx["setup_id"], "opened_at": iso(DET_MS), "ctx": ctx}, **extra)

    def test_duplicate_identical_open_is_idempotent(self):
        ctx = make_ctx()
        ctxs = v30.contexts_from_trace_events([self._open(ctx), self._open(json.loads(json.dumps(ctx)), opened_at="later")])
        self.assertEqual(ctxs, {ctx["setup_id"]: ctx})

    def test_conflicting_open_fails_closed(self):
        ctx = make_ctx()
        with self.assertRaises(v30.ContextConflictError):
            v30.contexts_from_trace_events([self._open(ctx), self._open(make_ctx(cons_pct=95.5))])
        with self.assertRaises(v30.ContextConflictError):
            v30.contexts_from_trace_events([self._open(ctx), self._open(make_ctx(market_ticker="OTHER"))])

    def test_cli_conflict_exits_nonzero_without_creating_log(self):
        d = filled_decision()
        with tempfile.TemporaryDirectory() as td:
            dec = Path(td) / "d.jsonl"
            tr = Path(td) / "t.jsonl"
            dec.write_text(json.dumps(d) + "\n")
            tr.write_text(json.dumps(self._open(make_ctx())) + "\n" + json.dumps(self._open(make_ctx(side="DOWN"))) + "\n")
            out_dir = Path(td) / "out"
            out, err = io.StringIO(), io.StringIO()
            so, se = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = out, err
            try:
                rc = lab.main(["--decisions", str(dec), "--traces", str(tr), "--policy", "ask_le_90c", "--output", str(out_dir)], environ={})
            finally:
                sys.stdout, sys.stderr = so, se
            self.assertEqual(rc, 2)
            self.assertIn("conflicting trace open events", err.getvalue())
            self.assertFalse(out_dir.exists(), "no output directory / event log may be created on conflict")

    def test_embedded_ctx_must_match_trace_ctx(self):
        d = filled_decision()
        ctx = make_ctx()
        with TmpStore() as ts:
            # identical embedded + trace: ok, and identical to a plain build
            rep, res = v30.process_decisions([dict(d, ctx=json.loads(json.dumps(ctx)))], {d["setup_id"]: ctx}, cfg(), SPECS, ts.store)
            self.assertEqual(rep["proposals_created"], 1)
            self.assertEqual(res[0][1].proposal, v30.build_proposal(d, ctx, cfg(), SPECS).proposal)
        with TmpStore() as ts:
            rep, res = v30.process_decisions([dict(d, ctx=make_ctx(cons_pct=94.0))], {d["setup_id"]: ctx}, cfg(), SPECS, ts.store)
            self.assertEqual((rep["proposals_created"], rep["invalid_rejected"]), (0, 1))
            self.assertEqual(res[0][1].reason, "context_conflict")
            self.assertEqual(ts.store.log.read(), [])
        with TmpStore() as ts:  # embedded only (no trace) is still accepted
            rep, _ = v30.process_decisions([dict(d, ctx=ctx)], {}, cfg(), SPECS, ts.store)
            self.assertEqual(rep["proposals_created"], 1)
        # a decision that reaches build_proposal with a stray `ctx` key is a schema violation
        self.assertEqual(v30.build_proposal(dict(d, ctx=ctx), ctx, cfg(), SPECS).reason, "unexpected_decision_fields:ctx")


class TestCausalStateMachine(unittest.TestCase):
    def _state(self):
        d = filled_decision()
        p = v30.build_proposal(d, make_ctx(), cfg(ttl_s=30.0), SPECS).proposal
        st, _ = v30.apply_proposal_event(None, v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], proposal=p))
        return p, st

    def test_terminal_before_creation_rejected(self):
        p, st = self._state()
        before = iso(DET_MS + 2000 - 1)
        for et in (v30.SIMULATED_APPROVED, v30.SIMULATED_REJECTED, v30.EXPIRED):
            with self.assertRaises(v30.ProposalError, msg=et):
                v30.apply_proposal_event(st, v30.make_event(et, p["proposal_id"], before))
        # exactly at creation is allowed for reviews
        st2, out = v30.apply_proposal_event(st, v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], p["created_at_utc"]))
        self.assertEqual(out, "applied")

    def test_expired_before_expires_at_rejected(self):
        p, st = self._state()
        exp_ms = int(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, v30.make_event(v30.EXPIRED, p["proposal_id"], iso(exp_ms - 1)))
        st2, out = v30.apply_proposal_event(st, v30.make_event(v30.EXPIRED, p["proposal_id"], iso(exp_ms)))
        self.assertEqual((out, st2["status"]), ("applied", v30.EXPIRED))

    def test_exact_duplicate_idempotent_and_terminal_immutable(self):
        p, st = self._state()
        ev = v30.make_event(v30.SIMULATED_REJECTED, p["proposal_id"], iso(DET_MS + 5000), "r")
        st2, _ = v30.apply_proposal_event(st, ev)
        st3, out = v30.apply_proposal_event(st2, dict(ev))
        self.assertEqual((out, st3), ("duplicate", st2))
        # same type, different time or reason: not a duplicate -> terminal immutability rejects it
        for other in (v30.make_event(v30.SIMULATED_REJECTED, p["proposal_id"], iso(DET_MS + 6000), "r"),
                      v30.make_event(v30.SIMULATED_REJECTED, p["proposal_id"], iso(DET_MS + 5000), "other"),
                      v30.make_event(v30.EXPIRED, p["proposal_id"], iso(ELIG_END + 1000))):
            with self.assertRaises(v30.ProposalError):
                v30.apply_proposal_event(st2, other)
        # persisted log with a causally impossible event fails replay closed
        bad = [v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], proposal=p),
               v30.make_event(v30.EXPIRED, p["proposal_id"], p["created_at_utc"])]
        with self.assertRaises(v30.ProposalError):
            v30.replay_proposals(bad, SPECS)


class TestAuditorRegressions(unittest.TestCase):
    """Increment 1.1 auditor findings A-L, each reproduced verbatim. Every case must be
    rejected (A-H, I, J, K, L) with the stated reason; A additionally proves invariance."""

    def _base(self):
        d = filled_decision()
        return d, make_ctx(), v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal

    def test_A_future_outcome_fields_alter_proposal(self):
        d, ctx, original = self._base()
        polluted = dict(d, kalshi_result="yes", kalshi_win=True, settlement={"ts": "x"}, underlying_final_close=1.0, pnl=9.9)
        r = v30.build_proposal(polluted, ctx, cfg(), SPECS)
        self.assertFalse(r.ok)
        self.assertTrue(r.reason.startswith("unexpected_decision_fields:"))
        clean = v30.build_proposal({k: polluted[k] for k in d}, ctx, cfg(), SPECS).proposal
        self.assertEqual(clean, original)  # full proposal, frozen_context included
        self.assertEqual(clean["content_hash"], original["content_hash"])
        self.assertEqual(clean["idempotency_key"], original["idempotency_key"])

    def test_B_family_mismatch(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, family="pullback"), ctx, cfg(), SPECS).reason, "policy_family_mismatch")

    def test_C_params_mismatch(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, params={"ask_le": 95.0}), ctx, cfg(), SPECS).reason, "policy_params_mismatch")

    def test_D_fillable_false(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, fillable_1_contract=False), ctx, cfg(), SPECS).reason, "not_fillable_1_contract")

    def test_E_liquidity_unverified(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, liquidity="unverified"), ctx, cfg(), SPECS).reason, "liquidity_unverified")

    def test_F_economically_valid_false(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, economically_valid=False), ctx, cfg(), SPECS).reason, "not_economically_valid")

    def test_G_effective_cost_mismatch(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(dict(d, effective_cost_cents=d["entry_ask_cents"]), ctx, cfg(), SPECS).reason,
                         "effective_cost_mismatch")

    def test_H_ctx_cohort_mismatch(self):
        d, ctx, _ = self._base()
        self.assertEqual(v30.build_proposal(d, dict(ctx, cohort="ledger_backfill"), cfg(), SPECS).reason, "context_cohort_mismatch")

    def test_I_hybrid_max_s_ttl_overrun(self):
        d = filled_decision("max10s_93c", [95.0] * 7 + [89.0])
        p = v30.build_proposal(d, make_ctx(), cfg("max10s_93c", ttl_s=30.0), SPECS).proposal
        self.assertLessEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, DET_MS + 10_000)
        self.assertEqual(v30.parse_iso_utc(p["expires_at_utc"]).timestamp() * 1000, DET_MS + 10_000)
        late = dict(d, decided_at=iso(DET_MS + 12_000), entry_tick_at=iso(DET_MS + 12_000), elapsed_at_decision_s=12.0, delay_s=12.0)
        self.assertEqual(v30.build_proposal(late, make_ctx(), cfg("max10s_93c", ttl_s=30.0), SPECS).reason, "decided_after_policy_deadline")

    def test_J_conflicting_duplicate_trace_open(self):
        ctx = make_ctx()
        opens = [{"event": "open", "setup_id": ctx["setup_id"], "ctx": ctx},
                 {"event": "open", "setup_id": ctx["setup_id"], "ctx": make_ctx(production_max_buy_cents=84.0)}]
        with self.assertRaises(v30.ContextConflictError):
            v30.contexts_from_trace_events(opens)
        self.assertEqual(v30.contexts_from_trace_events(opens[:1] + [dict(opens[0])]), {ctx["setup_id"]: ctx})

    def test_K_self_contained_ctx_conflicts_with_trace_ctx(self):
        d, ctx, _ = self._base()
        with TmpStore() as ts:
            rep, res = v30.process_decisions([dict(d, ctx=make_ctx(side="DOWN"))], {d["setup_id"]: ctx}, cfg(), SPECS, ts.store)
            self.assertEqual(res[0][1].reason, "context_conflict")
            self.assertEqual((rep["proposals_created"], len(ts.store.log.read())), (0, 0))

    def test_L_expired_before_expires_at(self):
        d, ctx, p = self._base()
        with TmpStore() as ts:
            ts.store.propose(p)
            early = v30.parse_iso_utc(p["expires_at_utc"]) - timedelta(milliseconds=1)
            with self.assertRaises(v30.ProposalError):
                ts.store.record(v30.make_event(v30.EXPIRED, p["proposal_id"], early))
            self.assertEqual(ts.store.index[p["proposal_id"]]["status"], v30.PROPOSED)
            self.assertEqual(len(ts.store.log.read()), 1)
            self.assertEqual(ts.store.expire_due(early), [])
            self.assertEqual(ts.store.expire_due(v30.parse_iso_utc(p["expires_at_utc"])), [p["proposal_id"]])


# ---------------------------------------------------------------------------
# Increment 1.2: event envelope integrity, untrusted-input fuzz, frozen quote, malformed opens
# ---------------------------------------------------------------------------
NAN, INF = float("nan"), float("inf")
FUZZ_VALUES = (None, "abc", [], {}, True, NAN, INF, -INF)


def proposed_event():
    d = filled_decision()
    p = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
    return p, v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], "proposed", proposal=p)


class TestEventEnvelopeIntegrity(unittest.TestCase):
    def _reject(self, event, needle, state=None):
        with self.assertRaises(v30.ProposalError) as cm:
            v30.apply_proposal_event(state, event, SPECS)
        self.assertIn(needle, str(cm.exception))
        with self.assertRaises(v30.ProposalError):
            v30.replay_proposals([event] if state is None else [state["_origin"], event], SPECS)

    def _proposed_state(self):
        p, ev = proposed_event()
        st, _ = v30.apply_proposal_event(None, ev, SPECS)
        st["_origin"] = ev
        return p, ev, st

    def test_validate_event_recomputes_event_id(self):
        p, ev = proposed_event()
        self.assertIsNone(v30.validate_event(ev))
        self.assertEqual(ev["event_id"], v30.event_id_for(ev))
        self.assertEqual(v30.EVENT_ID_FIELDS, ("schema_version", "event_type", "proposal_id", "at_utc", "reason", "content_hash"))

    def test_missing_event_id(self):
        p, ev = proposed_event()
        bad = {k: v for k, v in ev.items() if k != "event_id"}
        self.assertEqual(v30.validate_event(bad), "missing_event_id")
        self._reject(bad, "missing_event_id")

    def test_stale_event_id_after_at_utc_change(self):
        p, ev = proposed_event()
        bad = dict(ev, at_utc=iso(DET_MS + 9000))
        self.assertEqual(v30.validate_event(bad), "event_id_mismatch")
        self._reject(bad, "event_id_mismatch")

    def test_stale_event_id_after_reason_change(self):
        p, ev = proposed_event()
        self._reject(dict(ev, reason="tampered"), "event_id_mismatch")
        _, term = self._terminal()
        self._reject(dict(term, reason="tampered"), "event_id_mismatch", state=self._proposed_state()[2])

    def test_stale_event_id_after_content_hash_change(self):
        p, ev = proposed_event()
        self._reject(dict(ev, content_hash="0" * 64), "event_id_mismatch")

    def test_content_hash_consistent_but_payload_tampered(self):
        # attacker recomputes event_id after altering the envelope hash: payload/hash mismatch caught
        p, ev = proposed_event()
        bad = dict(ev, content_hash="0" * 64)
        bad["event_id"] = v30.event_id_for(bad)
        self._reject(bad, "event_content_hash_mismatch")
        # payload altered + both hashes recomputed but proposal.content_hash not: caught by proposal validation
        p2 = dict(p, limit_price_cents=88)
        bad = dict(ev, proposal=p2)  # envelope hashes agree with the stale proposal.content_hash
        self._reject(bad, "content_hash_mismatch")  # caught by proposal content validation
        p3 = dict(p2, content_hash=v30.content_hash(p2))
        bad = dict(ev, proposal=p3, content_hash=p3["content_hash"])
        bad["event_id"] = v30.event_id_for(bad)
        with self.assertRaises(v30.ProposalError) as cm:  # fully re-signed: caught by rebuild-from-frozen_context
            v30.apply_proposal_event(None, bad, SPECS)
        self.assertIn("not_reproducible", str(cm.exception))
        # tampering the frozen context itself (to make a forged limit "reproducible") changes what
        # build_proposal derives, so identity/content can no longer match a canonical build either
        p4 = json.loads(json.dumps(p))
        p4["frozen_context"]["decision"]["entry_ask_cents"] = 88.0
        p4["content_hash"] = v30.content_hash(p4)
        bad = dict(ev, proposal=p4, content_hash=p4["content_hash"])
        bad["event_id"] = v30.event_id_for(bad)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(None, bad, SPECS)

    def test_proposal_id_mismatch_and_status(self):
        p, ev = proposed_event()
        bad = dict(ev, proposal_id="v30p_" + "f" * 32)
        bad["event_id"] = v30.event_id_for(bad)
        self._reject(bad, "proposal_id_mismatch")

    def test_unexpected_envelope_fields_and_bad_recorded_at(self):
        p, ev = proposed_event()
        self._reject(dict(ev, extra=1), "unexpected_event_fields")
        self._reject(dict(ev, recorded_at_utc="garbage"), "invalid_recorded_at_utc")
        ok = dict(ev, recorded_at_utc=iso(DET_MS + 60_000))
        self.assertIsNone(v30.validate_event(ok))  # recorded_at outside identity: same event_id
        self.assertEqual(ok["event_id"], ev["event_id"])

    def _terminal(self):
        p, ev = proposed_event()
        return p, v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), "ok")

    def test_terminal_with_payload_rejected(self):
        p, ev, st = self._proposed_state()
        term = v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), "ok")
        bad = dict(term, proposal=p)
        bad["event_id"] = v30.event_id_for(bad)
        self._reject(bad, "terminal_event_carries_payload", state=st)
        bad = dict(term, content_hash=p["content_hash"])
        bad["event_id"] = v30.event_id_for(bad)
        self._reject(bad, "terminal_event_carries_payload", state=st)

    def test_exact_duplicate_idempotent_and_non_exact_conflicts(self):
        p, ev, st = self._proposed_state()
        st2, out = v30.apply_proposal_event(st, json.loads(json.dumps(ev)), SPECS)
        self.assertEqual(out, "duplicate")
        # same content_hash, different logical event (different reason) -> conflict, never absorbed
        other = v30.make_event(v30.PROPOSED, p["proposal_id"], p["created_at_utc"], "again", proposal=p)
        self.assertNotEqual(other["event_id"], ev["event_id"])
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, other, SPECS)
        term = v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), "ok")
        st3, _ = v30.apply_proposal_event(st, term, SPECS)
        self.assertEqual(v30.apply_proposal_event(st3, dict(term), SPECS)[1], "duplicate")

    def test_manipulated_log_fails_replay_closed(self):
        d = filled_decision()
        with TmpStore() as ts:
            v30.process_decisions([d], {d["setup_id"]: make_ctx()}, cfg(), SPECS, ts.store)
            pid = next(iter(ts.store.index))
            ts.store.record(v30.make_event(v30.SIMULATED_APPROVED, pid, iso(DET_MS + 5000), "ok"))
            lines = ts.path.read_text(encoding="utf-8").splitlines()
            for i, mutate in ((0, lambda e: e.update(schema_version=999)), (1, lambda e: e.update(reason="x")),
                              (1, lambda e: e.update(event_id="a" * 64)), (0, lambda e: e["proposal"].update(quantity=2)),
                              (0, lambda e: e.update(content_hash="b" * 64))):
                evs = [json.loads(x) for x in lines]
                mutate(evs[i])
                tampered = "\n".join(json.dumps(e) for e in evs) + "\n"
                ts.path.write_text(tampered, encoding="utf-8")
                with self.assertRaises(v30.ProposalError):
                    v30.ProposalStore(ts.path, SPECS)
            ts.path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self.assertEqual(v30.ProposalStore(ts.path, SPECS).index[pid]["status"], v30.SIMULATED_APPROVED)


class TestUntrustedInputFuzz(unittest.TestCase):
    """build_proposal must return Built|Rejection for any JSON-compatible garbage: never raise."""
    DECISION_NUMERIC = ("entry_ask_cents", "entry_bid_cents", "ask_size", "fee_model_cents", "fee_effective_cents",
                        "effective_cost_cents", "detection_ask_cents", "improvement_cents", "elapsed_at_decision_s",
                        "delay_s", "decided_seq", "entry_seq", "guardrail_rejections", "ticks_seen", "schema_version")
    DECISION_OTHER = ("decided_at", "entry_tick_at", "entry_quote_fetched_at", "params", "family", "cohort", "state",
                      "fill", "fillable_1_contract", "economically_valid", "liquidity", "setup_id", "policy", "trigger_rule",
                      "no_fill_reason", "tick_resolution")
    CTX_FIELDS = ("cons_pct", "fair_pct", "eligibility_end_ms", "detection_ms", "basis_ok",
                  "market_ticker", "side", "asset", "cohort", "setup_id", "threshold_bp", "distance_bp", "target_gap_bp",
                  "edge_min_points", "fee_buffer_cents", "window_start_ms")

    def _no_raise(self, decision, ctx, policy="ask_le_90c", cfgobj=None):
        try:
            r = v30.build_proposal(decision, ctx, cfgobj or cfg(policy), SPECS)
        except Exception as exc:  # noqa: BLE001
            self.fail(f"build_proposal raised {type(exc).__name__}: {exc}")
        self.assertIn(r.ok, (True, False))
        return r

    def test_decision_fields_fuzz(self):
        base = filled_decision()
        for k in self.DECISION_NUMERIC + self.DECISION_OTHER:
            for v in FUZZ_VALUES:
                r = self._no_raise(dict(base, **{k: v}), make_ctx())
                if (k in ("entry_bid_cents", "no_fill_reason") and v is None) or (v is True and base[k] is True) \
                        or (v == "abc" and k in ("trigger_rule", "tick_resolution")):
                    self.assertTrue(r.ok)  # legitimate values (no bid; boolean flags already True)
                    continue
                self.assertFalse(r.ok, f"{k}={v!r} must not build")

    def test_ctx_fields_fuzz(self):
        base = filled_decision()
        for k in self.CTX_FIELDS:
            for v in FUZZ_VALUES:
                r = self._no_raise(base, make_ctx(**{k: v}))
                if (k == "basis_ok" and v is True) or (k == "market_ticker" and v == "abc"):
                    self.assertTrue(r.ok)  # free-text / boolean fields where the value is legitimate
                    continue
                if k in ("threshold_bp", "distance_bp", "target_gap_bp", "edge_min_points", "fee_buffer_cents",
                         "window_start_ms", "fair_pct"):
                    # informational ctx fields: frozen when finite, rejected when non-finite, never a crash
                    if v in (NAN, INF, -INF) or (k == "fair_pct" and v is not None):
                        self.assertFalse(r.ok, f"{k}={v!r}")
                    continue
                self.assertFalse(r.ok, f"{k}={v!r} must not build")

    def test_production_max_buy_fuzz(self):
        d = filled_decision("production_baseline", (92.0, 83.0))
        for v in FUZZ_VALUES + (0, 100, 150, -1):
            r = self._no_raise(d, make_ctx(production_max_buy_cents=v), "production_baseline")
            self.assertEqual(r.reason, "invalid_production_max_buy", repr(v))
        self.assertTrue(self._no_raise(d, make_ctx(production_max_buy_cents=83), "production_baseline").ok)

    def test_config_fuzz(self):
        d = filled_decision()
        for v in FUZZ_VALUES:
            for field_name in ("quantity", "max_quantity", "ttl_s"):
                c = v30.GatewayConfig(policy_id="ask_le_90c", **{field_name: v})
                self.assertFalse(self._no_raise(d, make_ctx(), cfgobj=c).ok, f"{field_name}={v!r}")
        for v in FUZZ_VALUES + (5, "x"):
            self.assertFalse(self._no_raise(d, make_ctx(), cfgobj=v30.GatewayConfig(policy_id=v)).ok, repr(v))
        for garbage in (None, "abc", [], 1, True):
            self.assertFalse(self._no_raise(garbage, make_ctx()).ok)
            self.assertFalse(self._no_raise(d, garbage).ok)

    def test_non_finite_never_durable(self):
        d = filled_decision()
        # NaN in a field that is otherwise unvalidated numeric content still cannot reach the hash
        for k in ("guardrail_rejections", "ticks_seen"):
            self.assertFalse(v30.build_proposal(dict(d, **{k: NAN}), make_ctx(), cfg(), SPECS).ok)
        r = v30.build_proposal(d, make_ctx(threshold_bp=NAN), cfg(), SPECS)
        self.assertEqual(r.reason, "non_finite_content")
        with self.assertRaises(ValueError):
            v30.canonical_json({"x": NAN})
        p = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
        with self.assertRaises(v30.ProposalError):
            v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), reason="r")  # valid
            raise v30.ProposalError("sentinel")  # (make_event above must succeed)
        with TmpStore() as ts:
            ts.store.propose(p)
            ev = v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), "r")
            ev["reason"] = NAN
            with self.assertRaises(v30.ProposalError):
                ts.store.record(ev)
            self.assertEqual(len(ts.store.log.read()), 1)
            # a persisted line containing NaN (json.loads accepts it) fails replay closed
            ts.path.write_text(ts.path.read_text() + json.dumps(dict(ev, reason=NAN)) + "\n")
            with self.assertRaises(v30.ProposalError):
                v30.ProposalStore(ts.path, SPECS)


class TestFrozenQuoteConsistency(unittest.TestCase):
    def _r(self, **over):
        d = filled_decision()
        d.update(over)
        return v30.build_proposal(d, make_ctx(), cfg(), SPECS)

    def test_ask_size_with_fillable_true(self):
        for v in (None, 0, -1, 0.99, NAN, "5"):
            self.assertEqual(self._r(ask_size=v).reason, "invalid_ask_size", repr(v))
        self.assertTrue(self._r(ask_size=1).ok)

    def test_entry_bid_domain(self):
        for v in (-5, -0.01, 100.01, NAN, "80"):
            self.assertEqual(self._r(entry_bid_cents=v).reason, "invalid_entry_bid", repr(v))
        self.assertEqual(self._r(entry_bid_cents=89.5).reason, "crossed_entry_quote")
        self.assertTrue(self._r(entry_bid_cents=None).ok)
        self.assertTrue(self._r(entry_bid_cents=0.0).ok)

    def test_detection_ask_domain(self):
        for v in (150, 0, 100, -3):
            d = filled_decision()
            d["detection_ask_cents"] = v
            d["improvement_cents"] = round(v - d["entry_ask_cents"], 4)
            self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_detection_ask", v)

    def test_timestamps_must_be_iso_tz_aware(self):
        for v in ("garbage", "2026-09-01T13:38:04", None, 1, ""):
            self.assertEqual(self._r(entry_quote_fetched_at=v).reason, "invalid_entry_quote_fetched_at", repr(v))
        d = filled_decision()
        for v in ("garbage", "2026-09-01T13:38:04", None):
            self.assertEqual(self._r(decided_at=v, entry_tick_at=v).reason, "invalid_decided_at", repr(v))
            self.assertEqual(self._r(entry_tick_at=v).reason, "invalid_entry_tick_at", repr(v))
        self.assertEqual(self._r(entry_quote_fetched_at=iso(DET_MS + 9000)).reason, "quote_fetched_after_decision")
        # quote fetched earlier than the tick is the 2.8 detection-tick case: allowed
        self.assertTrue(self._r(entry_quote_fetched_at=iso(DET_MS)).ok)


class TestMalformedTraceOpen(unittest.TestCase):
    def _open(self, base, **over):
        rec = {"schema_version": 2, "event": "open", "setup_id": base["setup_id"], "opened_at": iso(DET_MS), "ctx": base}
        rec.update(over)
        return rec

    def test_valid_plus_malformed_same_setup(self):
        ctx = make_ctx()
        for bad in (self._open(ctx, ctx=None), self._open(ctx, ctx="x"), self._open(ctx, ctx={}),
                    self._open(ctx, ctx=dict(ctx, setup_id="other")), self._open(ctx, ctx=dict(ctx, cons_pct=NAN))):
            with self.assertRaises(v30.ContextInputError):
                v30.contexts_from_trace_events([self._open(ctx), bad])

    def test_malformed_only(self):
        ctx = make_ctx()
        for bad in (self._open(ctx, setup_id=None), self._open(ctx, setup_id=""), self._open(ctx, setup_id=5),
                    {"event": "open"}, "not-a-dict", 7):
            with self.assertRaises(v30.ContextInputError):
                v30.contexts_from_trace_events([bad])

    def test_valid_duplicate_still_idempotent_and_close_ignored(self):
        ctx = make_ctx()
        recs = [self._open(ctx), {"event": "close", "setup_id": ctx["setup_id"]}, self._open(json.loads(json.dumps(ctx)))]
        self.assertEqual(v30.contexts_from_trace_events(recs), {ctx["setup_id"]: ctx})

    def test_cli_malformed_open_exits_nonzero(self):
        d = filled_decision()
        with tempfile.TemporaryDirectory() as td:
            dec, tr = Path(td) / "d.jsonl", Path(td) / "t.jsonl"
            dec.write_text(json.dumps(d) + "\n")
            tr.write_text(json.dumps(self._open(make_ctx(), ctx=None)) + "\n")
            out, err = io.StringIO(), io.StringIO()
            so, se = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = out, err
            try:
                rc = lab.main(["--decisions", str(dec), "--traces", str(tr), "--policy", "ask_le_90c", "--output", str(Path(td) / "o")], environ={})
            finally:
                sys.stdout, sys.stderr = so, se
            self.assertEqual(rc, 2)
            self.assertIn("invalid ctx", err.getvalue())
            self.assertFalse((Path(td) / "o").exists())


class TestAuditorRegressions2(unittest.TestCase):
    """Increment 1.2 auditor findings M-X, reproduced verbatim."""

    def test_M_proposed_schema_version_manipulated(self):
        p, ev = proposed_event()
        bad = dict(ev, schema_version=999)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(None, bad, SPECS)
        bad["event_id"] = v30.event_id_for(bad)  # even re-signed
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(None, bad, SPECS)

    def test_N_proposed_event_content_hash_manipulated(self):
        p, ev = proposed_event()
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(None, dict(ev, content_hash="c" * 64), SPECS)

    def test_O_reason_manipulated_with_stale_event_id(self):
        p, ev = proposed_event()
        with self.assertRaises(v30.ProposalError) as cm:
            v30.apply_proposal_event(None, dict(ev, reason="manipulated"), SPECS)
        self.assertIn("event_id_mismatch", str(cm.exception))

    def test_P_terminal_arbitrary_event_id(self):
        p, ev = proposed_event()
        st, _ = v30.apply_proposal_event(None, ev, SPECS)
        term = v30.make_event(v30.SIMULATED_APPROVED, p["proposal_id"], iso(DET_MS + 5000), "ok")
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, dict(term, event_id="d" * 64), SPECS)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, dict(term, event_id=""), SPECS)

    def test_Q_terminal_schema_version_manipulated(self):
        p, ev = proposed_event()
        st, _ = v30.apply_proposal_event(None, ev, SPECS)
        term = v30.make_event(v30.EXPIRED, p["proposal_id"], p["expires_at_utc"], "ttl")
        bad = dict(term, schema_version=2)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, bad, SPECS)
        bad["event_id"] = v30.event_id_for(bad)
        with self.assertRaises(v30.ProposalError):
            v30.apply_proposal_event(st, bad, SPECS)

    def test_R_production_max_buy_garbage_is_rejection_not_exception(self):
        d = filled_decision("production_baseline", (92.0, 83.0))
        for v in ("abc", [], {}):
            r = v30.build_proposal(d, make_ctx(production_max_buy_cents=v), cfg("production_baseline"), SPECS)
            self.assertFalse(r.ok)
            self.assertEqual(r.reason, "invalid_production_max_buy")

    def test_S_fillable_true_with_bad_ask_size(self):
        for v in (None, 0, -1):
            d = filled_decision()
            d["ask_size"] = v
            self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_ask_size", v)

    def test_T_negative_or_invalid_bid(self):
        d = filled_decision()
        d["entry_bid_cents"] = -5
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_entry_bid")

    def test_U_detection_ask_out_of_domain(self):
        d = filled_decision()
        d["detection_ask_cents"] = 150
        d["improvement_cents"] = round(150 - d["entry_ask_cents"], 4)
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_detection_ask")

    def test_V_invalid_entry_quote_fetched_at(self):
        d = filled_decision()
        d["entry_quote_fetched_at"] = "garbage"
        self.assertEqual(v30.build_proposal(d, make_ctx(), cfg(), SPECS).reason, "invalid_entry_quote_fetched_at")

    def test_W_nan_infinity_cannot_enter_durable_json(self):
        d = filled_decision()
        for v in (NAN, INF):
            self.assertEqual(v30.build_proposal(d, make_ctx(distance_bp=v), cfg(), SPECS).reason, "non_finite_content")
            self.assertFalse(v30.build_proposal(dict(d, ticks_seen=v), make_ctx(), cfg(), SPECS).ok)
        p = v30.build_proposal(d, make_ctx(), cfg(), SPECS).proposal
        with TmpStore() as ts:
            ts.store.propose(p)
            ev = v30.make_event(v30.SIMULATED_REJECTED, p["proposal_id"], iso(DET_MS + 5000), "r")
            ev["reason"] = INF
            with self.assertRaises(v30.ProposalError):
                ts.store.log.append(ev)
            self.assertEqual(len(ts.store.log.read()), 1)
            self.assertNotIn("NaN", ts.path.read_text())
            self.assertNotIn("Infinity", ts.path.read_text())

    def test_X_malformed_trace_open_fails_closed(self):
        ctx = make_ctx()
        with self.assertRaises(v30.ContextInputError):
            v30.contexts_from_trace_events([{"event": "open", "setup_id": ctx["setup_id"], "ctx": None}])
        with self.assertRaises(v30.ContextInputError):
            v30.contexts_from_trace_events([{"event": "open", "setup_id": ctx["setup_id"], "ctx": ctx},
                                            {"event": "open", "setup_id": ctx["setup_id"], "ctx": "junk"}])


class TestGoldenFixture(unittest.TestCase):
    """T31: the committed synthetic fixture must always produce the same proposals (golden replay)."""
    FIX = ROOT / "tests" / "fixtures" / "v30"
    GOLDEN = {"v30p_858e2e1e2e9990e856123930528adb60": ("ETH", "UP", 89.0, 89),
              "v30p_31e94430d8045ca47d799f25bf7553d7": ("SOL", "DOWN", 90.0, 90)}

    def test_T31_golden_fixture_replay(self):
        decisions = lab.read_jsonl(self.FIX / "sample_decisions_v28.jsonl")
        contexts = lab.load_contexts(lab.read_jsonl(self.FIX / "sample_traces_v28.jsonl"))
        with TmpStore() as ts:
            rep, _ = v30.process_decisions(decisions, contexts, cfg(), SPECS, ts.store)
            self.assertEqual((rep["decisions_seen"], rep["eligible_decisions"], rep["proposals_created"]), (6, 2, 2))
            got = {pid: (st["proposal"]["asset"], st["proposal"]["side"], st["proposal"]["observed_ask_cents"],
                         st["proposal"]["limit_price_cents"]) for pid, st in ts.store.index.items()}
            self.assertEqual(got, self.GOLDEN)
            rep2, _ = v30.process_decisions(decisions, contexts, cfg(), SPECS, v30.ProposalStore(ts.path, SPECS))
            self.assertEqual(rep2["duplicate_suppressed"], 2)
