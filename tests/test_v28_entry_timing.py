import http.client
import inspect
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone

import app as core

# Capture the production 2.7 chain BEFORE the 2.8 overlay is imported.
import launcher_v27  # noqa: E402,F401  (as deployed: v27 hooks + lifecycle wrapper)
import vixion_v27 as v27  # noqa: E402

BASE_MONITOR_COMPUTE = core.Monitor.compute
BASE_ALT_COMPUTE = core.AltShadowMonitor.compute
BASE_OBSERVE_STATE = core.ShadowLedger.observe_state
BASE_JOURNAL = core.AltShadowMonitor.journal_if_confirmed
BASE_ALT_ALERT = core.AltShadowMonitor.maybe_alert
BASE_RESOLVE = core.ShadowLedger.maybe_resolve
BASE_STARTUP = core.AltShadowMonitor.send_startup
BASE_BTC_ALERT = core.Monitor.maybe_alert
BASE_MODELS = {k: dict(v) for k, v in core.ALT_LIVE_MODELS.items()}
BASE_DEFAULTS = dict(core.DEFAULTS_DATA)
BASE_RECORD_SETUP = core.ShadowLedger.record_setup

import launcher_v28  # noqa: E402,F401
import vixion_v28 as v28  # noqa: E402

WS_MS = 1_788_269_400_000  # 2026-09-01T13:30:00Z (a 15m boundary)
MINUTE = 8
DET_MS = WS_MS + MINUTE * 60000 + 2000  # detected 2 s after M8 closed
ELIG_END = WS_MS + (MINUTE + 1) * 60000


def iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


class FakePrimary:
    def __init__(self, fee=1.0, edge_min=10.0):
        self.config = {"edge_min_points": edge_min, "fee_buffer_cents": fee, "bankroll_dollars": 1000.0, "risk_cap_pct": 1.0}


class FakeLedger:
    def __init__(self):
        self.lock = threading.Lock()
        self.data = {"setups": {}}

    def get_setup(self, asset, window):
        rec = self.data["setups"].get(f"{asset}|{window}")
        return dict(rec) if rec else None


class FakeAlt:
    def __init__(self, fee=1.0, edge_min=10.0):
        self.primary = FakePrimary(fee, edge_min)
        self.ledger = FakeLedger()
        self.lock = threading.Lock()
        self.states = {}
        self.kalshi_last = {}


class Clock:
    def __init__(self, ms):
        self.ms = ms

    def __call__(self):
        return self.ms / 1000.0


def make_ctx(cons=95.0, fee=1.0, edge_min=10.0, cohort=v28.COHORT_FORWARD, sid="ETH|" + iso(WS_MS)):
    return {"cohort": cohort, "setup_id": sid, "asset": "ETH", "side": "UP", "window_start_utc": iso(WS_MS),
            "window_start_ms": WS_MS, "setup_minute": MINUTE, "detection_at": iso(DET_MS), "detection_ms": DET_MS,
            "eligibility_end_ms": ELIG_END, "market_ticker": "KXETH15M-TEST", "cons_pct": cons, "threshold_bp": 30.0,
            "distance_bp": 41.0, "target_gap_bp": 2.5, "basis_ok": True, "edge_min_points": edge_min,
            "fee_buffer_cents": fee, "production_max_buy_cents": 84.0, "quote_max_age_s": 5.0, "tick_resolution": "1s"}


def tick(ctx, seq, elapsed_s, ask, bid=None, **kw):
    q = DET_MS + int(elapsed_s * 1000)
    return v28.build_tick(ctx, seq, q, q, ask, bid, **kw)


def run_ticks(rt, spec):
    """spec: list of (elapsed_s, ask). Returns all decisions produced in order."""
    out = []
    for i, (t, ask) in enumerate(spec):
        out.extend(rt.on_tick(tick(rt.ctx, i, t, ask, (ask - 2.0) if ask else None)))
    return out


def dec(rt, policy):
    return next(ps.decision for ps in rt.policies if ps.spec["policy"] == policy)


def state(rt, policy):
    return next(ps for ps in rt.policies if ps.spec["policy"] == policy)


def sample_state(asset="ETH", ask=96.0, bid=94.0, status="SHADOW PRICE HIGH", basis_ok=True, ticker="KXETH15M-TEST"):
    cfg = {"fee_buffer_cents": 1.0}
    fee = core.effective_fee_cents(ask, cfg, 1.0) if ask is not None else None
    return {"updated_at": iso(DET_MS), "asset": asset, "status": status, "window_start_utc": iso(WS_MS),
            "current_minute": MINUTE + 1, "last_completed_minute": MINUTE, "side": "UP", "model_open": 4000.0,
            "live_price": 4016.4, "distance_bp": 41.0, "threshold_bp": 30.0, "fair_pct": 96.7,
            "conservative_fair_pct": 95.0, "sample_n": 1719, "holdout_n": 180, "holdout_pct": 95.0,
            "market_ticker": ticker, "kalshi_target": 4001.0, "target_gap_bp": 2.5, "selected_contract": "UP",
            "selected_ask_cents": ask, "selected_bid_cents": bid, "spread_cents": (ask - bid) if ask is not None and bid is not None else None,
            "estimated_taker_fee_cents": core.kalshi_trade_fee_cents(ask) if ask is not None else None,
            "effective_fee_cents": fee, "max_buy_cents": 84.0,
            "edge_points": (95.0 - ask - fee) if ask is not None else None, "basis_ok": basis_ok, "api_error": ""}


def orderbook_payload(ask, ask_qty=25, bid=None):
    """UP side: YES ask = 100 - best NO bid; YES bid from the yes book."""
    yes = [[f"{bid/100:.4f}", "10"]] if bid is not None else []
    no = [[f"{(100-ask)/100:.4f}", str(ask_qty)]] if ask is not None else []
    return {"orderbook_fp": {"yes_dollars": yes, "no_dollars": no}}


class RegressionPins(unittest.TestCase):
    def test_decision_functions_are_exact_2_6_objects_after_2_8(self):
        self.assertIs(core.Monitor.compute, BASE_MONITOR_COMPUTE)
        self.assertIs(core.AltShadowMonitor.compute, BASE_ALT_COMPUTE)
        self.assertIs(core.ShadowLedger.observe_state, BASE_OBSERVE_STATE)

    def test_2_7_chain_untouched_by_2_8(self):
        self.assertIs(core.AltShadowMonitor.journal_if_confirmed, BASE_JOURNAL)
        self.assertIs(core.AltShadowMonitor.maybe_alert, BASE_ALT_ALERT)
        self.assertIs(core.ShadowLedger.maybe_resolve, BASE_RESOLVE)
        self.assertIs(core.AltShadowMonitor.send_startup, BASE_STARTUP)
        self.assertIs(core.Monitor.maybe_alert, BASE_BTC_ALERT)

    def test_2_8_wraps_only_four_seams(self):
        wrapped = [name for name, fn in (("record_setup", core.ShadowLedger.record_setup), ("do_GET", core.Handler.do_GET),
                                         ("_send", core.Handler._send), ("__init__", core.AltShadowMonitor.__init__))
                   if fn.__module__ == "vixion_v28"]
        self.assertEqual(wrapped, ["record_setup", "do_GET", "_send", "__init__"])
        src = inspect.getsource(core.ShadowLedger.record_setup)
        self.assertIn("_original_record_setup", src)
        self.assertIn("try:", src)
        self.assertIn("return result", src)

    def test_strategy_constants_unchanged(self):
        self.assertEqual(core.ALT_LIVE_MODELS, BASE_MODELS)
        self.assertEqual(core.DEFAULTS_DATA, BASE_DEFAULTS)
        self.assertEqual(core.DEFAULTS_DATA["edge_min_points"], 10.0)
        self.assertEqual(core.DEFAULTS_DATA["fee_buffer_cents"], 1.0)
        self.assertEqual(core.DEFAULTS_DATA["max_basis_gap_bp"], 8.0)

    def test_version_bump_is_runtime_only(self):
        self.assertEqual(core.APP_VERSION, v27.APP_VERSION)
        try:
            self.assertEqual(v28.apply_version(), "2.8-executable-entry-timing-lab")
            self.assertEqual(core.APP_VERSION, "2.8-executable-entry-timing-lab")
        finally:
            core.APP_VERSION = v27.APP_VERSION

    def test_no_order_placement_or_telegram_surface_in_2_8(self):
        src = inspect.getsource(v28).lower()
        for forbidden in ("portfolio/orders", "create_order", "place_order", "submit_order", "post_telegram"):
            self.assertNotIn(forbidden, src)

    def test_policy_catalogue_is_the_37_from_the_spec(self):
        self.assertEqual(len(v28.POLICY_NAMES), 37)
        for name in ("entry_at_detection", "delay_60s", "ask_le_85c", "pullback_0.5c", "pullback_1.5c",
                     "max30s_93c", "max15s_pullback1c", "max30s_pullback2c", "production_baseline"):
            self.assertIn(name, v28.POLICY_NAMES)


class OnlineEngine(unittest.TestCase):
    def test_T1_first_touch_threshold_takes_first_not_min(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 96.0), (5, 93.0), (10, 92.0), (15, 91.0)])
        d = dec(rt, "ask_le_93c")
        self.assertEqual((d.state, d.entry_seq, d.entry_ask_cents, d.delay_s), (v28.FILLED, 1, 93.0, 5.0))
        self.assertEqual(d.improvement_cents, 3.0)

    def test_T2_delay_takes_first_quote_at_or_after_T_not_best_later(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 96.0), (5, 95.0), (10, 97.0), (15, 90.0)])
        d = dec(rt, "delay_10s")
        self.assertEqual((d.state, d.entry_seq, d.entry_ask_cents), (v28.FILLED, 2, 97.0))
        self.assertEqual(d.improvement_cents, -1.0)

    def test_T3_hybrid_timeout_spec_example_32s_gt_30s(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 95.5), (32, 91.2)])
        d = dec(rt, "max30s_93c")
        self.assertEqual((d.state, d.fill, d.no_fill_reason), (v28.TIMEOUT, False, "timeout"))
        self.assertIsNone(d.entry_ask_cents)  # the 91.2 quote never appears in the decision

    def test_T4_eligibility_expires_before_target_then_cheaper_ticks_ignored(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 96.0), (20, 95.0)])
        rt.close("eligibility_ended", ELIG_END)
        d = dec(rt, "ask_le_90c")
        self.assertEqual((d.state, d.no_fill_reason), (v28.EXPIRED, "eligibility_ended"))
        before = v28._decision_bytes(d)
        self.assertEqual(rt.on_tick(tick(rt.ctx, 99, 70, 80.0)), [])
        self.assertEqual(v28._decision_bytes(dec(rt, "ask_le_90c")), before)

    def test_T5_cost_ge_100_detection_and_delay_are_terminal_rejections(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 99.0), (6, 99.0), (12, 90.0)])
        self.assertEqual(dec(rt, "entry_at_detection").state, v28.REJECTED)
        self.assertEqual(dec(rt, "entry_at_detection").no_fill_reason, "rejected_effective_cost_ge_100")
        self.assertEqual(dec(rt, "delay_5s").state, v28.REJECTED)
        self.assertIsNone(dec(rt, "delay_5s").entry_ask_cents)
        self.assertEqual(dec(rt, "ask_le_90c").entry_ask_cents, 90.0)  # continuing families unaffected

    def test_T6_T7_resolution_pnl_and_roi(self):
        lab = v28.EntryTimingLab(FakePrimary(), FakeAlt(), data_dir=tempfile.mkdtemp(), autostart=False)
        res = {"state": "FILLED", "fill": True, "ask": 92.0, "fee": 1.0, "cost": 93.0, "delay": 4.0, "impr": 4.0, "guard": 0}
        win = lab._pnl(res, {"kalshi_win": True})
        loss = lab._pnl(res, {"kalshi_win": False})
        self.assertEqual((win["gross"], win["net"], win["cost"]), (8.0, 7.0, 93.0))
        self.assertEqual((loss["gross"], loss["net"]), (-92.0, -93.0))
        rows = [{"results": {"p": res}, "outcome": {"kalshi_win": True}}, {"results": {"p": res}, "outcome": {"kalshi_win": False}}]
        m = lab.policy_metrics(rows, "p")
        self.assertEqual((m["wins"], m["losses"], m["net_pnl_dollars"]), (1, 1, -0.86))
        self.assertAlmostEqual(m["roi_pct"], 100.0 * (-86.0) / 186.0, places=4)
        self.assertEqual(m["net_pnl_per_eligible_setup_cents"], -43.0)

    def test_T8_no_fill_counts_in_eligible_not_in_fills(self):
        lab = v28.EntryTimingLab(FakePrimary(), FakeAlt(), data_dir=tempfile.mkdtemp(), autostart=False)
        nofill = {"state": "EXPIRED", "fill": False, "reason": "eligibility_ended", "ask": None, "fee": None, "cost": None, "delay": None, "impr": None, "guard": 0}
        rows = [{"results": {"p": nofill}, "outcome": {"kalshi_win": True}}]
        m = lab.policy_metrics(rows, "p")
        self.assertEqual((m["eligible_setups"], m["fills"], m["no_fills"], m["fill_rate_pct"]), (1, 0, 1, 0.0))
        self.assertEqual(m["net_pnl_dollars"], 0.0)
        self.assertEqual(m["no_fill_reasons"], {"eligibility_ended": 1})
        self.assertIsNone(m["hit_pct"])

    def test_T9_duplicate_event_same_seq_ignored(self):
        a = v28.TraceRuntime(make_ctx())
        b = v28.TraceRuntime(make_ctx())
        run_ticks(a, [(0, 96.0), (5, 93.0), (10, 91.0)])
        b.on_tick(tick(b.ctx, 0, 0, 96.0, 94.0))
        b.on_tick(tick(b.ctx, 0, 0, 96.0, 94.0))  # same event replayed
        b.on_tick(tick(b.ctx, 1, 5, 93.0, 91.0))
        b.on_tick(tick(b.ctx, 1, 5, 93.0, 91.0))
        b.on_tick(tick(b.ctx, 2, 10, 91.0, 89.0))
        self.assertEqual(b.duplicates, 2)
        self.assertEqual([v28._decision_bytes(d) for d in a.decisions()], [v28._decision_bytes(d) for d in b.decisions()])

    def test_T10_duplicate_setup_start_creates_one_trace(self):
        alt = FakeAlt()
        lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), clock=Clock(DET_MS + 500), autostart=False)
        self.assertIsNotNone(lab.start_trace(sample_state()))
        self.assertIsNone(lab.start_trace(sample_state()))
        self.assertEqual(len(lab.runtimes), 1)
        self.assertEqual(lab.index["counters"]["skipped_duplicate"], 1)

    def test_T11_T19_restart_replays_decisions_and_durable_ticks(self):
        spec = [(5, 95.0), (10, 93.0), (20, 92.0), (31, 89.0), (45, 88.0)]
        data_dir = tempfile.mkdtemp()
        alt = FakeAlt()
        alt.kalshi_last["ETH"] = (DET_MS - 1000) / 1000.0
        lab = v28.EntryTimingLab(alt.primary, alt, data_dir=data_dir, clock=Clock(DET_MS + 500), autostart=False)
        sid = lab.start_trace(sample_state(ask=96.0, bid=94.0))
        seq0 = [json.loads(x) for x in open(lab.ticks_log.path)][0]
        single = v28.TraceRuntime(make_ctx())
        single.on_tick(seq0)
        for i, (el, ask) in enumerate(spec, start=1):
            tk = tick(make_ctx(), i, el, ask, ask - 2.0)
            single.on_tick(tk)
            if i <= 2:
                lab._ingest(sid, tk)
        single.close("eligibility_ended", ELIG_END)
        # crash: no index save after the last ingests (index on disk is stale: seq_last 0)
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=data_dir, clock=Clock(DET_MS + 11_000), autostart=False)
        self.assertIn(sid, lab2.runtimes)
        rt2 = lab2.runtimes[sid]
        self.assertEqual(rt2.last_seq, 2)  # max durable seq, not the stale index value
        self.assertEqual(lab2.index["traces"][sid]["restarts"], 1)
        self.assertEqual(rt2.ctx["detection_ask"], 96.0)
        self.assertEqual(state(rt2, "ask_le_93c").state, v28.FILLED)  # terminal decision is authoritative
        self.assertEqual(state(rt2, "ask_le_90c").state, v28.WAITING)
        self.assertEqual(state(rt2, "ask_le_90c").ticks_seen, 3)  # replayed ticks restore ticks_seen
        for i, (el, ask) in enumerate(spec[2:], start=3):
            lab2._ingest(sid, tick(make_ctx(), i, el, ask, ask - 2.0))
        lab2._close_trace(sid, rt2, lab2.index["traces"][sid], "eligibility_ended", ELIG_END)
        got = {d.policy: v28._decision_bytes(d) for d in rt2.decisions()}
        want = {d.policy: v28._decision_bytes(d) for d in single.decisions()}
        self.assertEqual(got, want)
        self.assertEqual(len(got), 37)
        self.assertEqual(lab2.index["traces"][sid]["status"], "closed")
        closes = [json.loads(x) for x in open(lab2.traces_log.path) if '"close"' in x]
        self.assertEqual(closes[-1]["setup_id"], sid)

    def test_T12_cohorts_never_mix(self):
        alt = FakeAlt()
        lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), clock=Clock(DET_MS + 500), autostart=False)
        # an old resolved ledger setup (pre first_boot) -> backfill cohort only
        old_sid = "SOL|2026-08-30T10:00:00+00:00"
        alt.ledger.data["setups"][old_sid] = {"setup_id": old_sid, "asset": "SOL", "window_start_utc": "2026-08-30T10:00:00+00:00",
                                              "recorded_at": "2026-08-30T10:08:02+00:00", "minute": 8, "side": "UP", "basis_ok": True,
                                              "ask_cents": 96.0, "edge_points": 95.0 - 96.0 - 1.0, "resolved": True, "kalshi_win": True,
                                              "kalshi_result": "yes", "entry_simulated": False, "distance_bp": 35.0, "threshold_bp": 30.0,
                                              "target_gap_bp": 1.0, "setup_spread_cents": 2.0}
        fwd = lab.report("forward")
        allc = lab.report("all")
        self.assertEqual(fwd["cohorts_shown"], [v28.COHORT_FORWARD])
        self.assertEqual(fwd["setups_observed"], 0)
        self.assertEqual(allc["cohorts"][v28.COHORT_BACKFILL]["setups_observed"], 1)
        self.assertEqual(allc["cohorts"][v28.COHORT_FORWARD]["setups_observed"], 0)
        bf = allc["cohorts"][v28.COHORT_BACKFILL]
        by = {p["policy"]: p for p in bf["policies"]}
        self.assertEqual(by["entry_at_detection"]["fills"], 1)
        self.assertEqual(by["delay_10s"]["eligible_setups"], 0)  # NOT_SIMULABLE is not "no_fill"
        self.assertEqual(bf["simulable_policies"], ["entry_at_detection", "production_baseline"])
        self.assertIn("never summed", allc["aggregation"])

    def test_T13_push_model_no_lookahead(self):
        rt = v28.TraceRuntime(make_ctx())
        # A policy only ever sees ticks pushed so far: ticks_seen equals the fill index + 1 and freezes.
        run_ticks(rt, [(0, 96.0), (5, 93.0), (10, 92.0), (15, 80.0)])
        ps = state(rt, "ask_le_93c")
        self.assertEqual((ps.decision.entry_seq, ps.ticks_seen), (1, 2))
        calls = {"n": 0}
        orig = v28.PolicyState.on_tick

        def counting(self_, t, ctx):
            if self_.spec["policy"] == "ask_le_93c":
                calls["n"] += 1
            return orig(self_, t, ctx)
        v28.PolicyState.on_tick = counting
        try:
            rt.on_tick(tick(rt.ctx, 4, 20, 50.0))
        finally:
            v28.PolicyState.on_tick = orig
        self.assertEqual(calls["n"], 0)

    def test_T14_outcome_invisible_to_engine(self):
        for f in v28.Decision.__dataclass_fields__:
            self.assertNotIn("win", f)
            self.assertNotIn("pnl", f)
            self.assertNotIn("result", f)
        t = tick(make_ctx(), 0, 0, 96.0)
        self.assertFalse(any(k for k in t if "win" in k or "pnl" in k or "result" in k))
        a = v28.TraceRuntime(make_ctx())
        b = v28.TraceRuntime(make_ctx())
        run_ticks(a, [(0, 96.0), (5, 93.0)])
        run_ticks(b, [(0, 96.0), (5, 93.0)])
        self.assertEqual([v28._decision_bytes(d) for d in a.decisions()], [v28._decision_bytes(d) for d in b.decisions()])

    def test_T15_scenario_b_min_ask_never_selected_by_earlier_fills(self):
        spec = [(0, 96.0), (4, 93.0), (8, 90.0), (12, 95.0), (16, 92.0), (20, 88.0), (24, 91.0)]
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, spec)
        rt.close("eligibility_ended", ELIG_END)
        min_ask = min(a for _, a in spec)
        min_seq = [a for _, a in spec].index(min_ask)
        for d in rt.decisions():
            if d.fill and d.entry_seq < min_seq:
                self.assertGreater(d.entry_ask_cents, min_ask)
            if d.fill:
                self.assertEqual(d.decided_seq, d.entry_seq)
        lab = v28.EntryTimingLab(FakePrimary(), FakeAlt(), data_dir=tempfile.mkdtemp(), autostart=False)
        text = json.dumps(lab.report("all")).lower()
        self.assertNotIn("scenario_b", text)
        self.assertNotIn("min_ask", text)
        self.assertFalse(lab.report("forward")["hindsight_metrics_included"])

    def test_T16_filled_decision_is_byte_for_byte_frozen(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 96.0), (5, 93.0)])
        ps = state(rt, "ask_le_93c")
        before = v28._decision_bytes(ps.decision)
        seen = ps.ticks_seen
        for i, ask in enumerate([90.0, 85.0, 70.0, 50.0, 10.0, 1.0], start=2):
            rt.on_tick(tick(rt.ctx, i, 6 + i, ask, ask - 0.5))
        self.assertEqual(v28._decision_bytes(ps.decision), before)
        self.assertEqual(ps.ticks_seen, seen)
        with self.assertRaises(Exception):
            ps.decision.entry_ask_cents = 1.0  # frozen dataclass

    def test_T17_D4_guardrail_continue_for_threshold_pullback_hybrid(self):
        # fee floor 5c: ask 95 -> effective_cost 100 -> guardrail, policy keeps waiting, fills at 94.
        rt = v28.TraceRuntime(make_ctx(fee=5.0))
        run_ticks(rt, [(0, 97.0), (5, 95.0), (10, 94.0)])
        d = dec(rt, "ask_le_95c")
        self.assertEqual((d.state, d.entry_ask_cents, d.guardrail_rejections, d.effective_cost_cents), (v28.FILLED, 94.0, 1, 99.0))
        h = dec(rt, "max20s_95c")
        self.assertEqual((h.state, h.entry_ask_cents, h.guardrail_rejections), (v28.FILLED, 94.0, 1))
        p = dec(rt, "pullback_2c")  # anchor 97 -> target 95: 95 rejected by guardrail, 94 fills
        self.assertEqual((p.state, p.entry_ask_cents, p.guardrail_rejections), (v28.FILLED, 94.0, 1))
        rt2 = v28.TraceRuntime(make_ctx(fee=5.0))
        run_ticks(rt2, [(0, 95.0), (6, 95.0), (12, 90.0)])
        self.assertEqual(dec(rt2, "delay_5s").state, v28.REJECTED)  # single-shot family stays terminal

    def test_T18_D10_same_price_later_timestamp_is_a_new_tick(self):
        rt = v28.TraceRuntime(make_ctx())
        run_ticks(rt, [(0, 96.0), (3, 96.0), (6, 96.0)])
        d = dec(rt, "delay_5s")
        self.assertEqual((d.state, d.entry_seq, d.entry_ask_cents, d.delay_s), (v28.FILLED, 2, 96.0, 6.0))

    def test_T20_production_baseline_matches_observe_state_rule(self):
        cfg = {"fee_buffer_cents": 1.0}
        for ask, expect in ((84.0, True), (85.0, False), (83.0, True)):
            rt = v28.TraceRuntime(make_ctx(cons=95.0, edge_min=10.0))
            run_ticks(rt, [(0, ask)])
            rule = (95.0 - ask - core.effective_fee_cents(ask, cfg, 1.0)) >= 10.0
            self.assertEqual(rule, expect)
            self.assertEqual(dec(rt, "production_baseline") is not None and dec(rt, "production_baseline").fill, expect)

    def test_T21_invalid_ticks_never_fill_but_advance_time(self):
        rt = v28.TraceRuntime(make_ctx())
        rt.on_tick(tick(rt.ctx, 0, 0, 96.0, 94.0))
        rt.on_tick(tick(rt.ctx, 1, 5, 80.0, 79.0, http_status=429))
        self.assertIsNone(state(rt, "ask_le_85c").decision)
        rt.on_tick(tick(rt.ctx, 2, 11, None, None, error="timeout"))
        self.assertEqual(dec(rt, "max10s_93c").state, v28.TIMEOUT)
        rt.on_tick(tick(rt.ctx, 3, 12, 70.0, 69.0, error="net_error"))
        self.assertIsNone(state(rt, "ask_le_85c").decision)
        rt.on_tick(tick(rt.ctx, 4, 13, 96.5, 96.6))  # crossed book -> invalid
        self.assertIsNone(state(rt, "ask_le_95c").decision if state(rt, "ask_le_95c").state == v28.WAITING else None)


class LabLifecycle(unittest.TestCase):
    def _lab(self, quotes, fee=1.0):
        """quotes: dict elapsed_second -> (ask, size) served by the fake fetcher."""
        alt = FakeAlt(fee=fee)
        clk = Clock(DET_MS + 400)
        calls = []

        def fetch(url):
            calls.append(url)
            elapsed = int(round((clk.ms - DET_MS) / 1000.0))
            q = quotes.get(elapsed)
            if q == "429":
                return 429, None, ""
            if q == "timeout":
                return 0, None, "timeout"
            ask, size = q if q else (96.0, 20)
            return 200, orderbook_payload(ask, size, bid=ask - 2.0), ""
        self._data_dir = tempfile.mkdtemp()
        lab = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=clk, fetch_fn=fetch, autostart=False)
        return alt, clk, lab, calls

    def test_end_to_end_capture_close_join_report(self):
        quotes = {2: (95.0, 30), 3: (95.0, 30), 8: (93.0, 5), 12: (92.0, 12), 40: (89.0, 40), 45: "429", 50: (85.0, 3)}
        alt, clk, lab, calls = self._lab(quotes)
        alt.kalshi_last["ETH"] = (DET_MS - 1500) / 1000.0
        sid = lab.start_trace(sample_state(ask=96.0, bid=94.0))
        self.assertEqual(sid, "ETH|" + iso(WS_MS))
        self.assertEqual(lab.index["traces"][sid]["detection_ask"], 96.0)  # seq 0 = production detection quote
        while sid in lab.runtimes:
            clk.ms += 1000
            lab.poll_once()
        tr = lab.index["traces"][sid]
        self.assertEqual(tr["status"], "closed")
        self.assertEqual(tr["end_reason"], "eligibility_ended")
        self.assertTrue(all("/orderbook" in u for u in calls))
        self.assertGreaterEqual(lab.index["counters"]["http_429"], 1)
        res = lab.results[sid]
        self.assertEqual(res["entry_at_detection"]["ask"], 96.0)
        self.assertEqual(res["entry_at_detection"]["liq"], "unverified")  # production quote carries no size
        self.assertEqual(res["ask_le_93c"]["ask"], 93.0)
        self.assertEqual(res["ask_le_93c"]["liq"], "verified")  # orderbook tick with ask_size 5
        self.assertEqual(res["delay_60s"]["state"], v28.EXPIRED)  # structural: window < 60 s
        self.assertEqual(res["max10s_93c"]["ask"], 93.0)
        self.assertEqual(res["max10s_92c" if "max10s_92c" in res else "max20s_93c"]["ask"], 93.0)
        self.assertEqual(res["production_baseline"]["state"], v28.EXPIRED)  # never <= 84c
        # decisions were written before later ticks: file order proves it
        lines = [json.loads(x) for x in open(lab.decisions_log.path)]
        fill = next(x for x in lines if x["policy"] == "ask_le_93c")
        ticks = [json.loads(x) for x in open(lab.ticks_log.path)]
        self.assertTrue(any(t["seq"] > fill["entry_seq"] for t in ticks))
        self.assertEqual(fill["entry_seq"], next(t["seq"] for t in ticks if t["ask_cents"] == 93.0))
        # outcome join after resolution; decisions unchanged
        before = open(lab.decisions_log.path).read()
        alt.ledger.data["setups"][sid] = {"setup_id": sid, "resolved": True, "kalshi_win": True, "kalshi_result": "yes",
                                          "entry_simulated": False, "entry_ask_cents": None, "resolved_at": iso(ELIG_END + 900000)}
        lab.join_outcomes()
        self.assertEqual(open(lab.decisions_log.path).read(), before)
        rep = lab.report("forward")
        self.assertEqual((rep["setups_observed"], rep["resolved_setups"]), (1, 1))
        by = {p["policy"]: p for p in rep["policies"]}
        self.assertEqual(by["ask_le_93c"]["wins"], 1)
        self.assertAlmostEqual(by["ask_le_93c"]["net_pnl_dollars"], 0.06)  # 100-93-1 = 6c
        self.assertAlmostEqual(by["entry_at_detection"]["net_pnl_dollars"], 0.03)
        self.assertEqual(by["ask_le_93c"]["vs_detection_pnl_delta_cents"], 3.0)
        self.assertEqual(by["ask_le_93c"]["vs_detection_avg_entry_improvement_cents"], 3.0)
        self.assertEqual((by["entry_at_detection"]["fills_liquidity_verified"], by["entry_at_detection"]["fills_liquidity_unverified"]), (0, 1))
        self.assertEqual((by["ask_le_93c"]["fills_liquidity_verified"], by["ask_le_93c"]["fills_liquidity_unverified"]), (1, 0))
        self.assertEqual(by["ask_le_93c"]["verified_only"]["net_pnl_dollars"], 0.06)
        self.assertFalse(by["ask_le_93c"]["liquidity_mixed"])
        # 88c and 85c are both first-touched by the 85c quote (89c never reached 88c): a tie at +14c
        self.assertIn(rep["ranking"][0]["policy"], ("ask_le_85c", "ask_le_88c"))
        self.assertEqual(by["ask_le_85c"]["net_pnl_per_eligible_setup_cents"], 14.0)
        self.assertEqual(by["ask_le_88c"]["net_pnl_per_eligible_setup_cents"], 14.0)
        self.assertEqual(by["delay_45s"]["fills"], 1)  # 58 s window: 45 s fits, 60 s cannot
        self.assertEqual(by["delay_60s"]["status_eligible"], "INSUFFICIENT_SAMPLE")
        seg = lab.report("forward", segment="ask_bucket")
        self.assertIn("95-96.9", seg["segments"])
        h = lab.health_summary()
        self.assertEqual((h["setups_captured"], h["resolved"], h["active_traces"]), (1, 1, 0))
        self.assertFalse(h["production_modified"])

    def test_filters_and_cap(self):
        alt, clk, lab, _ = self._lab({})
        self.assertIsNone(lab.start_trace(sample_state(basis_ok=False)))
        self.assertIsNone(lab.start_trace(sample_state(status="BASIS WARNING")))
        self.assertIsNone(lab.start_trace(sample_state(status="SHADOW ALREADY COUNTED")))
        self.assertIsNone(lab.start_trace(sample_state(ticker="")))
        c = lab.index["counters"]
        self.assertEqual((c["skipped_basis"], c["skipped_status"], c["skipped_no_market"]), (1, 2, 1))
        lab.max_traces = 1
        self.assertIsNotNone(lab.start_trace(sample_state(asset="ETH")))
        self.assertIsNone(lab.start_trace(sample_state(asset="SOL")))
        self.assertEqual(c["skipped_cap"], 1)
        os.environ["ENTRY_TIMING_ENABLED"] = "0"
        try:
            off = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), clock=clk, autostart=True)
            self.assertIsNone(off.start_trace(sample_state()))
            self.assertIsNone(off.thread)
            self.assertFalse(off.health_summary()["enabled"])
        finally:
            del os.environ["ENTRY_TIMING_ENABLED"]

    def test_backoff_and_source_fallback(self):
        alt, clk, lab, calls = self._lab({1: "timeout", 2: "timeout", 3: (95.0, 5)})
        sid = lab.start_trace(sample_state())
        clk.ms += 1000; lab.poll_once()
        tr = lab.index["traces"][sid]
        self.assertEqual(tr["consecutive_failures"], 1)
        self.assertGreater(tr["backoff_until_ms"], clk.ms)
        n = len(calls)
        ticks_before = lab.index["counters"]["ticks_recorded"]
        clk.ms += 500; lab.poll_once()
        self.assertEqual(len(calls), n)  # inside backoff: no request ...
        self.assertEqual(lab.index["counters"]["ticks_recorded"], ticks_before + 1)  # ... but a synthetic invalid tick
        self.assertEqual(lab.index["counters"]["backoff_ticks"], 1)
        last = json.loads(open(lab.ticks_log.path).read().splitlines()[-1])
        self.assertEqual((last["source"], last["quote_valid"], last["quote_invalid_reason"]), ("backoff", False, "backoff"))
        clk.ms += 3000; lab.poll_once()
        self.assertEqual(tr["consecutive_failures"], 0)
        # 3 consecutive parse errors on orderbook -> per-trace fallback to market endpoint
        lab.fetch_fn = lambda url: (200, {"garbage": True}, "")
        for _ in range(3):
            clk.ms += 1000; lab.poll_once()
        self.assertEqual(tr["source"], "market")
        self.assertEqual(lab.index["counters"]["source_fallbacks"], 1)
        self.assertEqual(lab.index["counters"]["parse_errors"], 3)
        urls = []

        def market_fetch(url):
            urls.append(url)
            return 200, {"market": {"yes_ask_dollars": "0.91", "yes_bid_dollars": "0.89"}}, ""
        lab.fetch_fn = market_fetch
        clk.ms += 1000; lab.poll_once()
        self.assertEqual(lab.results[sid]["ask_le_91c"]["ask"], 91.0)
        self.assertTrue(urls[-1].endswith("/markets/KXETH15M-TEST"))

    def test_record_setup_hook_calls_original_first_and_returns_its_result(self):
        alt = FakeAlt()
        lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), clock=Clock(DET_MS + 300), autostart=False)
        old_lab, old_path = core.ENTRY_TIMING_LAB, core.SHADOW_LEDGER_PATH
        core.ENTRY_TIMING_LAB = lab
        core.SHADOW_LEDGER_PATH = core.Path(tempfile.mkdtemp()) / "ledger.json"
        try:
            fake = FakeLedger()
            fake.alt = alt
            s = sample_state()
            self.assertTrue(core.ShadowLedger.record_setup(fake, s))
            self.assertIn("ETH|" + iso(WS_MS), fake.data["setups"])  # original ran
            self.assertIn("ETH|" + iso(WS_MS), lab.runtimes)  # then the lab started a trace
            self.assertFalse(core.ShadowLedger.record_setup(fake, s))  # original semantics preserved
            self.assertEqual(len(lab.runtimes), 1)
            self.assertIs(v28._original_record_setup, BASE_RECORD_SETUP)
        finally:
            core.ENTRY_TIMING_LAB, core.SHADOW_LEDGER_PATH = old_lab, old_path

    def test_v27_replay_cohort_uses_same_engine_and_stays_separate(self):
        alt, clk, lab, _ = self._lab({})
        clk.ms = ELIG_END + 20 * 60000
        sid = "SOL|" + iso(WS_MS)
        alt.ledger.data["setups"][sid] = {"setup_id": sid, "asset": "SOL", "window_start_utc": iso(WS_MS), "recorded_at": iso(DET_MS),
                                          "minute": MINUTE, "side": "UP", "basis_ok": True, "conservative_fair_pct": 94.12,
                                          "resolved": True, "kalshi_win": False, "kalshi_result": "no", "entry_simulated": False,
                                          "market_ticker": "KXSOL15M-T", "distance_bp": 33.0, "threshold_bp": 30.0, "target_gap_bp": 1.0}
        path = lab.data_dir / "decision_trace_v27.jsonl"
        with open(path, "w") as fh:
            for seq, (dt, ask) in enumerate([(0, 96.0), (2.8, 95.0), (5.6, 93.0), (8.4, 94.0), (58.5, 90.0), (61.0, 80.0)]):
                ts = DET_MS + int(dt * 1000)
                fh.write(json.dumps({"ts": iso(ts), "engine": "alt", "setup_id": sid, "asset": "SOL", "window_start_utc": iso(WS_MS),
                                     "signal_minute": MINUTE, "last_completed_minute": MINUTE if ts < ELIG_END else MINUTE + 1,
                                     "side": "UP", "ask_cents": ask, "bid_cents": ask - 2.0, "cons_pct": 94.12,
                                     "poll_seq": seq + 1, "prod_entry_simulated": False, "status": "SHADOW PRICE HIGH"}) + "\n")
            fh.write(json.dumps({"ts": iso(DET_MS), "engine": "btc", "setup_id": "BTC|x"}) + "\n")
        self.assertEqual(lab.replay_v27(path), 1)
        self.assertEqual(lab.replay_v27(path), 0)  # cursor/done: idempotent
        r = lab.replay_results[sid]
        self.assertEqual(r["ask_le_93c"]["ask"], 93.0)
        self.assertEqual(r["ask_le_90c"]["state"], v28.EXPIRED)  # 90c arrives at 58.5 s; eligibility ended at 58 s
        self.assertEqual(r["delay_5s"]["ask"], 93.0)
        rep = lab.report("all")
        self.assertEqual(rep["cohorts"][v28.COHORT_V27]["setups_observed"], 1)
        self.assertEqual(rep["cohorts"][v28.COHORT_FORWARD]["setups_observed"], 0)
        self.assertEqual(rep["cohorts"][v28.COHORT_V27]["tick_resolution"], "~3s")
        self.assertEqual(lab.report("forward")["setups_observed"], 0)

    def test_v27_replay_streams_with_watermark_and_cursor(self):
        alt, clk, lab, _ = self._lab({})
        path = lab.data_dir / "decision_trace_v27.jsonl"
        ws2 = WS_MS + 900000  # next window, still "open" for the clock below
        a, b = "ETH|" + iso(WS_MS), "SOL|" + iso(ws2)
        for sid in (a, b):
            asset = sid.split("|")[0]
            alt.ledger.data["setups"][sid] = {"setup_id": sid, "asset": asset, "recorded_at": iso(DET_MS if sid == a else DET_MS + 900000),
                                              "minute": MINUTE, "side": "UP", "basis_ok": True, "conservative_fair_pct": 95.0,
                                              "resolved": sid == a, "kalshi_win": True if sid == a else None, "kalshi_result": "yes" if sid == a else "",
                                              "entry_simulated": False, "market_ticker": "T", "distance_bp": 35.0, "threshold_bp": 30.0, "target_gap_bp": 1.0}

        def line(sid, ws, dt, ask, seq):
            ts = (DET_MS if sid == a else DET_MS + 900000) + int(dt * 1000)
            return json.dumps({"ts": iso(ts), "engine": "alt", "setup_id": sid, "asset": sid.split("|")[0], "window_start_utc": iso(ws),
                               "signal_minute": MINUTE, "side": "UP", "ask_cents": ask, "bid_cents": ask - 2.0, "cons_pct": 95.0, "poll_seq": seq}) + "\n"
        with open(path, "w") as fh:
            fh.write(line(a, WS_MS, 0, 96.0, 1)); fh.write(line(a, WS_MS, 3, 94.0, 2))
            fh.write(line(b, ws2, 0, 97.0, 1)); fh.write(line(b, ws2, 3, 92.0, 2))
        clk.ms = ws2 + 900000 + 30000  # a's window ended > 60 s ago; b's ended only 30 s ago
        self.assertEqual(lab.replay_v27(path), 1)
        self.assertIn(a, lab.replay_results)
        self.assertNotIn(b, lab.replay_results)
        self.assertEqual(lab.index["replay_v27"]["cursor"], len(line(a, WS_MS, 0, 96.0, 1)) + len(line(a, WS_MS, 3, 94.0, 2)))
        clk.ms += 60000
        self.assertEqual(lab.replay_v27(path), 1)
        self.assertEqual(lab.replay_results[b]["ask_le_92c"]["ask"], 92.0)
        self.assertEqual(lab.index["replay_v27"]["cursor"], path.stat().st_size)
        self.assertEqual(sorted(lab.index["replay_v27"]["done"]), sorted([a, b]))

    def _open_trace(self, quotes=None, fee=1.0):
        alt, clk, lab, calls = self._lab(quotes or {}, fee=fee)
        alt.kalshi_last["ETH"] = (DET_MS - 1000) / 1000.0
        sid = lab.start_trace(sample_state(ask=96.0, bid=94.0))
        return alt, clk, lab, calls, sid

    def test_T22_crash_after_tick_append_before_index_save_reconstructs_decision(self):
        alt, clk, lab, calls, sid = self._open_trace()
        ctx = lab.runtimes[sid].ctx
        durable_only = tick(ctx, 1, 6, 93.0, 91.0, ask_size=7)
        self.assertTrue(lab.ticks_log.append(durable_only))  # durable, never dispatched (crash right after append)
        n_dec = len(open(lab.decisions_log.path).read().splitlines())
        self.assertEqual(json.load(open(lab.index_path))["traces"][sid]["seq_last"], 0)  # index on disk is stale
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=Clock(DET_MS + 7000), autostart=False)
        rt = lab2.runtimes[sid]
        self.assertEqual(rt.last_seq, 1)
        d = state(rt, "ask_le_93c").decision
        self.assertIsNotNone(d)
        self.assertEqual((d.state, d.entry_seq, d.entry_ask_cents, d.liquidity), (v28.FILLED, 1, 93.0, "verified"))
        self.assertGreater(lab2.index["recovery"]["reconstructed_decisions"], 0)
        self.assertGreater(len(open(lab2.decisions_log.path).read().splitlines()), n_dec)
        self.assertEqual(lab2.index["recovery"]["replayed_ticks"], 2)
        # a durable seq can never be reused
        self.assertEqual(lab2._ingest(sid, tick(ctx, 1, 8, 50.0, 49.0)), [])
        self.assertEqual(lab2.index["counters"]["seq_reuse_rejected"], 1)
        self.assertEqual(state(rt, "ask_le_85c").state, v28.WAITING)
        lab2._ingest(sid, tick(ctx, 2, 8, 90.0, 89.0, ask_size=3))
        self.assertEqual(state(rt, "ask_le_90c").decision.entry_seq, 2)

    def test_T22b_crash_before_first_index_save_recovers_trace_from_open_record(self):
        alt, clk, lab, calls, sid = self._open_trace()
        os.remove(lab.index_path)  # index never reached disk; the open record + seq 0 tick did
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=Clock(DET_MS + 3000), autostart=False)
        self.assertIn(sid, lab2.runtimes)
        self.assertEqual(lab2.index["counters"]["recovered_traces"], 1)
        self.assertEqual(lab2.runtimes[sid].ctx["detection_ask"], 96.0)
        self.assertEqual(state(lab2.runtimes[sid], "entry_at_detection").state, v28.FILLED)  # already durable, not duplicated

    def test_T23_crash_after_decision_append_is_authoritative_and_not_duplicated(self):
        alt, clk, lab, calls, sid = self._open_trace()
        ctx = lab.runtimes[sid].ctx
        lab._ingest(sid, tick(ctx, 1, 6, 93.0, 91.0, ask_size=4))  # decision for ask_le_93c appended; index NOT saved
        lines_before = open(lab.decisions_log.path).read().splitlines()
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=Clock(DET_MS + 7000), autostart=False)
        rt = lab2.runtimes[sid]
        self.assertEqual(open(lab2.decisions_log.path).read().splitlines(), lines_before)  # nothing re-decided
        self.assertEqual(lab2.index["recovery"]["reconstructed_decisions"], 0)
        self.assertEqual(state(rt, "ask_le_93c").decision.entry_seq, 1)
        self.assertEqual(state(rt, "ask_le_90c").state, v28.WAITING)
        lab2._ingest(sid, tick(ctx, 2, 8, 89.0, 88.0, ask_size=4))
        self.assertEqual(state(rt, "ask_le_90c").decision.entry_ask_cents, 89.0)

    def test_T24_restart_with_guardrail_waiting_restores_counter(self):
        alt, clk, lab, calls, sid = self._open_trace(fee=5.0)
        ctx = lab.runtimes[sid].ctx
        lab._ingest(sid, tick(ctx, 1, 6, 95.0, 93.0, ask_size=4))  # cost 100 -> guardrail for ask_le_95c, stays WAITING
        self.assertEqual(state(lab.runtimes[sid], "ask_le_95c").guardrail_rejections, 1)
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=Clock(DET_MS + 7000), autostart=False)
        rt = lab2.runtimes[sid]
        self.assertEqual(state(rt, "ask_le_95c").state, v28.WAITING)
        self.assertEqual(state(rt, "ask_le_95c").guardrail_rejections, 1)
        lab2._ingest(sid, tick(ctx, 2, 8, 94.0, 92.0, ask_size=4))
        d = state(rt, "ask_le_95c").decision
        self.assertEqual((d.state, d.entry_ask_cents, d.guardrail_rejections), (v28.FILLED, 94.0, 1))

    def test_T25_tick_write_failure_skips_dispatch_and_never_reuses_seq(self):
        alt, clk, lab, calls, sid = self._open_trace()
        rt = lab.runtimes[sid]
        ctx = rt.ctx
        lab.ticks_log.append = lambda rec: False
        self.assertEqual(lab._ingest(sid, tick(ctx, 1, 6, 80.0, 79.0, ask_size=4)), [])
        self.assertEqual(rt.ticks, 1)  # seq 0 only; the failed tick was never dispatched
        self.assertEqual(state(rt, "ask_le_85c").state, v28.WAITING)
        self.assertEqual(lab.index["counters"]["tick_write_failures"], 1)
        self.assertEqual(rt.last_seq, 1)  # consumed on the attempt
        lab.ticks_log.append = type(lab.ticks_log).append.__get__(lab.ticks_log)
        self.assertEqual(lab._ingest(sid, tick(ctx, 1, 7, 80.0, 79.0, ask_size=4)), [])  # seq 1 is gone for good
        self.assertEqual(lab.index["counters"]["seq_reuse_rejected"], 1)
        lab._ingest(sid, tick(ctx, 2, 8, 90.0, 89.0, ask_size=4))
        self.assertEqual(state(rt, "ask_le_90c").decision.entry_seq, 2)

    def test_T26_decision_write_failure_blocks_trace_until_durable(self):
        quotes = {1: (93.0, 4), 2: (92.0, 4), 3: (91.0, 4), 4: (91.0, 4)}
        alt, clk, lab, calls, sid = self._open_trace(quotes)
        rt = lab.runtimes[sid]
        real_append = lab.decisions_log.append
        lab.decisions_log.append = lambda rec: False
        clk.ms += 1000; lab.poll_once()  # 93c: ask_le_93c/max*_93c decide but cannot be persisted
        self.assertTrue(rt.unpersisted)
        self.assertEqual(lab.index["counters"]["fills"], 1)  # only seq-0 entry_at_detection was durable
        self.assertNotIn("ask_le_93c", lab.results.get(sid, {}))
        self.assertTrue(lab.index["traces"][sid]["blocked"])
        n = len(calls)
        clk.ms += 1000; self.assertEqual(lab.step_trace(sid), "blocked")
        self.assertEqual(len(calls), n)  # no fetch while a Decision is not durable
        lab.decisions_log.append = real_append
        clk.ms += 1000; lab.poll_once()  # flush succeeds, then the fetch resumes in the same step
        self.assertEqual(rt.unpersisted, [])
        self.assertFalse(lab.index["traces"][sid]["blocked"])
        self.assertGreater(len(calls), n)
        self.assertEqual(lab.results[sid]["ask_le_93c"]["ask"], 93.0)
        self.assertGreaterEqual(lab.index["counters"]["decision_write_retries"], 1)
        lines = [json.loads(x) for x in open(lab.decisions_log.path)]
        idx93 = next(i for i, d in enumerate(lines) if d["policy"] == "ask_le_93c")
        idx92 = next(i for i, d in enumerate(lines) if d["policy"] == "ask_le_92c")
        self.assertLess(idx93, idx92)  # chronological order preserved across the retry
        self.assertEqual(lines[idx93]["decided_seq"], 1)
        self.assertEqual(sum(1 for d in lines if d["policy"] == "ask_le_93c"), 1)

    def test_T27_liquidity_half_contract_never_fills_and_null_is_unverified(self):
        rt = v28.TraceRuntime(make_ctx())
        t_half = tick(rt.ctx, 0, 0, 90.0, 88.0, ask_size=0.5)
        self.assertEqual((t_half["quote_valid"], t_half["quote_invalid_reason"], t_half["fillable_1_contract"]), (False, "unfillable_1_contract", False))
        rt.on_tick(t_half)
        self.assertIsNone(rt.ctx.get("detection_ask"))  # an unfillable quote is not an anchor either
        self.assertEqual(state(rt, "ask_le_90c").state, v28.WAITING)
        rt.on_tick(tick(rt.ctx, 1, 2, 90.0, 88.0, ask_size=1.0))
        d = state(rt, "ask_le_90c").decision
        self.assertEqual((d.state, d.liquidity, d.fillable_1_contract), (v28.FILLED, "verified", True))
        rt.on_tick(tick(rt.ctx, 2, 4, 88.0, 87.0))  # size unknown
        d2 = state(rt, "ask_le_88c").decision
        self.assertEqual((d2.state, d2.liquidity, d2.fillable_1_contract), (v28.FILLED, "unverified", None))
        lab = v28.EntryTimingLab(FakePrimary(), FakeAlt(), data_dir=tempfile.mkdtemp(), autostart=False)
        rows = [{"results": {"p": v28.compact_from_decision(d)}, "outcome": {"kalshi_win": True}},
                {"results": {"p": v28.compact_from_decision(d2)}, "outcome": {"kalshi_win": True}}]
        m = lab.policy_metrics(rows, "p")
        self.assertEqual((m["fills"], m["fills_liquidity_verified"], m["fills_liquidity_unverified"], m["liquidity_mixed"]), (2, 1, 1, True))
        self.assertEqual(m["verified_only"]["fills"], 1)
        self.assertAlmostEqual(m["verified_only"]["net_pnl_dollars"], 0.09)

    def test_T28_disabled_startup_touches_nothing(self):
        import hashlib
        alt, clk, lab, calls, sid = self._open_trace({1: (93.0, 4)})
        clk.ms += 1000; lab.poll_once()  # open trace with ticks + decisions, index deliberately stale
        data_dir = lab.data_dir
        del lab

        def digest():
            out = {}
            for p in sorted(os.listdir(data_dir)):
                out[p] = hashlib.sha256(open(os.path.join(data_dir, p), "rb").read()).hexdigest()
            return out
        before = digest()
        os.environ["ENTRY_TIMING_ENABLED"] = "0"
        try:
            off = v28.EntryTimingLab(alt.primary, alt, data_dir=data_dir, clock=Clock(ELIG_END + 5000), autostart=True)
            self.assertFalse(off.enabled)
            self.assertFalse(off.recovered)
            self.assertEqual(off.runtimes, {})
            self.assertIsNone(off.thread)
            self.assertIsNone(off.start_trace(sample_state(asset="SOL")))
            off.poll_once(); off.maintenance(); off.join_outcomes(); off.save(force=True)
            self.assertEqual(off.replay_v27(), 0)
            self.assertEqual(off.step_trace(sid), "disabled")
            self.assertFalse(off.health_summary()["enabled"])
            self.assertFalse(off.report("forward")["enabled"])
        finally:
            del os.environ["ENTRY_TIMING_ENABLED"]
        self.assertEqual(digest(), before)

    def test_T29_hybrid_timeout_fires_on_a_backoff_tick(self):
        quotes = {5: "429", 6: "429", 8: "429"}
        alt, clk, lab, calls, sid = self._open_trace(quotes)
        rt = lab.runtimes[sid]
        while state(rt, "max10s_93c").state == v28.WAITING and clk.ms < ELIG_END:
            clk.ms += 1000; lab.poll_once()
        d = state(rt, "max10s_93c").decision
        self.assertEqual((d.state, d.no_fill_reason), (v28.TIMEOUT, "timeout"))
        ticks = {t["seq"]: t for t in (json.loads(x) for x in open(lab.ticks_log.path))}
        self.assertEqual(ticks[d.decided_seq]["source"], "backoff")  # decided on a synthetic tick, no request
        self.assertEqual(lab.index["counters"]["requests"], sum(1 for t in ticks.values() if t["source"] == "orderbook"))
        self.assertGreater(lab.index["counters"]["backoff_ticks"], 0)
        self.assertGreater(d.elapsed_at_decision_s, 10.0)

    def test_T30_one_trace_timeout_does_not_block_other_traces(self):
        import time as _time
        alt = FakeAlt()
        now_ms = int(_time.time() * 1000)
        ws = now_ms - 8 * 60000 - 2000

        def live_state(asset):
            st = sample_state(asset=asset, ticker=f"KX{asset}15M-T")
            st["window_start_utc"] = iso(ws)
            st["updated_at"] = iso(now_ms)
            return st
        counts = {"ETH": 0, "SOL": 0}

        def fetch(url):
            asset = "ETH" if "KXETH" in url else "SOL"
            counts[asset] += 1
            if asset == "ETH":
                _time.sleep(1.0)  # slow venue for one asset only
            return 200, orderbook_payload(95.0, 20, bid=93.0), ""
        os.environ["ENTRY_TIMING_TICK_SECONDS"] = "0.5"
        try:
            lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), fetch_fn=fetch, autostart=True)
            lab.start_trace(live_state("ETH"))
            lab.start_trace(live_state("SOL"))
            _time.sleep(2.6)
            lab.stop()
            for w in list(lab.workers.values()):
                w.join(timeout=3)
        finally:
            del os.environ["ENTRY_TIMING_TICK_SECONDS"]
        self.assertGreaterEqual(counts["SOL"], 4)   # 0.5 s cadence kept while ETH requests take 1 s each
        self.assertLessEqual(counts["ETH"], 3)
        self.assertEqual(lab.index["counters"]["hook_errors"], 0)

    def test_T31_empty_orderbook_is_no_ask_not_parse_error(self):
        self.assertEqual(v28.EntryTimingLab.parse_orderbook({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}, "UP"), (None, None, None, None))
        self.assertEqual(v28.EntryTimingLab.parse_orderbook({"orderbook": {"yes": [], "no": []}}, "DOWN"), (None, None, None, None))
        with self.assertRaises(ValueError):
            v28.EntryTimingLab.parse_orderbook({"garbage": True}, "UP")
        with self.assertRaises(ValueError):
            v28.EntryTimingLab.parse_orderbook({"orderbook_fp": {"yes_dollars": [["x", "y"]], "no_dollars": []}}, "UP")
        alt, clk, lab, calls, sid = self._open_trace()
        lab.fetch_fn = lambda url: (200, {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}, "")
        for _ in range(4):
            clk.ms += 1000; lab.poll_once()
        tr = lab.index["traces"][sid]
        last = json.loads(open(lab.ticks_log.path).read().splitlines()[-1])
        self.assertEqual((last["quote_valid"], last["quote_invalid_reason"]), (False, "no_ask"))
        self.assertEqual((lab.index["counters"]["no_ask"], lab.index["counters"]["parse_errors"]), (4, 0))
        self.assertEqual((tr["source"], tr["consecutive_parse_errors"], lab.index["counters"]["source_fallbacks"]), ("orderbook", 0, 0))
        self.assertEqual(tr["backoff_until_ms"], 0)  # a valid empty book is not a transport failure

    def test_T32_fallback_only_after_three_truly_consecutive_parse_errors(self):
        alt, clk, lab, calls, sid = self._open_trace()
        tr = lab.index["traces"][sid]
        script = iter([(200, {"garbage": 1}, ""), (200, {"garbage": 1}, ""), (429, None, ""),
                       (200, {"garbage": 1}, ""), (200, {"garbage": 1}, ""),
                       (200, orderbook_payload(95.0, 5, bid=93.0), ""),
                       (200, {"garbage": 1}, ""), (200, {"garbage": 1}, ""), (200, {"garbage": 1}, "")])
        lab.fetch_fn = lambda url: next(script)
        seen_sources = []
        for _ in range(12):
            clk.ms += 1000
            if int(tr.get("backoff_until_ms", 0)) > clk.ms:
                lab.poll_once()  # backoff tick only
                continue
            lab.poll_once()
            seen_sources.append(tr["source"])
            if tr["source"] == "market":
                break
        self.assertEqual(lab.index["counters"]["source_fallbacks"], 1)
        # the streak was broken twice (429, then a valid book) before three truly consecutive parse errors
        self.assertEqual(seen_sources[:-1].count("market"), 0)
        self.assertEqual(lab.index["counters"]["parse_errors"], 7)

    def test_T33_spread_bucket_exact_boundaries_and_reason_schema(self):
        sb = v28.spread_bucket
        self.assertEqual([sb(0.5), sb(1.0), sb(1.0001), sb(2.0), sb(2.0001), sb(3.0), sb(3.0001), sb(None)],
                         ["<=1", "<=1", "1-2", "1-2", "2-3", "2-3", ">3", "unknown"])
        ctx = make_ctx()
        for status, err, want in ((503, "", "http_5xx"), (500, "", "http_5xx"), (404, "", "http_4xx"), (429, "", "http_429"),
                                  (302, "", "http_other"), (0, "timeout", "timeout"), (0, "net_error", "net_error"),
                                  (200, "parse_error", "parse_error"), (0, "backoff", "backoff"), (0, "weird", "net_error")):
            t = tick(ctx, 0, 0, 90.0, 89.0, http_status=status, error=err)
            self.assertEqual(t["quote_invalid_reason"], want, (status, err))
            self.assertIn(t["quote_invalid_reason"], v28.INVALID_REASONS)
        self.assertEqual(tick(ctx, 0, 0, 90.0, 89.0)["quote_invalid_reason"], None)

    def test_T34_global_request_budget_emits_rate_limited_ticks_and_still_times_out(self):
        os.environ["ENTRY_TIMING_MAX_RPS"] = "1"
        os.environ["ENTRY_TIMING_BURST"] = "1"
        try:
            alt, clk, lab, calls = self._lab({})
        finally:
            del os.environ["ENTRY_TIMING_MAX_RPS"]; del os.environ["ENTRY_TIMING_BURST"]
        alt.kalshi_last["ETH"] = (DET_MS - 1000) / 1000.0
        sids = [lab.start_trace(sample_state(asset=a, ticker=f"KX{a}15M-T")) for a in ("ETH", "SOL", "XRP")]
        self.assertTrue(all(sids))
        self.assertEqual(lab.index["counters"]["requests"], 0)  # seq 0 never consumes budget
        clk.ms += 1000; lab.poll_once()  # one token refilled for three traces
        self.assertEqual(lab.index["counters"]["requests"], 1)
        self.assertEqual(lab.index["counters"]["rate_limited_ticks"], 2)
        last_two = [json.loads(x) for x in open(lab.ticks_log.path).read().splitlines()[-2:]]
        for tk in last_two:
            self.assertEqual((tk["source"], tk["quote_valid"], tk["quote_invalid_reason"]), ("rate_limited", False, "rate_limited"))
            self.assertIn(tk["quote_invalid_reason"], v28.INVALID_REASONS)
        self.assertEqual(len(calls), 1)
        # budget practically closed: hybrid timeouts must still fire, on rate_limited ticks, without any request
        lab.max_rps = 1e-6
        with lab._budget_lock:
            lab._tokens = 0.0
        rt = lab.runtimes[sids[1]]
        while state(rt, "max10s_93c").state == v28.WAITING and clk.ms < ELIG_END:
            clk.ms += 1000; lab.poll_once()
        d = state(rt, "max10s_93c").decision
        self.assertEqual((d.state, d.no_fill_reason), (v28.TIMEOUT, "timeout"))
        ticks = {(t["setup_id"], t["seq"]): t for t in (json.loads(x) for x in open(lab.ticks_log.path))}
        self.assertEqual(ticks[(sids[1], d.decided_seq)]["source"], "rate_limited")
        self.assertEqual(len(calls), 1)
        self.assertEqual(lab.health_summary()["poller"]["max_rps"], 1e-6)

    def test_T35_budget_is_shared_across_workers_and_cadence_is_kept(self):
        import time as _time
        alt = FakeAlt()
        now_ms = int(_time.time() * 1000)
        ws = now_ms - 8 * 60000 - 2000
        assets = ("ETH", "SOL", "XRP", "DOGE", "BNB")

        def live_state(asset):
            st = sample_state(asset=asset, ticker=f"KX{asset}15M-T")
            st["window_start_utc"] = iso(ws)
            st["updated_at"] = iso(now_ms)
            return st
        stamps = []

        def fetch(url):
            stamps.append(_time.time())
            return 200, orderbook_payload(95.0, 20, bid=93.0), ""
        os.environ["ENTRY_TIMING_TICK_SECONDS"] = "0.2"
        os.environ["ENTRY_TIMING_MAX_RPS"] = "2"
        os.environ["ENTRY_TIMING_BURST"] = "2"
        try:
            lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), fetch_fn=fetch, autostart=True)
            t0 = _time.time()
            for a in assets:
                lab.start_trace(live_state(a))
            _time.sleep(2.5)
            lab.stop()
            elapsed = _time.time() - t0
            for w in list(lab.workers.values()):
                w.join(timeout=3)
        finally:
            for k in ("ENTRY_TIMING_TICK_SECONDS", "ENTRY_TIMING_MAX_RPS", "ENTRY_TIMING_BURST"):
                del os.environ[k]
        self.assertLessEqual(len(stamps), int(2 * elapsed) + 2 + 1)  # <= rps * T + burst (+1 tolerance)
        self.assertGreater(lab.index["counters"]["rate_limited_ticks"], 0)
        for a in assets:  # every trace kept ticking at its own cadence (real or rate_limited ticks)
            self.assertGreaterEqual(lab.index["traces"][f"{a}|{iso(ws)}"]["ticks"], 6)
        self.assertEqual(lab.index["counters"]["hook_errors"], 0)

    def test_T36_recovery_reads_rotated_logs(self):
        alt, clk, lab, calls, sid = self._open_trace()
        ctx = lab.runtimes[sid].ctx
        lab._ingest(sid, tick(ctx, 1, 4, 95.0, 93.0, ask_size=4))
        lab._ingest(sid, tick(ctx, 2, 6, 93.0, 91.0, ask_size=4))
        os.rename(lab.ticks_log.path, lab.ticks_log.path.with_suffix(".jsonl.1"))      # rotation mid-trace
        os.rename(lab.decisions_log.path, lab.decisions_log.path.with_suffix(".jsonl.1"))
        os.rename(lab.traces_log.path, lab.traces_log.path.with_suffix(".jsonl.1"))
        self.assertTrue(lab.ticks_log.append(tick(ctx, 3, 8, 90.0, 89.0, ask_size=4)))  # durable, never dispatched
        del lab
        lab2 = v28.EntryTimingLab(alt.primary, alt, data_dir=self._data_dir, clock=Clock(DET_MS + 9000), autostart=False)
        self.assertIn(sid, lab2.runtimes)
        rt = lab2.runtimes[sid]
        self.assertEqual(rt.last_seq, 3)
        self.assertEqual(lab2.index["recovery"]["replayed_ticks"], 4)
        self.assertEqual(state(rt, "ask_le_93c").decision.entry_seq, 2)  # authoritative, from the rotated decisions log
        self.assertEqual(state(rt, "ask_le_90c").decision.entry_seq, 3)  # reconstructed from the fresh ticks log
        self.assertEqual(lab2.results[sid]["entry_at_detection"]["ask"], 96.0)  # results loaded across the rotation
        self.assertEqual(lab2.trace_view(sid)["ticks"][0]["seq"], 0)

    def test_T37_blocked_flush_runs_under_the_lab_lock(self):
        src = inspect.getsource(v28.EntryTimingLab.step_trace)
        self.assertIn("with self.lock:\n                    flushed = self._flush_unpersisted(sid, rt)", src)
        self.assertLess(src.index("_take_token()"), src.index("self._fetch_quote(tr)"))
        self.assertLess(src.index('"backoff"'), src.index("_take_token()"))  # backoff ticks never consume budget


class FakeMonitor:
    def __init__(self):
        self.last_loop_at = 1e12
        self.kalshi_last = 0.0
        self.config = {"kalshi_poll_seconds": 2.0}


class FakeAltMonitor:
    class _Ledger:
        def metrics(self):
            return {"totals": {}}

    class _Early:
        def metrics(self):
            return {"totals": {}, "operational": {}}

    def __init__(self):
        self.last_loop_at = 1e12
        self.ledger = self._Ledger()
        self.early = self._Early()


class EndpointsAndHealth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import time as _time
        alt = FakeAlt()
        cls.lab = v28.EntryTimingLab(alt.primary, alt, data_dir=tempfile.mkdtemp(), autostart=False)
        cls.old = (core.ENTRY_TIMING_LAB, getattr(core, "MONITOR", None), getattr(core, "ALT_MONITOR", None), core.RESEARCH_LAB,
                   os.environ.get("DASHBOARD_PASSWORD"))
        core.ENTRY_TIMING_LAB = cls.lab
        core.MONITOR = FakeMonitor()
        core.MONITOR.last_loop_at = _time.time()
        core.ALT_MONITOR = FakeAltMonitor()
        core.ALT_MONITOR.last_loop_at = _time.time()
        core.RESEARCH_LAB = None
        os.environ["DASHBOARD_PASSWORD"] = ""
        cls.server = core.ThreadingHTTPServer(("127.0.0.1", 0), core.Handler)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        core.ENTRY_TIMING_LAB, core.MONITOR, core.ALT_MONITOR, core.RESEARCH_LAB, pw = cls.old
        if pw is None:
            os.environ.pop("DASHBOARD_PASSWORD", None)
        else:
            os.environ["DASHBOARD_PASSWORD"] = pw

    def _get(self, path):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("GET", path)
        r = c.getresponse()
        body = r.read()
        c.close()
        return r.status, body

    def test_entry_timing_endpoints_and_alias(self):
        for path in ("/api/research/entry-timing", "/api/research/entry-timing.json", "/research/entry-timing",
                     "/api/research/entry-timing?cohort=all", "/api/research/entry-timing?segment=asset&sort=roi_pct"):
            status, body = self._get(path)
            self.assertEqual(status, 200, path)
            j = json.loads(body)
            self.assertEqual(j["version"], "2.8-executable-entry-timing-lab")
            self.assertEqual(j["mode"], "forward-counterfactual-only")
            self.assertFalse(j["production_modified"])
            self.assertEqual(len(j["policies"]), 37)
        status, body = self._get("/api/research/entry-timing/status")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["lab_version"], "2.8")
        status, _ = self._get("/api/research/entry-timing/trace")
        self.assertEqual(status, 400)
        status, body = self._get("/api/research/entry-timing/trace?setup_id=ETH|x")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["ticks"], [])

    def test_v27_and_core_routes_still_served(self):
        status, body = self._get("/api/research/lab")
        self.assertEqual(status, 404)  # v27 lab is None in this harness -> v27's own 404, i.e. routed to v27
        self.assertEqual(json.loads(body)["error"], "research lab off")
        status, body = self._get("/health")
        self.assertEqual(status, 200)
        j = json.loads(body)
        self.assertIn("entry_timing_lab", j)
        self.assertIn("research_lab", j)  # v27 block still injected
        et = j["entry_timing_lab"]
        for key in ("version", "mode", "setups_captured", "resolved", "active_traces", "ticks_recorded", "last_capture_utc",
                    "production_modified", "poller"):
            self.assertIn(key, et)
        self.assertEqual(et["version"], "2.8")
        self.assertFalse(et["production_modified"])
        self.assertIn("http_429", et["poller"])


if __name__ == "__main__":
    unittest.main()
