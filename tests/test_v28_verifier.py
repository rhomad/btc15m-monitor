import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
import verify_v28_post_deploy as verify  # noqa: E402


def health_v28(requests=0, http_429=0, kalshi=True, binance=True, max_rps=2.0, burst=2, ok=True):
    return {
        "ok": ok, "version": verify.EXPECTED_VERSION_V28,
        "sources": {"kalshi": {"healthy": kalshi, "book_age_ms": 1200.0}, "binance": {"healthy": binance, "monitor_loop_age_ms": 800.0}},
        "research_lab": {"trace_setups": 0},
        "entry_timing_lab": {
            "version": "2.8", "enabled": True, "recovered": True, "production_modified": False,
            "setups_captured": 1, "active_traces": 0, "ticks_recorded": 58, "last_capture_utc": None,
            "poller": {"mode": "one worker thread per trace", "maintenance_thread_alive": True, "workers_alive": 0,
                       "requests": requests, "http_429": http_429, "rate_limited_ticks": 0, "backoff_ticks": 0,
                       "no_ask": 0, "parse_errors": 0, "hook_errors": 0, "max_rps": max_rps, "burst": burst},
            "persistence": {"tick_write_failures": 0, "decision_write_failures": 0, "trace_log_write_failures": 0, "seq_reuse_rejected": 0},
            "recovery": {"last": {"open_traces": 0}},
        },
    }


def health_v27(kalshi=True, binance=True, ok=True):
    return {"ok": ok, "version": verify.EXPECTED_VERSION_V27,
            "sources": {"kalshi": {"healthy": kalshi, "book_age_ms": 900.0}, "binance": {"healthy": binance, "monitor_loop_age_ms": 500.0}},
            "research_lab": {"trace_setups": 12}}


OTHER_ROUTES = {
    "/api/research/lab": (200, {"lab_version": "2.7", "mode": "observational; no strategy changes"}),
    "/api/research/entry-timing": (200, {"version": verify.EXPECTED_VERSION_V28, "mode": "forward-counterfactual-only",
                                         "policies": [{"policy": f"p{i}"} for i in range(37)], "hindsight_metrics_included": False,
                                         "cohorts_shown": ["forward_v28"], "setups_observed": 1, "resolved_setups": 0}),
    "/api/research/entry-timing/status": (200, {"lab_version": "2.8"}),
    "/research/entry-timing": (200, {"version": verify.EXPECTED_VERSION_V28}),
}


class FakeClient:
    """Scripted /health responses (one per observation, last one repeats); other routes canned."""
    def __init__(self, healths, routes=None):
        self.healths = list(healths)
        self.routes = dict(OTHER_ROUTES)
        self.routes.update(routes or {})
        self.calls = []

    def get(self, path, auth=True):
        self.calls.append(path)
        if path == "/health":
            h = self.healths.pop(0) if len(self.healths) > 1 else self.healths[0]
            return 200, json.loads(json.dumps(h))
        return self.routes.get(path.split("?")[0], (404, {"error": "not found"}))


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += float(s)


def fail_lines(out):
    return [l for l in out.splitlines() if l.startswith("FAIL ")]


def run(argv, healths, routes=None):
    client = FakeClient(healths, routes)
    clock = FakeClock()
    lines = []
    code = verify.main(argv, client=client, out=lines.append, now=clock.now, sleep=clock.sleep)
    return code, "\n".join(lines), client


class VerifierWatch(unittest.TestCase):
    def test_healthy_watch_exits_0(self):
        # 2 rps budget, burst 2: +20 requests per 10 s observation is within max_rps*dt + burst + tolerance
        healths = [health_v28(requests=10 + 20 * i) for i in range(4)]
        code, out, client = run(["http://x", "--watch", "0.5"], healths)
        self.assertEqual(code, 0, out)
        self.assertIn("ALL CHECKS PASSED", out)
        self.assertNotIn("FAIL ABORT", out)
        self.assertGreaterEqual(client.calls.count("/health"), 4)

    def test_429_during_watch_exits_1(self):
        healths = [health_v28(requests=10), health_v28(requests=30), health_v28(requests=50, http_429=1), health_v28(requests=70, http_429=1)]
        code, out, _ = run(["http://x", "--watch", "0.5"], healths)
        self.assertEqual(code, 1)
        self.assertIn("ABORT", out)
        self.assertIn("http_429 increased during watch 0 -> 1 (rule C)", out)
        self.assertNotIn("ALL CHECKS PASSED", out)
        self.assertIn("ABORT SIGNALS", out)

    def test_kalshi_turning_unhealthy_during_watch_exits_1(self):
        healths = [health_v28(requests=10), health_v28(requests=30), health_v28(requests=50, kalshi=False), health_v28(requests=70, kalshi=True)]
        code, out, _ = run(["http://x", "--watch", "0.5"], healths)
        self.assertEqual(code, 1)
        self.assertIn("sources.kalshi.healthy=False (rule A)", out)
        self.assertNotIn("ALL CHECKS PASSED", out)  # recovery later does not clear an observed abort

    def test_request_budget_above_published_limit_fails(self):
        # single check consumes the first payload; the watch then sees +60 requests in one 10 s interval
        # against max_rps 2 / burst 2 (+2 tolerance = 24 allowed)
        healths = [health_v28(requests=10), health_v28(requests=10), health_v28(requests=70), health_v28(requests=80)]
        code, out, _ = run(["http://x", "--watch", "0.34"], healths)
        self.assertEqual(code, 1)
        self.assertIn("request budget exceeded (rule D)", out)
        self.assertIn("allowed 24.0 = 2.0*10.0+2+2", out)
        self.assertNotIn("ALL CHECKS PASSED", out)

    def test_budget_check_uses_published_limits_not_a_hardcoded_gate(self):
        # Same +60/10 s traffic is fine when /health publishes max_rps 6 / burst 5 (allowed 6*10+5+2 = 67)
        healths = [health_v28(requests=10, max_rps=6.0, burst=5), health_v28(requests=10, max_rps=6.0, burst=5),
                   health_v28(requests=70, max_rps=6.0, burst=5), health_v28(requests=80, max_rps=6.0, burst=5)]
        code, out, _ = run(["http://x", "--watch", "0.34"], healths)
        self.assertEqual(code, 0, out)
        self.assertEqual(verify.budget_allowance(2.0, 2, 10.0), 24.0)
        self.assertEqual(verify.budget_allowance(6.0, 5, 10.0), 67.0)

    def test_budget_disabled_is_an_abort(self):
        code, out, _ = run(["http://x"], [health_v28(max_rps=0.0)])
        self.assertEqual(code, 1)
        self.assertIn("max_rps <= 0 (rule E)", out)

    def test_historical_429_requires_explicit_acceptance(self):
        code, out, _ = run(["http://x"], [health_v28(http_429=3)])
        self.assertEqual(code, 1)
        self.assertIn("above accepted historical value 0 (rule C)", out)
        code, out, _ = run(["http://x", "--accept-historical-429", "3"], [health_v28(http_429=3)])
        self.assertEqual(code, 0, out)
        code, out, _ = run(["http://x", "--accept-historical-429", "3", "--watch", "0.34"],
                           [health_v28(requests=1, http_429=3), health_v28(requests=1, http_429=3),
                            health_v28(requests=5, http_429=4), health_v28(requests=6, http_429=4)])
        self.assertEqual(code, 1)
        self.assertIn("increased during watch 3 -> 4", out)


class VerifierBaseline(unittest.TestCase):
    def test_healthy_2_7_baseline_exits_0(self):
        code, out, client = run(["http://x", "--baseline-v27"], [health_v27()])
        self.assertEqual(code, 0, out)
        self.assertIn("ALL CHECKS PASSED", out)
        self.assertEqual(client.calls, ["/health"])  # no v2.8 route is requested
        self.assertNotIn("entry_timing_lab block present", out.replace("info entry_timing_lab block present (v2.8 already deployed?)", ""))

    def test_sources_only_on_any_version(self):
        code, out, _ = run(["http://x", "--sources-only"], [health_v27()])
        self.assertEqual(code, 0, out)
        code, out, _ = run(["http://x", "--sources-only"], [health_v28()])
        self.assertEqual(code, 0, out)

    def test_baseline_unhealthy_kalshi_exits_1(self):
        code, out, _ = run(["http://x", "--baseline-v27"], [health_v27(kalshi=False)])
        self.assertEqual(code, 1)
        self.assertIn("(rule A)", out)

    def test_baseline_watch_reports_abort_when_source_drops(self):
        code, out, _ = run(["http://x", "--baseline-v27", "--watch", "0.5"],
                           [health_v27(), health_v27(), health_v27(binance=False), health_v27()])
        self.assertEqual(code, 1)
        self.assertIn("sources.binance.healthy=False (rule B)", out)

    def test_v28_single_check_on_a_2_7_deploy_fails_but_baseline_mode_does_not(self):
        code, out, _ = run(["http://x"], [health_v27()])
        self.assertEqual(code, 1)
        code, out, _ = run(["http://x", "--baseline-v27"], [health_v27()])
        self.assertEqual(code, 0)


CATALOGUE = [f"policy_{i:02d}" for i in range(37)]


def status_route(catalogue=None):
    return (200, {"lab_version": "2.8", "policies": [{"policy": p, "family": "x", "params": {}} for p in (catalogue or CATALOGUE)]})


def mk_ticks(seqs, asks=None, valid=None):
    out = []
    for i, seq in enumerate(seqs):
        ask = (asks[i] if asks else 95.0)
        out.append({"seq": seq, "ask_cents": ask, "quote_valid": (valid[i] if valid else True), "tick_at": f"T{seq}",
                    "quote_invalid_reason": None if (valid[i] if valid else True) else "http_429"})
    return out


def mk_decisions(names=None, fill_policy="policy_00", entry_seq=2, entry_ask=93.0, frozen=True):
    out = []
    for n in (names or CATALOGUE):
        if n == fill_policy and entry_seq is not None:
            out.append({"policy": n, "fill": True, "state": "FILLED", "entry_seq": entry_seq, "decided_seq": entry_seq,
                        "entry_ask_cents": entry_ask, "decided_at": f"T{entry_seq}" if frozen else "T9999",
                        "entry_tick_at": f"T{entry_seq}", "liquidity": "verified"})
        else:
            out.append({"policy": n, "fill": False, "state": "EXPIRED", "entry_seq": None, "decided_seq": 5, "entry_ask_cents": None,
                        "decided_at": "T5", "entry_tick_at": None, "liquidity": None})
    return out


def trace_route(ticks, decisions, status="closed"):
    return (200, {"setup_id": "ETH|w", "trace": {"status": status, "end_reason": "eligibility_ended" if status == "closed" else None,
                                                 "detection_ask": 96.0, "restarts": 0}, "ticks": ticks, "decisions": decisions, "outcome": None})


def run_trace(ticks, decisions, status="closed", catalogue_route=None):
    routes = {"/api/research/entry-timing/trace": trace_route(ticks, decisions, status),
              "/api/research/entry-timing/status": catalogue_route or status_route()}
    return run(["http://x", "--trace", "ETH|w"], [health_v28()], routes)


class TraceAudit(unittest.TestCase):
    # ticks 0..5: 96, 95, 93 (fill for policy_00 at seq 2), 94, 90 (valid cheaper later), 91
    GOOD_SEQS = [0, 1, 2, 3, 4, 5]
    GOOD_ASKS = [96.0, 95.0, 93.0, 94.0, 90.0, 91.0]

    def test_duplicate_seq_fails(self):
        code, out, _ = run_trace(mk_ticks([0, 1, 1, 2], [96.0, 95.0, 93.0, 90.0]), mk_decisions())
        self.assertEqual(code, 1)
        self.assertIn("FAIL tick seq values are unique  [duplicated seq [1]]", out)
        self.assertIn("FAIL tick seq is exactly 0..N-1 with no gaps", out)
        self.assertNotIn("ALL CHECKS PASSED", out)

    def test_gapped_or_missing_seq_fails(self):
        code, out, _ = run_trace(mk_ticks([0, 1, 3, 4], [96.0, 95.0, 93.0, 90.0]), mk_decisions(entry_seq=3))
        self.assertEqual(code, 1)
        self.assertIn("FAIL tick seq is exactly 0..N-1 with no gaps  [N=4 missing [2]", out)
        code, out, _ = run_trace(mk_ticks([1, 2, 3], [95.0, 93.0, 90.0]), mk_decisions(entry_seq=2))  # starts at 1: seq 0 missing
        self.assertEqual(code, 1)
        self.assertIn("missing [0]", out)
        code, out, _ = run_trace(mk_ticks([0, "1", 2], [96.0, 93.0, 90.0]), mk_decisions())  # non-integer seq
        self.assertEqual(code, 1)
        self.assertIn("FAIL every tick has an integer seq", out)

    def test_37_decisions_with_duplicate_and_missing_policy_fails(self):
        names = list(CATALOGUE)
        names[36] = "policy_05"  # policy_05 twice, policy_36 absent, still 37 decisions
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions(names))
        self.assertEqual(code, 1)
        self.assertIn("FAIL no policy has more than one decision  [duplicated ['policy_05']]", out)
        self.assertIn("FAIL closed trace: every catalogue policy has a decision  [missing ['policy_36']]", out)
        self.assertIn("FAIL closed trace: decision set equals the catalogue exactly", out)

    def test_unknown_policy_and_missing_catalogue_fail(self):
        names = list(CATALOGUE)
        names[3] = "not_in_catalogue"
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions(names))
        self.assertEqual(code, 1)
        self.assertIn("FAIL every decision policy belongs to the catalogue  [unknown ['not_in_catalogue']]", out)
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions(),
                                 catalogue_route=(404, {"error": "not found"}))
        # falls back to the report catalogue (37 metric rows named p0..p36 in OTHER_ROUTES) -> mismatch with policy_* names
        self.assertEqual(code, 1)
        self.assertIn("unknown", out)

    def test_closed_trace_with_37_unique_policies_passes_structurally_and_evidence(self):
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions())
        self.assertEqual(code, 0, out)
        self.assertIn("PASS tick seq is exactly 0..N-1 with no gaps  [N=6", out)
        self.assertIn("PASS closed trace: decision set equals the catalogue exactly", out)
        self.assertIn("PASS policy_00: Decision frozen at the fill tick timestamp", out)
        self.assertIn("PASS acceptance evidence SUFFICIENT for ETH|w", out)
        self.assertIn("2 valid later tick(s) cheaper (min 90.0)", out)
        self.assertIn("ALL CHECKS PASSED", out)

    def test_trace_without_fills_is_not_evidence(self):
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions(entry_seq=None))
        self.assertEqual(code, 3)
        self.assertEqual(fail_lines(out), [])
        self.assertIn("EVIDENCE INSUFFICIENT for ETH|w (no_fills)", out)
        self.assertIn("not a finding against the engine", out)
        self.assertNotIn("ALL CHECKS PASSED", out)
        self.assertIn("ACCEPTANCE EVIDENCE INSUFFICIENT", out)

    def test_fill_without_later_cheaper_valid_tick_is_not_sufficient(self):
        # later ticks: 94 (valid, not cheaper), 90 (cheaper but INVALID), 93 (valid, equal, not cheaper)
        ticks = mk_ticks([0, 1, 2, 3, 4, 5], [96.0, 95.0, 93.0, 94.0, 90.0, 93.0], [True, True, True, True, False, True])
        code, out, _ = run_trace(ticks, mk_decisions())
        self.assertEqual(code, 3)
        self.assertEqual(fail_lines(out), [])
        self.assertIn("EVIDENCE INSUFFICIENT for ETH|w (no_later_cheaper_valid_tick)", out)
        self.assertNotIn("ALL CHECKS PASSED", out)

    def test_fill_with_later_cheaper_valid_tick_and_frozen_decision_is_sufficient(self):
        ticks = mk_ticks([0, 1, 2, 3], [96.0, 93.0, 92.0, 95.0])
        code, out, _ = run_trace(ticks, mk_decisions(entry_seq=1, entry_ask=93.0))
        self.assertEqual(code, 0, out)
        self.assertIn("PASS acceptance evidence SUFFICIENT for ETH|w  [policy_00 filled at seq 1 ask 93.0; 1 valid later tick(s) cheaper (min 92.0)", out)

    def test_unfrozen_or_mismatching_decision_fails(self):
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), mk_decisions(frozen=False))
        self.assertEqual(code, 1)
        self.assertIn("FAIL policy_00: Decision frozen at the fill tick timestamp", out)
        decs = mk_decisions()
        decs[0]["decided_seq"] = 4  # decided later than its fill tick
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), decs)
        self.assertEqual(code, 1)
        self.assertIn("FAIL policy_00: decided on its own fill tick", out)
        decs = mk_decisions(entry_ask=92.0)  # decision ask differs from the tick ask at seq 2 (93)
        code, out, _ = run_trace(mk_ticks(self.GOOD_SEQS, self.GOOD_ASKS), decs)
        self.assertEqual(code, 1)
        self.assertIn("FAIL policy_00: fill tick exists, is valid and carries the same ask", out)

    def test_open_trace_is_not_evidence_but_not_a_failure(self):
        code, out, _ = run_trace(mk_ticks([0, 1, 2], [96.0, 93.0, 92.0]), mk_decisions(CATALOGUE[:5], entry_seq=1), status="open")
        self.assertEqual(code, 3)
        self.assertEqual(fail_lines(out), [])
        self.assertIn("(trace_not_closed)", out)


class RuleF(unittest.TestCase):
    def test_health_ok_dropping_during_watch_exits_1_even_if_it_recovers(self):
        healths = [health_v28(requests=10), health_v28(requests=30), health_v28(requests=50, ok=False), health_v28(requests=70, ok=True)]
        code, out, _ = run(["http://x", "--watch", "0.5"], healths)
        self.assertEqual(code, 1)
        self.assertIn("watch obs 2: /health ok=False (rule F)", out)
        self.assertIn("ABORT SIGNALS", out)
        self.assertNotIn("ALL CHECKS PASSED", out)

    def test_health_ok_false_in_single_check_and_baseline_is_an_abort(self):
        code, out, _ = run(["http://x"], [health_v28(ok=False)])
        self.assertEqual(code, 1)
        self.assertIn("/health ok is not true (rule F)", out)
        code, out, _ = run(["http://x", "--baseline-v27"], [health_v27(ok=False)])
        self.assertEqual(code, 1)
        self.assertIn("(rule F)", out)

    def test_rule_f_is_documented(self):
        self.assertIn("(F) /health ok is not true", verify.ABORT_RULE)


class AbortRuleConsistency(unittest.TestCase):
    def test_rule_text_is_shared_with_docs(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for name in ("README.md", "RUNBOOK_v28.md"):
            with open(os.path.join(root, name), encoding="utf-8") as fh:
                self.assertIn(verify.ABORT_RULE, fh.read(), name)

    def test_rule_c_needs_no_correlation(self):
        self.assertIn("no correlation with a production API ERROR is required", verify.ABORT_RULE)
        self.assertIn(f"burst + {verify.BUDGET_TOLERANCE_REQUESTS}", verify.ABORT_RULE)


if __name__ == "__main__":
    unittest.main()
