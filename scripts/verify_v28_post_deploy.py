#!/usr/bin/env python3
"""VIXION v2.8 deploy verifier (read-only, GET only, stdlib only).

Modes
  python scripts/verify_v28_post_deploy.py <base_url>                       post-deploy single check (v2.8 expected)
  python scripts/verify_v28_post_deploy.py <base_url> --watch 30            + observe /health every 10 s for 30 minutes
  python scripts/verify_v28_post_deploy.py <base_url> --trace "<setup_id>"  + audit one trace (fills decided on their own tick)
  python scripts/verify_v28_post_deploy.py <base_url> --baseline-v27        pre-deploy: /health + sources on 2.7, no v2.8 checks
  python scripts/verify_v28_post_deploy.py <base_url> --sources-only        any version: ok + sources only
  ... --accept-historical-429 N   accept a known lab http_429 counter value at start (default 0; counters persist on the volume)

Credentials only from the environment (DASHBOARD_USER defaults to admin, DASHBOARD_PASSWORD). Never prints secrets.
Exit codes: 0 = every check passed (including every --watch observation) and, when --trace is used, the trace provides
sufficient anti-hindsight acceptance evidence; 1 = at least one failed check or abort observation; 3 = no check failed
but the audited trace does not provide sufficient acceptance evidence (structurally valid trace; the engine is not
implicated; pick another closed trace). "ALL CHECKS PASSED" is printed only for exit 0.
"""
import base64
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

EXPECTED_VERSION_V28 = "2.8-executable-entry-timing-lab"
EXPECTED_VERSION_V27 = "2.7-railway-observability-lab"
POLICIES_TOTAL = 37
WATCH_POLL_SECONDS = 10.0
BUDGET_TOLERANCE_REQUESTS = 2  # explicit slack on top of max_rps * dt + burst (clock skew, in-flight requests)
EVIDENCE_SUFFICIENT = "SUFFICIENT"
EVIDENCE_INSUFFICIENT = "INSUFFICIENT"
EXIT_OK, EXIT_FAIL, EXIT_EVIDENCE_INSUFFICIENT = 0, 1, 3

# Single source of truth for the abort rule. README.md and RUNBOOK_v28.md quote this text verbatim.
ABORT_RULE = (
    "ABORT = FAIL (exit 1) as soon as ANY observation shows: "
    "(A) sources.kalshi.healthy is not true; "
    "(B) sources.binance.healthy is not true; "
    "(C) entry_timing_lab.poller.http_429 is above the accepted historical value at the start of the check "
    "(--accept-historical-429, default 0) or increases during the watch - any lab 429 is sufficient, no correlation "
    "with a production API ERROR is required; "
    "(D) lab requests exceed the budget published by /health: delta_requests > max_rps * delta_seconds + burst + "
    f"{BUDGET_TOLERANCE_REQUESTS}, checked cumulatively over the watch and per observation interval; "
    "(E) max_rps <= 0 (budget disabled); "
    "(F) /health ok is not true (production loops stale). "
    "Operator action on abort: set ENTRY_TIMING_ENABLED=0 (requires explicit approval); the lab becomes passive and "
    "never touches the volume."
)


class HttpClient:
    def __init__(self, base, timeout=10):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def get(self, path, auth=True):
        headers = {"User-Agent": "vixion-v28-verify"}
        if auth:
            user = os.getenv("DASHBOARD_USER", "admin")
            pw = os.getenv("DASHBOARD_PASSWORD", "")
            if pw:
                headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()
        req = Request(self.base + path, headers=headers)
        try:
            with urlopen(req, timeout=self.timeout) as r:
                body = r.read().decode("utf-8", "replace")
                try:
                    return r.status, json.loads(body)
                except ValueError:
                    return r.status, body
        except HTTPError as exc:
            return exc.code, None
        except (URLError, TimeoutError, OSError) as exc:
            return 0, str(exc)


def budget_allowance(max_rps, burst, delta_seconds):
    """Requests the lab may have made in delta_seconds according to its published budget (+ explicit tolerance)."""
    return float(max_rps) * max(0.0, float(delta_seconds)) + int(burst) + BUDGET_TOLERANCE_REQUESTS


class Verifier:
    def __init__(self, client, out=print, now=time.time, sleep=time.sleep, accept_historical_429=0):
        self.client = client
        self.out = out
        self.now = now
        self.sleep = sleep
        self.accept_429 = int(accept_historical_429)
        self.failures = 0
        self.aborts = []
        self.evidence = []  # one verdict dict per audited trace

    # ------------------------------------------------------------- reporting
    def check(self, ok, label, detail=""):
        self.out(("PASS " if ok else "FAIL ") + label + (f"  [{detail}]" if detail else ""))
        if not ok:
            self.failures += 1
        return bool(ok)

    def abort(self, label, detail=""):
        """An abort condition is a FAIL that is also recorded as an operator abort signal."""
        self.aborts.append(label)
        return self.check(False, "ABORT " + label, detail)

    def info(self, label, detail=""):
        self.out("info " + label + (f"  [{detail}]" if detail else ""))

    # ------------------------------------------------------------- building blocks
    def _health(self):
        code, h = self.client.get("/health", auth=False)
        if code == 200 and isinstance(h, dict):
            return h
        self.check(False, "/health reachable", f"status {code}")
        return None

    def _sources(self, h, prefix=""):
        src = h.get("sources") or {}
        kal = (src.get("kalshi") or {}).get("healthy")
        bin_ = (src.get("binance") or {}).get("healthy")
        ok_flag = h.get("ok") is True
        if not ok_flag:
            self.abort(prefix + "/health ok is not true (rule F)", json.dumps(h.get("ok")))
        else:
            self.check(True, prefix + "health ok=true (production loops alive)")
        if kal is not True:
            self.abort(prefix + "sources.kalshi.healthy is not true (rule A)", json.dumps(src.get("kalshi")))
        else:
            self.check(True, prefix + "sources.kalshi.healthy", json.dumps(src.get("kalshi")))
        if bin_ is not True:
            self.abort(prefix + "sources.binance.healthy is not true (rule B)", json.dumps(src.get("binance")))
        else:
            self.check(True, prefix + "sources.binance.healthy", json.dumps(src.get("binance")))
        return kal is True and bin_ is True and ok_flag

    def _poller(self, h):
        et = h.get("entry_timing_lab")
        if not isinstance(et, dict):
            return None, None
        return et, (et.get("poller") or {})

    def _check_429(self, poller, start_value=None, prefix=""):
        """Rule C. Returns the observed counter."""
        value = int(poller.get("http_429", 0) or 0)
        if start_value is None:
            if value > self.accept_429:
                self.abort(prefix + f"lab http_429={value} above accepted historical value {self.accept_429} (rule C)")
            else:
                self.check(True, prefix + "lab http_429 within accepted historical value", f"{value} <= {self.accept_429}")
        elif value > start_value:
            self.abort(prefix + f"lab http_429 increased during watch {start_value} -> {value} (rule C)")
        return value

    def _check_budget_published(self, poller, prefix=""):
        max_rps = float(poller.get("max_rps", 0) or 0)
        burst = int(poller.get("burst", 0) or 0)
        if max_rps <= 0:
            self.abort(prefix + "request budget disabled: max_rps <= 0 (rule E)", json.dumps({"max_rps": max_rps, "burst": burst}))
        else:
            self.check(True, prefix + "request budget published", f"max_rps={max_rps} burst={burst} tolerance=+{BUDGET_TOLERANCE_REQUESTS}")
        return max_rps, burst

    def _check_budget_delta(self, label, delta_req, delta_s, max_rps, burst):
        allowed = budget_allowance(max_rps, burst, delta_s)
        ok = delta_req <= allowed
        detail = f"{delta_req} requests in {delta_s:.1f}s; allowed {allowed:.1f} = {max_rps}*{delta_s:.1f}+{burst}+{BUDGET_TOLERANCE_REQUESTS}"
        if not ok:
            self.abort(label + " (rule D)", detail)
        return ok, detail

    # ------------------------------------------------------------- modes
    def verify_baseline(self, require_v27=True):
        h = self._health()
        if h is None:
            return None
        self.info("version", str(h.get("version")))
        if require_v27:
            self.check(h.get("version") == EXPECTED_VERSION_V27, "baseline version is 2.7", str(h.get("version")))
            self.check("research_lab" in h, "v2.7 research_lab block present")
        self._sources(h)
        if "entry_timing_lab" in h:
            self.info("entry_timing_lab block present (v2.8 already deployed?)")
        return h

    def verify_v28(self):
        h = self._health()
        if h is None:
            return None
        self.check(h.get("version") == EXPECTED_VERSION_V28, "core version bumped at runtime", str(h.get("version")))
        self._sources(h)
        self.check("research_lab" in h, "v2.7 research_lab block still injected")
        et, poller = self._poller(h)
        if not self.check(et is not None, "entry_timing_lab block present"):
            return h
        self.check(et.get("version") == "2.8", "lab version 2.8", str(et.get("version")))
        self.check(et.get("enabled") is True, "lab enabled")
        self.check(et.get("recovered") is True, "recovery completed before polling")
        self.check(et.get("production_modified") is False, "production_modified=false")
        self.check(poller.get("maintenance_thread_alive") is True, "maintenance thread alive")
        self.check(poller.get("mode") == "one worker thread per trace", "poller mode", str(poller.get("mode")))
        self._check_budget_published(poller)
        self._check_429(poller)
        self.info("counters", f"setups_captured={et.get('setups_captured')} active={et.get('active_traces')} "
                              f"workers_alive={poller.get('workers_alive')} ticks={et.get('ticks_recorded')} requests={poller.get('requests')} "
                              f"rate_limited_ticks={poller.get('rate_limited_ticks')} backoff_ticks={poller.get('backoff_ticks')} "
                              f"no_ask={poller.get('no_ask')} parse_errors={poller.get('parse_errors')}")
        pers = et.get("persistence") or {}
        for key in ("tick_write_failures", "decision_write_failures", "trace_log_write_failures", "seq_reuse_rejected"):
            self.check(int(pers.get(key, 0) or 0) == 0, f"persistence.{key} == 0", str(pers.get(key)))
        self.check(int(poller.get("hook_errors", 0) or 0) == 0, "no hook errors", str(poller.get("hook_errors")))
        self.info("recovery", json.dumps((et.get("recovery") or {}).get("last")))

        code, lab = self.client.get("/api/research/lab")
        self.check(code == 200 and isinstance(lab, dict) and lab.get("lab_version") == "2.7", "v2.7 /api/research/lab still served", f"status {code}")
        code, rpt = self.client.get("/api/research/entry-timing")
        ok = code == 200 and isinstance(rpt, dict)
        self.check(ok, "/api/research/entry-timing reachable", f"status {code}")
        if ok:
            self.check(rpt.get("version") == EXPECTED_VERSION_V28 and rpt.get("mode") == "forward-counterfactual-only", "report header")
            self.check(len(rpt.get("policies") or []) == POLICIES_TOTAL, "37 policies reported", str(len(rpt.get("policies") or [])))
            self.check(rpt.get("hindsight_metrics_included") is False, "no hindsight metrics in v2.8 report")
            self.check(rpt.get("cohorts_shown") == ["forward_v28"], "default cohort is forward_v28 only", str(rpt.get("cohorts_shown")))
            self.info("forward cohort", f"setups_observed={rpt.get('setups_observed')} resolved={rpt.get('resolved_setups')}")
        code, st = self.client.get("/api/research/entry-timing/status")
        self.check(code == 200 and isinstance(st, dict) and st.get("lab_version") == "2.8", "/api/research/entry-timing/status", f"status {code}")
        code, _ = self.client.get("/research/entry-timing")
        self.check(code == 200, "alias /research/entry-timing", f"status {code}")
        return h

    def policy_catalogue(self):
        """Canonical policy set as published by the service (status endpoint first, report as fallback)."""
        code, st = self.client.get("/api/research/entry-timing/status")
        if code == 200 and isinstance(st, dict) and isinstance(st.get("policies"), list):
            names = [p.get("policy") for p in st["policies"] if isinstance(p, dict) and p.get("policy")]
            if names:
                return "status", names
        code, rpt = self.client.get("/api/research/entry-timing")
        if code == 200 and isinstance(rpt, dict) and isinstance(rpt.get("policies"), list):
            names = [p.get("policy") for p in rpt["policies"] if isinstance(p, dict) and p.get("policy")]
            if names:
                return "report", names
        return None, []

    @staticmethod
    def _is_int(x):
        return isinstance(x, int) and not isinstance(x, bool)

    def audit_trace(self, setup_id):
        """Two separate verdicts: (1) structural validity of the trace (PASS/FAIL checks) and (2) whether the trace
        provides SUFFICIENT anti-hindsight acceptance evidence: closed trace, structurally valid, at least one fill
        whose Decision is frozen on its own tick, and at least one VALID later tick with an ask below that fill's ask.
        A structurally valid trace without that case is reported as INSUFFICIENT evidence (exit 3), which is not a
        failure of the engine."""
        verdict = {"setup_id": setup_id, "structural_ok": None, "evidence": EVIDENCE_INSUFFICIENT, "reason": None, "example": None}
        self.evidence.append(verdict)
        failures_before = self.failures
        code, tv = self.client.get("/api/research/entry-timing/trace?setup_id=" + quote(setup_id, safe=""))
        if not self.check(code == 200 and isinstance(tv, dict), "trace endpoint", f"status {code}"):
            verdict["structural_ok"] = False
            verdict["reason"] = "trace_unavailable"
            self._evidence_line(verdict)
            return verdict
        ticks = tv.get("ticks") or []
        decisions = tv.get("decisions") or []
        tr = tv.get("trace") or {}
        closed = tr.get("status") == "closed"
        self.info("trace", f"status={tr.get('status')} end_reason={tr.get('end_reason')} ticks={len(ticks)} decisions={len(decisions)} "
                           f"detection_ask={tr.get('detection_ask')} restarts={tr.get('restarts')}")

        # ---- (1a) seq integrity: every seq an integer, all unique, exactly 0..N-1 (duplicates are never collapsed)
        raw = [t.get("seq") for t in ticks]
        non_int = [x for x in raw if not self._is_int(x)]
        self.check(not non_int, "every tick has an integer seq", f"offending values {non_int[:5]}" if non_int else f"{len(raw)} ticks")
        ints = [x for x in raw if self._is_int(x)]
        counts = {}
        for x in ints:
            counts[x] = counts.get(x, 0) + 1
        dups = sorted(k for k, n in counts.items() if n > 1)
        self.check(not dups, "tick seq values are unique", f"duplicated seq {dups}" if dups else f"{len(ints)} unique")
        expected = list(range(len(raw)))
        exact = (not non_int) and (not dups) and sorted(ints) == expected
        missing = sorted(set(range((max(ints) + 1) if ints else 0)) - set(ints))
        self.check(exact, "tick seq is exactly 0..N-1 with no gaps",
                   f"N={len(raw)}" + (f" missing {missing[:5]}" if missing else "") + (f" max seq {max(ints)}" if ints else ""))
        by_seq = {}
        for t in ticks:
            if self._is_int(t.get("seq")) and t["seq"] not in by_seq:
                by_seq[t["seq"]] = t

        # ---- (1b) decisions against the published catalogue: unique, known, and complete when closed
        source, catalogue = self.policy_catalogue()
        cat = set(catalogue)
        if not self.check(bool(cat), "policy catalogue published by the service", f"source={source} size={len(cat)}"):
            self.check(False, "cannot verify the decision set without a catalogue")
        else:
            self.check(len(cat) == POLICIES_TOTAL and len(catalogue) == len(cat), f"catalogue has {POLICIES_TOTAL} unique policies", str(len(catalogue)))
        names = [d.get("policy") for d in decisions]
        pcounts = {}
        for n in names:
            pcounts[n] = pcounts.get(n, 0) + 1
        dup_pol = sorted(str(k) for k, n in pcounts.items() if n > 1)
        self.check(not dup_pol, "no policy has more than one decision", f"duplicated {dup_pol[:5]}" if dup_pol else f"{len(names)} decisions")
        if cat:
            unknown = sorted(str(n) for n in set(names) - cat)
            self.check(not unknown, "every decision policy belongs to the catalogue", f"unknown {unknown[:5]}" if unknown else "")
            if closed:
                missing_pol = sorted(cat - set(names))
                self.check(not missing_pol, "closed trace: every catalogue policy has a decision", f"missing {missing_pol[:5]}" if missing_pol else "")
                self.check(set(names) == cat and len(names) == len(cat), "closed trace: decision set equals the catalogue exactly",
                           f"{len(names)} decisions vs {len(cat)} policies")
        # ---- (1c) fills decided on their own tick and frozen there
        fills = [d for d in decisions if d.get("fill")]
        for d in fills:
            label = str(d.get("policy"))
            self.check(d.get("decided_seq") == d.get("entry_seq"), f"{label}: decided on its own fill tick",
                       f"decided_seq {d.get('decided_seq')} entry_seq {d.get('entry_seq')} ask {d.get('entry_ask_cents')} liq {d.get('liquidity')}")
            t = by_seq.get(d.get("entry_seq"))
            self.check(t is not None and t.get("ask_cents") == d.get("entry_ask_cents") and t.get("quote_valid") is True,
                       f"{label}: fill tick exists, is valid and carries the same ask")
            self.check(t is not None and d.get("decided_at") == t.get("tick_at") and d.get("entry_tick_at") == t.get("tick_at"),
                       f"{label}: Decision frozen at the fill tick timestamp",
                       f"decided_at {d.get('decided_at')} tick_at {t.get('tick_at') if t else None}")
        invalid = {}
        for t in ticks:
            if not t.get("quote_valid"):
                invalid[t.get("quote_invalid_reason")] = invalid.get(t.get("quote_invalid_reason"), 0) + 1
        self.info("invalid ticks by reason", json.dumps(invalid))
        verdict["structural_ok"] = self.failures == failures_before

        # ---- (2) acceptance evidence, only meaningful on a structurally valid closed trace
        if not verdict["structural_ok"]:
            verdict["reason"] = "structural_failure"
        elif not closed:
            verdict["reason"] = "trace_not_closed"
        elif not fills:
            verdict["reason"] = "no_fills"
        else:
            best = None
            for d in sorted(fills, key=lambda x: (x.get("entry_seq") if self._is_int(x.get("entry_seq")) else 10 ** 9)):
                later = [t for t in ticks if self._is_int(t.get("seq")) and t["seq"] > d["entry_seq"] and t.get("quote_valid") is True
                         and t.get("ask_cents") is not None and float(t["ask_cents"]) < float(d["entry_ask_cents"])]
                if later:
                    best = {"policy": d.get("policy"), "entry_seq": d.get("entry_seq"), "entry_ask_cents": d.get("entry_ask_cents"),
                            "later_cheaper_valid_ticks": len(later), "min_later_ask_cents": min(float(t["ask_cents"]) for t in later),
                            "decision_frozen": True}
                    break
            if best:
                verdict["evidence"] = EVIDENCE_SUFFICIENT
                verdict["example"] = best
            else:
                verdict["reason"] = "no_later_cheaper_valid_tick"
        self._evidence_line(verdict)
        return verdict

    def _evidence_line(self, verdict):
        if verdict["evidence"] == EVIDENCE_SUFFICIENT:
            e = verdict["example"]
            self.check(True, f"acceptance evidence SUFFICIENT for {verdict['setup_id']}",
                       f"{e['policy']} filled at seq {e['entry_seq']} ask {e['entry_ask_cents']}; {e['later_cheaper_valid_ticks']} valid later "
                       f"tick(s) cheaper (min {e['min_later_ask_cents']}) did not alter the frozen Decision")
        else:
            self.out(f"EVIDENCE INSUFFICIENT for {verdict['setup_id']} ({verdict['reason']}): not usable as final post-deploy "
                     f"acceptance; this is not a finding against the engine" + ("" if verdict["reason"] == "structural_failure" else
                     " - the trace is structurally valid; audit another closed trace with a fill and a later cheaper valid tick"))

    def watch(self, minutes, mode="v28"):
        """Observe /health every WATCH_POLL_SECONDS. Every abort condition is a FAIL that reaches the exit code."""
        start_t = self.now()
        end_t = start_t + float(minutes) * 60.0
        observations = 0
        first = None      # (t, requests) at the first budget-relevant observation
        prev = None
        start_429 = None
        budget = None
        while True:
            h = self._health()
            t = self.now()
            if h is not None:
                observations += 1
                src = h.get("sources") or {}
                kal = (src.get("kalshi") or {}).get("healthy")
                bin_ = (src.get("binance") or {}).get("healthy")
                line = f"obs {observations} t+{t - start_t:.0f}s ok={h.get('ok')} kalshi_healthy={kal} binance_healthy={bin_}"
                if h.get("ok") is not True:
                    self.abort(f"watch obs {observations}: /health ok={h.get('ok')} (rule F)")
                if kal is not True:
                    self.abort(f"watch obs {observations}: sources.kalshi.healthy={kal} (rule A)")
                if bin_ is not True:
                    self.abort(f"watch obs {observations}: sources.binance.healthy={bin_} (rule B)")
                if mode == "v28":
                    et, poller = self._poller(h)
                    if et is None:
                        self.check(False, f"watch obs {observations}: entry_timing_lab block missing")
                    else:
                        if budget is None:
                            budget = self._check_budget_published(poller, prefix="watch: ")
                        max_rps, burst = budget
                        v429 = self._check_429(poller, start_429, prefix=f"watch obs {observations}: ")
                        if start_429 is None:
                            start_429 = v429
                        req = int(poller.get("requests", 0) or 0)
                        if first is None or req < first[1]:
                            if first is not None:
                                self.info(f"watch obs {observations}: request counter decreased (restart?); re-anchoring budget window")
                            first = (t, req)
                            prev = (t, req)
                        elif max_rps > 0:
                            self._check_budget_delta(f"watch obs {observations}: cumulative request budget exceeded",
                                                     req - first[1], t - first[0], max_rps, burst)
                            self._check_budget_delta(f"watch obs {observations}: interval request budget exceeded",
                                                     req - prev[1], t - prev[0], max_rps, burst)
                            prev = (t, req)
                        line += (f" captured={et.get('setups_captured')} active={et.get('active_traces')} workers={poller.get('workers_alive')} "
                                 f"ticks={et.get('ticks_recorded')} requests={req} http_429={v429} rate_limited={poller.get('rate_limited_ticks')}")
                self.info(line)
            if t >= end_t:
                break
            self.sleep(WATCH_POLL_SECONDS)
        self.check(observations > 0, "watch: at least one observation", str(observations))
        self.check(not self.aborts, "watch: no abort condition observed", f"{len(self.aborts)} abort(s)")
        return observations

    def summary(self):
        insufficient = [v for v in self.evidence if v["evidence"] != EVIDENCE_SUFFICIENT]
        if self.failures == 0 and not insufficient:
            self.out("\nALL CHECKS PASSED")
            return EXIT_OK
        if self.failures == 0:
            reasons = ", ".join(f"{v['setup_id']}: {v['reason']}" for v in insufficient)
            self.out(f"\nALL STRUCTURAL CHECKS PASSED; ACCEPTANCE EVIDENCE INSUFFICIENT ({reasons}) - not usable as final acceptance")
            return EXIT_EVIDENCE_INSUFFICIENT
        self.out(f"\n{self.failures} CHECK(S) FAILED" + (f"; ABORT SIGNALS: {len(self.aborts)}" if self.aborts else ""))
        return EXIT_FAIL


def parse_args(argv):
    args = {"base": None, "watch": None, "trace": None, "mode": "v28", "accept_429": 0}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--watch":
            args["watch"] = float(argv[i + 1]); i += 2
        elif a == "--trace":
            args["trace"] = argv[i + 1]; i += 2
        elif a == "--accept-historical-429":
            args["accept_429"] = int(argv[i + 1]); i += 2
        elif a == "--baseline-v27":
            args["mode"] = "baseline-v27"; i += 1
        elif a == "--sources-only":
            args["mode"] = "sources-only"; i += 1
        elif a.startswith("--"):
            raise SystemExit(f"unknown option {a}")
        else:
            args["base"] = a; i += 1
    if not args["base"]:
        args["base"] = os.getenv("VIXION_BASE_URL", "")
    return args


def main(argv=None, client=None, out=print, now=time.time, sleep=time.sleep):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if not args["base"]:
        out(__doc__)
        return 2
    if client is None and not os.getenv("DASHBOARD_PASSWORD") and args["mode"] == "v28":
        out("warning: DASHBOARD_PASSWORD not set; authenticated routes will return 401")
    v = Verifier(client or HttpClient(args["base"]), out=out, now=now, sleep=sleep, accept_historical_429=args["accept_429"])
    out("abort rule: " + ABORT_RULE)
    if args["mode"] == "v28":
        v.verify_v28()
        if args["trace"]:
            v.audit_trace(args["trace"])
    else:
        v.verify_baseline(require_v27=(args["mode"] == "baseline-v27"))
    if args["watch"] is not None:
        v.watch(args["watch"], mode=("v28" if args["mode"] == "v28" else "sources"))
    return v.summary()


if __name__ == "__main__":
    sys.exit(main())
