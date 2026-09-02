#!/usr/bin/env python3
"""VIXION 2.8 — Executable Entry Timing Lab (forward-counterfactual-only overlay).

Layering: app.py (2.6 production engine) -> vixion_v27.py (observability overlay)
-> vixion_v28.py (this module). app.py, vixion_v27.py and launcher_v27.py stay
byte-for-byte untouched. This module only adds:

  * a dedicated ~1 Hz Kalshi quote poller for setups that the production
    ShadowLedger has already recorded (basis_ok setups only);
  * an ONLINE policy engine: every policy is a small state machine
    (WAITING -> FILLED | EXPIRED | TIMEOUT | REJECTED). A Decision is created
    exactly once, is immutable, and is written to disk before the next quote is
    requested. Later ticks are never dispatched to a terminal policy.
  * append-only persistence (ticks / decisions / v2.7 replay) + a small index;
  * read-only research endpoints and a `/health` block.

It never places orders, never changes thresholds, never mutates ledger records,
never sends Telegram and never feeds a production decision path.
"""
import json
import os
import socket
import statistics
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

import app as core
import vixion_v27 as v27

APP_VERSION = "2.8-executable-entry-timing-lab"
LAB_VERSION = "2.8"
LAB_MODE = "forward-counterfactual-only"
SCHEMA_VERSION = 2

# Contractual invalid-quote reasons (normalised; never raw HTTP codes other than 429).
INVALID_REASONS = ("no_ask", "out_of_range", "crossed", "stale", "ineligible", "unfillable_1_contract",
                   "http_429", "http_5xx", "http_4xx", "http_other", "timeout", "net_error", "parse_error", "backoff",
                   "rate_limited")
LIQ_VERIFIED, LIQ_UNVERIFIED = "verified", "unverified"

COHORT_FORWARD = "forward_v28"
COHORT_V27 = "trace_v27"
COHORT_BACKFILL = "ledger_backfill"
COHORTS = (COHORT_FORWARD, COHORT_V27, COHORT_BACKFILL)

# Only setups that production considers structurally valid AND basis_ok are traced.
TRACE_STATUSES = ("SHADOW CONFIRMED", "SHADOW ENTRY ZONE", "SHADOW PRICE HIGH")

WAITING, FILLED, EXPIRED, TIMEOUT, REJECTED, NOT_SIMULABLE = (
    "WAITING", "FILLED", "EXPIRED", "TIMEOUT", "REJECTED", "NOT_SIMULABLE")
TERMINAL_STATES = (FILLED, EXPIRED, TIMEOUT, REJECTED)

DELAY_SECONDS = [5, 10, 15, 20, 30, 45, 60]
THRESHOLD_CENTS = [95, 94, 93, 92, 91, 90, 88, 85]
PULLBACK_CENTS = [0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0]
HYBRID_THRESHOLDS = [(10, 95), (20, 95), (30, 95), (10, 94), (20, 94), (30, 94), (10, 93), (20, 93), (30, 93)]
HYBRID_PULLBACKS = [(15, 1.0), (30, 1.0), (15, 2.0), (30, 2.0)]

ASK_BUCKETS = v27.ASK_BUCKETS
DISTANCE_EXCESS_BUCKETS = [("0-4.9", 0.0, 5.0), ("5-9.9", 5.0, 10.0), ("10-19.9", 10.0, 20.0), ("20+", 20.0, 1e9)]
BASIS_GAP_BUCKETS = [("0-1.9", 0.0, 2.0), ("2-3.9", 2.0, 4.0), ("4-5.9", 4.0, 6.0), ("6-8", 6.0, 1e9)]
SPREAD_BUCKET_EDGES = [("<=1", 1.0), ("1-2", 2.0), ("2-3", 3.0)]  # right-closed: <=1, (1,2], (2,3], >3
SEGMENT_KEYS = ("asset", "minute", "side", "ask_bucket", "distance_bucket", "basis_bucket", "spread_bucket")
SORT_KEYS = ("net_pnl_per_eligible_setup", "wilson_low_pct", "fill_rate_pct", "roi_pct")

ELIGIBILITY_DEFINITION = (
    "production minute-lock: a quote can fill only while last_completed_minute == signal minute "
    "(quote_fetched_at < window_start + (minute+1)*60s) and basis_ok is True (frozen at detection). "
    "'not entered' is per-policy state (FILLED); production's own entry is reported separately as production_actual."
)


def _env_flag(name, default="1"):
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes")


def _env_float(name, default):
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _iso_to_ms(value):
    try:
        dt = core.parse_iso(value)
        return int(dt.timestamp() * 1000) if dt else None
    except Exception:
        return None


def _fmt_num(x):
    x = float(x)
    return str(int(x)) if x == int(x) else str(x)


def _mean(values):
    vals = [float(v) for v in values if v is not None]
    return (sum(vals) / len(vals)) if vals else None


def _median(values):
    vals = [float(v) for v in values if v is not None]
    return statistics.median(vals) if vals else None


def _round(x, nd=4):
    return None if x is None else round(float(x), nd)


def sample_status(n):
    n = int(n or 0)
    if n < 30:
        return "INSUFFICIENT_SAMPLE"
    if n < 100:
        return "EARLY_RESEARCH"
    if n < 200:
        return "RESEARCH_SAMPLE"
    return "REVIEW_ELIGIBLE"


def _bucket(value, buckets):
    if value is None:
        return "unknown"
    try:
        v = float(value)
    except Exception:
        return "unknown"
    for label, lo, hi in buckets:
        if lo <= v < hi:
            return label
    return "unknown"


def spread_bucket(value):
    """<=1 | (1,2] | (2,3] | >3 (right-closed on every edge)."""
    if value is None:
        return "unknown"
    try:
        v = float(value)
    except Exception:
        return "unknown"
    for label, edge in SPREAD_BUCKET_EDGES:
        if v <= edge + 1e-9:
            return label
    return ">3"


def normalise_http_reason(http_status, error):
    """Map a fetch outcome onto the contractual reason schema (None = usable response)."""
    if error:
        return error if error in INVALID_REASONS else "net_error"
    try:
        code = int(http_status)
    except Exception:
        return "http_other"
    if code == 200:
        return None
    if code == 429:
        return "http_429"
    if 500 <= code <= 599:
        return "http_5xx"
    if 400 <= code <= 499:
        return "http_4xx"
    return "http_other"


# ---------------------------------------------------------------------------
# Policy catalogue: pure data. Every executable policy belongs to one of seven
# families; no per-policy code exists outside PolicyState._match.
# ---------------------------------------------------------------------------
def build_policy_specs():
    specs = [{"policy": "entry_at_detection", "family": "detection", "params": {}, "baseline": True}]
    for t in DELAY_SECONDS:
        specs.append({"policy": f"delay_{t}s", "family": "delay", "params": {"delay_s": float(t)}})
    for c in THRESHOLD_CENTS:
        specs.append({"policy": f"ask_le_{c}c", "family": "threshold", "params": {"ask_le": float(c)}})
    for c in PULLBACK_CENTS:
        specs.append({"policy": f"pullback_{_fmt_num(c)}c", "family": "pullback", "params": {"pullback": float(c)}})
    for t, c in HYBRID_THRESHOLDS:
        specs.append({"policy": f"max{t}s_{c}c", "family": "hybrid_threshold",
                      "params": {"max_s": float(t), "ask_le": float(c)}})
    for t, c in HYBRID_PULLBACKS:
        specs.append({"policy": f"max{t}s_pullback{_fmt_num(c)}c", "family": "hybrid_pullback",
                      "params": {"max_s": float(t), "pullback": float(c)}})
    specs.append({"policy": "production_baseline", "family": "production", "params": {}, "baseline": True})
    for s in specs:
        s.setdefault("baseline", False)
    return specs


POLICY_SPECS = build_policy_specs()
POLICY_NAMES = [s["policy"] for s in POLICY_SPECS]
BACKFILL_SIMULABLE = ("entry_at_detection", "production_baseline")


@dataclass(frozen=True)
class Decision:
    """Immutable terminal decision of one policy for one setup. Created once; never mutated.
    Contains NO outcome field: PnL is joined at reporting time from the ledger outcome."""
    schema_version: int
    cohort: str
    setup_id: str
    policy: str
    family: str
    params: tuple
    state: str
    fill: bool
    decided_at: str
    decided_seq: int
    elapsed_at_decision_s: float
    detection_ask_cents: float
    guardrail_rejections: int
    ticks_seen: int
    trigger_rule: str
    no_fill_reason: str
    entry_seq: int
    entry_tick_at: str
    entry_quote_fetched_at: str
    delay_s: float
    entry_ask_cents: float
    entry_bid_cents: float
    ask_size: float
    fillable_1_contract: bool
    fee_model_cents: float
    fee_effective_cents: float
    effective_cost_cents: float
    economically_valid: bool
    improvement_cents: float
    liquidity: str
    tick_resolution: str

    def to_dict(self):
        d = asdict(self)
        d["params"] = dict(self.params)
        return d

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        d["params"] = tuple(sorted((k, v) for k, v in (d.get("params") or {}).items()))
        fields = cls.__dataclass_fields__
        return cls(**{k: d.get(k) for k in fields})


def _decision_bytes(decision):
    return json.dumps(decision.to_dict(), sort_keys=True, separators=(",", ":"), default=str)


class PolicyState:
    """One policy's online state machine. `on_tick` is only ever called while WAITING."""
    __slots__ = ("spec", "state", "guardrail_rejections", "ticks_seen", "decision")

    def __init__(self, spec):
        self.spec = spec
        self.state = WAITING
        self.guardrail_rejections = 0
        self.ticks_seen = 0
        self.decision = None

    def _match(self, tick, ctx):
        fam = self.spec["family"]
        p = self.spec["params"]
        ask = tick["ask_cents"]
        if fam == "detection":
            return True, "first valid quote after detection"
        if fam == "delay":
            if tick["elapsed_s"] >= p["delay_s"]:
                return True, f"first valid quote with elapsed {tick['elapsed_s']:.1f}s >= {_fmt_num(p['delay_s'])}s"
            return False, ""
        if fam in ("threshold", "hybrid_threshold"):
            if ask <= p["ask_le"] + 1e-9:
                rule = f"ask<={_fmt_num(p['ask_le'])} at seq {tick['seq']} (ask {ask:.2f}, elapsed {tick['elapsed_s']:.1f}s"
                rule += f" <= {_fmt_num(p['max_s'])}s)" if fam == "hybrid_threshold" else ")"
                return True, rule
            return False, ""
        if fam in ("pullback", "hybrid_pullback"):
            anchor = ctx.get("detection_ask")
            if anchor is None:
                return False, ""
            target = float(anchor) - p["pullback"]
            if ask <= target + 1e-9:
                rule = (f"ask<=detection_ask-{_fmt_num(p['pullback'])} ({anchor:.2f}-{_fmt_num(p['pullback'])}={target:.2f}) "
                        f"at seq {tick['seq']} (ask {ask:.2f}, elapsed {tick['elapsed_s']:.1f}s")
                rule += f" <= {_fmt_num(p['max_s'])}s)" if fam == "hybrid_pullback" else ")"
                return True, rule
            return False, ""
        if fam == "production":
            edge = tick.get("edge_points")
            if edge is not None and float(edge) >= float(ctx["edge_min_points"]) - 1e-9:
                return True, (f"cons-ask-fee_effective={edge:.2f} >= edge_min {ctx['edge_min_points']:.1f} at seq {tick['seq']} "
                              f"(ask {ask:.2f}) [ShadowLedger.observe_state rule]")
            return False, ""
        return False, ""

    def _base(self, tick, ctx, state, fill, rule, reason):
        return dict(
            schema_version=SCHEMA_VERSION, cohort=ctx["cohort"], setup_id=ctx["setup_id"],
            policy=self.spec["policy"], family=self.spec["family"],
            params=tuple(sorted(self.spec["params"].items())), state=state, fill=fill,
            decided_at=tick["tick_at"], decided_seq=int(tick["seq"]),
            elapsed_at_decision_s=_round(tick["elapsed_s"], 3),
            detection_ask_cents=ctx.get("detection_ask"), guardrail_rejections=self.guardrail_rejections,
            ticks_seen=self.ticks_seen, trigger_rule=rule, no_fill_reason=reason,
            entry_seq=None, entry_tick_at=None, entry_quote_fetched_at=None, delay_s=None,
            entry_ask_cents=None, entry_bid_cents=None, ask_size=None, fillable_1_contract=None,
            fee_model_cents=None, fee_effective_cents=None, effective_cost_cents=None,
            economically_valid=None, improvement_cents=None, liquidity=None,
            tick_resolution=ctx.get("tick_resolution", "1s"),
        )

    def _terminal(self, tick, ctx, state, reason, rule):
        if self.decision is not None:
            return None
        self.state = state
        self.decision = Decision(**self._base(tick, ctx, state, False, rule, reason))
        return self.decision

    def _fill(self, tick, ctx, rule):
        if self.decision is not None:
            return None
        self.state = FILLED
        base = self._base(tick, ctx, FILLED, True, rule, None)
        det = ctx.get("detection_ask")
        base.update(
            entry_seq=int(tick["seq"]), entry_tick_at=tick["tick_at"],
            entry_quote_fetched_at=tick["quote_fetched_at"], delay_s=_round(tick["elapsed_s"], 3),
            entry_ask_cents=tick["ask_cents"], entry_bid_cents=tick.get("bid_cents"),
            ask_size=tick.get("ask_size"), fillable_1_contract=tick.get("fillable_1_contract"),
            fee_model_cents=tick.get("fee_model_cents"), fee_effective_cents=tick.get("fee_effective_cents"),
            effective_cost_cents=tick.get("effective_cost_cents"), economically_valid=tick.get("economically_valid"),
            improvement_cents=_round(float(det) - float(tick["ask_cents"]), 4) if det is not None else None,
            liquidity=(LIQ_VERIFIED if tick.get("fillable_1_contract") is True else LIQ_UNVERIFIED),
        )
        self.decision = Decision(**base)
        return self.decision

    def on_tick(self, tick, ctx):
        if self.state != WAITING:
            return None
        fam = self.spec["family"]
        p = self.spec["params"]
        elapsed = tick["elapsed_s"]
        if fam in ("hybrid_threshold", "hybrid_pullback") and elapsed is not None and elapsed > p["max_s"] + 1e-9:
            return self._terminal(tick, ctx, TIMEOUT, "timeout",
                                  f"elapsed {elapsed:.1f}s > {_fmt_num(p['max_s'])}s while WAITING")
        if not tick["entry_eligible"]:
            reason = "eligibility_ended" if ctx.get("detection_ask") is not None else "no_valid_quote"
            return self._terminal(tick, ctx, EXPIRED, reason,
                                  f"eligibility ended at seq {tick['seq']} ({tick.get('quote_invalid_reason') or 'ineligible'})")
        if not tick["quote_valid"]:
            return None
        matched, rule = self._match(tick, ctx)
        if not matched:
            return None
        if not tick["economically_valid"]:
            if fam in ("detection", "delay"):
                return self._terminal(tick, ctx, REJECTED, "rejected_effective_cost_ge_100",
                                      rule + f" but effective_cost {tick['effective_cost_cents']:.2f} >= 100")
            self.guardrail_rejections += 1
            return None
        return self._fill(tick, ctx, rule)


def build_tick(ctx, seq, tick_ms, quote_ms, ask, bid=None, ask_size=None, bid_size=None,
               source="orderbook", http_status=200, error="", quote_age_s=None, underlying=None):
    """Pure tick constructor shared by the live poller, the v2.7 replay and the tests."""
    ask = _round(ask, 2) if ask is not None else None
    bid = _round(bid, 2) if bid is not None else None
    elapsed = max(0.0, (int(quote_ms) - int(ctx["detection_ms"])) / 1000.0)
    minute_at_quote = int((int(quote_ms) - int(ctx["window_start_ms"])) // 60000)
    eligible = bool(int(quote_ms) < int(ctx["eligibility_end_ms"]))
    fee_model = fee_eff = eff_cost = econ = edge = None
    if ask is not None:
        fee_model = core.kalshi_trade_fee_cents(ask, 1.0)
        fee_eff = core.effective_fee_cents(ask, {"fee_buffer_cents": ctx["fee_buffer_cents"]}, 1.0)
        eff_cost = _round(ask + fee_eff, 4)
        econ = bool(eff_cost < 100.0)
        edge = _round(float(ctx["cons_pct"]) - ask - fee_eff, 4)
    fillable = None if ask_size is None else bool(float(ask_size) >= 1.0)
    reason = normalise_http_reason(http_status, error)
    if reason is None:
        if ask is None:
            reason = "no_ask"
        elif not (1.0 <= ask <= 99.0):
            reason = "out_of_range"
        elif bid is not None and bid > ask + 1e-9:
            reason = "crossed"
        elif fillable is False:
            reason = "unfillable_1_contract"  # a known size < 1 contract can never produce FILLED
        elif quote_age_s is not None and float(quote_age_s) > float(ctx.get("quote_max_age_s", 5.0)):
            reason = "stale"
        elif not eligible:
            reason = "ineligible"
    tick = {
        "schema_version": SCHEMA_VERSION, "cohort": ctx["cohort"], "setup_id": ctx["setup_id"],
        "asset": ctx["asset"], "seq": int(seq), "tick_at": _ms_to_iso(tick_ms), "tick_ms": int(tick_ms),
        "quote_fetched_at": _ms_to_iso(quote_ms), "quote_ms": int(quote_ms),
        "elapsed_s": _round(elapsed, 3), "minute_at_quote": minute_at_quote,
        "seconds_to_eligibility_end": _round((int(ctx["eligibility_end_ms"]) - int(quote_ms)) / 1000.0, 3),
        "entry_eligible": eligible, "ask_cents": ask, "bid_cents": bid,
        "spread_cents": _round(ask - bid, 4) if ask is not None and bid is not None else None,
        "ask_size": ask_size, "bid_size": bid_size,
        "fillable_1_contract": fillable,
        "liquidity": (LIQ_VERIFIED if fillable is True else LIQ_UNVERIFIED),
        "fee_model_cents": fee_model, "fee_effective_cents": fee_eff, "effective_cost_cents": eff_cost,
        "economically_valid": econ, "edge_points": edge,
        "production_max_buy_cents": ctx.get("production_max_buy_cents"), "cons_pct": ctx["cons_pct"],
        "basis_ok": True, "basis_gap_bp": ctx.get("target_gap_bp"),
        "quote_valid": reason is None, "quote_invalid_reason": reason, "quote_age_s": quote_age_s,
        "http_status": http_status, "source": source,
    }
    if underlying:
        tick.update({"live_price": underlying.get("live_price"), "distance_bp": underlying.get("distance_bp"),
                     "underlying_age_s": underlying.get("underlying_age_s")})
    return tick


class TraceRuntime:
    """One setup's online evaluation. Ticks are dispatched only to WAITING policies;
    the detection anchor (first valid quote) is set exactly once before dispatch."""

    def __init__(self, ctx, decisions=None):
        self.ctx = dict(ctx)
        self.policies = [PolicyState(spec) for spec in POLICY_SPECS]
        self.last_seq = -1
        self.ticks = 0
        self.valid_ticks = 0
        self.duplicates = 0
        self.closed = False
        self.end_reason = None
        self.unpersisted = []  # terminal Decisions created but not yet durable (fail-closed queue)
        for d in decisions or []:
            self._rehydrate(d)

    def next_seq(self):
        return self.last_seq + 1

    def _rehydrate(self, d):
        dec = d if isinstance(d, Decision) else Decision.from_dict(d)
        for ps in self.policies:
            if ps.spec["policy"] == dec.policy and ps.decision is None:
                ps.decision = dec
                ps.state = dec.state
                ps.guardrail_rejections = int(dec.guardrail_rejections or 0)
                ps.ticks_seen = int(dec.ticks_seen or 0)
        if dec.detection_ask_cents is not None and self.ctx.get("detection_ask") is None:
            self.ctx["detection_ask"] = dec.detection_ask_cents

    def waiting(self):
        return [ps for ps in self.policies if ps.state == WAITING]

    def on_tick(self, tick):
        if self.closed:
            return []
        if int(tick["seq"]) <= self.last_seq:
            self.duplicates += 1
            return []
        self.last_seq = int(tick["seq"])
        self.ticks += 1
        if tick["quote_valid"]:
            self.valid_ticks += 1
            if self.ctx.get("detection_ask") is None:
                self.ctx["detection_ask"] = tick["ask_cents"]
                self.ctx["detection_seq"] = tick["seq"]
                self.ctx["detection_bid"] = tick.get("bid_cents")
                self.ctx["detection_spread"] = tick.get("spread_cents")
                self.ctx["detection_quote_fetched_at"] = tick["quote_fetched_at"]
                self.ctx["detection_quote_delay_s"] = tick["elapsed_s"]
        out = []
        for ps in self.policies:
            if ps.state != WAITING:
                continue
            ps.ticks_seen = self.ticks  # ticks dispatched to this (still WAITING) policy so far
            d = ps.on_tick(tick, self.ctx)
            if d is not None:
                out.append(d)
        return out

    def close(self, reason, close_ms):
        """Only WAITING policies are finalised (EXPIRED). Never creates a fill."""
        if self.closed:
            return []
        self.closed = True
        self.end_reason = reason
        pseudo = {"seq": self.last_seq, "tick_at": _ms_to_iso(close_ms),
                  "elapsed_s": _round(max(0.0, (int(close_ms) - int(self.ctx["detection_ms"])) / 1000.0), 3)}
        out = []
        for ps in self.policies:
            if ps.state != WAITING:
                continue
            why = "eligibility_ended" if self.ctx.get("detection_ask") is not None else "no_valid_quote"
            ps.ticks_seen = self.ticks
            d = ps._terminal(pseudo, self.ctx, EXPIRED, why, f"trace closed ({reason}) while WAITING")
            if d is not None:
                out.append(d)
        return out

    def decisions(self):
        return [ps.decision for ps in self.policies if ps.decision is not None]


def compact_from_decision(d):
    d = d.to_dict() if isinstance(d, Decision) else d
    return {"state": d.get("state"), "fill": bool(d.get("fill")), "reason": d.get("no_fill_reason"),
            "ask": d.get("entry_ask_cents"), "fee": d.get("fee_effective_cents"), "cost": d.get("effective_cost_cents"),
            "delay": d.get("delay_s"), "impr": d.get("improvement_cents"), "guard": d.get("guardrail_rejections"),
            "seq": d.get("decided_seq"), "det": d.get("detection_ask_cents"), "liq": d.get("liquidity")}


# ---------------------------------------------------------------------------
# The lab
# ---------------------------------------------------------------------------
class EntryTimingLab:
    """Durable state = three append-only logs (traces / ticks / decisions); the JSON index is a
    rebuildable cache. Every write path is fail-closed: a tick that is not durable is never
    dispatched, a Decision that is not durable blocks its trace (no further fetch), and recovery
    replays durable ticks onto the policies that were still WAITING."""

    def __init__(self, primary, alt, data_dir=None, clock=None, fetch_fn=None, autostart=True):
        self.primary = primary
        self.alt = alt
        self.clock = clock or time.time
        self.fetch_fn = fetch_fn or self._fetch_http
        self.data_dir = Path(data_dir) if data_dir else core.DATA_DIR
        self.enabled = _env_flag("ENTRY_TIMING_ENABLED", "1")
        self.autostart = bool(autostart)
        self.tick_seconds = max(0.5, _env_float("ENTRY_TIMING_TICK_SECONDS", 1.0))
        self.max_traces = max(1, int(_env_float("ENTRY_TIMING_MAX_TRACES", 6)))
        self.tick_source = (os.getenv("ENTRY_TIMING_TICK_SOURCE", "orderbook") or "orderbook").strip().lower()
        if self.tick_source not in ("orderbook", "market"):
            self.tick_source = "orderbook"
        self.quote_max_age_s = _env_float("ENTRY_TIMING_QUOTE_MAX_AGE_S", 5.0)
        self.http_timeout = _env_float("ENTRY_TIMING_HTTP_TIMEOUT_S", 4.0)
        self.join_seconds = max(5.0, _env_float("ENTRY_TIMING_JOIN_SECONDS", 20.0))
        self.replay_enabled = _env_flag("ENTRY_TIMING_V27_REPLAY", "1")
        self.replay_seconds = max(60.0, _env_float("ENTRY_TIMING_V27_REPLAY_SECONDS", 600.0))
        # Global request budget shared by every worker: the lab must never push production's Kalshi
        # usage over the venue rate limit. 0 disables the budget (Increment 1.1 behaviour).
        self.max_rps = max(0.0, _env_float("ENTRY_TIMING_MAX_RPS", 2.0))
        self.burst = max(1, int(_env_float("ENTRY_TIMING_BURST", 2)))
        self._budget_lock = threading.Lock()
        self._tokens = float(self.burst)
        self._tokens_at = (clock or time.time)()
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.traces_log = v27.DecisionTrace(self.data_dir / "entry_timing_traces_v28.jsonl")
        self.ticks_log = v27.DecisionTrace(self.data_dir / "entry_timing_ticks_v28.jsonl")
        self.decisions_log = v27.DecisionTrace(self.data_dir / "entry_timing_decisions_v28.jsonl")
        self.replay_log = v27.DecisionTrace(self.data_dir / "entry_timing_replay_v27.jsonl")
        self.index_path = self.data_dir / "entry_timing_lab_v28.json"
        self.runtimes = {}
        self.workers = {}
        self.results = {}
        self.replay_results = {}
        self.dirty = False
        self.recovered = False
        self.last_save = 0.0
        self.last_maintenance = 0.0
        self.last_replay = 0.0
        self.thread = None
        self._replay_thread = None
        self.index = {
            "schema_version": SCHEMA_VERSION, "lab_version": LAB_VERSION, "mode": LAB_MODE,
            "created_at": _now_iso(), "first_boot_at": _now_iso(), "last_boot_at": _now_iso(),
            "traces": {}, "outcomes": {}, "replay_v27": {"cursor": 0, "done": []}, "replay_traces": {},
            "recovery": {}, "counters": {k: 0 for k in (
                "setups_seen", "setups_captured", "skipped_disabled", "skipped_status", "skipped_basis",
                "skipped_no_market", "skipped_late", "skipped_cap", "skipped_duplicate", "ticks_recorded",
                "backoff_ticks", "decisions_written", "fills", "closed", "restart_expired", "outcomes_joined",
                "requests", "rate_limited_ticks", "http_2xx", "http_429", "http_5xx", "http_4xx", "http_other", "timeouts", "net_errors",
                "parse_errors", "no_ask", "source_fallbacks", "hook_errors", "save_errors",
                "tick_write_failures", "decision_write_failures", "decision_write_retries", "trace_log_write_failures",
                "seq_reuse_rejected", "orphan_ticks", "recovered_traces", "reconstructed_decisions", "replayed_ticks",
                "replay_setups", "replay_errors")},
        }
        # Read-only loads are harmless even when disabled; nothing below writes unless enabled.
        self._load_index()
        self._load_results()
        if self.enabled:
            self._recover()
            self.recovered = True
            self.save(force=True)
            if self.autostart:
                for sid in list(self.runtimes):
                    self._spawn_worker(sid)
                self.thread = threading.Thread(target=self.run, name="entry-timing-lab", daemon=True)
                self.thread.start()

    # ------------------------------------------------------------- persistence (index cache)
    def _load_index(self):
        try:
            if self.index_path.exists():
                old = core.load_json(self.index_path)
                if isinstance(old, dict):
                    created = old.get("created_at") or self.index["created_at"]
                    first_boot = old.get("first_boot_at") or created
                    fresh = self.index
                    self.index = old
                    self.index["created_at"] = created
                    self.index["first_boot_at"] = first_boot
                    self.index["last_boot_at"] = fresh["last_boot_at"]
                    for k in ("traces", "outcomes", "replay_traces", "recovery", "counters"):
                        self.index.setdefault(k, {})
                    self.index.setdefault("replay_v27", {"cursor": 0, "done": []})
                    for k, v in fresh["counters"].items():
                        self.index["counters"].setdefault(k, v)
        except Exception as exc:
            print("entry timing lab index load:", exc, flush=True)

    def _count(self, key, amount=1):
        with self.lock:
            c = self.index["counters"]
            c[key] = int(c.get(key, 0)) + amount
            self.dirty = True

    def save(self, force=False):
        if not self.enabled:
            return
        try:
            with self.lock:
                if not force and not self.dirty:
                    return
                core.save_json(self.index_path, self.index)
                self.dirty = False
                self.last_save = self.clock()
        except Exception as exc:
            with self.lock:
                self.index["counters"]["save_errors"] = int(self.index["counters"].get("save_errors", 0)) + 1
            print("entry timing lab save:", exc, flush=True)

    def _iter_jsonl(self, path):
        """Chronological read across a DecisionTrace rotation: `<name>.jsonl.1` (older) then `<name>.jsonl`."""
        path = Path(path)
        for part in (path.with_suffix(".jsonl.1"), path):
            try:
                if not part.exists():
                    continue
                with open(part, "r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            yield json.loads(line)
                        except Exception:
                            continue
            except Exception:
                continue

    def _load_results(self):
        for rec in self._iter_jsonl(self.decisions_log.path):
            sid, pol = rec.get("setup_id"), rec.get("policy")
            if sid and pol:
                self.results.setdefault(sid, {})[pol] = compact_from_decision(rec)
        for rec in self._iter_jsonl(self.replay_log.path):
            sid, pol = rec.get("setup_id"), rec.get("policy")
            if sid and pol:
                self.replay_results.setdefault(sid, {})[pol] = compact_from_decision(rec)

    def _index_entry(self, sid, ctx, opened_at):
        return {"setup_id": sid, "cohort": COHORT_FORWARD, "status": "open", "ctx": ctx,
                "started_at": opened_at, "seq_last": -1, "ticks": 0, "valid_ticks": 0,
                "detection_ask": None, "detection_bid": None, "detection_spread": None,
                "detection_quote_delay_s": None, "end_reason": None, "closed_at": None,
                "source": self.tick_source, "consecutive_failures": 0, "consecutive_parse_errors": 0,
                "backoff_until_ms": 0, "restarts": 0, "resolved": False, "blocked": False, "last_tick_at": None}

    # ------------------------------------------------------------- recovery (synchronous, before any fetch)
    def _recover(self):
        """Source of truth = logs. Terminal Decisions are authoritative; durable ticks are replayed
        chronologically onto policies still WAITING (this reconstructs any Decision lost between a
        tick append and its Decision append). No fetch and no outcome join happen before this returns."""
        now_ms = int(self.clock() * 1000)
        opens, closes = {}, {}
        for rec in self._iter_jsonl(self.traces_log.path):
            sid = rec.get("setup_id")
            if not sid:
                continue
            if rec.get("event") == "open" and isinstance(rec.get("ctx"), dict):
                opens[sid] = rec
            elif rec.get("event") == "close":
                closes[sid] = rec
        with self.lock:
            for sid, tr in self.index["traces"].items():  # index-only traces (cache ahead of an unreadable log)
                if sid not in opens and isinstance(tr.get("ctx"), dict):
                    opens[sid] = {"setup_id": sid, "ctx": tr["ctx"], "opened_at": tr.get("started_at")}
                    if tr.get("status") == "closed":
                        closes.setdefault(sid, {"end_reason": tr.get("end_reason"), "closed_at": tr.get("closed_at")})
            for sid, rec in opens.items():
                tr = self.index["traces"].get(sid)
                if tr is None:
                    tr = self._index_entry(sid, rec["ctx"], rec.get("opened_at") or _now_iso())
                    self.index["traces"][sid] = tr
                    self._count("recovered_traces")
                if sid in closes and tr.get("status") != "closed":
                    tr["status"] = "closed"
                    tr["end_reason"] = closes[sid].get("end_reason") or tr.get("end_reason") or "closed"
                    tr["closed_at"] = closes[sid].get("closed_at") or tr.get("closed_at")
            open_sids = [sid for sid in opens if sid not in closes]
        decisions = {sid: [] for sid in open_sids}
        for rec in self._iter_jsonl(self.decisions_log.path):
            sid = rec.get("setup_id")
            if sid in decisions:
                decisions[sid].append(rec)
        ticks = {sid: [] for sid in open_sids}
        for t in self._iter_jsonl(self.ticks_log.path):
            sid = t.get("setup_id")
            if sid in ticks:
                ticks[sid].append(t)
            elif sid and sid not in opens:
                self._count("orphan_ticks")
        summary = {"at": _now_iso(), "open_traces": len(open_sids), "replayed_ticks": 0, "reconstructed_decisions": 0,
                   "closed_on_restart": 0}
        for sid in open_sids:
            tr = self.index["traces"][sid]
            ctx = tr["ctx"]
            rt = TraceRuntime(ctx, decisions=decisions[sid])
            seen = set()
            for t in sorted(ticks[sid], key=lambda x: int(x.get("seq", -1))):
                seq = int(t.get("seq", -1))
                if seq < 0 or seq in seen:
                    continue
                seen.add(seq)
                produced = rt.on_tick(t)  # dispatched only to WAITING policies; terminal ones are skipped
                summary["replayed_ticks"] += 1
                for d in produced:
                    summary["reconstructed_decisions"] += 1
                    self._persist_decision(sid, rt, d)
            if seen:
                rt.last_seq = max(seen)  # never reuse a durable seq
            tr["restarts"] = int(tr.get("restarts", 0)) + 1
            self._sync_index_from_runtime(tr, rt)
            self.runtimes[sid] = rt
            if now_ms >= int(ctx["eligibility_end_ms"]):
                self._close_trace(sid, rt, tr, "restart_expired", now_ms)
                self._count("restart_expired")
                summary["closed_on_restart"] += 1
            self.dirty = True
        self._count("replayed_ticks", summary["replayed_ticks"])
        self._count("reconstructed_decisions", summary["reconstructed_decisions"])
        with self.lock:
            self.index["recovery"] = summary

    def _sync_index_from_runtime(self, tr, rt):
        tr["seq_last"] = rt.last_seq
        tr["ticks"] = rt.ticks
        tr["valid_ticks"] = rt.valid_ticks
        tr["duplicates"] = rt.duplicates
        tr["blocked"] = bool(rt.unpersisted)
        if tr.get("detection_ask") is None and rt.ctx.get("detection_ask") is not None:
            tr["detection_ask"] = rt.ctx["detection_ask"]
            tr["detection_bid"] = rt.ctx.get("detection_bid")
            tr["detection_spread"] = rt.ctx.get("detection_spread")
            tr["detection_quote_delay_s"] = rt.ctx.get("detection_quote_delay_s")

    # ------------------------------------------------------------- trace start
    def start_trace(self, state):
        """Called (via hook) right after ShadowLedger.record_setup returned True. Never raises."""
        try:
            return self._start_trace(state)
        except Exception as exc:
            self._count("hook_errors")
            print("entry timing lab start:", exc, flush=True)
            return None

    def _start_trace(self, s):
        self._count("setups_seen")
        if not self.enabled:
            self._count("skipped_disabled")
            return None
        asset, window = s.get("asset"), s.get("window_start_utc")
        sid = f"{asset}|{window}"
        if s.get("status") not in TRACE_STATUSES or s.get("side") not in ("UP", "DOWN") \
                or s.get("last_completed_minute") not in (7, 8, 9, 10):
            self._count("skipped_status")
            return None
        if s.get("basis_ok") is not True:
            self._count("skipped_basis")
            return None
        if not s.get("market_ticker"):
            self._count("skipped_no_market")
            return None
        ws_ms = _iso_to_ms(window)
        det_ms = _iso_to_ms(s.get("updated_at"))
        if ws_ms is None or det_ms is None or s.get("conservative_fair_pct") is None:
            self._count("skipped_status")
            return None
        minute = int(s["last_completed_minute"])
        elig_end = ws_ms + (minute + 1) * 60000
        now_ms = int(self.clock() * 1000)
        if now_ms >= elig_end:
            self._count("skipped_late")
            return None
        cfg = getattr(getattr(self.alt, "primary", None), "config", None) or {}
        ctx = {
            "cohort": COHORT_FORWARD, "setup_id": sid, "asset": asset, "side": s["side"],
            "window_start_utc": window, "window_start_ms": ws_ms, "setup_minute": minute,
            "detection_at": s.get("updated_at"), "detection_ms": det_ms, "eligibility_end_ms": elig_end,
            "market_ticker": s.get("market_ticker"), "cons_pct": float(s.get("conservative_fair_pct")),
            "threshold_bp": s.get("threshold_bp"), "distance_bp": s.get("distance_bp"),
            "target_gap_bp": s.get("target_gap_bp"), "basis_ok": True,
            "edge_min_points": float(cfg.get("edge_min_points", 10.0)),
            "fee_buffer_cents": float(cfg.get("fee_buffer_cents", 1.0)),
            "production_max_buy_cents": s.get("max_buy_cents"), "signal_status": s.get("status"),
            "quote_max_age_s": self.quote_max_age_s, "tick_resolution": "1s",
            "production_detection_ask_cents": s.get("selected_ask_cents"),
            "production_detection_bid_cents": s.get("selected_bid_cents"),
        }
        opened_at = _now_iso()
        with self.lock:
            if sid in self.index["traces"] or sid in self.runtimes:
                self._count("skipped_duplicate")
                return None
            if len(self.runtimes) >= self.max_traces:
                self._count("skipped_cap")
                return None
            # Durable trace header BEFORE the first tick: recovery never depends on the index.
            if not self.traces_log.append({"schema_version": SCHEMA_VERSION, "event": "open", "setup_id": sid,
                                           "opened_at": opened_at, "ctx": ctx}):
                self._count("trace_log_write_failures")
                return None
            rt = TraceRuntime(ctx)
            self.runtimes[sid] = rt
            self.index["traces"][sid] = self._index_entry(sid, ctx, opened_at)
            self._count("setups_captured")
        # Seq 0: the quote production held at detection (cache age <= ALT_KALSHI_POLL_SECONDS); size unknown.
        try:
            fetched = None
            kl = getattr(self.alt, "kalshi_last", None)
            if isinstance(kl, dict) and kl.get(asset):
                fetched = int(float(kl[asset]) * 1000)
            quote_ms = fetched if fetched else det_ms
            age = max(0.0, (det_ms - quote_ms) / 1000.0)
            tick = build_tick(ctx, 0, det_ms, min(quote_ms, det_ms), s.get("selected_ask_cents"), s.get("selected_bid_cents"),
                              source="production_detection_poll", quote_age_s=age,
                              underlying={"live_price": s.get("live_price"), "distance_bp": s.get("distance_bp"),
                                          "underlying_age_s": 0.0})
            self._ingest(sid, tick)
        except Exception as exc:
            self._count("hook_errors")
            print("entry timing lab seq0:", exc, flush=True)
        if self.autostart:
            self._spawn_worker(sid)
        self.save()
        return sid

    # ------------------------------------------------------------- ingest (fail-closed)
    def _ingest(self, sid, tick):
        """seq is consumed on the attempt (never reused). Tick not durable -> not dispatched.
        Every terminal Decision is appended before this returns; failures queue the Decision and
        block the trace until it is durable."""
        with self.lock:
            rt = self.runtimes.get(sid)
            tr = self.index["traces"].get(sid)
            if rt is None or tr is None or rt.closed:
                return []
            seq = int(tick["seq"])
            if seq != rt.next_seq():
                self._count("seq_reuse_rejected")
                return []
            if not self.ticks_log.append(tick):
                rt.last_seq = seq
                tr["seq_last"] = seq
                self._count("tick_write_failures")
                return []
            self._count("ticks_recorded")
            if tick.get("source") == "backoff":
                self._count("backoff_ticks")
            decisions = rt.on_tick(tick)
            tr["last_tick_at"] = tick["tick_at"]
            for d in decisions:
                self._persist_decision(sid, rt, d)
            self._sync_index_from_runtime(tr, rt)
            self.dirty = True
        return decisions

    def _persist_decision(self, sid, rt, d):
        if rt.unpersisted:  # keep write order strictly chronological
            rt.unpersisted.append(d)
            self._count("decision_write_failures")
            return False
        if not self.decisions_log.append(d.to_dict()):
            rt.unpersisted.append(d)
            self._count("decision_write_failures")
            return False
        self._on_decision_durable(sid, d)
        return True

    def _on_decision_durable(self, sid, d):
        self._count("decisions_written")
        if d.fill:
            self._count("fills")
        with self.lock:
            self.results.setdefault(sid, {})[d.policy] = compact_from_decision(d)

    def _flush_unpersisted(self, sid, rt):
        """Retry queued Decisions in order. True when the queue is empty."""
        while rt.unpersisted:
            d = rt.unpersisted[0]
            self._count("decision_write_retries")
            if not self.decisions_log.append(d.to_dict()):
                return False
            rt.unpersisted.pop(0)
            self._on_decision_durable(sid, d)
        return True

    def _close_trace(self, sid, rt, tr, reason, now_ms):
        """Close = expire WAITING policies, make every Decision durable, then append the close
        record. While anything is not durable the trace stays 'closing' and is retried."""
        with self.lock:
            decisions = []
            if not rt.closed:
                decisions = rt.close(reason, now_ms)
                for d in decisions:
                    self._persist_decision(sid, rt, d)
                tr["end_reason"] = reason
                tr["closed_at"] = _ms_to_iso(now_ms)
            self._sync_index_from_runtime(tr, rt)
            self._finalize_close(sid, rt, tr)
            self.dirty = True
        return decisions

    def _finalize_close(self, sid, rt, tr):
        with self.lock:
            if tr.get("status") == "closed":
                return True
            if not self._flush_unpersisted(sid, rt):
                tr["status"] = "closing"
                tr["blocked"] = True
                return False
            if not self.traces_log.append({"schema_version": SCHEMA_VERSION, "event": "close", "setup_id": sid,
                                           "end_reason": tr.get("end_reason"), "closed_at": tr.get("closed_at"),
                                           "seq_last": rt.last_seq}):
                self._count("trace_log_write_failures")
                tr["status"] = "closing"
                return False
            tr["status"] = "closed"
            tr["blocked"] = False
            self.runtimes.pop(sid, None)
            self.workers.pop(sid, None)
            self._count("closed")
            return True

    # ------------------------------------------------------------- quote source
    def _fetch_http(self, url):
        req = Request(url, headers={"User-Agent": f"BTC15M-Monitor/{APP_VERSION} entry-timing-lab"})
        try:
            with urlopen(req, timeout=self.http_timeout) as r:
                return 200, json.loads(r.read().decode("utf-8")), ""
        except HTTPError as exc:
            return int(exc.code), None, ""
        except (socket.timeout, TimeoutError):
            return 0, None, "timeout"
        except URLError as exc:
            if isinstance(getattr(exc, "reason", None), socket.timeout):
                return 0, None, "timeout"
            return 0, None, "net_error"
        except ValueError:
            return 200, None, "parse_error"
        except Exception:
            return 0, None, "net_error"

    @staticmethod
    def parse_orderbook(payload, side):
        """(ask, ask_size, bid, bid_size) in cents for the contract we would buy.
        Raises ValueError on a structurally invalid response; an empty side is (None, ...) = no_ask."""
        if not isinstance(payload, dict):
            raise ValueError("orderbook payload is not an object")
        ob = payload.get("orderbook_fp") if isinstance(payload.get("orderbook_fp"), dict) else payload.get("orderbook")
        if not isinstance(ob, dict):
            raise ValueError("orderbook payload has no orderbook object")
        if "yes_dollars" in ob or "no_dollars" in ob:
            yes_rows, no_rows, scale = ob.get("yes_dollars") or [], ob.get("no_dollars") or [], 100.0
        elif "yes" in ob or "no" in ob:
            yes_rows, no_rows, scale = ob.get("yes") or [], ob.get("no") or [], 1.0
        else:
            raise ValueError("orderbook object has no yes/no levels")

        def best(rows):
            if not isinstance(rows, list):
                raise ValueError("orderbook levels are not a list")
            top = None
            for row in rows:
                try:
                    price = float(row[0]) * scale
                    qty = float(row[1])
                except Exception:
                    raise ValueError("unparsable orderbook level")
                if qty <= 0:
                    continue
                if top is None or price > top[0]:
                    top = (price, qty)
            return top
        yes_bid, no_bid = best(yes_rows), best(no_rows)
        if side == "UP":
            ask = (100.0 - no_bid[0], no_bid[1]) if no_bid else (None, None)
            bid = yes_bid or (None, None)
        else:
            ask = (100.0 - yes_bid[0], yes_bid[1]) if yes_bid else (None, None)
            bid = no_bid or (None, None)
        return ask[0], ask[1], bid[0], bid[1]

    @staticmethod
    def parse_market(payload, side):
        if not isinstance(payload, dict) or not isinstance(payload.get("market"), dict):
            raise ValueError("market payload has no market object")
        yb, ya, nb, na = core.get_price_fields(payload["market"])
        if side == "UP":
            return (ya * 100.0 if ya is not None else None), (yb * 100.0 if yb is not None else None)
        return (na * 100.0 if na is not None else None), (nb * 100.0 if nb is not None else None)

    def _fetch_quote(self, tr):
        """Returns (http_status, error, ask, ask_size, bid, bid_size, source).
        error '' means a usable response (ask may still be None = no_ask)."""
        ctx = tr["ctx"]
        source = tr.get("source") or self.tick_source
        ticker = ctx["market_ticker"]
        if source == "orderbook":
            url = core.KALSHI_BASE + "/markets/" + ticker + "/orderbook?" + urlencode({"depth": 5})
        else:
            url = core.KALSHI_BASE + "/markets/" + ticker
        self._count("requests")
        status, payload, err = self.fetch_fn(url)
        if err == "timeout":
            self._count("timeouts")
        elif err == "parse_error":
            self._count("parse_errors")
        elif err:
            self._count("net_errors")
        elif status == 429:
            self._count("http_429")
        elif 500 <= int(status) <= 599:
            self._count("http_5xx")
        elif status == 200:
            self._count("http_2xx")
        elif 400 <= int(status) <= 499:
            self._count("http_4xx")
        else:
            self._count("http_other")
        ask = ask_size = bid = bid_size = None
        if status == 200 and not err:
            try:
                if source == "orderbook":
                    ask, ask_size, bid, bid_size = self.parse_orderbook(payload, ctx["side"])
                else:
                    ask, bid = self.parse_market(payload, ctx["side"])
            except ValueError:
                err = "parse_error"
                self._count("parse_errors")
            else:
                if ask is None:
                    self._count("no_ask")
        return status, err, ask, ask_size, bid, bid_size, source

    def _take_token(self):
        """Token bucket over self.clock (deterministic in tests). True = a request may be made now."""
        if self.max_rps <= 0:
            return True
        with self._budget_lock:
            now = self.clock()
            self._tokens = min(float(self.burst), self._tokens + max(0.0, now - self._tokens_at) * self.max_rps)
            self._tokens_at = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def _underlying_snapshot(self, asset):
        try:
            with self.alt.lock:
                st = dict(self.alt.states.get(asset) or {})
            age = None
            ts = _iso_to_ms(st.get("updated_at"))
            if ts:
                age = _round(max(0.0, self.clock() - ts / 1000.0), 2)
            return {"live_price": st.get("live_price"), "distance_bp": st.get("distance_bp"), "underlying_age_s": age}
        except Exception:
            return None

    # ------------------------------------------------------------- per-trace stepping
    def step_trace(self, sid):
        """One poller iteration for one trace. Returns the trace status after the step.
        Order of checks: closing retries -> blocked (unpersisted) retries -> eligibility end ->
        backoff (synthetic invalid tick, no request) -> fetch -> ingest."""
        if not self.enabled or not self.recovered:
            return "disabled"
        with self.lock:
            rt = self.runtimes.get(sid)
            tr = self.index["traces"].get(sid)
        if rt is None or tr is None:
            return "closed"
        try:
            now_ms = int(self.clock() * 1000)
            if tr.get("status") == "closing" or rt.closed:
                self._finalize_close(sid, rt, tr)
                return tr.get("status")
            if rt.unpersisted:
                with self.lock:
                    flushed = self._flush_unpersisted(sid, rt)
                    if flushed:
                        tr["blocked"] = False
                if not flushed:
                    return "blocked"  # no fetch, no tick until every Decision is durable
            if now_ms >= int(tr["ctx"]["eligibility_end_ms"]):
                self._close_trace(sid, rt, tr, "eligibility_ended", now_ms)
                return tr.get("status")
            if int(tr.get("backoff_until_ms", 0)) > now_ms:
                tick = build_tick(tr["ctx"], rt.next_seq(), now_ms, now_ms, None, None, source="backoff", http_status=0,
                                  error="backoff", underlying=self._underlying_snapshot(tr["ctx"]["asset"]))
                self._ingest(sid, tick)
                return tr.get("status")
            if not self._take_token():
                # Global budget exhausted: no request, but time still advances for every WAITING policy.
                tick = build_tick(tr["ctx"], rt.next_seq(), now_ms, now_ms, None, None, source="rate_limited", http_status=0,
                                  error="rate_limited", underlying=self._underlying_snapshot(tr["ctx"]["asset"]))
                self._count("rate_limited_ticks")
                self._ingest(sid, tick)
                return tr.get("status")
            t_req = int(self.clock() * 1000)
            status, err, ask, ask_size, bid, bid_size, source = self._fetch_quote(tr)
            quote_ms = int(self.clock() * 1000)
            with self.lock:
                if err == "parse_error":
                    tr["consecutive_parse_errors"] = int(tr.get("consecutive_parse_errors", 0)) + 1
                    tr["consecutive_failures"] = 0
                    if source == "orderbook" and tr["consecutive_parse_errors"] >= 3:
                        tr["source"] = "market"
                        tr["consecutive_parse_errors"] = 0
                        self._count("source_fallbacks")
                elif err or status != 200:
                    tr["consecutive_parse_errors"] = 0
                    tr["consecutive_failures"] = int(tr.get("consecutive_failures", 0)) + 1
                    n = tr["consecutive_failures"]
                    tr["backoff_until_ms"] = quote_ms + int(min(8.0, 2 ** (n - 1)) * 1000)
                else:
                    tr["consecutive_parse_errors"] = 0
                    tr["consecutive_failures"] = 0
                    tr["backoff_until_ms"] = 0
            tick = build_tick(tr["ctx"], rt.next_seq(), quote_ms, quote_ms, ask, bid, ask_size, bid_size,
                              source=source, http_status=status, error=err,
                              quote_age_s=_round((quote_ms - t_req) / 1000.0, 3),
                              underlying=self._underlying_snapshot(tr["ctx"]["asset"]))
            self._ingest(sid, tick)
            if not tick["entry_eligible"]:
                self._close_trace(sid, rt, tr, "eligibility_ended", quote_ms)
            return tr.get("status")
        except Exception as exc:
            self._count("hook_errors")
            print("entry timing lab step:", sid, exc, flush=True)
            return tr.get("status")

    def poll_once(self):
        """Synchronous driver (tests / no-thread mode): one step for every open trace."""
        with self.lock:
            sids = list(self.runtimes)
        for sid in sids:
            self.step_trace(sid)

    def _spawn_worker(self, sid):
        with self.lock:
            if sid in self.workers and self.workers[sid].is_alive():
                return self.workers[sid]
            t = threading.Thread(target=self._worker_loop, args=(sid,), name=f"entry-timing-{sid}", daemon=True)
            self.workers[sid] = t
        t.start()
        return t

    def _worker_loop(self, sid):
        """Independent cadence per trace: a slow/timeouting request never delays other traces."""
        while not self.stop_event.is_set():
            t0 = self.clock()
            status = self.step_trace(sid)
            if status in ("closed", "disabled"):
                break
            self.stop_event.wait(max(0.05, self.tick_seconds - (self.clock() - t0)))

    def run(self):
        """Maintenance loop: outcome joins, closing retries, v2.7 replay, index saves."""
        while not self.stop_event.is_set():
            try:
                if self.clock() - self.last_maintenance >= self.join_seconds:
                    self.maintenance()
                elif self.dirty and self.clock() - self.last_save >= 5.0:
                    self.save()
            except Exception as exc:
                self._count("hook_errors")
                print("entry timing lab maintenance:", exc, flush=True)
            self.stop_event.wait(1.0)

    def stop(self):
        self.stop_event.set()

    # ------------------------------------------------------------- maintenance / outcome join
    def maintenance(self):
        if not self.enabled or not self.recovered:
            return
        self.last_maintenance = self.clock()
        with self.lock:
            closing = [(sid, self.runtimes[sid], self.index["traces"][sid]) for sid in list(self.runtimes)
                       if self.index["traces"].get(sid, {}).get("status") == "closing"]
        for sid, rt, tr in closing:
            self._finalize_close(sid, rt, tr)
        self.join_outcomes()
        if self.replay_enabled and self.clock() - self.last_replay >= self.replay_seconds:
            self.last_replay = self.clock()
            self.replay_v27_async()
        self.save()

    def _ledger_record(self, sid):
        try:
            asset, window = sid.split("|", 1)
            return self.alt.ledger.get_setup(asset, window)
        except Exception:
            return None

    def join_outcomes(self):
        """Copy Kalshi outcome + production's actual entry from the ledger. Decisions are never touched."""
        if not self.enabled or not self.recovered:
            return
        with self.lock:
            pending = [sid for sid, tr in self.index["traces"].items()
                       if tr.get("status") == "closed" and not tr.get("resolved")]
        for sid in pending:
            rec = self._ledger_record(sid)
            if not rec or not rec.get("resolved") or rec.get("kalshi_win") is None:
                continue
            outcome = {
                "setup_id": sid, "kalshi_result": rec.get("kalshi_result"), "kalshi_win": bool(rec.get("kalshi_win")),
                "kalshi_status": rec.get("kalshi_status"), "resolved_at": rec.get("resolved_at"),
                "joined_at": _now_iso(),
                "production_actual": {
                    "entry_simulated": bool(rec.get("entry_simulated")), "entry_time": rec.get("entry_time"),
                    "entry_minute": rec.get("entry_minute"), "entry_ask_cents": rec.get("entry_ask_cents"),
                    "entry_fee_cents": rec.get("entry_fee_cents"), "net_pnl_cents": rec.get("net_pnl_cents"),
                },
            }
            with self.lock:
                self.index["outcomes"][sid] = outcome
                self.index["traces"][sid]["resolved"] = True
                self._count("outcomes_joined")

    # ------------------------------------------------------------- v2.7 replay cohort
    def replay_v27_async(self, path=None):
        """Run the v2.7 replay in its own daemon thread so a large first scan never stalls the pollers."""
        t = self._replay_thread
        if t is not None and t.is_alive():
            return False
        self._replay_thread = threading.Thread(target=self.replay_v27, args=(path,), name="entry-timing-replay-v27", daemon=True)
        self._replay_thread.start()
        return True

    def replay_v27(self, path=None):
        """Replay decision_trace_v27.jsonl (engine=alt) through the SAME online engine.
        Streams the file from a persisted byte cursor; a setup is replayed once the stream (or the
        clock) is past its window end + 60 s. Coarse (~3 s) ticks, liquidity unverified; results live
        in their own file/cohort and are never merged with forward_v28."""
        if not self.enabled:
            return 0
        path = Path(path) if path else v27.DECISION_TRACE_PATH
        try:
            if not path.exists():
                return 0
            with self.lock:
                cursor = int(self.index["replay_v27"].get("cursor", 0))
                done = set(self.index["replay_v27"].get("done", []))
            size = path.stat().st_size
            if size < cursor:
                cursor = 0
            groups, starts, meta = {}, {}, {}
            replayed = [0]

            def flush(sid):
                lines = groups.pop(sid)
                starts.pop(sid, None)
                ws_ms, minute = meta.pop(sid)
                try:
                    if self._replay_one(sid, lines, ws_ms, minute, ws_ms + (minute + 1) * 60000):
                        replayed[0] += 1
                except Exception as exc:
                    self._count("replay_errors")
                    print("entry timing lab replay:", sid, exc, flush=True)
                with self.lock:
                    self.index["replay_v27"].setdefault("done", []).append(sid)
                    done.add(sid)

            with open(path, "rb") as fh:
                fh.seek(cursor)
                offset = cursor
                for raw in fh:
                    start = offset
                    offset += len(raw)
                    try:
                        rec = json.loads(raw.decode("utf-8", "replace"))
                    except Exception:
                        continue
                    if rec.get("engine") != "alt":
                        continue
                    sid = rec.get("setup_id")
                    ts_ms = _iso_to_ms(rec.get("ts"))
                    if not sid or sid in done or ts_ms is None:
                        continue
                    for other in [k for k, (w, m) in meta.items() if w + 900000 + 60000 < ts_ms]:
                        flush(other)
                    if sid not in groups:
                        ws_ms = _iso_to_ms(rec.get("window_start_utc"))
                        minute = rec.get("signal_minute")
                        if ws_ms is None or minute not in (7, 8, 9, 10):
                            continue
                        groups[sid], starts[sid], meta[sid] = [], start, (ws_ms, int(minute))
                    groups[sid].append(rec)
            now_ms = int(self.clock() * 1000)
            for sid in [k for k, (w, m) in meta.items() if now_ms > w + 900000 + 60000]:
                flush(sid)
            pending_start = min(starts.values()) if starts else offset
            with self.lock:
                self.index["replay_v27"]["cursor"] = int(pending_start)
                self.dirty = True
            return replayed[0]
        except Exception as exc:
            self._count("replay_errors")
            print("entry timing lab replay:", exc, flush=True)
            return 0

    def _replay_one(self, sid, lines, ws_ms, minute, elig_end):
        rec = self._ledger_record(sid) or {}
        if rec and rec.get("basis_ok") is not True:
            return False
        lines.sort(key=lambda r: (int(r.get("poll_seq") or 0), r.get("ts") or ""))
        first = lines[0]
        det_ms = _iso_to_ms(rec.get("recorded_at")) or _iso_to_ms(first.get("ts"))
        cons = rec.get("conservative_fair_pct") if rec else first.get("cons_pct")
        if det_ms is None or cons is None or first.get("side") not in ("UP", "DOWN"):
            return False
        cfg = getattr(getattr(self.alt, "primary", None), "config", None) or {}
        ctx = {
            "cohort": COHORT_V27, "setup_id": sid, "asset": first.get("asset"), "side": first.get("side"),
            "window_start_utc": first.get("window_start_utc"), "window_start_ms": ws_ms, "setup_minute": minute,
            "detection_at": _ms_to_iso(det_ms), "detection_ms": det_ms, "eligibility_end_ms": elig_end,
            "market_ticker": rec.get("market_ticker"), "cons_pct": float(cons),
            "threshold_bp": rec.get("threshold_bp") or first.get("threshold_bp"),
            "distance_bp": rec.get("distance_bp") or first.get("distance_bp"),
            "target_gap_bp": rec.get("target_gap_bp") or first.get("target_gap_bp"), "basis_ok": True,
            "edge_min_points": float(cfg.get("edge_min_points", 10.0)),
            "fee_buffer_cents": float(cfg.get("fee_buffer_cents", 1.0)),
            "production_max_buy_cents": rec.get("max_buy_cents") or first.get("max_buy_cents"),
            "quote_max_age_s": 1e9, "tick_resolution": "~3s (v2.7 production poll cadence)",
        }
        rt = TraceRuntime(ctx)
        seq = 0
        for line in lines:
            ts_ms = _iso_to_ms(line.get("ts"))
            if ts_ms is None:
                continue
            tick = build_tick(ctx, seq, ts_ms, ts_ms, line.get("ask_cents"), line.get("bid_cents"),
                              source="v27_decision_trace", underlying={"live_price": None,
                              "distance_bp": line.get("distance_bp"), "underlying_age_s": None})
            seq += 1
            for d in rt.on_tick(tick):
                self._persist_replay(sid, d)
            if rt.closed or not tick["entry_eligible"]:
                break
        for d in rt.close("replay_end", max(elig_end, (_iso_to_ms(lines[-1].get("ts")) or elig_end))):
            self._persist_replay(sid, d)
        with self.lock:
            self.index.setdefault("replay_traces", {})[sid] = {
                "setup_id": sid, "cohort": COHORT_V27, "asset": ctx["asset"], "side": ctx["side"],
                "setup_minute": minute, "window_start_utc": ctx["window_start_utc"],
                "detection_ask": rt.ctx.get("detection_ask"), "detection_spread": rt.ctx.get("detection_spread"),
                "distance_bp": ctx["distance_bp"], "threshold_bp": ctx["threshold_bp"],
                "target_gap_bp": ctx["target_gap_bp"], "ticks": rt.ticks, "valid_ticks": rt.valid_ticks,
                "replayed_at": _now_iso(),
            }
            self._count("replay_setups")
        return True

    def _persist_replay(self, sid, d):
        if not self.replay_log.append(d.to_dict()):
            self._count("decision_write_failures")
            return
        with self.lock:
            self.replay_results.setdefault(sid, {})[d.policy] = compact_from_decision(d)

    # ------------------------------------------------------------- cohorts
    def _outcome_for(self, sid, from_index=True):
        if from_index:
            with self.lock:
                o = self.index["outcomes"].get(sid)
            if o:
                return o
        rec = self._ledger_record(sid)
        if rec and rec.get("resolved") and rec.get("kalshi_win") is not None:
            return {"setup_id": sid, "kalshi_result": rec.get("kalshi_result"), "kalshi_win": bool(rec.get("kalshi_win")),
                    "production_actual": {"entry_simulated": bool(rec.get("entry_simulated")),
                                          "entry_ask_cents": rec.get("entry_ask_cents"),
                                          "entry_fee_cents": rec.get("entry_fee_cents"),
                                          "net_pnl_cents": rec.get("net_pnl_cents")}}
        return None

    def _rows_forward(self):
        with self.lock:
            traces = [dict(tr) for tr in self.index["traces"].values() if tr.get("status") == "closed"]
        rows = []
        for tr in traces:
            sid = tr["setup_id"]
            ctx = tr["ctx"]
            with self.lock:
                res = dict(self.results.get(sid, {}))
            rows.append({
                "setup_id": sid, "asset": ctx["asset"], "minute": ctx["setup_minute"], "side": ctx["side"],
                "detection_ask": tr.get("detection_ask"), "detection_spread": tr.get("detection_spread"),
                "distance_bp": ctx.get("distance_bp"), "threshold_bp": ctx.get("threshold_bp"),
                "target_gap_bp": ctx.get("target_gap_bp"), "results": res,
                "outcome": self._outcome_for(sid),
            })
        return rows

    def _rows_v27(self):
        with self.lock:
            traces = [dict(x) for x in self.index.get("replay_traces", {}).values()]
            forward_ids = set(self.index["traces"])
        rows = []
        for tr in traces:
            sid = tr["setup_id"]
            with self.lock:
                res = dict(self.replay_results.get(sid, {}))
            rows.append({
                "setup_id": sid, "asset": tr.get("asset"), "minute": tr.get("setup_minute"), "side": tr.get("side"),
                "detection_ask": tr.get("detection_ask"), "detection_spread": tr.get("detection_spread"),
                "distance_bp": tr.get("distance_bp"), "threshold_bp": tr.get("threshold_bp"),
                "target_gap_bp": tr.get("target_gap_bp"), "results": res,
                "outcome": self._outcome_for(sid, from_index=False), "overlaps_forward": sid in forward_ids,
            })
        return rows

    def _rows_backfill(self):
        """Detection-only records from the production ledger (pre-v2.8 setups). Only the
        detection policies are simulable; every timing policy is NOT_SIMULABLE by definition."""
        try:
            with self.alt.ledger.lock:
                recs = [dict(x) for x in self.alt.ledger.data.get("setups", {}).values()]
        except Exception:
            return []
        with self.lock:
            first_boot = self.index.get("first_boot_at") or ""
            forward_ids = set(self.index["traces"])
        cfg = getattr(getattr(self.alt, "primary", None), "config", None) or {}
        edge_min = float(cfg.get("edge_min_points", 10.0))
        rows = []
        for rec in recs:
            sid = rec.get("setup_id")
            if not sid or sid in forward_ids or not rec.get("resolved") or rec.get("kalshi_win") is None:
                continue
            if rec.get("basis_ok") is not True or rec.get("side") not in ("UP", "DOWN") or rec.get("minute") not in (7, 8, 9, 10):
                continue
            if str(rec.get("recorded_at") or "") >= first_boot:
                continue
            ask = rec.get("ask_cents")
            try:
                ask = float(ask)
                if not (0 < ask < 100):
                    continue
            except Exception:
                continue
            fee = core.effective_fee_cents(ask, {"fee_buffer_cents": float(cfg.get("fee_buffer_cents", 1.0))}, 1.0)
            cost = ask + fee
            econ = cost < 100.0
            det = ({"state": FILLED, "fill": True, "reason": None, "ask": ask, "fee": fee, "cost": cost, "delay": 0.0,
                    "impr": 0.0, "guard": 0, "seq": 0, "det": ask, "liq": LIQ_UNVERIFIED} if econ else
                   {"state": REJECTED, "fill": False, "reason": "rejected_effective_cost_ge_100", "ask": None, "fee": None,
                    "cost": None, "delay": None, "impr": None, "guard": 0, "seq": 0, "det": ask, "liq": None})
            edge = rec.get("edge_points")
            prod_fill = edge is not None and float(edge) >= edge_min and econ
            prod = (dict(det, state=FILLED) if prod_fill else
                    {"state": EXPIRED, "fill": False, "reason": "eligibility_ended", "ask": None, "fee": None, "cost": None,
                     "delay": None, "impr": None, "guard": 0, "seq": 0, "det": ask, "liq": None})
            results = {p: {"state": NOT_SIMULABLE, "fill": False, "reason": "not_simulable_detection_only_record",
                           "ask": None, "fee": None, "cost": None, "delay": None, "impr": None, "guard": 0, "seq": None,
                           "det": ask, "liq": None} for p in POLICY_NAMES}
            results["entry_at_detection"] = det
            results["production_baseline"] = prod
            rows.append({
                "setup_id": sid, "asset": rec.get("asset"), "minute": rec.get("minute"), "side": rec.get("side"),
                "detection_ask": ask, "detection_spread": rec.get("setup_spread_cents"),
                "distance_bp": rec.get("distance_bp"), "threshold_bp": rec.get("threshold_bp"),
                "target_gap_bp": rec.get("target_gap_bp"), "results": results,
                "outcome": {"setup_id": sid, "kalshi_result": rec.get("kalshi_result"), "kalshi_win": bool(rec.get("kalshi_win")),
                            "production_actual": {"entry_simulated": bool(rec.get("entry_simulated")),
                                                  "entry_ask_cents": rec.get("entry_ask_cents"),
                                                  "entry_fee_cents": rec.get("entry_fee_cents"),
                                                  "net_pnl_cents": rec.get("net_pnl_cents")}},
            })
        return rows

    def cohort_rows(self, cohort):
        if cohort == COHORT_FORWARD:
            return self._rows_forward()
        if cohort == COHORT_V27:
            return self._rows_v27()
        if cohort == COHORT_BACKFILL:
            return self._rows_backfill()
        return []

    # ------------------------------------------------------------- metrics
    @staticmethod
    def _pnl(res, outcome):
        if not res or not res.get("fill") or not outcome or outcome.get("kalshi_win") is None:
            return None
        ask, fee = float(res["ask"]), float(res["fee"])
        gross = (100.0 - ask) if outcome["kalshi_win"] else -ask
        net = gross - fee
        return {"gross": gross, "net": net, "cost": ask + fee, "win": bool(outcome["kalshi_win"])}

    def policy_metrics(self, rows, policy, detection_metrics=None):
        eligible = [r for r in rows if (r["results"].get(policy) or {}).get("state") not in (None, NOT_SIMULABLE)]
        fills = [r for r in eligible if r["results"][policy].get("fill")]
        resolved_eligible = [r for r in eligible if r.get("outcome")]
        resolved_fills = [r for r in fills if r.get("outcome")]
        pnls = [(r, self._pnl(r["results"][policy], r["outcome"])) for r in resolved_fills]
        pnls = [(r, p) for r, p in pnls if p]
        wins = sum(1 for _, p in pnls if p["win"])
        net_total = sum(p["net"] for _, p in pnls)
        cost_total = sum(p["cost"] for _, p in pnls)
        reasons = {}
        guard_total = 0
        for r in eligible:
            res = r["results"][policy]
            guard_total += int(res.get("guard") or 0)
            if not res.get("fill"):
                reasons[res.get("reason") or res.get("state")] = reasons.get(res.get("reason") or res.get("state"), 0) + 1
        asks = [r["results"][policy]["ask"] for r in fills]
        fees = [r["results"][policy]["fee"] for r in fills]
        costs = [r["results"][policy]["cost"] for r in fills]
        delays = [r["results"][policy]["delay"] for r in fills]
        imprs = [r["results"][policy]["impr"] for r in fills if r["results"][policy].get("impr") is not None]
        verified = [r for r in fills if r["results"][policy].get("liq") == LIQ_VERIFIED]
        unverified = [r for r in fills if r["results"][policy].get("liq") != LIQ_VERIFIED]
        v_pnls = [(r, p) for r, p in pnls if r["results"][policy].get("liq") == LIQ_VERIFIED]
        v_wins = sum(1 for _, p in v_pnls if p["win"])
        v_net = sum(p["net"] for _, p in v_pnls)
        n_re = len(resolved_eligible)
        out = {
            "policy": policy,
            "family": next((s["family"] for s in POLICY_SPECS if s["policy"] == policy), None),
            "is_baseline": policy in ("entry_at_detection", "production_baseline"),
            "eligible_setups": len(eligible), "fills": len(fills), "no_fills": len(eligible) - len(fills),
            "fill_rate_pct": _round(100.0 * len(fills) / len(eligible), 3) if eligible else None,
            "resolved_eligible": n_re, "resolved_fills": len(pnls),
            "wins": wins, "losses": len(pnls) - wins,
            "hit_pct": _round(100.0 * wins / len(pnls), 3) if pnls else None,
            "wilson_low_pct": _round(core.wilson_low(wins, len(pnls)), 3) if pnls else None,
            "avg_ask_cents": _round(_mean(asks)), "median_ask_cents": _round(_median(asks)),
            "avg_fee_cents": _round(_mean(fees)), "avg_effective_cost_cents": _round(_mean(costs)),
            "avg_delay_seconds": _round(_mean(delays)), "median_delay_seconds": _round(_median(delays)),
            "net_pnl_dollars": _round(net_total / 100.0), "avg_net_pnl_per_fill_cents": _round(net_total / len(pnls)) if pnls else None,
            "roi_pct": _round(100.0 * net_total / cost_total) if cost_total > 0 else None,
            "net_pnl_per_eligible_setup_cents": _round(net_total / n_re) if n_re else None,
            "wins_per_eligible_setup": _round(wins / n_re) if n_re else None,
            "guardrail_rejections_total": guard_total, "no_fill_reasons": reasons,
            "status_eligible": sample_status(n_re), "status_fills": sample_status(len(pnls)),
            "vs_detection_pnl_delta_cents": None, "vs_detection_avg_entry_improvement_cents": _round(_mean(imprs)),
            # Liquidity is never silently mixed: verified = ask_size >= 1 observed on the book at fill time.
            "fills_liquidity_verified": len(verified), "fills_liquidity_unverified": len(unverified),
            "liquidity_mixed": bool(verified and unverified),
            "verified_only": {"fills": len(verified), "resolved_fills": len(v_pnls), "wins": v_wins,
                              "net_pnl_dollars": _round(v_net / 100.0),
                              "net_pnl_per_eligible_setup_cents": _round(v_net / n_re) if n_re else None},
        }
        if detection_metrics and detection_metrics.get("net_pnl_per_eligible_setup_cents") is not None \
                and out["net_pnl_per_eligible_setup_cents"] is not None:
            out["vs_detection_pnl_delta_cents"] = _round(out["net_pnl_per_eligible_setup_cents"]
                                                         - detection_metrics["net_pnl_per_eligible_setup_cents"])
        return out

    def _production_actual(self, rows):
        resolved = [r for r in rows if r.get("outcome")]
        entries = [r for r in resolved if (r["outcome"].get("production_actual") or {}).get("entry_simulated")]
        nets = [float((r["outcome"]["production_actual"] or {}).get("net_pnl_cents") or 0.0) for r in entries]
        wins = sum(1 for r in entries if r["outcome"].get("kalshi_win"))
        agree = None
        if resolved:
            same = 0
            for r in resolved:
                sim = bool((r["results"].get("production_baseline") or {}).get("fill"))
                act = bool((r["outcome"].get("production_actual") or {}).get("entry_simulated"))
                same += int(sim == act)
            agree = _round(100.0 * same / len(resolved), 3)
        return {"label": "production_actual (ShadowLedger.observe_state entries, observed, not simulated)",
                "resolved_setups": len(resolved), "entries": len(entries), "wins": wins, "losses": len(entries) - wins,
                "net_pnl_dollars": _round(sum(nets) / 100.0),
                "baseline_agreement_pct": agree,
                "baseline_agreement_note": "share of resolved setups where the 1 Hz production_baseline simulation and the real ledger entry agree (fill vs no fill)"}

    def _segment_value(self, row, key):
        if key == "asset":
            return row.get("asset")
        if key == "minute":
            return f"M{row.get('minute')}"
        if key == "side":
            return row.get("side")
        if key == "ask_bucket":
            return _bucket(row.get("detection_ask"), ASK_BUCKETS)
        if key == "distance_bucket":
            d, t = row.get("distance_bp"), row.get("threshold_bp")
            return _bucket((float(d) - float(t)) if d is not None and t is not None else None, DISTANCE_EXCESS_BUCKETS)
        if key == "basis_bucket":
            g = row.get("target_gap_bp")
            return _bucket(abs(float(g)) if g is not None else None, BASIS_GAP_BUCKETS)
        if key == "spread_bucket":
            return spread_bucket(row.get("detection_spread"))
        return "unknown"

    def cohort_report(self, cohort, segment=None, sort=None):
        rows = self.cohort_rows(cohort)
        det = self.policy_metrics(rows, "entry_at_detection")
        policies = [self.policy_metrics(rows, p, det) for p in POLICY_NAMES]
        by_name = {p["policy"]: p for p in policies}
        sort = sort if sort in SORT_KEYS else "net_pnl_per_eligible_setup"
        skey = {"net_pnl_per_eligible_setup": "net_pnl_per_eligible_setup_cents", "wilson_low_pct": "wilson_low_pct",
                "fill_rate_pct": "fill_rate_pct", "roi_pct": "roi_pct"}[sort]

        def rank_key(p):
            v = p.get(skey)
            return (0 if v is None else 1, v if v is not None else 0.0, p.get("wilson_low_pct") or 0.0,
                    p.get("fill_rate_pct") or 0.0, p.get("roi_pct") or 0.0)
        ranking = [{"rank": i + 1, "policy": p["policy"], "net_pnl_per_eligible_setup_cents": p["net_pnl_per_eligible_setup_cents"],
                    "wilson_low_pct": p["wilson_low_pct"], "fill_rate_pct": p["fill_rate_pct"], "roi_pct": p["roi_pct"],
                    "vs_detection_pnl_delta_cents": p["vs_detection_pnl_delta_cents"],
                    "vs_detection_avg_entry_improvement_cents": p["vs_detection_avg_entry_improvement_cents"],
                    "fills_liquidity_unverified": p["fills_liquidity_unverified"],
                    "status_eligible": p["status_eligible"], "status_fills": p["status_fills"]}
                   for i, p in enumerate(sorted(policies, key=rank_key, reverse=True))]
        report = {
            "cohort": cohort, "tick_resolution": ("1s" if cohort == COHORT_FORWARD else "~3s" if cohort == COHORT_V27 else "detection-only"),
            "liquidity_verification": ("orderbook ticks verify ask_size >= 1 contract; the production detection quote (seq 0), "
                                       "the market-endpoint fallback and every v2.7/backfill quote carry no size and are 'unverified'"),
            "setups_observed": len(rows), "resolved_setups": sum(1 for r in rows if r.get("outcome")),
            "overlaps_forward_v28": sum(1 for r in rows if r.get("overlaps_forward")) if cohort == COHORT_V27 else None,
            "simulable_policies": (list(BACKFILL_SIMULABLE) if cohort == COHORT_BACKFILL else "all"),
            "baselines": {"entry_at_detection": by_name["entry_at_detection"],
                          "production_baseline": by_name["production_baseline"],
                          "production_actual": self._production_actual(rows)},
            "policies": policies, "ranking": ranking, "sorted_by": sort,
        }
        if segment in SEGMENT_KEYS:
            groups = {}
            for r in rows:
                groups.setdefault(self._segment_value(r, segment), []).append(r)
            seg = {}
            for value, grp in sorted(groups.items(), key=lambda kv: str(kv[0])):
                gdet = self.policy_metrics(grp, "entry_at_detection")
                seg[value] = {"setups": len(grp), "resolved": sum(1 for r in grp if r.get("outcome")),
                              "policies": {p: {k: m.get(k) for k in ("eligible_setups", "fills", "fill_rate_pct", "wins", "hit_pct",
                                                                   "wilson_low_pct", "net_pnl_per_eligible_setup_cents",
                                                                   "avg_ask_cents", "vs_detection_pnl_delta_cents",
                                                                   "fills_liquidity_unverified")}
                                           for p in POLICY_NAMES for m in [self.policy_metrics(grp, p, gdet)]}}
            report["segment"] = segment
            report["segments"] = seg
            report["segments_note"] = "research-only stratification; never used to build adaptive rules"
        return report

    def report(self, cohort="forward", segment=None, sort=None):
        header = {
            "version": APP_VERSION, "lab_version": LAB_VERSION, "mode": LAB_MODE, "production_modified": False,
            "enabled": self.enabled, "generated_at": _now_iso(), "eligibility_definition": ELIGIBILITY_DEFINITION,
            "fee_model": "fee_effective = max(fee_buffer_cents, quadratic 0.07 taker model); effective_cost = ask + fee_effective; ROI = net / effective_cost",
            "policies_total": len(POLICY_NAMES), "sample_status_rule": {"<30": "INSUFFICIENT_SAMPLE", "30-99": "EARLY_RESEARCH",
                                                                       "100-199": "RESEARCH_SAMPLE", ">=200": "REVIEW_ELIGIBLE"},
            "hindsight_metrics_included": False,
            "note": "Scenario B / min(ask) ex-post metrics live only in the v2.7 endpoints and are never mixed here.",
        }
        if cohort in ("all", "*"):
            header["cohort_requested"] = "all"
            header["cohorts_shown"] = list(COHORTS)
            header["cohorts"] = {c: self.cohort_report(c, segment, sort) for c in COHORTS}
            header["aggregation"] = "none: cohorts are reported side by side and are never summed"
            fwd = header["cohorts"][COHORT_FORWARD]
            header["setups_observed"] = fwd["setups_observed"]
            header["resolved_setups"] = fwd["resolved_setups"]
            header["policies"] = fwd["policies"]
            return header
        c = COHORT_FORWARD if cohort in ("forward", COHORT_FORWARD, None, "") else cohort
        if c not in COHORTS:
            c = COHORT_FORWARD
        header["cohort_requested"] = cohort or "forward"
        header["cohorts_shown"] = [c]
        header.update(self.cohort_report(c, segment, sort))
        return header

    # ------------------------------------------------------------- views
    def trace_view(self, setup_id):
        ticks = [t for t in self._iter_jsonl(self.ticks_log.path) if t.get("setup_id") == setup_id]
        decisions = [d for d in self._iter_jsonl(self.decisions_log.path) if d.get("setup_id") == setup_id]
        with self.lock:
            tr = dict(self.index["traces"].get(setup_id) or {})
            outcome = self.index["outcomes"].get(setup_id)
        return {"version": APP_VERSION, "setup_id": setup_id, "trace": tr, "ticks": ticks, "decisions": decisions,
                "outcome": outcome, "note": "decisions are immutable; outcome is joined after the trace closed"}

    def health_summary(self):
        try:
            with self.lock:
                c = dict(self.index["counters"])
                traces = self.index["traces"]
                active = len(self.runtimes)
                last_capture = max((tr.get("last_tick_at") or "" for tr in traces.values()), default="") or None
                now_ms = int(self.clock() * 1000)
                backoff = sum(1 for tr in traces.values() if tr.get("status") == "open" and int(tr.get("backoff_until_ms", 0)) > now_ms)
                blocked = sum(1 for tr in traces.values() if tr.get("status") in ("open", "closing") and tr.get("blocked"))
                closing = sum(1 for tr in traces.values() if tr.get("status") == "closing")
                workers_alive = sum(1 for t in self.workers.values() if t.is_alive())
                recovery = dict(self.index.get("recovery") or {})
            return {
                "version": LAB_VERSION, "app_version": APP_VERSION, "mode": LAB_MODE, "enabled": self.enabled,
                "recovered": self.recovered,
                "setups_captured": int(c.get("setups_captured", 0)),
                "resolved": int(c.get("outcomes_joined", 0)), "active_traces": active,
                "ticks_recorded": int(c.get("ticks_recorded", 0)), "last_capture_utc": last_capture,
                "production_modified": False,
                "poller": {"tick_seconds": self.tick_seconds, "max_traces": self.max_traces, "tick_source": self.tick_source,
                           "mode": "one worker thread per trace", "workers_alive": workers_alive,
                           "max_rps": self.max_rps, "burst": self.burst, "tokens_available": _round(self._tokens, 2),
                           "rate_limited_ticks": int(c.get("rate_limited_ticks", 0)),
                           "maintenance_thread_alive": bool(self.thread and self.thread.is_alive()),
                           "requests": int(c.get("requests", 0)), "http_2xx": int(c.get("http_2xx", 0)),
                           "http_429": int(c.get("http_429", 0)), "http_5xx": int(c.get("http_5xx", 0)),
                           "http_4xx": int(c.get("http_4xx", 0)), "http_other": int(c.get("http_other", 0)),
                           "timeouts": int(c.get("timeouts", 0)), "net_errors": int(c.get("net_errors", 0)),
                           "parse_errors": int(c.get("parse_errors", 0)), "no_ask": int(c.get("no_ask", 0)),
                           "source_fallbacks": int(c.get("source_fallbacks", 0)), "backoff_active": backoff,
                           "backoff_ticks": int(c.get("backoff_ticks", 0)),
                           "skipped": {k: int(c.get(f"skipped_{k}", 0)) for k in ("disabled", "status", "basis", "no_market", "late", "cap", "duplicate")},
                           "hook_errors": int(c.get("hook_errors", 0)), "save_errors": int(c.get("save_errors", 0))},
                "persistence": {"tick_write_failures": int(c.get("tick_write_failures", 0)),
                                "decision_write_failures": int(c.get("decision_write_failures", 0)),
                                "decision_write_retries": int(c.get("decision_write_retries", 0)),
                                "trace_log_write_failures": int(c.get("trace_log_write_failures", 0)),
                                "seq_reuse_rejected": int(c.get("seq_reuse_rejected", 0)),
                                "traces_blocked": blocked, "traces_closing": closing},
                "recovery": {"last": recovery, "recovered_traces": int(c.get("recovered_traces", 0)),
                             "reconstructed_decisions": int(c.get("reconstructed_decisions", 0)),
                             "replayed_ticks": int(c.get("replayed_ticks", 0)), "orphan_ticks": int(c.get("orphan_ticks", 0)),
                             "restart_expired": int(c.get("restart_expired", 0))},
                "files": {"traces_bytes": self.traces_log.size_bytes(), "ticks_bytes": self.ticks_log.size_bytes(),
                          "decisions_bytes": self.decisions_log.size_bytes(), "replay_v27_bytes": self.replay_log.size_bytes()},
                "replay_v27": {"enabled": self.replay_enabled, "setups": int(c.get("replay_setups", 0)),
                               "errors": int(c.get("replay_errors", 0))},
            }
        except Exception as exc:
            return {"version": LAB_VERSION, "error": str(exc)}

    def status(self):
        with self.lock:
            return {"version": APP_VERSION, "lab_version": LAB_VERSION, "mode": LAB_MODE, "enabled": self.enabled,
                    "eligibility_definition": ELIGIBILITY_DEFINITION, "invalid_quote_reasons": list(INVALID_REASONS),
                    "policies": POLICY_SPECS,
                    "index_path": str(self.index_path), "created_at": self.index.get("created_at"),
                    "first_boot_at": self.index.get("first_boot_at"), "last_boot_at": self.index.get("last_boot_at"),
                    "traces_open": len(self.runtimes), "traces_total": len(self.index["traces"]),
                    "outcomes": len(self.index["outcomes"]), "counters": dict(self.index["counters"]),
                    "replay_v27": {"cursor": self.index["replay_v27"].get("cursor"),
                                   "done": len(self.index["replay_v27"].get("done", []))},
                    "health": self.health_summary()}


# ---------------------------------------------------------------------------
# Hooks. Observers only: each wrapper calls the wrapped function first, isolates
# lab work in try/except and returns the original result. Decision functions
# (Monitor.compute, AltShadowMonitor.compute, ShadowLedger.observe_state) and the
# v2.7 journal_if_confirmed chain are not touched.
# ---------------------------------------------------------------------------
core.ENTRY_TIMING_LAB = None

_original_alt_init_v27 = core.AltShadowMonitor.__init__
def _alt_init(self, primary_monitor):
    _original_alt_init_v27(self, primary_monitor)
    self.entry_timing = None
    try:
        if core.ENTRY_TIMING_LAB is None:
            core.ENTRY_TIMING_LAB = EntryTimingLab(primary_monitor, self)
        self.entry_timing = core.ENTRY_TIMING_LAB
    except Exception as exc:
        print("entry timing lab init failed (monitor continues):", exc, flush=True)
core.AltShadowMonitor.__init__ = _alt_init

_original_record_setup = core.ShadowLedger.record_setup
def _record_setup(self, s):
    result = _original_record_setup(self, s)
    try:
        lab = getattr(core, "ENTRY_TIMING_LAB", None)
        if result is True and lab is not None:
            lab.start_trace(dict(s))
    except Exception as exc:
        print("entry timing lab hook:", exc, flush=True)
    return result
core.ShadowLedger.record_setup = _record_setup

_original_send_v27 = core.Handler._send
def _send(self, code, body, content_type="application/json; charset=utf-8", extra_headers=None):
    try:
        if urlparse(self.path).path == "/health" and code == 200:
            raw = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body
            payload = json.loads(raw)
            lab = getattr(core, "ENTRY_TIMING_LAB", None)
            payload["entry_timing_lab"] = lab.health_summary() if lab else {"version": LAB_VERSION, "enabled": False, "mode": LAB_MODE,
                                                                             "recovered": False, "setups_captured": 0, "resolved": 0,
                                                                             "active_traces": 0, "ticks_recorded": 0, "last_capture_utc": None,
                                                                             "production_modified": False}
            body = json.dumps(payload)
    except Exception:
        pass
    return _original_send_v27(self, code, body, content_type, extra_headers)
core.Handler._send = _send

ENTRY_TIMING_PATHS = ("/api/research/entry-timing", "/api/research/entry-timing.json",
                      "/research/entry-timing", "/research/entry-timing.json")
ENTRY_TIMING_PREFIX = "/api/research/entry-timing/"

_original_get_v27 = core.Handler.do_GET
def _do_get(self):
    path = urlparse(self.path).path
    if path in ENTRY_TIMING_PATHS or path.startswith(ENTRY_TIMING_PREFIX):
        if not self._authorized():
            return
        lab = getattr(core, "ENTRY_TIMING_LAB", None)
        if not lab:
            self._send(404, json.dumps({"error": "entry timing lab off"})); return
        params = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        try:
            if path in ENTRY_TIMING_PATHS:
                self._send(200, json.dumps(lab.report(params.get("cohort", "forward"), params.get("segment"), params.get("sort")), default=str)); return
            if path == ENTRY_TIMING_PREFIX + "status":
                self._send(200, json.dumps(lab.status(), default=str)); return
            if path == ENTRY_TIMING_PREFIX + "trace":
                sid = params.get("setup_id", "")
                if not sid:
                    self._send(400, json.dumps({"error": "setup_id required"})); return
                self._send(200, json.dumps(lab.trace_view(sid), default=str)); return
            if path == ENTRY_TIMING_PREFIX + "ticks.jsonl":
                p = lab.ticks_log.path
                self._send(200, p.read_bytes() if p.exists() else b"", "application/x-ndjson; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=entry_timing_ticks_v28.jsonl"}); return
            if path == ENTRY_TIMING_PREFIX + "decisions.jsonl":
                p = lab.decisions_log.path
                self._send(200, p.read_bytes() if p.exists() else b"", "application/x-ndjson; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=entry_timing_decisions_v28.jsonl"}); return
            if path == ENTRY_TIMING_PREFIX + "traces.jsonl":
                p = lab.traces_log.path
                self._send(200, p.read_bytes() if p.exists() else b"", "application/x-ndjson; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=entry_timing_traces_v28.jsonl"}); return
            if path == ENTRY_TIMING_PREFIX + "replay_v27.jsonl":
                p = lab.replay_log.path
                self._send(200, p.read_bytes() if p.exists() else b"", "application/x-ndjson; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=entry_timing_replay_v27.jsonl"}); return
        except Exception as exc:
            self._send(500, json.dumps({"error": str(exc)[:300]})); return
        self._send(404, json.dumps({"error": "not found"})); return
    return _original_get_v27(self)
core.Handler.do_GET = _do_get


def apply_version():
    """Runtime-only version bump (boot line, /health.version, User-Agent). Not applied at import
    so the v2.7 test suite keeps seeing its own version constant."""
    core.APP_VERSION = APP_VERSION
    return APP_VERSION


def main():
    apply_version()
    print(f"VIXION entry timing lab {LAB_VERSION} | mode={LAB_MODE} | enabled={_env_flag('ENTRY_TIMING_ENABLED', '1')} "
          f"| tick={_env_float('ENTRY_TIMING_TICK_SECONDS', 1.0)}s | source={os.getenv('ENTRY_TIMING_TICK_SOURCE', 'orderbook')} "
          f"| production_modified=false", flush=True)
    v27.main()


if __name__ == "__main__":
    main()
