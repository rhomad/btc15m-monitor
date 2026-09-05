#!/usr/bin/env python3
"""VIXION 3.0 — Increment 1: Proposal-Only Execution Gateway.

Scope of this module (and of the whole increment):

    2.8 Decision (frozen)  ->  Proposal (frozen)  ->  append-only event log  ->  replay

It converts an already-observed, immutable VIXION 2.8 ``Decision`` (state FILLED)
into an auditable ORDER PROPOSAL. It has no capability to place, amend or cancel
an order: there is no network import, no Kalshi client, no credential access and
no "execute" abstraction anywhere in this file. Everything below is pure
computation over dicts plus local append-only JSONL persistence.

Design rules (see RUNBOOK_v30_INCREMENT1.md):

  * FAIL CLOSED: any missing/invalid input, unknown policy, invalid price, invalid
    quantity or persistence failure produces NO proposal (a ``Rejection``), never a
    silently corrected proposal.
  * NO HINDSIGHT: a proposal is a pure function of (decision, trace context,
    policy spec, gateway config). It never receives ticks, quotes or outcomes.
  * DETERMINISTIC IDENTITY: ``idempotency_key`` is a SHA-256 over the logical
    identity; the same logical event always yields the same ``proposal_id``.
  * APPEND-ONLY TRUTH: ``proposal_events.jsonl`` is the source of truth; any index
    is a cache rebuilt by ``replay_proposals``.
"""
import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

GATEWAY_VERSION = "3.0-increment1-proposal-only"
SCHEMA_VERSION = 1
SOURCE = "vixion_v28_decision"

# --- 2.8 constants mirrored as *validation expectations* (never redefined logic) ---
V28_FILLED = "FILLED"
V28_ALLOWED_COHORTS = ("forward_v28",)
V28_REQUIRED_SCHEMA_VERSION = 2

# --- Proposal state machine ---------------------------------------------------
PROPOSED = "PROPOSED"
EXPIRED = "EXPIRED"
SIMULATED_APPROVED = "SIMULATED_APPROVED"
SIMULATED_REJECTED = "SIMULATED_REJECTED"
STATES = (PROPOSED, EXPIRED, SIMULATED_APPROVED, SIMULATED_REJECTED)
TERMINAL_STATES = (EXPIRED, SIMULATED_APPROVED, SIMULATED_REJECTED)
EVENT_TYPES = STATES  # every event type names the state it moves the proposal into
TRANSITIONS = {
    None: {PROPOSED},
    PROPOSED: {EXPIRED, SIMULATED_APPROVED, SIMULATED_REJECTED},
    EXPIRED: set(),
    SIMULATED_APPROVED: set(),
    SIMULATED_REJECTED: set(),
}

# Kalshi binary contracts are priced in whole cents 1..99.
PRICE_MIN_CENTS = 1
PRICE_MAX_CENTS = 99
SIDES = ("UP", "DOWN")

DEFAULT_QUANTITY = 1
DEFAULT_MAX_QUANTITY = 1
DEFAULT_TTL_S = 30.0
DEFAULT_OUTPUT_SUBDIR = "v30"
EVENT_LOG_NAME = "proposal_events.jsonl"

# Fields that define proposal *content* (hashed, compared on replay). Envelope
# fields such as recorded_at_utc are deliberately excluded.
CONTENT_FIELDS = (
    "schema_version", "proposal_id", "idempotency_key", "created_at_utc", "expires_at_utc",
    "setup_id", "trace_id", "asset", "policy_id", "source_decision_seq", "source_tick_seq",
    "source_decided_at_utc", "contract_ticker", "side", "observed_ask_cents", "limit_price_cents",
    "quantity", "fair_pct", "conservative_fair_pct", "edge_cents", "reason", "source", "frozen_context",
)


class ProposalError(Exception):
    """Raised for programming/contract violations (invalid transition, corrupt log)."""


class PersistenceError(ProposalError):
    """Raised when a durable write or a log read cannot be trusted."""


# ---------------------------------------------------------------------------
# Small helpers (no wall clock, no I/O)
# ---------------------------------------------------------------------------
def canonical_json(obj):
    """Canonical JSON for hashing/persistence. Non-finite floats raise ValueError (never durable)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False, default=_json_default)


def json_finite(obj):
    """True iff every float reachable in ``obj`` is finite (NaN/Infinity never enter durable content)."""
    if isinstance(obj, bool) or obj is None or isinstance(obj, (str, int)):
        return True
    if isinstance(obj, float):
        return math.isfinite(obj)
    if isinstance(obj, dict):
        return all(json_finite(k) and json_finite(v) for k, v in obj.items())
    if isinstance(obj, (list, tuple, set)):
        return all(json_finite(x) for x in obj)
    return False


def _json_default(o):
    if isinstance(o, (set, tuple)):
        return list(o)
    raise TypeError(f"not JSON serialisable: {type(o).__name__}")


def sha256_hex(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_iso_utc(value):
    """Strict ISO-8601 parser returning an aware UTC datetime, or None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def iso_utc(dt):
    return dt.astimezone(timezone.utc).isoformat()


def ms_to_iso(ms):
    return iso_utc(datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc))


def _is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


# ---------------------------------------------------------------------------
# Gateway configuration (explicit; nothing is read from os.environ implicitly)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GatewayConfig:
    policy_id: str
    quantity: int = DEFAULT_QUANTITY
    max_quantity: int = DEFAULT_MAX_QUANTITY
    ttl_s: float = DEFAULT_TTL_S
    allowed_cohorts: tuple = V28_ALLOWED_COHORTS

    def to_dict(self):
        return {"policy_id": self.policy_id, "quantity": self.quantity, "max_quantity": self.max_quantity,
                "ttl_s": self.ttl_s, "allowed_cohorts": list(self.allowed_cohorts)}


def config_from_env(policy_id, environ=None):
    """Build a GatewayConfig from an explicit environment mapping.

    Only V30_* keys are consulted. Kalshi credential variables are never read.
    Malformed values fail closed (ValueError) instead of falling back to a default.
    """
    env = os.environ if environ is None else environ
    qty = env.get("V30_PROPOSAL_QTY", str(DEFAULT_QUANTITY))
    max_qty = env.get("V30_MAX_PROPOSAL_QTY", str(DEFAULT_MAX_QUANTITY))
    ttl = env.get("V30_PROPOSAL_TTL_S", str(DEFAULT_TTL_S))
    try:
        return GatewayConfig(policy_id=policy_id, quantity=int(qty), max_quantity=int(max_qty), ttl_s=float(ttl))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid V30_* configuration: {exc}") from exc


# ---------------------------------------------------------------------------
# Canonical 2.8 policy catalogue (loaded, never redefined)
# ---------------------------------------------------------------------------
def load_canonical_policy_specs():
    """Return the VIXION 2.8 policy catalogue as {policy_id: spec}.

    The import is lazy so the pure engine can be exercised with any explicit
    catalogue in tests. vixion_v28 is pure data at this call site (POLICY_SPECS);
    nothing is started, polled or written by importing it.
    """
    import vixion_v28 as v28  # noqa: WPS433 (deliberate lazy import)
    return {s["policy"]: dict(s, params=dict(s["params"])) for s in v28.POLICY_SPECS}


def load_canonical_decision_fields():
    """The exact field set VIXION 2.8 persists per decision (Decision.to_dict()); loaded, never copied."""
    import vixion_v28 as v28  # noqa: WPS433 (deliberate lazy import; pure data)
    return tuple(v28.Decision.__dataclass_fields__)


def canonical_params(params):
    """Normalise a policy params mapping for comparison: sorted keys, floats."""
    if params is None:
        return None
    if isinstance(params, (list, tuple)):
        try:
            params = dict(params)
        except (TypeError, ValueError):
            return None
    if not isinstance(params, dict):
        return None
    out = {}
    for k, v in params.items():
        if not _is_num(v):
            return None
        out[str(k)] = float(v)
    return out


def catalog_fingerprint(specs):
    return sha256_hex(canonical_json(sorted(specs.values(), key=lambda s: s["policy"])))


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Rejection:
    """Fail-closed outcome. ``category`` is one of: not_eligible, invalid, duplicate, conflict, persist."""
    category: str
    reason: str
    setup_id: str = None
    policy_id: str = None

    ok = False


@dataclass(frozen=True)
class Built:
    proposal: dict
    ok = True


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------
IDENTITY_FIELDS = ("schema_version", "cohort", "setup_id", "policy_id", "contract_ticker", "side", "source_decision_seq")


def identity_key(cohort, setup_id, policy_id, contract_ticker, side, source_decision_seq):
    """Formal proposal identity: SHA-256 over the canonical JSON of IDENTITY_FIELDS.

    Two decisions that agree on these fields describe the same logical event and
    MUST map to the same proposal; anything else (price, quantity, timestamps)
    is content, not identity. Content disagreement under the same identity is
    reported as a conflict, never merged.
    """
    ident = {"schema_version": SCHEMA_VERSION, "cohort": cohort, "setup_id": setup_id, "policy_id": policy_id,
             "contract_ticker": contract_ticker, "side": side, "source_decision_seq": int(source_decision_seq)}
    return sha256_hex(canonical_json(ident))


def proposal_id_from_key(idempotency_key):
    return "v30p_" + idempotency_key[:32]


def content_hash(proposal):
    return sha256_hex(canonical_json({k: proposal.get(k) for k in CONTENT_FIELDS}))


# ---------------------------------------------------------------------------
# Price rule
# ---------------------------------------------------------------------------
def policy_price_cap_cents(spec, ctx, decision):
    """Hard price guardrail implied by the policy family, or None if the family has none.

    Mirrors PolicyState._match in vixion_v28 *as a cap*, without re-deciding anything:
      threshold / hybrid_threshold -> ask_le
      pullback / hybrid_pullback   -> decision.detection_ask_cents - pullback
                                      (the anchor frozen INTO the 2.8 decision; the
                                      trace `open` ctx does not carry it)
      production                   -> production_max_buy_cents (ShadowLedger max-buy)
      detection / delay            -> no policy cap (observed ask is the only bound)
    Returns a float, None, or an error string.
    """
    fam = spec.get("family")
    p = spec.get("params") or {}
    if fam in ("threshold", "hybrid_threshold"):
        return float(p["ask_le"])
    if fam in ("pullback", "hybrid_pullback"):
        det = decision.get("detection_ask_cents")
        if not _is_num(det):
            return "missing_detection_ask"
        return float(det) - float(p["pullback"])
    if fam == "production":
        cap = ctx.get("production_max_buy_cents")
        if not _is_num(cap) or not (PRICE_MIN_CENTS <= cap <= PRICE_MAX_CENTS):
            return "invalid_production_max_buy"
        return float(cap)
    if fam in ("detection", "delay"):
        return None
    return "unknown_family"


def limit_price_from(observed_ask_cents, cap_cents):
    """limit = floor(min(observed_ask, cap)). Floor guarantees we never propose paying
    above either the observed ask or the policy guardrail once prices are whole cents."""
    bound = float(observed_ask_cents) if cap_cents is None else min(float(observed_ask_cents), float(cap_cents))
    return int(math.floor(bound + 1e-9))


# ---------------------------------------------------------------------------
# build_proposal / validate_proposal
# ---------------------------------------------------------------------------
def build_proposal(decision, ctx, config, policy_specs, decision_fields=None, _rebuild=False):
    """Pure: (2.8 decision dict, 2.8 trace ctx dict, GatewayConfig, catalogue) -> Built | Rejection.

    Never consults the clock, the filesystem, the network or any tick. Everything
    that influenced the proposal is copied into ``frozen_context``.

    ``decision_fields`` is the canonical 2.8 Decision schema-v2 field set. The input
    must carry EXACTLY those keys: unknown keys (e.g. future outcome fields) are
    rejected as ``unexpected_decision_fields``; missing keys as ``missing_decision_fields``.
    """
    if not isinstance(decision, dict):
        return Rejection("invalid", "decision_not_object")
    sid = decision.get("setup_id")
    pol = decision.get("policy")
    if not isinstance(config, GatewayConfig) or not config.policy_id:
        return Rejection("invalid", "missing_policy_id", sid, pol)
    if config.policy_id not in (policy_specs or {}):
        return Rejection("invalid", "unknown_policy_id", sid, config.policy_id)
    spec = policy_specs[config.policy_id]
    fields = tuple(decision_fields) if decision_fields is not None else load_canonical_decision_fields()

    # --- strict schema: exactly the canonical 2.8 Decision field set ---
    if decision.get("schema_version") != V28_REQUIRED_SCHEMA_VERSION:
        return Rejection("invalid", "unsupported_decision_schema", sid, pol)
    unexpected = sorted(set(decision) - set(fields))
    if unexpected:
        return Rejection("invalid", "unexpected_decision_fields:" + ",".join(unexpected)[:200], sid, pol)
    missing = sorted(set(fields) - set(decision))
    if missing:
        return Rejection("invalid", "missing_decision_fields:" + ",".join(missing)[:200], sid, pol)

    # --- eligibility (not an error: the lab counts these separately) ---
    if pol != config.policy_id:
        return Rejection("not_eligible", "policy_not_selected", sid, pol)
    if decision.get("cohort") not in config.allowed_cohorts:
        return Rejection("not_eligible", "cohort_not_allowed", sid, pol)
    if decision.get("state") != V28_FILLED or decision.get("fill") is not True:
        return Rejection("not_eligible", "decision_not_filled", sid, pol)

    # --- decision must match the canonical policy spec ---
    if decision.get("family") != spec.get("family"):
        return Rejection("invalid", "policy_family_mismatch", sid, pol)
    dparams, cparams = canonical_params(decision.get("params")), canonical_params(spec.get("params"))
    if dparams is None or cparams is None or dparams != cparams:
        return Rejection("invalid", "policy_params_mismatch", sid, pol)

    # --- required identity fields ---
    if not isinstance(sid, str) or not sid:
        return Rejection("invalid", "missing_setup_id", sid, pol)
    if not isinstance(ctx, dict) or ctx.get("setup_id") != sid:
        return Rejection("invalid", "missing_or_mismatched_context", sid, pol)
    ticker = ctx.get("market_ticker")
    if not isinstance(ticker, str) or not ticker.strip():
        return Rejection("invalid", "missing_contract", sid, pol)
    side = ctx.get("side")
    if side not in SIDES:
        return Rejection("invalid", "invalid_side", sid, pol)
    asset = ctx.get("asset")
    if not isinstance(asset, str) or not asset:
        return Rejection("invalid", "missing_asset", sid, pol)
    if ctx.get("basis_ok") is not True:
        return Rejection("invalid", "basis_not_ok", sid, pol)
    if ctx.get("cohort") != decision.get("cohort"):
        return Rejection("invalid", "context_cohort_mismatch", sid, pol)
    if sid.split("|", 1)[0] != asset:
        return Rejection("invalid", "setup_id_asset_mismatch", sid, pol)

    # --- FILLED internal consistency (2.8 invariants; never corrected, only checked) ---
    if decision.get("no_fill_reason") is not None:
        return Rejection("invalid", "filled_with_no_fill_reason", sid, pol)
    if decision.get("fillable_1_contract") is not True:
        return Rejection("invalid", "not_fillable_1_contract", sid, pol)
    if decision.get("liquidity") != "verified":
        return Rejection("invalid", "liquidity_unverified", sid, pol)
    if decision.get("economically_valid") is not True:
        return Rejection("invalid", "not_economically_valid", sid, pol)
    fee_model = decision.get("fee_model_cents")
    if not _is_num(fee_model) or fee_model < 0:
        return Rejection("invalid", "invalid_fee_model", sid, pol)
    ask_size = decision.get("ask_size")
    if not _is_num(ask_size) or ask_size < 1.0:
        return Rejection("invalid", "invalid_ask_size", sid, pol)
    for k in ("guardrail_rejections", "ticks_seen"):
        if not _is_int(decision.get(k)) or decision.get(k) < 0:
            return Rejection("invalid", f"invalid_{k}", sid, pol)
    for k in ("trigger_rule", "tick_resolution"):
        if not isinstance(decision.get(k), str):
            return Rejection("invalid", f"invalid_{k}", sid, pol)

    # --- source sequencing (2.8 invariant: decided on its own tick) ---
    dseq, eseq = decision.get("decided_seq"), decision.get("entry_seq")
    if not _is_int(dseq) or dseq < 0 or not _is_int(eseq) or eseq < 0:
        return Rejection("invalid", "invalid_source_seq", sid, pol)
    if dseq != eseq:
        return Rejection("invalid", "decided_seq_ne_entry_seq", sid, pol)
    decided_at = parse_iso_utc(decision.get("decided_at"))
    if decided_at is None:
        return Rejection("invalid", "invalid_decided_at", sid, pol)
    if parse_iso_utc(decision.get("entry_tick_at")) is None:
        return Rejection("invalid", "invalid_entry_tick_at", sid, pol)
    if decision.get("entry_tick_at") != decision.get("decided_at"):
        return Rejection("invalid", "decision_not_frozen_on_tick", sid, pol)
    fetched_at = parse_iso_utc(decision.get("entry_quote_fetched_at"))
    if fetched_at is None:
        return Rejection("invalid", "invalid_entry_quote_fetched_at", sid, pol)
    if fetched_at > decided_at:
        return Rejection("invalid", "quote_fetched_after_decision", sid, pol)

    # --- price ---
    ask = decision.get("entry_ask_cents")
    if not _is_num(ask) or not (PRICE_MIN_CENTS <= ask <= PRICE_MAX_CENTS):
        return Rejection("invalid", "invalid_observed_ask", sid, pol)
    cap = policy_price_cap_cents(spec, ctx, decision)
    if isinstance(cap, str):
        return Rejection("invalid", cap, sid, pol)
    if cap is not None and (not _is_num(cap) or ask > cap + 1e-9):
        # The 2.8 decision claims FILLED above its own guardrail: inconsistent input.
        return Rejection("invalid", "observed_ask_above_policy_cap", sid, pol)
    limit = limit_price_from(ask, cap)
    if not (PRICE_MIN_CENTS <= limit <= PRICE_MAX_CENTS):
        return Rejection("invalid", "invalid_limit_price", sid, pol)
    fee = decision.get("fee_effective_cents")
    if not _is_num(fee) or fee < 0:
        return Rejection("invalid", "invalid_fee", sid, pol)
    if fee + 1e-9 < fee_model:
        return Rejection("invalid", "fee_effective_below_model", sid, pol)
    eff_cost = decision.get("effective_cost_cents")
    if not _is_num(eff_cost) or abs(float(eff_cost) - (float(ask) + float(fee))) > 0.005:
        return Rejection("invalid", "effective_cost_mismatch", sid, pol)
    if float(eff_cost) >= 100.0 or limit + fee >= 100.0:
        return Rejection("invalid", "effective_cost_ge_100", sid, pol)
    bid = decision.get("entry_bid_cents")
    if bid is not None:
        if not _is_num(bid) or not (0.0 <= bid <= 100.0):
            return Rejection("invalid", "invalid_entry_bid", sid, pol)
        if bid > ask + 1e-9:
            return Rejection("invalid", "crossed_entry_quote", sid, pol)
    det_ask = decision.get("detection_ask_cents")
    if not _is_num(det_ask):
        return Rejection("invalid", "missing_detection_ask", sid, pol)
    if not (PRICE_MIN_CENTS <= det_ask <= PRICE_MAX_CENTS):
        return Rejection("invalid", "invalid_detection_ask", sid, pol)
    impr = decision.get("improvement_cents")
    if not _is_num(impr) or abs(float(impr) - (float(det_ask) - float(ask))) > 0.005:
        return Rejection("invalid", "improvement_mismatch", sid, pol)
    cons = ctx.get("cons_pct")
    if not _is_num(cons) or not (0.0 < cons <= 100.0):
        return Rejection("invalid", "invalid_conservative_fair", sid, pol)
    fair = ctx.get("fair_pct")
    if fair is not None and not _is_num(fair):
        return Rejection("invalid", "invalid_fair", sid, pol)
    edge = round(float(cons) - float(limit) - float(fee), 4)

    # --- quantity ---
    qty, max_qty = config.quantity, config.max_quantity
    if not _is_int(max_qty) or max_qty <= 0:
        return Rejection("invalid", "invalid_max_quantity", sid, pol)
    if not _is_int(qty) or qty <= 0:
        return Rejection("invalid", "invalid_quantity", sid, pol)
    if qty > max_qty:
        return Rejection("invalid", "quantity_above_max", sid, pol)

    # --- TTL (deterministic: decision time + ttl, never beyond 2.8 eligibility end) ---
    if not _is_num(config.ttl_s) or config.ttl_s <= 0:
        return Rejection("invalid", "invalid_ttl", sid, pol)
    elig_end_ms = ctx.get("eligibility_end_ms")
    if not _is_int(elig_end_ms):
        return Rejection("invalid", "missing_eligibility_end", sid, pol)
    decided_ms = int(round(decided_at.timestamp() * 1000))
    if decided_ms >= elig_end_ms:
        return Rejection("invalid", "decided_after_eligibility_end", sid, pol)
    detection_ms = ctx.get("detection_ms")
    if not _is_int(detection_ms) or decided_ms < detection_ms:
        return Rejection("invalid", "invalid_detection_ms", sid, pol)
    elapsed = decision.get("elapsed_at_decision_s")
    if not _is_num(elapsed) or abs((decided_ms - detection_ms) / 1000.0 - float(elapsed)) > 0.01:
        return Rejection("invalid", "elapsed_mismatch", sid, pol)
    delay = decision.get("delay_s")
    if not _is_num(delay) or abs(float(delay) - float(elapsed)) > 0.0015:
        return Rejection("invalid", "delay_mismatch", sid, pol)
    policy_deadline_ms = None
    if "max_s" in cparams:
        policy_deadline_ms = detection_ms + int(round(cparams["max_s"] * 1000))
        if decided_ms >= policy_deadline_ms:
            return Rejection("invalid", "decided_after_policy_deadline", sid, pol)
    expires_ms = min(decided_ms + int(round(config.ttl_s * 1000)), elig_end_ms,
                     policy_deadline_ms if policy_deadline_ms is not None else elig_end_ms)

    cohort = decision["cohort"]
    key = identity_key(cohort, sid, config.policy_id, ticker, side, dseq)
    frozen = {
        "gateway_version": GATEWAY_VERSION,
        "decision": {k: decision[k] for k in fields},  # canonical field set only
        "trace_ctx": {k: ctx.get(k) for k in ("cohort", "setup_id", "asset", "side", "window_start_utc", "window_start_ms",
                                                 "setup_minute", "detection_at", "detection_ms", "eligibility_end_ms",
                                                 "market_ticker", "cons_pct", "fair_pct", "threshold_bp", "distance_bp",
                                                 "target_gap_bp", "basis_ok", "edge_min_points", "fee_buffer_cents",
                                                 "production_max_buy_cents", "signal_status",
                                                 "production_detection_ask_cents")},
        "policy_spec": {"policy": spec["policy"], "family": spec["family"], "params": dict(spec["params"])},
        "policy_cap_cents": cap,
        "policy_deadline_utc": ms_to_iso(policy_deadline_ms) if policy_deadline_ms is not None else None,
        "ttl_rule": "expires=min(decided_at+ttl, eligibility_end, detection+max_s if hybrid)",
        "price_rule": "limit=floor(min(observed_ask, policy_cap)); edge=cons_pct-limit-fee_effective(decision)",
        "config": config.to_dict(),
        "eligibility_end_utc": ms_to_iso(elig_end_ms),
    }
    proposal = {
        "schema_version": SCHEMA_VERSION,
        "proposal_id": proposal_id_from_key(key),
        "idempotency_key": key,
        "created_at_utc": iso_utc(decided_at),
        "expires_at_utc": ms_to_iso(expires_ms),
        "setup_id": sid,
        "trace_id": sid,  # 2.8 traces are keyed by setup_id (one trace per setup)
        "asset": asset,
        "policy_id": config.policy_id,
        "source_decision_seq": int(dseq),
        "source_tick_seq": int(eseq),
        "source_decided_at_utc": iso_utc(decided_at),
        "contract_ticker": ticker.strip(),
        "side": side,
        "observed_ask_cents": float(ask),
        "limit_price_cents": int(limit),
        "quantity": int(qty),
        "fair_pct": float(fair) if fair is not None else None,
        "conservative_fair_pct": float(cons),
        "edge_cents": edge,
        "status": PROPOSED,
        "reason": f"v28 {decision.get('policy')} FILLED at seq {dseq}: {decision.get('trigger_rule') or ''}"[:300],
        "source": SOURCE,
        "frozen_context": frozen,
    }
    if not json_finite(proposal):
        return Rejection("invalid", "non_finite_content", sid, pol)
    try:
        proposal["content_hash"] = content_hash(proposal)
    except (TypeError, ValueError):
        return Rejection("invalid", "non_finite_content", sid, pol)
    err = validate_proposal(proposal, policy_specs, config, rebuild=_rebuild)
    if err:
        return Rejection("invalid", f"self_validation_failed:{err}", sid, pol)
    return Built(proposal)


def rebuild_from_frozen(proposal, policy_specs):
    """Re-derive a proposal purely from its own frozen_context through build_proposal.

    Returns the rebuilt proposal dict, or a reason string. A persisted proposal is only
    trusted if it is reproducible: an internally consistent but forged payload (e.g. a
    re-signed limit price) cannot survive this check because the frozen decision, ctx,
    config and canonical spec determine every content field.
    """
    fc = proposal.get("frozen_context")
    if not isinstance(fc, dict):
        return "frozen_context_missing"
    cfgd = fc.get("config")
    dec, ctx = fc.get("decision"), fc.get("trace_ctx")
    if not isinstance(cfgd, dict) or not isinstance(dec, dict) or not isinstance(ctx, dict):
        return "frozen_context_incomplete"
    try:
        config = GatewayConfig(policy_id=cfgd.get("policy_id"), quantity=cfgd.get("quantity"),
                               max_quantity=cfgd.get("max_quantity"), ttl_s=cfgd.get("ttl_s"),
                               allowed_cohorts=tuple(cfgd.get("allowed_cohorts") or ()))
    except (TypeError, ValueError):
        return "frozen_config_invalid"
    res = build_proposal(dec, ctx, config, policy_specs, _rebuild=False)
    if not res.ok:
        return f"not_reproducible:{res.reason}"
    return res.proposal


def validate_proposal(proposal, policy_specs=None, config=None, rebuild=True):
    """Structural validation of a proposal dict. Returns None when valid, else a reason string.

    Used on build (self-check, rebuild=False) and on replay (integrity of persisted
    content; with policy_specs the proposal is additionally re-derived from its own
    frozen_context and must hash identically).
    """
    p = proposal
    if not isinstance(p, dict):
        return "not_object"
    if p.get("schema_version") != SCHEMA_VERSION:
        return "schema_version"
    for k in CONTENT_FIELDS:
        if k not in p:
            return f"missing:{k}"
    for k in ("setup_id", "trace_id", "asset", "policy_id", "contract_ticker", "idempotency_key", "proposal_id", "source"):
        if not isinstance(p.get(k), str) or not p[k]:
            return f"empty:{k}"
    if p["side"] not in SIDES:
        return "invalid_side"
    if policy_specs is not None and p["policy_id"] not in policy_specs:
        return "unknown_policy_id"
    if config is not None and p["policy_id"] != config.policy_id:
        return "policy_mismatch"
    if not _is_int(p["source_decision_seq"]) or p["source_decision_seq"] < 0:
        return "invalid_source_decision_seq"
    if p["source_tick_seq"] != p["source_decision_seq"]:
        return "source_tick_seq_ne_decision_seq"
    if not _is_num(p["observed_ask_cents"]) or not (PRICE_MIN_CENTS <= p["observed_ask_cents"] <= PRICE_MAX_CENTS):
        return "invalid_observed_ask"
    if not _is_int(p["limit_price_cents"]) or not (PRICE_MIN_CENTS <= p["limit_price_cents"] <= PRICE_MAX_CENTS):
        return "invalid_limit_price"
    if p["limit_price_cents"] > p["observed_ask_cents"] + 1e-9:
        return "limit_above_observed_ask"
    cap = (p.get("frozen_context") or {}).get("policy_cap_cents")
    if cap is not None and (not _is_num(cap) or p["limit_price_cents"] > cap + 1e-9):
        return "limit_above_policy_cap"
    if not _is_int(p["quantity"]) or p["quantity"] <= 0:
        return "invalid_quantity"
    if config is not None and p["quantity"] > config.max_quantity:
        return "quantity_above_max"
    if not _is_num(p["conservative_fair_pct"]):
        return "invalid_conservative_fair"
    if p["fair_pct"] is not None and not _is_num(p["fair_pct"]):
        return "invalid_fair"
    if not _is_num(p["edge_cents"]):
        return "invalid_edge"
    c, e, d = parse_iso_utc(p["created_at_utc"]), parse_iso_utc(p["expires_at_utc"]), parse_iso_utc(p["source_decided_at_utc"])
    if c is None or e is None or d is None:
        return "invalid_timestamps"
    if e <= c:
        return "expires_not_after_created"
    if c != d:
        return "created_ne_decided"
    if p["status"] not in STATES:
        return "invalid_status"
    if p["source"] != SOURCE:
        return "invalid_source"
    expected_key = identity_key((p["frozen_context"] or {}).get("decision", {}).get("cohort"), p["setup_id"], p["policy_id"],
                                p["contract_ticker"], p["side"], p["source_decision_seq"])
    if p["idempotency_key"] != expected_key or p["proposal_id"] != proposal_id_from_key(expected_key):
        return "identity_mismatch"
    if not json_finite(p):
        return "non_finite_content"
    try:
        if p.get("content_hash") != content_hash(p):
            return "content_hash_mismatch"
    except (TypeError, ValueError):
        return "non_finite_content"
    if rebuild and policy_specs is not None:
        rebuilt = rebuild_from_frozen(p, policy_specs)
        if isinstance(rebuilt, str):
            return rebuilt
        if rebuilt.get("content_hash") != p.get("content_hash"):
            return "not_reproducible:content_hash_differs"
    return None


def evaluate_expiry(proposal, as_of_utc):
    """Pure, deterministic: True iff the proposal's frozen expires_at_utc <= as_of_utc."""
    exp = parse_iso_utc(proposal["expires_at_utc"])
    if exp is None:
        raise ProposalError("proposal has invalid expires_at_utc")
    return exp <= as_of_utc.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Event log (append-only source of truth) and state machine
# ---------------------------------------------------------------------------
EVENT_ID_FIELDS = ("schema_version", "event_type", "proposal_id", "at_utc", "reason", "content_hash")
EVENT_ENVELOPE_FIELDS = ("schema_version", "event_type", "proposal_id", "at_utc", "reason", "event_id",
                         "proposal", "content_hash", "recorded_at_utc")
REASON_MAX_LEN = 300


def event_id_for(event):
    """The single deterministic event identity formula (used by make_event AND validate_event)."""
    return sha256_hex(canonical_json({k: event.get(k) for k in EVENT_ID_FIELDS}))


def validate_event(event):
    """Central envelope check. Returns None when the event is well-formed and self-consistent,
    else a reason string. Nothing in the envelope is trusted: event_id is recomputed."""
    if not isinstance(event, dict):
        return "event_not_object"
    if set(event) - set(EVENT_ENVELOPE_FIELDS):
        return "unexpected_event_fields"
    if event.get("schema_version") != SCHEMA_VERSION:
        return "event_schema_version"
    et = event.get("event_type")
    if et not in EVENT_TYPES:
        return "unknown_event_type"
    pid = event.get("proposal_id")
    if not isinstance(pid, str) or not pid:
        return "invalid_proposal_id"
    if parse_iso_utc(event.get("at_utc")) is None:
        return "invalid_at_utc"
    reason = event.get("reason")
    if not isinstance(reason, str) or len(reason) > REASON_MAX_LEN:
        return "invalid_reason"
    if "recorded_at_utc" in event and parse_iso_utc(event.get("recorded_at_utc")) is None:
        return "invalid_recorded_at_utc"
    eid = event.get("event_id")
    if not isinstance(eid, str) or len(eid) != 64:
        return "missing_event_id"
    if not json_finite(event):
        return "non_finite_event"
    try:
        if eid != event_id_for(event):
            return "event_id_mismatch"
    except (TypeError, ValueError):
        return "non_finite_event"
    if et == PROPOSED:
        proposal = event.get("proposal")
        if not isinstance(proposal, dict):
            return "missing_proposal_payload"
        ch = event.get("content_hash")
        if not isinstance(ch, str) or not ch:
            return "missing_event_content_hash"
        if ch != proposal.get("content_hash"):
            return "event_content_hash_mismatch"
        if proposal.get("proposal_id") != pid:
            return "proposal_id_mismatch"
        if proposal.get("status") != PROPOSED:
            return "proposal_status_not_proposed"
    else:
        if "proposal" in event or "content_hash" in event:
            return "terminal_event_carries_payload"
    return None


def make_event(event_type, proposal_id, at_utc, reason="", proposal=None, recorded_at_utc=None):
    if event_type not in EVENT_TYPES:
        raise ProposalError(f"unknown event type {event_type!r}")
    if event_type == PROPOSED and proposal is None:
        raise ProposalError("PROPOSED event requires the proposal payload")
    if event_type != PROPOSED and proposal is not None:
        raise ProposalError("only PROPOSED carries a payload")
    at = at_utc if isinstance(at_utc, str) else iso_utc(at_utc)
    if parse_iso_utc(at) is None:
        raise ProposalError("event at_utc must be ISO-8601 UTC")
    ev = {"schema_version": SCHEMA_VERSION, "event_type": event_type, "proposal_id": proposal_id,
          "at_utc": at, "reason": str(reason or "")[:REASON_MAX_LEN]}
    if proposal is not None:
        ev["proposal"] = proposal
        ev["content_hash"] = proposal.get("content_hash")
    # Deterministic event identity: same logical event -> same event_id. recorded_at_utc
    # (wall clock of the append) is deliberately outside the identity: it is envelope
    # metadata, not part of the logical event, so a replay on another machine/day of the
    # same logical event is still recognised as the same event.
    try:
        ev["event_id"] = event_id_for(ev)
    except (TypeError, ValueError) as exc:
        raise ProposalError(f"event content is not finite JSON: {exc}") from exc
    if recorded_at_utc is not None:
        ev["recorded_at_utc"] = recorded_at_utc if isinstance(recorded_at_utc, str) else iso_utc(recorded_at_utc)
    err = validate_event(ev)
    if err:
        raise ProposalError(f"cannot build event: {err}")
    return ev


def apply_proposal_event(state, event, policy_specs=None):
    """Pure state-machine step.

    ``state`` is None (unknown proposal) or a dict {proposal, status, history}.
    Returns (new_state, outcome) where outcome is 'applied', 'duplicate' (idempotent
    no-op) or raises ProposalError on an invalid transition / corrupt payload.
    Terminal states never change.
    """
    err = validate_event(event)
    if err:
        raise ProposalError(f"invalid event envelope: {err}")
    et = event["event_type"]
    current = None if state is None else state["status"]
    if et == PROPOSED:
        proposal = event["proposal"]
        err = validate_proposal(proposal, policy_specs)
        if err:
            raise ProposalError(f"corrupt PROPOSED payload: {err}")
        if state is not None:
            # Only the SAME logical event (identical event_id) is idempotent. Same content
            # under a different envelope, or different content, is a conflict.
            if event["event_id"] == state["history"][0]:
                return state, "duplicate"
            raise ProposalError("PROPOSED conflict: proposal already exists with a different event")
        return {"proposal": dict(proposal), "status": PROPOSED, "history": [event["event_id"]]}, "applied"
    if state is None:
        raise ProposalError(f"{et} for unknown proposal {event['proposal_id']}")
    if event["event_id"] in state["history"]:
        return state, "duplicate"
    if et not in TRANSITIONS[current]:
        raise ProposalError(f"invalid transition {current} -> {et}")
    # Causal invariants: no terminal event before creation; EXPIRED never before expires_at.
    at = parse_iso_utc(event.get("at_utc"))
    created = parse_iso_utc(state["proposal"]["created_at_utc"])
    expires = parse_iso_utc(state["proposal"]["expires_at_utc"])
    if at is None or created is None or expires is None:
        raise ProposalError("event/proposal timestamps invalid")
    if at < created:
        raise ProposalError(f"{et} at {event.get('at_utc')} precedes creation {state['proposal']['created_at_utc']}")
    if et == EXPIRED and at < expires:
        raise ProposalError(f"EXPIRED at {event.get('at_utc')} precedes expires_at {state['proposal']['expires_at_utc']}")
    new = {"proposal": dict(state["proposal"], status=et), "status": et, "history": list(state["history"]) + [event["event_id"]]}
    return new, "applied"


class ProposalEventLog:
    """Append-only JSONL. Every append is flushed and fsynced; a failed append raises
    PersistenceError so callers fail closed (the proposal is NOT considered created)."""

    def __init__(self, path):
        self.path = Path(path)

    def append(self, event):
        err = validate_event(event)
        if err:
            raise ProposalError(f"refusing to persist invalid event: {err}")
        try:
            line = canonical_json(event)
        except (TypeError, ValueError) as exc:
            raise ProposalError(f"event is not finite JSON: {exc}") from exc
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8", newline="\n") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            raise PersistenceError(f"append failed: {exc}") from exc
        return event

    def read(self):
        if not self.path.exists():
            return []
        out = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise PersistenceError(f"corrupt event log line {n}: {exc}") from exc
                out.append(rec)
        return out


def replay_proposals(events, policy_specs=None):
    """Rebuild the full state (the cache/index) from the event stream. Raises on corruption.

    Returns {"proposals": {proposal_id: state}, "stats": {...}}. Deterministic: same
    events in the same order -> identical output.
    """
    proposals = {}
    stats = {"events": 0, "applied": 0, "duplicate": 0}
    for ev in events:
        stats["events"] += 1
        pid = ev.get("proposal_id")
        new, outcome = apply_proposal_event(proposals.get(pid), ev, policy_specs)
        proposals[pid] = new
        stats[outcome] += 1
    return {"proposals": proposals, "stats": stats}


class ProposalStore:
    """Log-first store: the in-memory index is only a cache of ``replay_proposals(log)``."""

    def __init__(self, log_path, policy_specs=None):
        self.log = ProposalEventLog(log_path)
        self.policy_specs = policy_specs
        self.rebuild()

    def rebuild(self):
        self.index = replay_proposals(self.log.read(), self.policy_specs)["proposals"]
        return self.index

    def get(self, proposal_id):
        return self.index.get(proposal_id)

    def record(self, event):
        """Validate the transition in memory, then write, then commit to the cache.
        If the write fails the cache is untouched and PersistenceError propagates."""
        pid = event["proposal_id"]
        new, outcome = apply_proposal_event(self.index.get(pid), event, self.policy_specs)
        if outcome == "duplicate":
            return outcome
        self.log.append(event)
        self.index[pid] = new
        return outcome

    def propose(self, proposal, recorded_at_utc=None):
        return self.record(make_event(PROPOSED, proposal["proposal_id"], proposal["created_at_utc"],
                                      "proposed", proposal=proposal, recorded_at_utc=recorded_at_utc))

    def expire_due(self, as_of_utc, recorded_at_utc=None):
        expired = []
        for pid, st in sorted(self.index.items()):
            if st["status"] == PROPOSED and evaluate_expiry(st["proposal"], as_of_utc):
                self.record(make_event(EXPIRED, pid, as_of_utc, "ttl_elapsed", recorded_at_utc=recorded_at_utc))
                expired.append(pid)
        return expired


# ---------------------------------------------------------------------------
# Gateway: decisions -> proposals -> store (the only orchestration in Increment 1)
# ---------------------------------------------------------------------------
class ContextConflictError(ProposalError):
    """Two `open` trace events for the same setup_id carry different ctx."""


class ContextInputError(ProposalError):
    """A trace record that claims to be an `open` is malformed (or a record is not an object)."""


def contexts_from_trace_events(records):
    """setup_id -> ctx from 2.8 `open` trace events.

    First valid open is accepted; a canonical-identical duplicate is an idempotent
    no-op; ANY differing duplicate raises ContextConflictError (fail closed, no
    first-open-wins). A record that declares event="open" but has an invalid
    setup_id/ctx raises ContextInputError. Other 2.8 event kinds (e.g. "close") are ignored.
    """
    ctxs = {}
    for n, rec in enumerate(records, 1):
        if not isinstance(rec, dict):
            raise ContextInputError(f"trace record {n} is not an object")
        if rec.get("event") != "open":
            continue
        sid = rec.get("setup_id")
        ctx = rec.get("ctx")
        if not isinstance(sid, str) or not sid:
            raise ContextInputError(f"trace open record {n} has an invalid setup_id")
        if not isinstance(ctx, dict) or ctx.get("setup_id") != sid or not json_finite(ctx):
            raise ContextInputError(f"trace open record {n} ({sid}) has an invalid ctx")
        try:
            cj = canonical_json(ctx)
        except (TypeError, ValueError) as exc:
            raise ContextInputError(f"trace open record {n} ({sid}) ctx is not finite JSON: {exc}") from exc
        if sid in ctxs:
            if canonical_json(ctxs[sid]) != cj:
                raise ContextConflictError(f"conflicting trace open events for setup_id {sid}")
            continue
        ctxs[sid] = ctx
    return ctxs


def empty_report():
    return {"decisions_seen": 0, "eligible_decisions": 0, "proposals_created": 0, "duplicate_suppressed": 0,
            "invalid_rejected": 0, "conflict_rejected": 0, "persist_failed": 0, "expired": 0,
            "simulated_approved": 0, "simulated_rejected": 0,
            "by_asset": {}, "by_policy_id": {}, "by_side": {}, "rejection_reasons": {}}


def _bump(d, key):
    d[str(key)] = d.get(str(key), 0) + 1


def process_decisions(decisions, contexts, config, policy_specs, store, recorded_at_utc=None):
    """Run the gateway over already-observed 2.8 decisions.

    ``contexts`` maps setup_id -> trace ctx. Returns (report, results) where results is
    a list of (decision, Built|Rejection). Persistence failures are fail-closed:
    the proposal is counted under persist_failed and never reported as created.
    """
    report = empty_report()
    results = []
    fields = load_canonical_decision_fields()
    for dec in decisions:
        report["decisions_seen"] += 1
        ctx = contexts.get(dec.get("setup_id")) if isinstance(dec, dict) else None
        if isinstance(dec, dict) and "ctx" in dec:
            # Self-contained record: the embedded ctx is an explicit envelope key, stripped
            # before schema validation. It never overrides a trace ctx: both must be
            # canonical-identical, otherwise the record is rejected.
            embedded = dec["ctx"]
            dec = {k: v for k, v in dec.items() if k != "ctx"}
            try:
                conflict = ctx is not None and canonical_json(ctx) != canonical_json(embedded)
            except (TypeError, ValueError):
                conflict = True
            if conflict:
                report["invalid_rejected"] += 1
                _bump(report["rejection_reasons"], "context_conflict")
                results.append((dec, Rejection("invalid", "context_conflict", dec.get("setup_id"), dec.get("policy"))))
                continue
            ctx = embedded
        res = build_proposal(dec, ctx, config, policy_specs, fields)
        if isinstance(res, Rejection):
            if res.category == "not_eligible":
                pass
            else:
                report["invalid_rejected"] += 1
                _bump(report["rejection_reasons"], res.reason)
            results.append((dec, res))
            continue
        report["eligible_decisions"] += 1
        p = res.proposal
        try:
            outcome = store.propose(p, recorded_at_utc=recorded_at_utc)
        except PersistenceError as exc:
            report["persist_failed"] += 1
            _bump(report["rejection_reasons"], "persist_failed")
            results.append((dec, Rejection("persist", f"persist_failed:{exc}", p["setup_id"], p["policy_id"])))
            continue
        except ProposalError as exc:
            report["conflict_rejected"] += 1
            _bump(report["rejection_reasons"], "idempotency_conflict")
            results.append((dec, Rejection("conflict", f"idempotency_conflict:{exc}", p["setup_id"], p["policy_id"])))
            continue
        if outcome == "duplicate":
            report["duplicate_suppressed"] += 1
            results.append((dec, Rejection("duplicate", "duplicate_suppressed", p["setup_id"], p["policy_id"])))
            continue
        report["proposals_created"] += 1
        _bump(report["by_asset"], p["asset"])
        _bump(report["by_policy_id"], p["policy_id"])
        _bump(report["by_side"], p["side"])
        results.append((dec, res))
    return report, results


def summarize_store(store, report=None):
    """Fill the state-distribution counters from the (replayed) store."""
    report = report or empty_report()
    for st in store.index.values():
        s = st["status"]
        if s == EXPIRED:
            report["expired"] += 1
        elif s == SIMULATED_APPROVED:
            report["simulated_approved"] += 1
        elif s == SIMULATED_REJECTED:
            report["simulated_rejected"] += 1
    report["proposals_in_store"] = len(store.index)
    return report
