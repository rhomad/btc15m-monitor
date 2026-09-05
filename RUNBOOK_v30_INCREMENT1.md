# RUNBOOK v3.0 — Increment 1: Proposal-Only Execution Gateway

Status: **research / audit only. Not deployed. Not wired into production.** VIXION 2.8
(`main @ 7e68373`) remains intact; nothing in this increment is imported by `app.py`,
`vixion_v27.py`, `vixion_v28.py`, either launcher or the `Dockerfile`.

## 1. What Increment 1 is

```
2.8 Decision (frozen, FILLED)  ─►  build_proposal  ─►  Proposal (frozen)
                                                         │
                                        proposal_events.jsonl (append-only, fsync)
                                                         │
                                          replay_proposals ─► index (cache)
```

`vixion_v30_proposal.py` turns an already-observed, immutable VIXION 2.8 `Decision`
plus the frozen trace context (`entry_timing_traces_v28.jsonl` → `event=open` → `ctx`)
into an auditable **order proposal**. It is a pure module: no network import, no
Kalshi client, no credentials, no `execute()` abstraction, no order surface. Nothing
here can place, amend or cancel an order — by construction, not by configuration.

Files:

| File | Role |
|---|---|
| `vixion_v30_proposal.py` | pure core: `build_proposal`, `validate_proposal`, `apply_proposal_event`, `replay_proposals`, `ProposalEventLog`, `ProposalStore`, `process_decisions` |
| `scripts/v30_proposal_lab.py` | offline CLI over local JSONL files |
| `tests/test_v30_proposal_gateway.py` | T01–T31, Increment 1.1 consistency/TTL/context/causality suites, envelope integrity, fuzz, quote consistency, malformed opens, auditor regressions A–L and M–X |
| `tests/fixtures/v30/*` | **synthetic** fixtures built with the real 2.8 engine (no production data) |
| `.github/workflows/v30-proposal-regression.yml` | feature-branch / PR gate, no deploy |

## 2. Running the lab (offline)

```
python scripts/v30_proposal_lab.py \
    --decisions data/entry_timing_decisions_v28.jsonl \
    --traces    data/entry_timing_traces_v28.jsonl \
    --policy    ask_le_90c \
    --output    ./data/v30
```
`--policy` is mandatory and must be one of the 37 canonical 2.8 policies
(`vixion_v28.POLICY_NAMES`, loaded, never copied). No automatic "best policy" exists.

Lifecycle helpers (all deterministic, all local):

* `--as-of <ISO-UTC>` — emits `EXPIRED` for `PROPOSED` proposals whose frozen
  `expires_at_utc <= as-of`.
* `--reviews reviews.jsonl` — applies explicit `SIMULATED_APPROVED` / `SIMULATED_REJECTED`
  events (`{"proposal_id","verdict","at_utc","reason"}`). Any other verdict is skipped.
* `--replay-only` — rebuilds the index from the log and prints the report; builds nothing.
* `--json` — machine-readable summary (includes `policy_catalog_sha256`).

Configuration (explicit `V30_*` variables only; nothing else is read):

| Variable | Default | Rule |
|---|---|---|
| `V30_PROPOSAL_QTY` | 1 | `<= 0` → reject |
| `V30_MAX_PROPOSAL_QTY` | 1 | `qty > max` → reject; malformed → fail closed |
| `V30_PROPOSAL_TTL_S` | 30 | `expires_at = min(decided_at + ttl, 2.8 eligibility_end, policy_deadline if hybrid)` — see §5c |

Exit codes: `0` ok; `2` fail-closed (missing/unknown policy, bad config, corrupt log,
persistence failure).

## 3. Proposal schema (schema_version 1)

`schema_version, proposal_id, idempotency_key, created_at_utc, expires_at_utc, setup_id,
trace_id, asset, policy_id, source_decision_seq, source_tick_seq, source_decided_at_utc,
contract_ticker, side, observed_ask_cents, limit_price_cents, quantity, fair_pct,
conservative_fair_pct, edge_cents, status, reason, source, frozen_context, content_hash`

* `created_at_utc == source_decided_at_utc` (the proposal is logically created at the
  decision instant; wall-clock is recorded separately as the event envelope's
  `recorded_at_utc` and excluded from hashing).
* `trace_id == setup_id` (2.8 traces are keyed by setup_id, one trace per setup).
* `fair_pct` is `null` when the 2.8 trace ctx does not carry it (it carries `cons_pct`
  only); `conservative_fair_pct` is mandatory.
* **Strict decision schema (Increment 1.1):** the input must carry exactly the 31
  canonical 2.8 `Decision` schema-v2 fields (loaded from
  `vixion_v28.Decision.__dataclass_fields__`, never copied). Unknown keys — e.g. future
  outcome fields such as `kalshi_result`, `settlement`, `pnl` — are rejected as
  `unexpected_decision_fields:<keys>`; missing keys as `missing_decision_fields:<keys>`.
  Ignoring was deliberately NOT chosen: rejection is deterministic and leaves no
  path by which a polluted record could reach `content_hash`.
* `frozen_context` is sufficient to rebuild the proposal (§6b): `decision` (canonical
  field set), `trace_ctx` subset, `policy_spec`, `config`, resolved cap/deadline.
* `frozen_context.decision` = exactly the canonical field set; plus the declared trace
  ctx subset, policy spec, resolved price cap, policy deadline, config and eligibility
  end. Nothing is recomputed later; ctx extras are never frozen.
* `content_hash` = SHA-256 over the content fields; verified on every replay.

## 4. Identity / idempotency

```
idempotency_key = sha256(canonical_json({schema_version, cohort, setup_id, policy_id,
                                         contract_ticker, side, source_decision_seq}))
proposal_id     = "v30p_" + idempotency_key[:32]
```
Same logical event → same proposal (T10). Replaying the same decision is suppressed as a
duplicate (T11). Same identity with **different content** (e.g. a different TTL config)
is a `conflict`: rejected, never merged, never appended.

## 5. Price rule

```
policy_cap = ask_le                      (threshold / hybrid_threshold)
           = detection_ask_cents - pullback   (pullback / hybrid_pullback; anchor frozen in the 2.8 decision)
           = production_max_buy_cents    (production_baseline; ShadowLedger max-buy)
           = none                        (entry_at_detection / delay_*)
limit_price_cents = floor(min(observed_ask_cents, policy_cap))     # whole cents, 1..99
edge_cents        = cons_pct - limit_price_cents - fee_effective_cents(decision)
```
A decision that claims FILLED above its own cap is rejected (`observed_ask_above_policy_cap`).
`limit + fee >= 100` is rejected. The limit can never exceed the observed ask nor the cap
(also re-checked by `validate_proposal` on replay).

## 5b. Decision ↔ canonical policy and FILLED consistency (Increment 1.1)

Before any proposal: `decision.family == spec.family`; `canonical_params(decision.params)
== canonical_params(spec.params)` (sorted keys, floats; tuple-of-pairs accepted);
`ctx.cohort == decision.cohort`; `ctx.setup_id == decision.setup_id`; `setup_id` asset
prefix == `ctx.asset`. For FILLED: `no_fill_reason is None`, `fillable_1_contract is True`,
`liquidity == "verified"`, `economically_valid is True`, `fee_model_cents ≥ 0`,
`fee_effective_cents ≥ fee_model_cents`, `|effective_cost − (ask + fee_effective)| ≤ 0.005`,
`effective_cost < 100`, `entry_bid ≤ ask`, `detection_ask_cents` numeric,
`|improvement − (detection_ask − ask)| ≤ 0.005`, `decided_seq == entry_seq`,
`entry_tick_at == decided_at`, `|(decided − detection_ms)/1000 − elapsed| ≤ 0.01 s`,
`|delay_s − elapsed| ≤ 0.0015`. Nothing is corrected; every violation is a rejection.

## 5c. Policy-specific TTL (Increment 1.1)

```
policy_deadline = detection_ms + params.max_s * 1000     (hybrid_threshold / hybrid_pullback only)
expires_at      = min(decided_at + ttl, eligibility_end, policy_deadline)
decided_at >= policy_deadline  -> reject decided_after_policy_deadline
```
`frozen_context.policy_deadline_utc` records the deadline (`null` for non-hybrid).

## 5d. Trace contexts (Increment 1.1)

`contexts_from_trace_events`: first valid `open` accepted; canonical-identical duplicate
= no-op; any differing duplicate → `ContextConflictError`. The CLI validates all inputs
before opening the store and exits 2 without creating the output directory or log.
A self-contained decision record (`ctx` key) never overrides a trace ctx: if both exist
they must be canonical-identical, else `context_conflict` (rejected, nothing appended).

## 5e. Frozen quote consistency and untrusted input (Increment 1.2)

For a FILLED decision additionally: `ask_size` numeric, finite, `≥ 1`; `entry_bid_cents`
`null` or numeric in `[0, 100]` and `≤ ask` (crossed → reject); `detection_ask_cents` in
`[1, 99]`; `decided_at`, `entry_tick_at`, `entry_quote_fetched_at` ISO-8601 timezone-aware;
`entry_quote_fetched_at ≤ decided_at`; `guardrail_rejections`/`ticks_seen` non-negative
ints; `trigger_rule`/`tick_resolution` strings. `production_max_buy_cents` (production
family only) must be numeric, finite, in `[1, 99]` → else `invalid_production_max_buy`.

`build_proposal` returns `Built | Rejection` for ANY JSON-compatible input (fuzzed with
`None, "abc", [], {}, True, NaN, ±Infinity` over every decision/ctx/config field); no
`float()`/`int()` is applied to external data before a type/finiteness check.
`canonical_json` uses `allow_nan=False`; any NaN/Infinity reaching durable content is
`non_finite_content` (build), `non_finite_event` (envelope) or a `ProposalError` before
the append — never a crash and never a byte on disk.

## 5f. Trace opens (Increment 1.2)

A record declaring `event="open"` with an invalid `setup_id` or `ctx` (not an object,
`ctx.setup_id` ≠ record `setup_id`, non-finite) raises `ContextInputError`; non-object
records too. Other 2.8 event kinds (`close`) are ignored. The CLI exits 2 before opening
the store.

## 6. State machine

```
PROPOSED ──► EXPIRED
         ──► SIMULATED_APPROVED
         ──► SIMULATED_REJECTED
```
Terminal states are immutable. Exact duplicate events (same `event_id`, which covers
type, proposal_id, at_utc, reason and payload hash) are idempotent no-ops; any other
transition out of a terminal state raises `ProposalError`. Causal invariants
(Increment 1.1): every terminal event needs `at_utc >= created_at_utc`; `EXPIRED`
additionally needs `at_utc >= expires_at_utc`. A persisted log violating these fails
replay closed. Only these four event types exist; there is no SUBMITTED/LIVE/ORDER state.

## 6b. EVENT LOG INTEGRITY (Increment 1.2)

Nothing in the envelope is trusted. `validate_event` runs before ANY apply (in-memory,
on append and on replay) and checks: the envelope has only the declared fields
(`schema_version, event_type, proposal_id, at_utc, reason, event_id, proposal,
content_hash, recorded_at_utc`); `schema_version == 1`; `event_type` in the catalogue;
non-empty `proposal_id`; `at_utc` ISO tz-aware; `reason` string ≤ 300; `recorded_at_utc`
(optional) ISO tz-aware; `event_id` present and **equal to the recomputation** of
`sha256(canonical_json({schema_version, event_type, proposal_id, at_utc, reason,
content_hash}))` — the single formula shared by `make_event`. For `PROPOSED`: payload
present, `event.content_hash == proposal.content_hash`, envelope `proposal_id ==
proposal.proposal_id`, `status == PROPOSED`; then `validate_proposal` re-hashes the real
content and **re-derives the proposal from its own frozen_context through
`build_proposal`**, requiring an identical `content_hash` (a re-signed but forged payload
is `not_reproducible`). For terminal events: no payload, no `content_hash` key, plus the
causal rules (`at_utc ≥ created_at`; `EXPIRED` needs `at_utc ≥ expires_at`) and the
transition table.

Replay therefore verifies, per event: schema · event_id · event content hash · proposal
content hash · reproducibility from frozen context · causality · state transition. Any
manipulated line fails replay closed (`ProposalError`); the store never opens.

Duplicate semantics: only an exact copy of the same logical event (identical
`event_id`) is an idempotent no-op. A `PROPOSED` with the same `proposal.content_hash`
but a different envelope (`at_utc`/`reason`) is a conflict, not a duplicate.

`recorded_at_utc` stays outside `event_id` on purpose: it is the wall clock of the
append (envelope metadata), so the same logical event replayed elsewhere/later is still
recognised as the same event; it is validated for format only.

## 7. Persistence

`<output>/proposal_events.jsonl` is the only source of truth: one canonical-JSON event
per line, flushed and `fsync`ed. The in-memory index is rebuilt by `replay_proposals`
on every store open and after every CLI run; no mutable index file is written. If the
append fails, `PersistenceError` propagates, the cache is untouched and the proposal is
counted as `persist_failed` (never as created).

## 8. Anti-hindsight

`build_proposal(decision, ctx, config, policy_specs)` has no access to ticks, quotes,
the clock, the filesystem or the network (T19–T21, `test_build_is_pure_no_clock_no_io`).
The 2.8 invariant `decided_seq == entry_seq` and `entry_tick_at == decided_at` is enforced
as an input check. Expiry is evaluated against an explicit `as_of` instant only.

## 9. Fail-closed catalogue

`missing_policy_id, unknown_policy_id, unsupported_decision_schema, unexpected_decision_fields:*,
missing_decision_fields:*, policy_family_mismatch, policy_params_mismatch, context_cohort_mismatch,
setup_id_asset_mismatch, filled_with_no_fill_reason, not_fillable_1_contract, liquidity_unverified,
not_economically_valid, invalid_fee_model, fee_effective_below_model, effective_cost_mismatch,
crossed_entry_quote, missing_detection_ask, improvement_mismatch, invalid_detection_ms,
elapsed_mismatch, delay_mismatch, decided_after_policy_deadline, context_conflict, invalid_ask_size,
invalid_entry_bid, invalid_detection_ask, invalid_entry_tick_at, invalid_entry_quote_fetched_at,
quote_fetched_after_decision, invalid_production_max_buy, invalid_guardrail_rejections, invalid_ticks_seen,
invalid_trigger_rule, invalid_tick_resolution, non_finite_content, missing_setup_id,
missing_or_mismatched_context, missing_contract, invalid_side, missing_asset, basis_not_ok,
invalid_source_seq, decided_seq_ne_entry_seq, invalid_decided_at, decision_not_frozen_on_tick,
invalid_observed_ask, price_cap_unavailable:*, observed_ask_above_policy_cap,
invalid_limit_price, invalid_fee, effective_cost_ge_100, invalid_conservative_fair,
invalid_fair, invalid_max_quantity, invalid_quantity, quantity_above_max, invalid_ttl,
missing_eligibility_end, decided_after_eligibility_end, self_validation_failed:*,
persist_failed, idempotency_conflict` — every one yields **no proposal**.

Not-eligible (counted, not an error): `policy_not_selected, cohort_not_allowed,
decision_not_filled`. Only cohort `forward_v28` is accepted by default.

## 10. No live-order surface (release blocker)

Static tests T23–T25 scan `vixion_v30_proposal.py` and `scripts/v30_proposal_lab.py` for
`/portfolio/orders`, `place/submit/create/cancel/amend_order`, `requests.*`,
`method="POST"` and friends, `urlopen`, `urllib.request`, `http.client`, `socket`,
credential names and any non-`V30_*` environment read. T22 runs the core and the CLI
with `socket.socket` monkey-patched to raise. The CI workflow repeats the grep.

## 11. Verification commands

```
python -m py_compile vixion_v30_proposal.py scripts/v30_proposal_lab.py tests/test_v30_proposal_gateway.py
python -W ignore -m unittest tests.test_v30_proposal_gateway -v
python -W ignore -m unittest discover -s tests -v          # full regression (2.7 + 2.8 + 3.0)
python scripts/v30_proposal_lab.py --decisions tests/fixtures/v30/sample_decisions_v28.jsonl \
    --traces tests/fixtures/v30/sample_traces_v28.jsonl --policy ask_le_90c --output /tmp/v30demo
```

## 12. Out of scope (Increment 1)

No Kelly, no bankroll, no portfolio sizing, no PnL, no policy selection, no live
runtime hook, no Railway change, no deploy. Increment 2+ requires a separate
human-approved decision.
