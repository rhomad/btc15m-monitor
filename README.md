# BTC15M Monitor — Railway

VIXION production monitor for BTC + multiasset crypto 15-minute pullback research.

## Production strategy

`app.py` remains the 2.6 production decision engine. The v2.7 branch intentionally keeps that file unchanged and boots through `vixion_v27.py`, a passive observational overlay.

Production settings remain unchanged:

- `EDGE_MIN_POINTS=10`
- `FEE_BUFFER_CENTS=1`
- `MAX_BASIS_GAP_BP=8`
- existing BTC/multiasset thresholds, first-signal guard and minute-lock are unchanged.

The scanner is read-only and does not place orders.

## v2.7 Observability Lab

`vixion_v27.py` adds only research instrumentation:

- append-only `decision_trace_v27.jsonl`;
- per-setup price trajectory summaries;
- detection-entry and ex-post best-eligible counterfactual accounting;
- analytics-only edge threshold sweep: `10, 7.5, 5, 4, 3, 2, 1, 0`;
- ask-price buckets;
- BTC research ledger;
- basis-gate analysis;
- one-time `/data/backups/pre_v27` backup before monitor state is opened.

Research endpoints are protected by the same dashboard Basic Auth as the existing API:

- `/api/research/lab`
- `/api/research/decision-trace`
- `/api/research/decision-trace.jsonl`
- `/api/research/trajectory-summary`
- `/api/research/counterfactual`
- `/api/research/threshold-sweep`
- `/api/research/ask-buckets`
- `/api/research/btc-shadow`
- `/api/research/basis-gate`
- `/api/research/lab.json`

`/health` retains the existing fields and adds `research_lab`.

## Railway

Required Railway variable: `DASHBOARD_PASSWORD`. Optional: `TELEGRAM_BOT_TOKEN`, `BANKROLL_DOLLARS`.

Recommended production values: `MIN_DISTANCE_BP=10`, `EDGE_MIN_POINTS=10`, `FEE_BUFFER_CENTS=1`, `KELLY_FRACTION=0.125`, `RISK_CAP_PCT=1`, `MAX_BASIS_GAP_BP=8`.

Health check: `/health`. Volume mount: `/data`.

## Regression

The branch includes `.github/workflows/v27-regression.yml` and `tests/test_v27_observability.py`. CI verifies that the three key decision functions from 2.6 remain the exact same function objects after the 2.7 overlay loads:

- `Monitor.compute`
- `AltShadowMonitor.compute`
- `ShadowLedger.observe_state`

It also freezes live thresholds/config defaults and checks that no order-placement surface is introduced.

## v2.8 Executable Entry Timing Lab

`vixion_v28.py` is a second passive overlay on top of the 2.7 chain. `app.py` (2.6 engine), `vixion_v27.py` and `launcher_v27.py` stay byte-for-byte unchanged; the container boots through `launcher_v28.py`.

Question it answers (research only): after a valid setup is detected, which *executable* entry-timing policy gets a better price without destroying fill rate/accuracy? 37 policies (detection, fixed delay, ask threshold, pullback, hybrid, production baseline) are evaluated **online**: a dedicated ~1 Hz Kalshi quote poller feeds each tick to every policy that is still `WAITING`; a policy moves once to `FILLED | EXPIRED | TIMEOUT | REJECTED`, its `Decision` is immutable and is written to disk before the next quote is requested. Trace close only expires policies still waiting. No hindsight metric is produced here (Scenario B stays in the v2.7 endpoints).

- Eligibility: production minute-lock semantics (quote fetched before `window_start + (minute+1)*60s`, `basis_ok` frozen at detection). Alt assets only (ETH/SOL/XRP/DOGE/BNB); BTC excluded from this increment.
- Fees: `fee_effective = max(FEE_BUFFER_CENTS, quadratic 0.07 model)`; `effective_cost = ask + fee_effective`; `effective_cost >= 100` is a hard guardrail.
- Cohorts, never aggregated: `forward_v28` (headline, starts at zero), `trace_v27` (same engine replayed over `decision_trace_v27.jsonl`, ~3 s ticks), `ledger_backfill` (detection-only records; timing policies are `NOT_SIMULABLE`).
- Durable state = three append-only logs on the volume: `entry_timing_traces_v28.jsonl` (trace open/close records with the frozen context), `entry_timing_ticks_v28.jsonl`, `entry_timing_decisions_v28.jsonl`; `entry_timing_replay_v27.jsonl` holds the replay cohort and `entry_timing_lab_v28.json` is a rebuildable index cache. Writes are fail-closed: a tick that is not durable is never dispatched, a Decision that is not durable blocks its trace (no further fetch) until it is retried successfully, and a seq number is never reused. Recovery at boot is synchronous: terminal Decisions are authoritative, durable ticks are replayed chronologically onto the policies still WAITING (reconstructing any Decision lost between a tick append and its Decision append), and no fetch or outcome join happens before it completes.
- Liquidity: orderbook ticks verify `ask_size >= 1`; a known size below one contract can never produce `FILLED`; quotes without size (production detection quote, market-endpoint fallback, v2.7 replay, backfill) are flagged `unverified` and reported separately from verified fills.
- Poller: one worker thread per trace (a slow venue for one asset never delays the others); during backoff a synthetic invalid tick is emitted every interval without a request so hybrid timeouts keep advancing; invalid reasons follow a fixed schema (`no_ask`, `out_of_range`, `crossed`, `stale`, `ineligible`, `unfillable_1_contract`, `http_429`, `http_5xx`, `http_4xx`, `http_other`, `timeout`, `net_error`, `parse_error`, `backoff`, `rate_limited`).
- `ENTRY_TIMING_ENABLED=0` leaves every file byte-for-byte untouched (no recovery, no close, no append, no save).
- Env: `ENTRY_TIMING_ENABLED` (1), `ENTRY_TIMING_TICK_SECONDS` (1.0), `ENTRY_TIMING_MAX_TRACES` (6), `ENTRY_TIMING_MAX_RPS` (2.0; global request budget shared by all workers, 0 disables), `ENTRY_TIMING_BURST` (2), `ENTRY_TIMING_TICK_SOURCE` (`orderbook`|`market`), `ENTRY_TIMING_QUOTE_MAX_AGE_S` (5), `ENTRY_TIMING_HTTP_TIMEOUT_S` (4), `ENTRY_TIMING_JOIN_SECONDS` (20), `ENTRY_TIMING_V27_REPLAY` (1), `ENTRY_TIMING_V27_REPLAY_SECONDS` (600).
- Rate limit: the venue limit is shared with production (same IP, same public endpoints), so the lab never exceeds `ENTRY_TIMING_MAX_RPS` in total; when the token bucket is empty a worker emits a synthetic invalid tick `rate_limited` (no request) so hybrid timeouts keep advancing. Recovery and reporting read a rotated `<log>.jsonl.1` before the live log.
- Endpoints (dashboard Basic Auth): `/api/research/entry-timing[.json]?cohort=forward|all&segment=asset|minute|side|ask_bucket|distance_bucket|basis_bucket|spread_bucket&sort=net_pnl_per_eligible_setup|wilson_low_pct|fill_rate_pct|roi_pct`, alias `/research/entry-timing`, `/api/research/entry-timing/status`, `/api/research/entry-timing/trace?setup_id=`, `/api/research/entry-timing/{traces,ticks,decisions,replay_v27}.jsonl`. `/health` adds `entry_timing_lab`.
- Regression: `.github/workflows/v28-regression.yml` compiles everything, asserts `app.py`/`vixion_v27.py`/`launcher_v27.py` are identical to `main`, and runs both test suites (`tests/test_v27_*.py`, `tests/test_v28_entry_timing.py`).
- Rollback: `ENTRY_TIMING_ENABLED=0` (hooks stay passive) or redeploy the previous image; the four v2.8 files can be deleted without touching any ledger.
- Deploy verification and abort rule: `RUNBOOK_v28.md` and `scripts/verify_v28_post_deploy.py` (`--baseline-v27`/`--sources-only` before the deploy, single check, `--watch N`, `--trace <setup_id>` after it; exit 1 on any failed check or abort observation, exit 3 when the audited trace is structurally valid but does not provide sufficient anti-hindsight acceptance evidence). Abort rule, quoted from the script: ABORT = FAIL (exit 1) as soon as ANY observation shows: (A) sources.kalshi.healthy is not true; (B) sources.binance.healthy is not true; (C) entry_timing_lab.poller.http_429 is above the accepted historical value at the start of the check (--accept-historical-429, default 0) or increases during the watch - any lab 429 is sufficient, no correlation with a production API ERROR is required; (D) lab requests exceed the budget published by /health: delta_requests > max_rps * delta_seconds + burst + 2, checked cumulatively over the watch and per observation interval; (E) max_rps <= 0 (budget disabled); (F) /health ok is not true (production loops stale). Operator action on abort: set ENTRY_TIMING_ENABLED=0 (requires explicit approval); the lab becomes passive and never touches the volume.
