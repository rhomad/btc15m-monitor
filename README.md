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
