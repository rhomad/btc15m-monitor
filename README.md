# BTC15M Monitor — Railway

Upload `app.py` and `Dockerfile` to the repository root. Railway can deploy directly from this repo.

Required Railway variable: `DASHBOARD_PASSWORD`. Optional: `TELEGRAM_BOT_TOKEN`, `BANKROLL_DOLLARS`.

Recommended: `MIN_DISTANCE_BP=10`, `EDGE_MIN_POINTS=10`, `FEE_BUFFER_CENTS=1`, `KELLY_FRACTION=0.125`, `RISK_CAP_PCT=1`, `MAX_BASIS_GAP_BP=8`.

Health check: `/health`. Volume mount: `/data`. This scanner is read-only and does not place orders.
