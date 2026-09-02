# RUNBOOK v2.8 — deploy, verification, abort and rollback

Scope: the v2.8 Executable Entry Timing Lab overlay. Production (`app.py` 2.6, `vixion_v27.py`, `launcher_v27.py`) is not
modified by this release; every step below is read-only except the deploy itself and the rollback actions, which require
explicit approval.

## 1. Pre-deploy baseline (on the running 2.7)

```
DASHBOARD_PASSWORD=... python scripts/verify_v28_post_deploy.py https://<host> --baseline-v27
DASHBOARD_PASSWORD=... python scripts/verify_v28_post_deploy.py https://<host> --baseline-v27 --watch 10
```
Expected: `ALL CHECKS PASSED` (exit 0): `/health` ok, `sources.kalshi.healthy=true`, `sources.binance.healthy=true`,
`research_lab` present, version 2.7. `--sources-only` runs the same source checks on any version.

## 2. Deploy

Branch `v2.8-entry-timing-lab` → PR (`v2.8 regression` workflow green, including the gate asserting the three production
files are identical to `main`) → merge → Railway builds from the repo `Dockerfile` (CMD `launcher_v28.py`). No variable
changes are required; the lab starts with its defaults (`ENTRY_TIMING_ENABLED=1`, 1 Hz, 6 traces, `ENTRY_TIMING_MAX_RPS=2`,
`ENTRY_TIMING_BURST=2`, source `orderbook`).

## 3. Post-deploy verification

```
DASHBOARD_PASSWORD=... python scripts/verify_v28_post_deploy.py https://<host>              # must exit 0
DASHBOARD_PASSWORD=... python scripts/verify_v28_post_deploy.py https://<host> --watch 30   # until the first captured setup
DASHBOARD_PASSWORD=... python scripts/verify_v28_post_deploy.py https://<host> --trace "<setup_id>"
```
Boot log must show `VIXION entry timing lab 2.8 | mode=forward-counterfactual-only | ... | production_modified=false` and
`BTC 15M Monitor 2.8-executable-entry-timing-lab | LIVE`.

Trace audit (`--trace`) has two separate verdicts:

- **Structural validity** (PASS/FAIL, exit 1 on any FAIL): every tick has an integer `seq`, all `seq` unique and exactly
  `0..N-1` (duplicates such as `[0,1,1,2]` fail); decision policies unique, all in the catalogue published by
  `/api/research/entry-timing/status` (report as fallback) and, for a closed trace, exactly that set; every fill decided on
  its own tick (`decided_seq == entry_seq`), on a valid tick with the same ask, and frozen there (`decided_at == tick_at`).
- **Acceptance evidence**: `SUFFICIENT` only for a closed, structurally valid trace with at least one fill and at least one
  VALID later tick whose ask is below that fill's ask. Otherwise the verifier prints `EVIDENCE INSUFFICIENT (<reason>)` and
  exits **3**: the trace is not usable as final post-deploy acceptance, which is not a finding against the engine; audit
  another closed trace. Final acceptance requires at least one trace with `acceptance evidence SUFFICIENT` (exit 0).

## 4. Abort rule (single source of truth: `ABORT_RULE` in `scripts/verify_v28_post_deploy.py`)

ABORT = FAIL (exit 1) as soon as ANY observation shows: (A) sources.kalshi.healthy is not true; (B) sources.binance.healthy is not true; (C) entry_timing_lab.poller.http_429 is above the accepted historical value at the start of the check (--accept-historical-429, default 0) or increases during the watch - any lab 429 is sufficient, no correlation with a production API ERROR is required; (D) lab requests exceed the budget published by /health: delta_requests > max_rps * delta_seconds + burst + 2, checked cumulatively over the watch and per observation interval; (E) max_rps <= 0 (budget disabled); (F) /health ok is not true (production loops stale). Operator action on abort: set ENTRY_TIMING_ENABLED=0 (requires explicit approval); the lab becomes passive and never touches the volume.

The verifier exits 1 whenever any of these was observed at any point of the run, including during `--watch`; a later
recovery never clears an observed abort. Rule F (`/health.ok`) is evaluated on every observation exactly like A and B:
it is an abort rule, not a soft warning. Exit codes: 0 all checks passed (and, with `--trace`, evidence sufficient);
1 any failed check or abort; 3 no failure but acceptance evidence insufficient. Counters are cumulative on the volume: a known historical `http_429` value must be
accepted explicitly with `--accept-historical-429 N`.

## 5. Rollback (explicit approval required)

- Without redeploying code: set `ENTRY_TIMING_ENABLED=0`; Railway redeploys the same image, the lab becomes passive and never
  touches the volume.
- Back to exact 2.7: Railway `Redeploy` of deployment `5aca9077-f594-4305-addd-fbec4e576191` (main @ ce932e2) or revert the
  merge on `main`.
- The v2.8 files on the volume (`entry_timing_*.jsonl`, `entry_timing_lab_v28.json`) can be deleted without touching any ledger.
