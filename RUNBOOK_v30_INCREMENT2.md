# RUNBOOK v3.0 — Increment 2.1: Real V2.8 Source Observer / Reconciler

Status: **research / audit only. Not deployed, not wired into any launcher, not in the
Dockerfile.** The approved Increment 1 core (`vixion_v30_proposal.py`, schema v1) is
byte-identical to `main` and is only *called*, never modified.

## 1. What it does

```
DATA_DIR/entry_timing_traces_v28.jsonl.1  ─┐
DATA_DIR/entry_timing_traces_v28.jsonl     ├─► strict snapshot reader (binary, strict UTF-8)
DATA_DIR/entry_timing_decisions_v28.jsonl.1├─► source reconciliation (trace-open index,
DATA_DIR/entry_timing_decisions_v28.jsonl ─┘   decision identity, conflicts, provenance)
        │
        ▼
vixion_v30_proposal.process_decisions → build_proposal  (UNCHANGED Increment 1 core)
        │
        ▼
ProposalStore → <output>/proposal_events.jsonl  → ProposalStore.expire_due(as_of)
```

No ticks (`entry_timing_ticks_v28.jsonl` is never opened), no market results, no
settlement, no ledger, no HTTP, no sockets, no credentials, no orders. A proposal is a
pure function of (durable V2.8 Decision, durable V2.8 trace `open` ctx, explicit
policy + `V30_*` config) — exactly the no-hindsight contract of Increment 1.

Files:

| File | Role |
|---|---|
| `vixion_v30_observer.py` | reader, reconciler, provenance verifier, single-scan orchestration |
| `scripts/v30_observe_v28.py` | CLI: `--once` / `--watch N` |
| `tests/test_v30_real_observer.py` | O01–O60 + extra invariants + auditor reproductions A–H |
| `.github/workflows/v30-observer-regression.yml` | feature-branch / PR gate, no deploy |

## 2. Running

```
# one full scan, expiry evaluated at "now" (or --as-of)
python scripts/v30_observe_v28.py --source-data-dir /data --policy ask_le_90c \
    --output /data/v30_observer --once [--as-of 2026-09-04T12:00:00+00:00] [--json]

# periodic local rescans (min 1 s), Ctrl+C exits cleanly (130)
python scripts/v30_observe_v28.py --source-data-dir /data --policy ask_le_90c \
    --output /data/v30_observer --watch 2
```

`--policy` is mandatory, has no default and must be one of the 37 canonical V2.8
policies (`vixion_v28.POLICY_SPECS`, loaded, never copied). There is no ranking, no
comparison, no "best policy": the observer processes only decisions whose
`decision.policy == --policy`. The core keeps enforcing cohort `forward_v28`, `FILLED`,
`fill == True`, the strict schema and all Increment 1 consistency rules.

Configuration: `V30_PROPOSAL_QTY` (1), `V30_MAX_PROPOSAL_QTY` (1), `V30_PROPOSAL_TTL_S`
(30) via the approved `vixion_v30_proposal.config_from_env` — no other variable is read
(`DASHBOARD_PASSWORD`, `KALSHI*`, `TELEGRAM*`, `BINANCE*`, `RAILWAY*` never touched).

Exit codes: `0` ok · `2` fail-closed (unknown policy, bad config, corrupt/conflicting
source, persistence failure) · `3` missing trace context in `--once` · `130` Ctrl+C.

Output: `<output>/proposal_events.jsonl` (default `<source>/v30_observer/`).

**Output aliasing guard (Increment 2.1.1).** Before the store is opened (and again right
before the first write) `validate_output_log` fails closed (`OutputAliasError`, exit 2)
if: output dir == source dir; the output dir or the log path is a symlink; the log's
real path equals any protected artefact (`entry_timing_traces_v28.jsonl[.1]`,
`entry_timing_decisions_v28.jsonl[.1]`); the log is the same file (hardlink) as any
existing protected artefact (`os.path.samefile`); or the log lands directly inside the
source directory through a link. The read-only guarantee therefore covers misconfigured
paths, not only the normal layout (O41–O44, A).

## 3. Source snapshot semantics

* Files are opened with `"rb"` only; the whole file is read once per scan. No lock is
  taken, no producer is blocked, nothing is written, renamed, rotated or removed.
* Chronological order: `<file>.jsonl.1` (older, rotated) then `<file>.jsonl` (active).
* A line is **visible** iff it is terminated by `"\n"` at read time. A trailing `"\r"`
  before it is tolerated (stripped for decoding and hashing only).
* Every visible line must be strict UTF-8, valid JSON (NaN/Infinity rejected), non-empty
  and an object; otherwise `SourceCorruptError`.
* **Partial tail:** the final unterminated segment of the **active** file is a concurrent
  partial write by V2.8 → deferred (invisible this scan, `partial_tail_deferred` counted,
  retried next scan; not corruption). The same in a **rotated** `.1` file is corruption
  (rotated files are never appended by V2.8).
* Files are opened directly (`open(..., "rb")`), never `exists()` then `open()`: a file
  absent at that instant (V2.8 rotation race) is a transient snapshot state reported as
  `exists: false` / empty, not an error; the next scan sees it again. Any other `OSError`
  (permissions, I/O) is `SourceIOError` → fail closed. No raw `OSError` escapes the CLI
  (O52–O54, G).
* The entire snapshot (both files × both parts) is read and validated **before** the
  `ProposalStore` is opened. A corrupt/conflicting scan writes nothing.
* **Source Decision validation (Increment 2.1.1).** Every decision of the selected policy
  is structurally validated *before* it is hashed, used as a dict key or classified:
  object, exactly the canonical 31-field V2.8 schema, `schema_version == 2`, `cohort`
  string, non-empty `setup_id` string, `policy` == selected, `decided_seq` non-bool int,
  finite JSON. `decided_seq` semantics (Increment 2.1.2): normal terminal tick decisions
  carry `≥ 0`; the special V2.8 **no-tick close** may carry `-1` — `TraceRuntime.last_seq`
  starts at `-1` and `TraceRuntime.close()` finalises still-WAITING policies with a pseudo
  tick whose `seq` is that value (e.g. recovery after a crash between the durable trace
  `open` and the first persisted tick). `-1` is accepted **only** as `state == EXPIRED`,
  `fill is False`, `entry_seq`/`entry_tick_at`/`entry_quote_fetched_at` all `null`; it
  reaches the core and is reported `not_eligible` (`decision_not_filled`). `-1` on
  FILLED/TIMEOUT/REJECTED, `-1` with any entry field set, or any `< -1` is
  `SourceCorruptError`. `-1` never represents a fill and never becomes a proposal source
  (O55–O60). Violations are `SourceCorruptError`, never a `TypeError`, and never
  `DEFERRED_MISSING_CONTEXT` (O45–O48, B, C, E). Decisions of other policies are only
  filtered, never validated here (the core validates the rest for selected ones).

## 4. Rotation semantics

V2.8 (`vixion_v27.DecisionTrace.append`) rotates by `os.replace(active → .1)` (unlinking
the previous `.1`) and starts a fresh active file. Because every scan re-reads both parts
and `ProposalStore` suppresses duplicates by deterministic identity, rotation between
scans never creates a duplicate durable proposal (O21). Data older than the `.1` file is
gone for V2.8 too; stored proposals for such decisions are reported `source_missing`
(never re-created, never deleted).

## 5. Reconciliation model (no cursor)

Every scan is a **full rescan**: cost O(size of the four source files). There is no
persistent cursor, no byte offsets, no inode tracking — by design, to avoid rotation /
crash-during-cursor-save bugs. Idempotency comes from Increment 1: same logical decision
→ same `proposal_id` → `duplicate_suppressed`. Restart + full rescan = 0 duplicated
durable proposals (O17, O18).

## 6. Duplicate / conflict semantics

| Source situation | Result |
|---|---|
| trace `open` duplicated, canonical-identical | idempotent (first line's hash kept) |
| trace `open` duplicated, any difference | `SourceConflictError` → fail closed, nothing written |
| trace `open` malformed (bad `setup_id`/`ctx`, `ctx.setup_id` mismatch, non-finite) | `SourceCorruptError` → fail closed |
| decision duplicated by `(cohort, setup_id, policy, decided_seq)`, canonical-identical | no-op (`decision_duplicate_records`) |
| same identity, different content | `SourceConflictError` → fail closed |
| same terminal key `(cohort, setup_id, policy)`, **different `decided_seq`** | `SourceConflictError` `multiple_terminal_decisions` → fail closed (V2.8 emits exactly one terminal decision per PolicyState; two valid-looking ones can never yield two proposals — O49–O51, F) |
| selected decision with no trace `open` ctx, `--once` | `SourceMissingContextError`, exit 3, nothing written |
| selected decision with no trace `open` ctx, `--watch` | `DEFERRED_MISSING_CONTEXT`: skipped, counted, retried next scan; other setups proceed |

Rationale for the once/watch difference: in `--watch` the trace `open` and the decision
are written by V2.8 at different moments and are not atomic across files, so a decision
whose `open` is not yet visible is a benign race. In `--once` there is no next scan, so
the same state is reported as a hard failure. Malformed or conflicting trace data is
never deferred in either mode.

## 7. Expiry

After a fully valid snapshot: (1) build/persist new proposals, (2)
`ProposalStore.expire_due(as_of)`. Two distinct instants exist and are reported
separately (Increment 2.1.1): `as_of_utc` — **exclusively** the expiry-evaluation instant
(`--once` default now, `--as-of` explicit); `last_scan_at` — the real orchestration
instant (`scan_at_utc`, wall clock, injectable in tests; also the events'
`recorded_at_utc`). A historical `--as-of` never falsifies `last_scan_at` (H). `--watch`
uses UTC now for both. Neither reaches `build_proposal`: a proposal built at `EARLY` and
one built at `LATER` are content-identical. A historical decision can legitimately
produce `PROPOSED` then `EXPIRED` in the same scan.

## 8. Source provenance (report only, schema untouched)

For every stored proposal of the selected policy the report carries:

* `decision_source_sha256` — SHA-256 over the raw durable bytes of the decision line,
  after removing exactly one trailing `"\n"` and, if present, one trailing `"\r"`.
* `trace_open_source_sha256` — same over the trace `open` line for the `setup_id`.
* `status`: `source_verified` / `source_missing` / `source_mismatch` (+ `detail`).

`verify_proposal_against_source` checks `frozen_context.decision == source decision`,
`frozen_context.trace_ctx == source ctx projected onto the keys the core froze`,
`policy_id`, `setup_id`, `source_decision_seq`, and finally **rebuilds the proposal from
the source records with the frozen config** and requires the same `content_hash`. These
hashes are never persisted inside the proposal (Increment 1 schema is untouched).

## 9. Report fields

`observer_version, gateway_version, policy_id, source{traces_rotated_records,
traces_active_records, decisions_rotated_records, decisions_active_records,
partial_tail_deferred, decision_duplicate_records, files}, decisions_seen,
selected_policy_decisions, selected_filled_decisions, proposals_created,
duplicate_suppressed, not_eligible, invalid_rejected, conflict_rejected, persist_failed,
expired_now, deferred_missing_context, source_verified, source_missing, source_mismatch,
source_conflicts, by_asset, by_side, rejection_reasons, last_source_decided_at,
as_of_utc, last_scan_at, proposals, provenance, missing_context_setups`. Human mode ends with
`NO ORDER SURFACE · local V2.8 observer only · nothing sent anywhere`.

## 10. Safety proofs (tests)

* Read-only: SHA-256 of every source file before/after repeated scans identical; the
  observer source contains `open(..., "rb")` only and none of `os.replace/rename/unlink/
  truncate/write_text/write_bytes/shutil` (O06).
* No network: `socket.socket` / `create_connection` patched to raise while core + CLI
  (`--once` and `--watch`) run end-to-end (O30); no network imports/objects (O32).
* No credentials: hostile env with `KALSHI_*`, `DASHBOARD_PASSWORD`, `TELEGRAM_*` never
  read or echoed; only `V30_*` via the approved helper (O31).
* No live-order surface: static needle scan + no `execute/broker/trading` abstractions (O32).
* No hindsight: a cheaper ticks log and a changed ledger leave the proposal identical
  (O34, O35); `as_of` never reaches the build.
* Runtime / Increment 1 core / Dockerfile byte-identical to `main` (O36–O39); the core is
  pinned to the approved Increment 1.2 hash `89745d77…dced9af`.

## 11. Verification commands

```
python -m py_compile vixion_v30_observer.py scripts/v30_observe_v28.py tests/test_v30_real_observer.py
python -W ignore -m unittest tests.test_v30_real_observer -v
python -W ignore -m unittest discover -s tests -v
```

## 12. Real Railway data

Tests use **synthetic** fixtures produced by the real V2.8 engine; no claim is made about
production data. To run over an export of the Railway volume (read-only copy of
`/data`), from the repo root:

```
python scripts/v30_observe_v28.py --source-data-dir /path/to/railway-data-export \
    --policy <POLICY_ID> --output /path/to/railway-data-export/v30_observer --once --json
```

Do not copy the export into the repository.

## 13. Out of scope

No launcher integration, no Dockerfile change, no Railway change, no persistent cursor,
no Kelly/bankroll, no policy selection, no execution. Increment 2.2+ requires a separate
human-approved decision.
