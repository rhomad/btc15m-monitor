#!/usr/bin/env python3
"""VIXION 2.7 observational overlay.

This module deliberately leaves app.py (2.6 production strategy) byte-for-byte
untouched. It imports the 2.6 engine, adds passive tracing/counterfactual research
hooks, then calls the original main(). No trading/shadow decision predicate is
replaced or relaxed.
"""
import csv
import hashlib
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import app as core

APP_VERSION = "2.7-railway-observability-lab"
RESEARCH_LAB_VERSION = "2.7"
core.APP_VERSION = APP_VERSION
DATA_DIR = core.DATA_DIR
DECISION_TRACE_PATH = DATA_DIR / "decision_trace_v27.jsonl"
RESEARCH_LAB_PATH = DATA_DIR / "research_lab_v27.json"
BACKUP_DIR = DATA_DIR / "backups"
CF_THRESHOLDS = [10.0, 7.5, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0]
ASK_BUCKETS = [
    ("70-79.9", 70.0, 80.0), ("80-84.9", 80.0, 85.0),
    ("85-89.9", 85.0, 90.0), ("90-92.9", 90.0, 93.0),
    ("93-94.9", 93.0, 95.0), ("95-96.9", 95.0, 97.0),
    ("97-98.9", 97.0, 99.0), ("99+", 99.0, 1000.0),
]
ALT_SIGNAL_STATUSES = (
    "SHADOW CONFIRMED", "SHADOW ENTRY ZONE", "SHADOW PRICE HIGH",
    "BASIS WARNING", "SHADOW ALREADY COUNTED",
)
BTC_SIGNAL_STATUSES = (
    "CONFIRMED", "ENTRY ZONE", "READY / PRICE HIGH", "BASIS WARNING",
    "ALREADY COUNTED",
)
BEST_ELIGIBLE_LABEL = (
    "EX-POST RESEARCH METRIC: lowest ask observed while the setup was still "
    "entry-eligible under production rules. Hindsight metric; NOT an executable strategy."
)
RESEARCH_LIVE_FORCE_CLOSE_MS = 6 * 3600 * 1000


def _iso_to_ms(value):
    try:
        dt = core.parse_iso(value)
        return int(dt.timestamp() * 1000) if dt else None
    except Exception:
        return None


def _mean(values):
    vals = [float(x) for x in values if x is not None]
    return sum(vals) / len(vals) if vals else None


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def backup_ledgers_once(tag="pre_v27"):
    """Copy persisted state once before 2.7 starts. Failure never stops VIXION."""
    try:
        import shutil
        dest = BACKUP_DIR / tag
        manifest_path = dest / "manifest.json"
        if manifest_path.exists():
            return core.load_json(manifest_path)
        dest.mkdir(parents=True, exist_ok=True)
        names = [
            "multiasset_shadow_ledger_v23.json", "early_entry_lab_v25.json",
            "early_entry_lab_v25.pre_v26.json", "config.json",
            "btc_first_signal_v24.json", "signal_journal.csv",
            "multiasset_shadow_journal.csv",
        ]
        files = []
        for name in names:
            src = DATA_DIR / name
            if src.exists() and src.is_file():
                shutil.copy2(src, dest / name)
                files.append({"name": name, "bytes": src.stat().st_size,
                              "sha256": _sha256(dest / name)})
        manifest = {
            "tag": tag, "created_at": datetime.now(timezone.utc).isoformat(),
            "app_version": APP_VERSION, "files": files,
        }
        core.save_json(manifest_path, manifest)
        print(f"backup {tag}: {len(files)} files -> {dest}", flush=True)
        return manifest
    except Exception as exc:
        print("backup failed:", exc, flush=True)
        return {"error": str(exc)}


def backup_manifest(tag="pre_v27"):
    try:
        path = BACKUP_DIR / tag / "manifest.json"
        return core.load_json(path) if path.exists() else None
    except Exception:
        return None


def _pnl_scenario(ask, win, fee, extra=None):
    if ask is None or win is None or fee is None:
        return None
    ask, fee = float(ask), float(fee)
    gross = (100.0 - ask) if bool(win) else -ask
    net = gross - fee
    cost = ask + fee
    out = {
        "ask_cents": ask, "estimated_fee_cents": fee,
        "effective_cost_cents": cost, "win": bool(win),
        "gross_pnl_cents": gross, "net_pnl_cents": net,
        "roi_pct": (net / cost * 100.0) if cost > 0 else None,
    }
    if extra:
        out.update(extra)
    return out


class DecisionTrace:
    """Append-only JSONL. All I/O errors are swallowed and counted."""
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.lines = 0
        self.errors = 0
        self.last_error = ""

    def append(self, record):
        try:
            line = json.dumps(record, separators=(",", ":"), default=str)
            with self.lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                if (self.lines % 1000 == 0 and self.path.exists()
                        and self.path.stat().st_size > 256 * 1024 * 1024):
                    rotated = self.path.with_suffix(".jsonl.1")
                    try:
                        if rotated.exists():
                            rotated.unlink()
                    except Exception:
                        pass
                    os.replace(self.path, rotated)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                    fh.flush()
                    try:
                        os.fsync(fh.fileno())
                    except Exception:
                        pass
                self.lines += 1
            return True
        except Exception as exc:
            self.errors += 1
            self.last_error = str(exc)[:200]
            return False

    def read(self, setup_id=None, engine=None, limit=500, max_bytes=8_000_000):
        out = []
        try:
            if not self.path.exists():
                return out
            size = self.path.stat().st_size
            with open(self.path, "rb") as fh:
                if size > max_bytes:
                    fh.seek(size - max_bytes)
                    fh.readline()
                text = fh.read().decode("utf-8", "replace")
            for line in text.splitlines():
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if setup_id and rec.get("setup_id") != setup_id:
                    continue
                if engine and rec.get("engine") != engine:
                    continue
                out.append(rec)
            if limit and len(out) > int(limit):
                out = out[-int(limit):]
        except Exception:
            pass
        return out

    def size_bytes(self):
        try:
            return self.path.stat().st_size if self.path.exists() else 0
        except Exception:
            return 0


class ResearchLab:
    """Passive trajectory + counterfactual lab. Never feeds a decision path."""
    def __init__(self, primary, alt):
        self.primary = primary
        self.alt = alt
        self.lock = threading.RLock()
        self.trace = DecisionTrace(DECISION_TRACE_PATH)
        self.last_btc_resolve = 0.0
        self.last_save_ts = 0.0
        self.dirty = False
        self.data = {
            "version": RESEARCH_LAB_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "live": {}, "summaries": {}, "counterfactual": {}, "btc": {},
            "counters": {
                "polls_traced": 0, "btc_polls_traced": 0,
                "summaries_finalized": 0, "cf_backfilled": 0,
                "btc_backfilled": 0, "btc_resolved": 0,
                "save_errors": 0, "hook_errors": 0, "force_closed": 0,
            },
        }
        if RESEARCH_LAB_PATH.exists():
            try:
                old = core.load_json(RESEARCH_LAB_PATH)
                if isinstance(old, dict):
                    self.data.update(old)
            except Exception as exc:
                print("research lab load:", exc, flush=True)
        for key in ("live", "summaries", "counterfactual", "btc", "counters"):
            self.data.setdefault(key, {})
        for key in ("polls_traced", "btc_polls_traced", "summaries_finalized",
                    "cf_backfilled", "btc_backfilled", "btc_resolved",
                    "save_errors", "hook_errors", "force_closed"):
            self.data["counters"].setdefault(key, 0)
        self.data["version"] = RESEARCH_LAB_VERSION
        self.backfill_from_ledger()
        self.backfill_btc_from_journal()
        self.save(force=True)

    def _count(self, key, amount=1):
        with self.lock:
            self.data["counters"][key] = int(self.data["counters"].get(key, 0)) + amount
            self.dirty = True

    def save(self, force=False):
        try:
            with self.lock:
                if not force and not self.dirty:
                    return
                core.save_json(RESEARCH_LAB_PATH, self.data)
                self.dirty = False
                self.last_save_ts = time.time()
        except Exception as exc:
            with self.lock:
                self.data["counters"]["save_errors"] = int(self.data["counters"].get("save_errors", 0)) + 1
            print("research lab save:", exc, flush=True)

    def _mark_dirty(self, immediate=False):
        with self.lock:
            self.dirty = True
        if immediate or time.time() - self.last_save_ts >= 5.0:
            self.save()

    def _quality(self, ticker, ask, bid, basis_ok, api_error=""):
        reasons = []
        if api_error:
            reasons.append("api_error")
        if not ticker:
            reasons.append("missing_book")
        if ask is None or bid is None:
            reasons.append("missing_bid_or_ask")
        if basis_ok is not True:
            reasons.append("basis_invalid")
        return {"status": "INVALID" if api_error else ("DEGRADED_OBSERVATION" if reasons else "VALID"),
                "reasons": reasons, "ask_present": ask is not None, "bid_present": bid is not None}

    def observe_alt_poll(self, state, raw_status=None):
        try:
            self._observe_alt_poll(state, raw_status)
        except Exception as exc:
            self._count("hook_errors")
            print("research lab alt poll:", exc, flush=True)

    def _observe_alt_poll(self, state, raw_status=None):
        asset = state.get("asset")
        window = state.get("window_start_utc")
        if not asset or not window:
            return
        rec = self.alt.ledger.get_setup(asset, window)
        if not rec:
            return
        sid = rec.get("setup_id") or f"{asset}|{window}"
        now = core.parse_iso(state.get("updated_at")) or datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        ws_ms = rec.get("window_start_ms") or _iso_to_ms(window)
        we_ms = rec.get("window_end_ms") or ((ws_ms + 900000) if ws_ms else None)
        signal_minute = rec.get("minute")
        lcm = state.get("last_completed_minute")
        entered = bool(rec.get("entry_simulated"))
        past_window = bool(we_ms and now_ms >= we_ms)
        eligible = bool(not entered and not past_window and lcm == signal_minute)
        if entered:
            end_reason = "ENTERED"
        elif past_window:
            end_reason = "WINDOW_END"
        elif lcm != signal_minute:
            end_reason = "MINUTE_LOCK"
        else:
            end_reason = None
        ask = state.get("selected_ask_cents")
        bid = state.get("selected_bid_cents")
        basis_ok = state.get("basis_ok")
        max_buy = state.get("max_buy_cents")
        edge = state.get("edge_points")
        valid_ask = False
        try:
            valid_ask = ask is not None and 0 < float(ask) < 100
        except Exception:
            pass
        dq = self._quality(state.get("market_ticker"), ask, bid, basis_ok, state.get("api_error") or "")
        with self.lock:
            live = self.data["live"].get(sid)
            if live is None:
                live = {
                    "setup_id": sid, "asset": asset, "window_start_utc": window,
                    "window_start_ms": ws_ms, "window_end_ms": we_ms,
                    "signal_minute": signal_minute, "side": rec.get("side"),
                    "detected_at": rec.get("recorded_at"), "signal_status": rec.get("signal_status"),
                    "distance_bp": rec.get("distance_bp"), "threshold_bp": rec.get("threshold_bp"),
                    "cons_pct": rec.get("conservative_fair_pct"),
                    "ask_at_detection": rec.get("ask_cents"),
                    "bid_at_detection": rec.get("setup_bid_cents"),
                    "spread_at_detection": rec.get("setup_spread_cents"),
                    "edge_at_detection": rec.get("edge_points"),
                    "max_buy_cents": rec.get("max_buy_cents"),
                    "basis_ok_at_detection": rec.get("basis_ok"),
                    "target_gap_at_detection": rec.get("target_gap_bp"),
                    "polls_while_entry_eligible": 0, "polls_after_entry_eligibility": 0,
                    "entry_eligibility_end_reason": None, "entry_eligibility_end_ts": None,
                    "min_ask_seen": None, "max_ask_seen": None,
                    "min_ask_seen_timestamp": None, "min_ask_seen_minute": None,
                    "min_ask_eligible": None, "min_ask_eligible_timestamp": None,
                    "min_ask_eligible_minute": None,
                    "min_distance_to_max_buy_cents": None,
                    "min_distance_to_max_buy_eligible_cents": None,
                    "ever_reached_current_max_buy": False,
                    "lowest_edge_seen": None, "highest_edge_seen": None,
                    "basis_values_seen": [], "basis_changed_during_window": False,
                    "status_transitions": [], "dq_at_detection": dq,
                    "first_poll_ts": state.get("updated_at"), "last_poll_ts": None,
                    "seq": 0,
                }
                self.data["live"][sid] = live
            live["seq"] = int(live.get("seq", 0)) + 1
            live["last_poll_ts"] = state.get("updated_at")
            if eligible:
                live["polls_while_entry_eligible"] += 1
            else:
                live["polls_after_entry_eligibility"] += 1
                if end_reason and live.get("entry_eligibility_end_reason") is None:
                    live["entry_eligibility_end_reason"] = end_reason
                    live["entry_eligibility_end_ts"] = state.get("updated_at")
            if valid_ask:
                value = float(ask)
                if live["min_ask_seen"] is None or value < live["min_ask_seen"]:
                    live["min_ask_seen"] = value
                    live["min_ask_seen_timestamp"] = state.get("updated_at")
                    live["min_ask_seen_minute"] = lcm
                if live["max_ask_seen"] is None or value > live["max_ask_seen"]:
                    live["max_ask_seen"] = value
                if max_buy is not None:
                    delta = value - float(max_buy)
                    if (live["min_distance_to_max_buy_cents"] is None
                            or delta < live["min_distance_to_max_buy_cents"]):
                        live["min_distance_to_max_buy_cents"] = delta
                    if eligible and basis_ok is True:
                        if (live["min_distance_to_max_buy_eligible_cents"] is None
                                or delta < live["min_distance_to_max_buy_eligible_cents"]):
                            live["min_distance_to_max_buy_eligible_cents"] = delta
                        if delta <= 0:
                            live["ever_reached_current_max_buy"] = True
                        if live["min_ask_eligible"] is None or value < live["min_ask_eligible"]:
                            live["min_ask_eligible"] = value
                            live["min_ask_eligible_timestamp"] = state.get("updated_at")
                            live["min_ask_eligible_minute"] = lcm
            if edge is not None:
                edge = float(edge)
                if live["lowest_edge_seen"] is None or edge < live["lowest_edge_seen"]:
                    live["lowest_edge_seen"] = edge
                if live["highest_edge_seen"] is None or edge > live["highest_edge_seen"]:
                    live["highest_edge_seen"] = edge
            if basis_ok is not None:
                if basis_ok not in live["basis_values_seen"]:
                    live["basis_values_seen"].append(basis_ok)
                live["basis_changed_during_window"] = len(live["basis_values_seen"]) > 1
            status = state.get("status")
            if not live["status_transitions"] or live["status_transitions"][-1][1] != status:
                live["status_transitions"].append([state.get("updated_at"), status])
            self.dirty = True
            seq = live["seq"]
        self.trace.append({
            "ts": state.get("updated_at"), "app_version": APP_VERSION,
            "engine": "alt", "setup_id": sid, "asset": asset,
            "window_start_utc": window, "signal_minute": signal_minute,
            "current_minute": state.get("current_minute"),
            "last_completed_minute": lcm, "side": state.get("side"),
            "distance_bp": state.get("distance_bp"), "basis_ok": basis_ok,
            "target_gap_bp": state.get("target_gap_bp"), "bid_cents": bid,
            "ask_cents": ask, "spread_cents": state.get("spread_cents"),
            "effective_fee_cents": state.get("effective_fee_cents"),
            "cons_pct": rec.get("conservative_fair_pct"),
            "calculated_edge_points": state.get("edge_points"),
            "max_buy_cents": max_buy, "status": state.get("status"),
            "raw_status": raw_status or state.get("status"),
            "still_entry_eligible": eligible,
            "entry_eligibility_end_reason": end_reason,
            "prod_entry_simulated": entered, "data_quality": dq,
            "poll_seq": seq,
        })
        self._count("polls_traced")
        self._mark_dirty()

    def on_cycle_end(self):
        try:
            self._finalize_resolved()
            self._maybe_resolve_btc()
            self.save()
        except Exception as exc:
            self._count("hook_errors")
            print("research lab cycle:", exc, flush=True)

    def _finalize_resolved(self):
        now_ms = int(time.time() * 1000)
        with self.lock:
            ids = list(self.data["live"])
        for sid in ids:
            with self.lock:
                live = dict(self.data["live"].get(sid) or {})
            if not live:
                continue
            asset, window = sid.split("|", 1)
            rec = self.alt.ledger.get_setup(asset, window)
            if not rec:
                continue
            if rec.get("resolved"):
                self._finalize_one(sid, rec, live, False)
            elif live.get("window_end_ms") and now_ms > int(live["window_end_ms"]) + RESEARCH_LIVE_FORCE_CLOSE_MS:
                self._finalize_one(sid, rec, live, True)
                self._count("force_closed")
        self.backfill_from_ledger()

    def _finalize_one(self, sid, rec, live, forced):
        start_ms = _iso_to_ms(live.get("detected_at")) or _iso_to_ms(live.get("first_poll_ts"))
        end_ms = _iso_to_ms(live.get("entry_eligibility_end_ts")) or live.get("window_end_ms")
        seconds = ((end_ms - start_ms) / 1000.0) if start_ms and end_ms else None
        summary = dict(live)
        summary.update({
            "time_entry_eligible_seconds": round(seconds, 2) if seconds is not None else None,
            "final_outcome": {
                "resolved": rec.get("resolved"), "kalshi_status": rec.get("kalshi_status"),
                "kalshi_result": rec.get("kalshi_result"), "kalshi_win": rec.get("kalshi_win"),
                "underlying_model_win": rec.get("underlying_model_win"),
                "benchmark_disagreement": rec.get("benchmark_disagreement"),
                "forced_close": forced,
            },
            "actual_shadow_entry": {
                "entry_simulated": rec.get("entry_simulated"),
                "entry_ask_cents": rec.get("entry_ask_cents"),
                "entry_minute": rec.get("entry_minute"),
                "net_pnl_cents": rec.get("net_pnl_cents"),
            },
            "finalized_at": datetime.now(timezone.utc).isoformat(),
        })
        cf = self._counterfactual_from(rec, live, "trace")
        with self.lock:
            self.data["summaries"][sid] = summary
            if cf:
                self.data["counterfactual"][sid] = cf
            self.data["live"].pop(sid, None)
            self.data["counters"]["summaries_finalized"] += 1
            self.dirty = True

    def _counterfactual_from(self, rec, live, source):
        if not rec.get("resolved") or rec.get("kalshi_win") is None:
            return None
        reasons = []
        if rec.get("side") not in ("UP", "DOWN") or rec.get("minute") not in (7, 8, 9, 10):
            reasons.append("structure")
        if rec.get("basis_ok") is not True:
            reasons.append("basis_invalid")
        ask = rec.get("ask_cents")
        try:
            if ask is None or not (0 < float(ask) < 100):
                reasons.append("ask_invalid")
        except Exception:
            reasons.append("ask_invalid")
        win = bool(rec.get("kalshi_win"))
        scenario_a = _pnl_scenario(
            ask, win,
            core.effective_fee_cents(ask, self.primary.config, 1.0) if ask is not None else None,
            {"minute": rec.get("minute"), "timestamp": rec.get("recorded_at"),
             "label": "buy 1 contract at the ask observed at setup detection"},
        )
        scenario_b = None
        if live and live.get("min_ask_eligible") is not None:
            b_ask = live["min_ask_eligible"]
            scenario_b = _pnl_scenario(
                b_ask, win, core.effective_fee_cents(b_ask, self.primary.config, 1.0),
                {"minute": live.get("min_ask_eligible_minute"),
                 "timestamp": live.get("min_ask_eligible_timestamp"),
                 "ex_post": True, "label": BEST_ELIGIBLE_LABEL},
            )
        return {
            "setup_id": rec.get("setup_id"), "asset": rec.get("asset"),
            "source": source, "window_start_utc": rec.get("window_start_utc"),
            "minute": rec.get("minute"), "side": rec.get("side"),
            "distance_bp": rec.get("distance_bp"), "basis_gap_bp": rec.get("target_gap_bp"),
            "basis_ok": rec.get("basis_ok"), "spread_cents": rec.get("setup_spread_cents"),
            "cons_pct": rec.get("conservative_fair_pct"), "max_buy_cents": rec.get("max_buy_cents"),
            "edge_at_detection": rec.get("edge_points"), "valid": not reasons,
            "invalid_reasons": reasons, "kalshi_result": rec.get("kalshi_result"),
            "kalshi_win": win, "benchmark_disagreement": rec.get("benchmark_disagreement"),
            "prod_entry_simulated": bool(rec.get("entry_simulated")),
            "prod_net_pnl_cents": rec.get("net_pnl_cents"),
            "scenarios": {"A_detection_entry": scenario_a, "B_best_eligible_entry": scenario_b},
        }

    def backfill_from_ledger(self):
        added = 0
        with self.alt.ledger.lock:
            rows = [dict(x) for x in self.alt.ledger.data.get("setups", {}).values()]
        for rec in rows:
            sid = rec.get("setup_id")
            if not sid or not rec.get("resolved"):
                continue
            with self.lock:
                if sid in self.data["counterfactual"] or sid in self.data["live"]:
                    continue
            cf = self._counterfactual_from(rec, None, "ledger_backfill")
            if cf:
                with self.lock:
                    self.data["counterfactual"][sid] = cf
                    self.data["counters"]["cf_backfilled"] += 1
                    self.dirty = True
                added += 1
        return added

    def counterfactual_rows(self, prospective_only=False, valid_only=True):
        with self.lock:
            rows = [dict(x) for x in self.data["counterfactual"].values()]
        if prospective_only:
            rows = [x for x in rows if x.get("source") == "trace"]
        if valid_only:
            rows = [x for x in rows if x.get("valid")]
        rows.sort(key=lambda x: (x.get("window_start_utc") or "", x.get("asset") or ""))
        return rows

    def threshold_sweep(self, prospective_only=False):
        rows = self.counterfactual_rows(prospective_only, True)
        out = []
        for threshold in CF_THRESHOLDS:
            entries = [x for x in rows if x.get("edge_at_detection") is not None
                       and float(x["edge_at_detection"]) >= threshold
                       and x.get("scenarios", {}).get("A_detection_entry")]
            wins = sum(1 for x in entries if x.get("kalshi_win"))
            nets = [float(x["scenarios"]["A_detection_entry"]["net_pnl_cents"]) for x in entries]
            costs = [float(x["scenarios"]["A_detection_entry"]["effective_cost_cents"]) for x in entries]
            out.append({
                "threshold": threshold, "opportunities": len(rows), "entries": len(entries),
                "wins": wins, "losses": len(entries) - wins,
                "hit_rate_pct": 100.0 * wins / len(entries) if entries else None,
                "wilson_low_pct": core.wilson_low(wins, len(entries)) if entries else None,
                "avg_ask_cents": _mean([x["scenarios"]["A_detection_entry"]["ask_cents"] for x in entries]),
                "avg_fee_cents": _mean([x["scenarios"]["A_detection_entry"]["estimated_fee_cents"] for x in entries]),
                "net_pnl_dollars": round(sum(nets) / 100.0, 4),
                "avg_pnl_per_contract_cents": _mean(nets),
                "roi_pct": (sum(nets) / sum(costs) * 100.0) if costs and sum(costs) > 0 else None,
                "max_loss_cents": min(nets) if nets else None,
            })
        return {"version": APP_VERSION, "mode": "counterfactual-analytics-only",
                "prospective_only": bool(prospective_only), "sample_size": len(rows), "thresholds": out}

    def ask_buckets(self, prospective_only=False):
        rows = self.counterfactual_rows(prospective_only, True)
        out = []
        for label, lo, hi in ASK_BUCKETS:
            bucket = [x for x in rows if x.get("scenarios", {}).get("A_detection_entry")
                      and lo <= float(x["scenarios"]["A_detection_entry"]["ask_cents"]) < hi]
            wins = sum(1 for x in bucket if x.get("kalshi_win"))
            asks = [float(x["scenarios"]["A_detection_entry"]["ask_cents"]) for x in bucket]
            fees = [float(x["scenarios"]["A_detection_entry"]["estimated_fee_cents"]) for x in bucket]
            nets = [float(x["scenarios"]["A_detection_entry"]["net_pnl_cents"]) for x in bucket]
            out.append({
                "bucket": label, "n": len(bucket), "wins": wins, "losses": len(bucket)-wins,
                "hit_pct": 100.0 * wins / len(bucket) if bucket else None,
                "wilson_low_pct": core.wilson_low(wins, len(bucket)) if bucket else None,
                "avg_ask_cents": _mean(asks), "avg_fee_cents": _mean(fees),
                "breakeven_win_rate_pct": _mean([a + f for a, f in zip(asks, fees)]),
                "net_counterfactual_pnl_dollars": round(sum(nets) / 100.0, 4),
            })
        return {"version": APP_VERSION, "mode": "counterfactual-analytics-only",
                "prospective_only": bool(prospective_only), "sample_size": len(rows), "buckets": out}

    # ----------------------------- BTC research ledger
    def observe_btc_poll(self, state):
        try:
            if state.status not in BTC_SIGNAL_STATUSES:
                return
            dt = core.parse_iso(state.updated_at) or datetime.now(timezone.utc)
            now_ms = int(dt.timestamp() * 1000)
            ws_ms = (now_ms // 900000) * 900000
            we_ms = ws_ms + 900000
            window = datetime.fromtimestamp(ws_ms / 1000, tz=timezone.utc).isoformat()
            sid = f"BTC|{window}"
            ask = state.selected_ask_cents
            bid = state.yes_bid_cents if state.side == "UP" else (state.no_bid_cents if state.side == "DOWN" else None)
            with self.lock:
                rec = self.data["btc"].get(sid)
                if rec is None:
                    row = core.fair_row(self.primary.model, state.distance_bp or 0)
                    rec = {
                        "setup_id": sid, "source": "live", "recorded_at": state.updated_at,
                        "window_start_utc": window, "window_start_ms": ws_ms, "window_end_ms": we_ms,
                        "minute": state.last_completed_minute, "tier_min_bp": row.get("min_bp") if row else None,
                        "distance_bp": state.distance_bp, "side": state.side, "quality": state.quality,
                        "model_open": state.model_open, "market_ticker": state.market_ticker,
                        "kalshi_target": state.kalshi_target, "target_gap_bp": state.target_gap_bp,
                        "basis_ok": state.basis_ok, "first_status": state.status,
                        "ask_cents": ask, "bid_cents": bid,
                        "spread_cents": (float(ask)-float(bid)) if ask is not None and bid is not None else None,
                        "max_buy_cents": state.max_buy_cents, "edge_points": state.edge_points,
                        "entry_zone_seen": state.status == "ENTRY ZONE",
                        "entry_zone_ask_cents": ask if state.status == "ENTRY ZONE" else None,
                        "entry_zone_ts": state.updated_at if state.status == "ENTRY ZONE" else None,
                        "resolved": False, "kalshi_status": "", "kalshi_result": "", "kalshi_win": None,
                        "underlying_final_close": None, "underlying_model_win": None,
                        "benchmark_disagreement": None, "net_pnl_cents": None,
                        "resolution_error": "", "created_by_version": APP_VERSION,
                    }
                    self.data["btc"][sid] = rec
                elif state.status == "ENTRY ZONE" and not rec.get("entry_zone_seen"):
                    rec["entry_zone_seen"] = True
                    rec["entry_zone_ask_cents"] = ask
                    rec["entry_zone_ts"] = state.updated_at
                self.dirty = True
            self.trace.append({
                "ts": state.updated_at, "app_version": APP_VERSION, "engine": "btc",
                "setup_id": sid, "asset": "BTC", "window_start_utc": window,
                "signal_minute": rec.get("minute"), "last_completed_minute": state.last_completed_minute,
                "side": state.side, "distance_bp": state.distance_bp, "basis_ok": state.basis_ok,
                "target_gap_bp": state.target_gap_bp, "bid_cents": bid, "ask_cents": ask,
                "calculated_edge_points": state.edge_points, "max_buy_cents": state.max_buy_cents,
                "status": state.status, "prod_entry_simulated": bool(rec.get("entry_zone_seen")),
            })
            self._count("btc_polls_traced")
            self._mark_dirty()
        except Exception as exc:
            self._count("hook_errors")
            print("research lab btc poll:", exc, flush=True)

    def backfill_btc_from_journal(self):
        path = core.JOURNAL_PATH
        if not path.exists():
            return 0
        added = 0
        try:
            with open(path, "r", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))
            groups = {}
            for row in rows:
                dt = core.parse_iso(row.get("timestamp_utc"))
                if not dt:
                    continue
                ws_ms = (int(dt.timestamp()*1000)//900000)*900000
                groups.setdefault(ws_ms, []).append(row)
            for ws_ms, group in groups.items():
                window = datetime.fromtimestamp(ws_ms/1000, tz=timezone.utc).isoformat()
                sid = f"BTC|{window}"
                with self.lock:
                    if sid in self.data["btc"]:
                        continue
                first = group[0]
                def num(value):
                    try: return float(value) if value not in (None, "", "None") else None
                    except Exception: return None
                entry = next((x for x in group if x.get("status") == "ENTRY ZONE"), None)
                rec = {
                    "setup_id": sid, "source": "journal_backfill",
                    "recorded_at": first.get("timestamp_utc"), "window_start_utc": window,
                    "window_start_ms": ws_ms, "window_end_ms": ws_ms + 900000,
                    "minute": int(num(first.get("minute"))) if num(first.get("minute")) is not None else None,
                    "distance_bp": num(first.get("distance_bp")), "side": first.get("side"),
                    "model_open": num(first.get("model_open")), "market_ticker": first.get("market_ticker") or "",
                    "kalshi_target": num(first.get("kalshi_target")), "target_gap_bp": num(first.get("target_gap_bp")),
                    "basis_ok": first.get("status") != "BASIS WARNING",
                    "ask_cents": num(first.get("ask_cents")), "max_buy_cents": num(first.get("max_buy_cents")),
                    "edge_points": num(first.get("edge_points")), "entry_zone_seen": bool(entry),
                    "entry_zone_ask_cents": num(entry.get("ask_cents")) if entry else None,
                    "entry_zone_ts": entry.get("timestamp_utc") if entry else None,
                    "resolved": False, "kalshi_status": "", "kalshi_result": "", "kalshi_win": None,
                    "underlying_final_close": None, "underlying_model_win": None,
                    "benchmark_disagreement": None, "net_pnl_cents": None,
                    "resolution_error": "", "created_by_version": APP_VERSION,
                }
                with self.lock:
                    self.data["btc"][sid] = rec
                    self.data["counters"]["btc_backfilled"] += 1
                    self.dirty = True
                added += 1
        except Exception as exc:
            print("btc journal backfill:", exc, flush=True)
        return added

    def _maybe_resolve_btc(self):
        if time.time() - self.last_btc_resolve < 20:
            return
        self.last_btc_resolve = time.time()
        now_ms = int(time.time()*1000)
        with self.lock:
            ids = [k for k, r in self.data["btc"].items()
                   if not r.get("resolved") and r.get("window_end_ms")
                   and now_ms > int(r["window_end_ms"]) + 15000]
        for sid in ids[:20]:
            with self.lock:
                rec = dict(self.data["btc"].get(sid) or {})
            ticker = rec.get("market_ticker")
            if not ticker:
                continue
            try:
                market = core.http_json(core.KALSHI_BASE + "/markets/" + ticker).get("market", {})
                result = str(market.get("result") or "").lower()
                status = str(market.get("status") or "")
                rec["kalshi_status"] = status
                final = status.lower() in ("finalized", "settled") or bool(market.get("settlement_ts"))
                if result not in ("yes", "no") or not final:
                    continue
                win = (result == "yes" and rec.get("side") == "UP") or (result == "no" and rec.get("side") == "DOWN")
                rec["kalshi_result"] = result
                rec["kalshi_win"] = bool(win)
                rec["resolved"] = True
                rec["resolved_at"] = datetime.now(timezone.utc).isoformat()
                if rec.get("entry_zone_seen") and rec.get("entry_zone_ask_cents") is not None:
                    ask = rec["entry_zone_ask_cents"]
                    sc = _pnl_scenario(ask, win, core.effective_fee_cents(ask, self.primary.config, 1.0))
                    rec["net_pnl_cents"] = sc["net_pnl_cents"]
                rec["resolution_error"] = ""
                with self.lock:
                    self.data["btc"][sid] = rec
                    self.data["counters"]["btc_resolved"] += 1
                    self.dirty = True
            except Exception as exc:
                with self.lock:
                    if sid in self.data["btc"]:
                        self.data["btc"][sid]["resolution_error"] = str(exc)[:300]
                        self.dirty = True
            time.sleep(0.05)

    def trajectory_view(self, setup_id=None):
        with self.lock:
            live = [dict(x) for x in self.data["live"].values()]
            summaries = [dict(x) for x in self.data["summaries"].values()]
        if setup_id:
            live = [x for x in live if x.get("setup_id") == setup_id]
            summaries = [x for x in summaries if x.get("setup_id") == setup_id]
        return {"version": APP_VERSION, "totals": {
                    "live": len(live), "finalized": len(summaries),
                    "ever_reached_current_max_buy": sum(1 for x in summaries if x.get("ever_reached_current_max_buy")),
                    "avg_time_entry_eligible_seconds": _mean([x.get("time_entry_eligible_seconds") for x in summaries]),
                    "avg_polls_while_entry_eligible": _mean([x.get("polls_while_entry_eligible") for x in summaries]),
                }, "live": live, "summaries": summaries}

    def counterfactual_view(self, prospective_only=False):
        rows = self.counterfactual_rows(prospective_only, False)
        valid = [x for x in rows if x.get("valid")]
        def agg(key):
            scenarios = [x.get("scenarios", {}).get(key) for x in valid if x.get("scenarios", {}).get(key)]
            wins = sum(1 for x in scenarios if x.get("win"))
            nets = [float(x["net_pnl_cents"]) for x in scenarios]
            costs = [float(x["effective_cost_cents"]) for x in scenarios]
            return {"n": len(scenarios), "wins": wins, "losses": len(scenarios)-wins,
                    "hit_pct": 100*wins/len(scenarios) if scenarios else None,
                    "wilson_low_pct": core.wilson_low(wins, len(scenarios)) if scenarios else None,
                    "avg_ask_cents": _mean([x["ask_cents"] for x in scenarios]),
                    "net_pnl_dollars": round(sum(nets)/100, 4),
                    "roi_pct": (sum(nets)/sum(costs)*100) if costs and sum(costs)>0 else None}
        return {"version": APP_VERSION, "mode": "counterfactual-accounting-only",
                "records": len(rows), "valid_records": len(valid),
                "scenario_A_detection_entry": agg("A_detection_entry"),
                "scenario_B_best_eligible_entry": dict(agg("B_best_eligible_entry"), ex_post=True, label=BEST_ELIGIBLE_LABEL),
                "rows": rows}

    def btc_view(self):
        with self.lock:
            rows = [dict(x) for x in self.data["btc"].values()]
        entries = [x for x in rows if x.get("resolved") and x.get("entry_zone_seen") and x.get("net_pnl_cents") is not None]
        wins = sum(1 for x in entries if x.get("kalshi_win"))
        return {"version": APP_VERSION, "mode": "btc-research-ledger; strategy untouched",
                "totals": {"records": len(rows), "resolved": sum(1 for x in rows if x.get("resolved")),
                           "entry_zone_records": sum(1 for x in rows if x.get("entry_zone_seen")),
                           "entry_zone_resolved": len(entries), "entry_zone_wins": wins,
                           "entry_zone_losses": len(entries)-wins,
                           "entry_zone_hit_pct": 100*wins/len(entries) if entries else None,
                           "entry_zone_net_pnl_dollars": round(sum(float(x.get("net_pnl_cents") or 0) for x in entries)/100, 4)},
                "records": rows}

    def basis_gate_view(self):
        with self.alt.ledger.lock:
            rows = [dict(x) for x in self.alt.ledger.data.get("setups", {}).values()]
        compact = [{"setup_id": x.get("setup_id"), "asset": x.get("asset"),
                    "window_start_utc": x.get("window_start_utc"), "model_open": x.get("model_open"),
                    "kalshi_target": x.get("kalshi_target"), "gap_bp": x.get("target_gap_bp"),
                    "side": x.get("side"), "basis_ok": x.get("basis_ok"),
                    "resolved": x.get("resolved"), "kalshi_win": x.get("kalshi_win"),
                    "benchmark_disagreement": x.get("benchmark_disagreement"),
                    "ask_cents": x.get("ask_cents"), "max_buy_cents": x.get("max_buy_cents")} for x in rows]
        resolved = [x for x in compact if x.get("resolved")]
        fails = [x for x in resolved if x.get("basis_ok") is False]
        oks = [x for x in resolved if x.get("basis_ok") is True]
        return {"version": APP_VERSION,
                "max_basis_gap_bp": float(self.primary.config.get("max_basis_gap_bp", 8.0)),
                "totals": {"resolved": len(resolved), "basis_fail": len(fails),
                           "basis_fail_kalshi_losses": sum(1 for x in fails if x.get("kalshi_win") is False),
                           "basis_ok_kalshi_losses": sum(1 for x in oks if x.get("kalshi_win") is False)},
                "rows": compact}

    def best_threshold(self):
        candidates = [x for x in self.threshold_sweep(False)["thresholds"] if x["entries"] > 0]
        if not candidates:
            return {"threshold": None, "n": 0, "net_pnl_dollars": None}
        best = max(candidates, key=lambda x: (x["net_pnl_dollars"], x["threshold"]))
        return {"threshold": best["threshold"], "n": best["entries"], "net_pnl_dollars": best["net_pnl_dollars"]}

    def health_summary(self):
        try:
            with self.lock:
                cf = list(self.data["counterfactual"].values())
                btc = list(self.data["btc"].values())
                n_setups = len(self.data["live"]) + len(self.data["summaries"])
            return {"trace_setups": n_setups,
                    "counterfactual_resolved": sum(1 for x in cf if x.get("kalshi_win") is not None),
                    "btc_research_resolved": sum(1 for x in btc if x.get("resolved")),
                    "best_observed_counterfactual_threshold_n": self.best_threshold(),
                    "trace_errors": self.trace.errors}
        except Exception as exc:
            return {"error": str(exc)}

    def status(self):
        with self.lock:
            return {"version": APP_VERSION, "lab_version": RESEARCH_LAB_VERSION,
                    "mode": "observational; no strategy changes",
                    "trace_path": str(DECISION_TRACE_PATH), "trace_bytes": self.trace.size_bytes(),
                    "trace_lines_this_process": self.trace.lines, "trace_errors": self.trace.errors,
                    "live_setups": len(self.data["live"]), "summaries": len(self.data["summaries"]),
                    "counterfactual_records": len(self.data["counterfactual"]),
                    "btc_records": len(self.data["btc"]), "counters": dict(self.data["counters"]),
                    "backup_pre_v27": backup_manifest("pre_v27")}


# ---------------------------------------------------------------------------
# Monkey-patches. They add observers around 2.6 methods; decision functions are
# never replaced: Monitor.compute, AltShadowMonitor.compute and
# ShadowLedger.observe_state remain the exact objects defined in app.py 2.6.
# ---------------------------------------------------------------------------
core.RESEARCH_LAB = None

_original_alt_init = core.AltShadowMonitor.__init__
def _alt_init(self, primary_monitor):
    _original_alt_init(self, primary_monitor)
    self.research = None
    try:
        if core.RESEARCH_LAB is None:
            core.RESEARCH_LAB = ResearchLab(primary_monitor, self)
        self.research = core.RESEARCH_LAB
    except Exception as exc:
        print("research lab init failed (monitor continues):", exc, flush=True)
core.AltShadowMonitor.__init__ = _alt_init

_original_alt_journal = core.AltShadowMonitor.journal_if_confirmed
def _alt_journal(self, state):
    result = _original_alt_journal(self, state)
    try:
        if getattr(self, "research", None) and state.get("status") in ALT_SIGNAL_STATUSES:
            self.research.observe_alt_poll(state, state.get("status"))
    except Exception as exc:
        print("research lab poll:", exc, flush=True)
    return result
core.AltShadowMonitor.journal_if_confirmed = _alt_journal

_original_resolve = core.ShadowLedger.maybe_resolve
def _resolve(self):
    result = _original_resolve(self)
    try:
        lab = getattr(core, "RESEARCH_LAB", None)
        if lab:
            lab.on_cycle_end()
    except Exception as exc:
        print("research lab cycle:", exc, flush=True)
    return result
core.ShadowLedger.maybe_resolve = _resolve

_original_btc_alert = core.Monitor.maybe_alert
def _btc_alert(self, state):
    result = _original_btc_alert(self, state)
    try:
        lab = getattr(core, "RESEARCH_LAB", None)
        if lab:
            lab.observe_btc_poll(state)
    except Exception as exc:
        print("research lab btc:", exc, flush=True)
    return result
core.Monitor.maybe_alert = _btc_alert


def _startup(self):
    if self.startup_sent:
        return
    self.startup_sent = True
    lines = [
        "Multiasset EXECUTION LAB v2.7 OBSERVABILITY ✅",
        "MAIN: ETH ≥30bp · SOL ≥30bp · XRP ≥40bp · DOGE ≥35bp · BNB ≥20bp",
        "EARLY LAB: M6-M7 · BTC ≥10bp · ETH/SOL ≥25bp · XRP ≥35bp · DOGE ≥30bp · BNB ≥15bp",
        "Early Lab es experimental: captura precio antes; NO es señal operativa.",
        "BTC sigue igual. HYPE excluido. Sin órdenes automáticas.",
    ]
    core.post_telegram(core.telegram_token(), core.telegram_chat_id(self.primary.config), "\n".join(lines))
core.AltShadowMonitor.send_startup = _startup

_original_send = core.Handler._send
def _send(self, code, body, content_type="application/json; charset=utf-8", extra_headers=None):
    try:
        if urlparse(self.path).path == "/health" and code == 200:
            raw = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body
            payload = json.loads(raw)
            lab = getattr(core, "RESEARCH_LAB", None)
            payload["research_lab"] = lab.health_summary() if lab else None
            body = json.dumps(payload)
    except Exception:
        pass
    return _original_send(self, code, body, content_type, extra_headers)
core.Handler._send = _send

_original_get = core.Handler.do_GET
def _do_get(self):
    path = urlparse(self.path).path
    legacy = {"/api/research/status", "/api/research/file"}
    if path.startswith("/api/research/") and path not in legacy:
        if not self._authorized():
            return
        lab = getattr(core, "RESEARCH_LAB", None)
        if not lab:
            self._send(404, json.dumps({"error": "research lab off"})); return
        params = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        prospective = str(params.get("prospective_only", "0")).lower() in ("1", "true", "yes")
        if path == "/api/research/lab":
            self._send(200, json.dumps(lab.status())); return
        if path == "/api/research/decision-trace":
            limit = min(max(int(params.get("limit", "500") or 500), 1), 5000)
            rows = lab.trace.read(params.get("setup_id"), params.get("engine"), limit)
            self._send(200, json.dumps({"version": APP_VERSION, "count": len(rows), "lines": rows})); return
        if path == "/api/research/decision-trace.jsonl":
            if DECISION_TRACE_PATH.exists():
                self._send(200, DECISION_TRACE_PATH.read_bytes(), "application/x-ndjson; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=decision_trace_v27.jsonl"})
            else:
                self._send(404, b"", "text/plain")
            return
        if path == "/api/research/trajectory-summary":
            self._send(200, json.dumps(lab.trajectory_view(params.get("setup_id")))); return
        if path == "/api/research/counterfactual":
            self._send(200, json.dumps(lab.counterfactual_view(prospective))); return
        if path == "/api/research/threshold-sweep":
            self._send(200, json.dumps(lab.threshold_sweep(prospective))); return
        if path == "/api/research/ask-buckets":
            self._send(200, json.dumps(lab.ask_buckets(prospective))); return
        if path == "/api/research/btc-shadow":
            self._send(200, json.dumps(lab.btc_view())); return
        if path == "/api/research/basis-gate":
            self._send(200, json.dumps(lab.basis_gate_view())); return
        if path == "/api/research/lab.json":
            if RESEARCH_LAB_PATH.exists():
                self._send(200, RESEARCH_LAB_PATH.read_bytes(), "application/json; charset=utf-8",
                           {"Content-Disposition": "attachment; filename=research_lab_v27.json"})
            else:
                self._send(404, b"", "text/plain")
            return
        self._send(404, json.dumps({"error": "not found"})); return
    return _original_get(self)
core.Handler.do_GET = _do_get


def main():
    # Must run before core.main instantiates Monitor/EarlyEntryLab and writes state.
    backup_ledgers_once("pre_v27")
    core.main()


if __name__ == "__main__":
    main()
