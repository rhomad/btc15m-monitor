#!/usr/bin/env python3
import base64
import csv
import json
import math
import os
import re
import sys
import time
import threading
import webbrowser
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
MODEL_DATA = {'model_name': 'BTC 15m pullback continuation', 'version': '2026-08-29-v1', 'training_window': '2026-03-01 to 2026-07-31 UTC', 'source': 'BTCUSDT Binance Spot 1m', 'minutes_analyzed': 220320, 'candles_15m': 14688, 'signal_window_minutes': [7, 8, 9, 10], 'thresholds': [{'min_bp': 10, 'n': 5257, 'fair_pct': 90.184516, 'wilson_low_pct': 89.350648, 'worst_month_pct': 88.283582}, {'min_bp': 15, 'n': 3244, 'fair_pct': 93.218249, 'wilson_low_pct': 92.300891, 'worst_month_pct': 91.861761}, {'min_bp': 20, 'n': 2041, 'fair_pct': 95.492406, 'wilson_low_pct': 94.503633, 'worst_month_pct': 93.993506}, {'min_bp': 25, 'n': 1321, 'fair_pct': 96.66919, 'wilson_low_pct': 95.558177, 'worst_month_pct': 95.104895}, {'min_bp': 30, 'n': 885, 'fair_pct': 97.514124, 'wilson_low_pct': 96.2648, 'worst_month_pct': 96.629213}, {'min_bp': 35, 'n': 648, 'fair_pct': 97.839506, 'wilson_low_pct': 96.406367, 'worst_month_pct': 96.551724}, {'min_bp': 40, 'n': 467, 'fair_pct': 98.072805, 'wilson_low_pct': 96.378297, 'worst_month_pct': 97.350993}], 'notes': ['Fair conservador = min(Wilson 95% lower bound, worst observed month).', 'Signal is confirmed only on a completed 1-minute candle.', 'A live intraminute pullback is shown only as FORMING and is not assigned the same evidentiary status.', 'This model uses Binance Spot. Kalshi settlement uses its own benchmark; the app shows target/anchor basis when target is available.']}
DEFAULTS_DATA = {'min_distance_bp': 10, 'edge_min_points': 10.0, 'fee_buffer_cents': 1.0, 'bankroll_dollars': 1000.0, 'kelly_fraction': 0.125, 'risk_cap_pct': 1.0, 'max_basis_gap_bp': 8.0, 'poll_seconds': 1.0, 'kalshi_poll_seconds': 2.0, 'telegram_alert_statuses': ['FORMING', 'CONFIRMED', 'ENTRY ZONE', 'BASIS WARNING']}
DASHBOARD_HTML = '<!doctype html>\n<html lang="es">\n<head>\n<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">\n<title>BTC 15M Monitor</title>\n<style>\n:root{--bg:#07090d;--card:#11151d;--card2:#171c25;--text:#f7f7f7;--muted:#98a2b3;--line:#283141;--good:#46d369;--warn:#ffbe44;--bad:#ff625d;--blue:#5ba7ff}\n*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,Arial,sans-serif}.wrap{max-width:1180px;margin:0 auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:end;gap:18px;margin-bottom:18px}.title{font-size:30px;font-weight:800}.sub{color:var(--muted);font-size:13px}.pill{border:1px solid var(--line);border-radius:999px;padding:8px 12px;color:var(--muted);font-size:12px}.status{padding:22px;border-radius:18px;background:var(--card);border:1px solid var(--line);margin-bottom:16px}.status.big-entry{border-color:var(--good);box-shadow:0 0 0 1px var(--good) inset}.status.big-forming{border-color:var(--warn)}.status-name{font-size:36px;font-weight:900;letter-spacing:-1px}.status-detail{color:var(--muted);margin-top:5px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;min-height:112px}.label{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}.value{font-size:27px;font-weight:800;margin-top:7px}.small{font-size:13px;color:var(--muted);margin-top:4px}.up{color:var(--good)}.down{color:#ff6b45}.warn{color:var(--warn)}.section{font-size:18px;font-weight:800;margin:24px 0 10px}.settings{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.field{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}.field label{display:block;color:var(--muted);font-size:12px;margin-bottom:6px}.field input,.field select{width:100%;background:#0b0e13;color:white;border:1px solid var(--line);border-radius:8px;padding:9px;font-size:15px}.button{margin-top:10px;background:var(--blue);color:#06101c;border:0;border-radius:10px;font-weight:800;padding:10px 16px;cursor:pointer}.footer{color:var(--muted);font-size:12px;margin:24px 0 40px;line-height:1.5}.bar{height:8px;background:#202734;border-radius:99px;overflow:hidden;margin-top:10px}.bar>div{height:100%;background:var(--good);width:0%}.error{color:var(--bad);font-size:12px;margin-top:10px;white-space:pre-wrap}.a-plus{color:#b5ff77}.a{color:#6dff91}.b{color:#ffd76d}.c{color:#c1cad7}\n@media(max-width:800px){.grid,.settings{grid-template-columns:repeat(2,1fr)}.title{font-size:24px}.status-name{font-size:30px}}\n@media(max-width:500px){.wrap{padding:14px}.grid,.settings{grid-template-columns:1fr 1fr}.value{font-size:22px}.card{min-height:100px;padding:13px}.top{align-items:start;flex-direction:column}.status-name{font-size:28px}}\n</style>\n</head>\n<body><div class="wrap">\n<div class="top"><div><div class="title">BTC 15M · Pullback Monitor Cloud</div><div class="sub">M7–M10 · fair/edge/Kelly · Railway 24/7 + Telegram</div></div><div class="pill" id="updated">Conectando…</div></div>\n<div class="status" id="statusBox"><div class="status-name" id="status">STARTING</div><div class="status-detail" id="statusDetail">Esperando datos…</div><div class="error" id="error"></div></div>\n\n<div class="grid">\n <div class="card"><div class="label">BTC live</div><div class="value" id="btc">—</div><div class="small">Open modelo: <span id="modelOpen">—</span></div></div>\n <div class="card"><div class="label">Vela</div><div class="value" id="minute">—</div><div class="small"><span id="left">—</span> restantes</div></div>\n <div class="card"><div class="label">Dirección / distancia</div><div class="value" id="side">—</div><div class="small"><span id="dist">—</span> bp · calidad <b id="quality">—</b></div></div>\n <div class="card"><div class="label">Último minuto</div><div class="value" id="action">—</div><div class="small">Paso: <span id="step">—</span> bp</div></div>\n</div>\n\n<div class="section">Fair value</div>\n<div class="grid">\n <div class="card"><div class="label">Fair central</div><div class="value" id="fair">—</div><div class="small">Histórico del tier</div></div>\n <div class="card"><div class="label">Fair conservador</div><div class="value" id="consFair">—</div><div class="small">Peor entre Wilson 95% y peor mes</div></div>\n <div class="card"><div class="label">Muestra</div><div class="value" id="sample">—</div><div class="small">Estados históricos</div></div>\n <div class="card"><div class="label">A+ extra</div><div class="value" id="neverCross">—</div><div class="small">Nunca cruzó el open por cierre 1m</div></div>\n</div>\n\n<div class="section">Kalshi</div>\n<div class="grid">\n <div class="card"><div class="label">Target / basis</div><div class="value" id="target">—</div><div class="small">Gap vs open Binance: <span id="basis">—</span> bp</div></div>\n <div class="card"><div class="label">UP ask</div><div class="value up" id="yesAsk">—</div><div class="small">bid <span id="yesBid">—</span></div></div>\n <div class="card"><div class="label">DOWN ask</div><div class="value down" id="noAsk">—</div><div class="small">bid <span id="noBid">—</span></div></div>\n <div class="card"><div class="label">Contrato elegido</div><div class="value" id="selected">—</div><div class="small">Ticker: <span id="ticker">—</span></div></div>\n</div>\n\n<div class="section">Entrada y tamaño</div>\n<div class="grid">\n <div class="card"><div class="label">Ask actual</div><div class="value" id="ask">—</div><div class="small">lado del modelo</div></div>\n <div class="card"><div class="label">Máximo a pagar</div><div class="value" id="maxBuy">—</div><div class="small">fair cons − edge mínimo − fee buffer</div></div>\n <div class="card"><div class="label">Edge conservador</div><div class="value" id="edge">—</div><div class="bar"><div id="edgeBar"></div></div></div>\n <div class="card"><div class="label">Riesgo sugerido</div><div class="value" id="risk">—</div><div class="small"><span id="kelly">—</span> Kelly fraccional · <span id="contracts">—</span> contratos aprox.</div></div>\n</div>\n\n<div class="section">Ajustes</div>\n<div class="settings">\n <div class="field"><label>Distancia mínima (bp)</label><select id="minbp"><option>10</option><option>15</option><option>20</option><option>25</option></select></div>\n <div class="field"><label>Edge mínimo (puntos)</label><input id="edgeMin" type="number" step="0.5"></div>\n <div class="field"><label>Fee buffer (¢)</label><input id="fee" type="number" step="0.1"></div>\n <div class="field"><label>Bankroll ($)</label><input id="bankroll" type="number" step="50"></div>\n <div class="field"><label>Kelly fraction (0.125 = 1/8)</label><input id="kf" type="number" step="0.025"></div>\n <div class="field"><label>Hard cap riesgo (%)</label><input id="cap" type="number" step="0.25"></div>\n <div class="field"><label>Máx gap Kalshi/Binance (bp)</label><input id="basisMax" type="number" step="1"></div>\n <div class="field"><label>Journal</label><button class="button" onclick="location.href=\'/api/journal\'">Descargar CSV</button></div>\n</div>\n<button class="button" onclick="saveSettings()">Guardar ajustes</button>\n\n<div class="section">Telegram / servidor</div>\n<div class="grid">\n <div class="card"><div class="label">Telegram</div><div class="value" id="tgStatus">—</div><div class="small" id="tgDetail">Configura TELEGRAM_BOT_TOKEN en Railway.</div></div>\n <div class="card"><div class="label">Conectar chat</div><button class="button" onclick="discoverTelegram()">Detectar chat</button><div class="small">Primero manda /start a tu bot.</div></div>\n <div class="card"><div class="label">Prueba</div><button class="button" onclick="testTelegram()">Enviar prueba</button><div class="small" id="tgMessage">—</div></div>\n <div class="card"><div class="label">Cloud</div><div class="value" id="cloudVersion">—</div><div class="small">Journal persistente si Railway Volume está montado.</div></div>\n</div>\n\n<div class="footer">Esta v1 es un scanner read-only: no coloca órdenes. “FORMING” es intraminuto y sirve como aviso temprano; la evidencia histórica se asigna a “CONFIRMED”, después del cierre del minuto. El modelo usa BTCUSDT Binance Spot y el contrato de Kalshi puede liquidar con otro benchmark. Si el target se puede extraer, el dashboard muestra el gap entre ambos anchors.</div>\n</div>\n<script>\nlet initialized=false,lastStatus=\'\';\nconst $=id=>document.getElementById(id);\nconst money=x=>x==null?\'—\':\'$\'+Number(x).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});\nconst num=(x,d=1)=>x==null?\'—\':Number(x).toFixed(d);\nconst cents=x=>x==null?\'—\':Number(x).toFixed(1)+\'¢\';\nfunction beep(){try{let a=new (window.AudioContext||window.webkitAudioContext)();let o=a.createOscillator(),g=a.createGain();o.connect(g);g.connect(a.destination);o.frequency.value=740;g.gain.value=.06;o.start();o.stop(a.currentTime+.18)}catch(e){}}\nfunction fmtTime(sec){if(sec==null)return\'—\';let m=Math.floor(sec/60),s=sec%60;return m+\':\'+String(s).padStart(2,\'0\')}\nasync function refresh(){\n try{\n  const r=await fetch(\'/api/state\',{cache:\'no-store\'}),j=await r.json(),s=j.state,c=j.settings;\n  $(\'updated\').textContent=(s.mode===\'demo\'?\'DEMO · \':\'\')+\'actualizado \'+new Date(s.updated_at).toLocaleTimeString();\n  $(\'status\').textContent=s.status;$(\'statusDetail\').textContent=s.status_detail;$(\'error\').textContent=s.api_error||\'\';\n  $(\'statusBox\').className=\'status \'+(s.status===\'ENTRY ZONE\'?\'big-entry\':s.status===\'FORMING\'?\'big-forming\':\'\');\n  if(s.status!==lastStatus && [\'FORMING\',\'CONFIRMED\',\'ENTRY ZONE\'].includes(s.status))beep();lastStatus=s.status;\n  $(\'btc\').textContent=money(s.btc_price);$(\'modelOpen\').textContent=money(s.model_open);\n  $(\'minute\').textContent=\'M\'+(s.current_minute??\'—\');$(\'left\').textContent=fmtTime(s.seconds_left);\n  $(\'side\').textContent=s.side;$(\'side\').className=\'value \'+(s.side===\'UP\'?\'up\':s.side===\'DOWN\'?\'down\':\'\');\n  $(\'dist\').textContent=num(s.distance_bp,1);$(\'quality\').textContent=s.quality;$(\'quality\').className=s.quality===\'A+\'?\'a-plus\':s.quality===\'A\'?\'a\':s.quality===\'B\'?\'b\':\'c\';\n  $(\'action\').textContent=s.pullback_confirmed?\'PULLBACK ✓\':s.pullback_forming?\'FORMING…\':\'—\';$(\'step\').textContent=num(s.last_step_bp,1);\n  $(\'fair\').textContent=s.fair_pct==null?\'—\':num(s.fair_pct,1)+\'%\';$(\'consFair\').textContent=s.conservative_fair_pct==null?\'—\':num(s.conservative_fair_pct,1)+\'%\';$(\'sample\').textContent=s.sample_n??\'—\';$(\'neverCross\').textContent=s.never_crossed_open?\'YES · A+\':\'No / n.a.\';\n  $(\'target\').textContent=money(s.kalshi_target);$(\'basis\').textContent=num(s.target_gap_bp,1);$(\'yesAsk\').textContent=cents(s.yes_ask_cents);$(\'yesBid\').textContent=cents(s.yes_bid_cents);$(\'noAsk\').textContent=cents(s.no_ask_cents);$(\'noBid\').textContent=cents(s.no_bid_cents);$(\'selected\').textContent=s.selected_contract;$(\'ticker\').textContent=s.market_ticker||\'—\';\n  $(\'ask\').textContent=cents(s.selected_ask_cents);$(\'maxBuy\').textContent=cents(s.max_buy_cents);$(\'edge\').textContent=s.edge_points==null?\'—\':num(s.edge_points,1)+\' pt\';$(\'edgeBar\').style.width=Math.max(0,Math.min(100,(s.edge_points||0)*4))+\'%\';\n  $(\'risk\').textContent=s.risk_dollars==null?\'—\':money(s.risk_dollars);$(\'kelly\').textContent=s.fractional_kelly_pct==null?\'—\':num(s.fractional_kelly_pct,2)+\'%\';$(\'contracts\').textContent=s.contracts_suggested==null?\'—\':s.contracts_suggested;\n  $(\'tgStatus\').textContent=c.telegram_configured?\'READY\':\'OFF\';$(\'tgStatus\').className=\'value \'+(c.telegram_configured?\'up\':\'warn\');$(\'tgDetail\').textContent=c.telegram_configured?\'Bot + chat configurados\':(c.telegram_token_present?\'Token OK; falta detectar chat\':\'Falta TELEGRAM_BOT_TOKEN\');$(\'cloudVersion\').textContent=c.app_version||\'v2\';\n  if(!initialized){$(\'minbp\').value=c.min_distance_bp;$(\'edgeMin\').value=c.edge_min_points;$(\'fee\').value=c.fee_buffer_cents;$(\'bankroll\').value=c.bankroll_dollars;$(\'kf\').value=c.kelly_fraction;$(\'cap\').value=c.risk_cap_pct;$(\'basisMax\').value=c.max_basis_gap_bp;initialized=true}\n }catch(e){$(\'error\').textContent=\'Dashboard: \'+e}\n}\nasync function discoverTelegram(){let r=await fetch(\'/api/telegram/discover\',{method:\'POST\'}),j=await r.json();$(\'tgMessage\').textContent=j.ok?(j.message+\' · \'+j.chat_id):(j.error||\'Error\');initialized=false;refresh()}\nasync function testTelegram(){let r=await fetch(\'/api/telegram/test\',{method:\'POST\'}),j=await r.json();$(\'tgMessage\').textContent=j.message||j.error||\'—\'}\nasync function saveSettings(){let body={min_distance_bp:+$(\'minbp\').value,edge_min_points:+$(\'edgeMin\').value,fee_buffer_cents:+$(\'fee\').value,bankroll_dollars:+$(\'bankroll\').value,kelly_fraction:+$(\'kf\').value,risk_cap_pct:+$(\'cap\').value,max_basis_gap_bp:+$(\'basisMax\').value};await fetch(\'/api/settings\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(body)});}\nsetInterval(refresh,1000);refresh();\n</script></body></html>\n'

# Railway exposes RAILWAY_VOLUME_MOUNT_PATH automatically when a volume exists.
# Local fallback keeps data inside ./data.
DATA_DIR = Path(os.getenv("DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or (ROOT / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = DATA_DIR / "config.json"
JOURNAL_PATH = DATA_DIR / "signal_journal.csv"

BINANCE_BASE = "https://data-api.binance.vision"
KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = os.getenv("KALSHI_SERIES", "KXBTC15M")
APP_VERSION = "2.2-railway-multiasset-shadow"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def env_num(name, default, caster=float):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return caster(v)
    except Exception:
        return default


def merge_config():
    cfg = dict(DEFAULTS_DATA)
    if CONFIG_PATH.exists():
        try:
            persisted = load_json(CONFIG_PATH)
            if isinstance(persisted, dict):
                cfg.update(persisted)
        except Exception:
            pass
    # Environment variables win at boot. Secrets are intentionally not persisted.
    overrides = {
        "min_distance_bp": ("MIN_DISTANCE_BP", float),
        "edge_min_points": ("EDGE_MIN_POINTS", float),
        "fee_buffer_cents": ("FEE_BUFFER_CENTS", float),
        "bankroll_dollars": ("BANKROLL_DOLLARS", float),
        "kelly_fraction": ("KELLY_FRACTION", float),
        "risk_cap_pct": ("RISK_CAP_PCT", float),
        "max_basis_gap_bp": ("MAX_BASIS_GAP_BP", float),
        "poll_seconds": ("POLL_SECONDS", float),
        "kalshi_poll_seconds": ("KALSHI_POLL_SECONDS", float),
    }
    for key, (env_name, caster) in overrides.items():
        if os.getenv(env_name) not in (None, ""):
            try:
                cfg[key] = caster(os.getenv(env_name))
            except Exception:
                pass
    statuses = os.getenv("TELEGRAM_ALERT_STATUSES")
    if statuses:
        cfg["telegram_alert_statuses"] = [x.strip() for x in statuses.split(",") if x.strip()]
    save_json(CONFIG_PATH, cfg)
    return cfg


def http_json(url, timeout=5):
    req = Request(url, headers={"User-Agent": f"BTC15M-Monitor/{APP_VERSION}"})
    with urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def telegram_token():
    return os.getenv("TELEGRAM_BOT_TOKEN", "").strip()


def telegram_chat_id(config):
    return os.getenv("TELEGRAM_CHAT_ID", "").strip() or str(config.get("telegram_chat_id", "")).strip()


def post_telegram(token, chat_id, text):
    if not token or not chat_id:
        return False, "Telegram no configurado"
    try:
        body = urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body, method="POST")
        with urlopen(req, timeout=6) as r:
            payload = json.loads(r.read().decode("utf-8"))
        if payload.get("ok"):
            return True, "OK"
        return False, payload.get("description", "Telegram error")
    except Exception as e:
        return False, str(e)


def discover_telegram_chat(token):
    if not token:
        return None, "Falta TELEGRAM_BOT_TOKEN"
    try:
        data = http_json(f"https://api.telegram.org/bot{token}/getUpdates?limit=50&timeout=0", timeout=6)
        results = data.get("result", []) if data.get("ok") else []
        # Prefer latest direct message; fall back to latest chat of any kind.
        candidates = []
        for u in results:
            msg = u.get("message") or u.get("edited_message") or u.get("channel_post")
            if not msg or not msg.get("chat"):
                continue
            chat = msg["chat"]
            candidates.append((u.get("update_id", 0), chat))
        if not candidates:
            return None, "No veo mensajes. Abre tu bot en Telegram, pulsa Start y envíale /start; luego prueba otra vez."
        candidates.sort(key=lambda x: x[0])
        chat = candidates[-1][1]
        return str(chat.get("id")), f"Detectado: {chat.get('first_name') or chat.get('title') or chat.get('username') or chat.get('id')}"
    except Exception as e:
        return None, str(e)


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def dollars(v):
    try:
        return float(v)
    except Exception:
        return None


def bp(a, b):
    if not a or not b:
        return None
    return (a / b - 1.0) * 10000.0


def extract_target(market):
    """Best-effort extraction of the Kalshi target/strike for assets at any price scale."""
    for key in ("floor_strike", "cap_strike", "strike", "strike_value"):
        v = market.get(key)
        try:
            x = float(v)
            if math.isfinite(x) and x > 0:
                return x
        except Exception:
            pass
    candidates = []
    for key in ("functional_strike", "custom_strike", "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary", "rules_secondary", "_event_json"):
        v = market.get(key)
        if v is not None:
            candidates.append(json.dumps(v) if isinstance(v, (dict, list)) else str(v))
    # Prefer explicitly labelled target/strike values.
    for text in candidates:
        m = re.search(r"(?:target(?:\s+price)?|strike|price\s+to\s+beat)[^0-9$]{0,40}\$?\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)", text, re.I)
        if m:
            try:
                x = float(m.group(1).replace(",", ""))
                if x > 0:
                    return x
            except Exception:
                pass
    # Then accept dollar-denominated values, including sub-dollar crypto.
    for text in candidates:
        vals = re.findall(r"\$\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)", text)
        for raw in vals:
            try:
                x = float(raw.replace(",", ""))
                if x > 0:
                    return x
            except Exception:
                pass
    return None


def get_price_fields(m):
    yes_bid = dollars(m.get("yes_bid_dollars"))
    yes_ask = dollars(m.get("yes_ask_dollars"))
    no_bid = dollars(m.get("no_bid_dollars"))
    no_ask = dollars(m.get("no_ask_dollars"))
    if yes_ask is None and no_bid is not None:
        yes_ask = 1.0 - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = 1.0 - yes_bid
    return yes_bid, yes_ask, no_bid, no_ask


def market_is_current(m, now):
    op = parse_iso(m.get("open_time"))
    cl = parse_iso(m.get("close_time"))
    if cl and cl <= now:
        return False
    if op and op > now:
        return False
    return True


def choose_kalshi_market(markets, now):
    current = [m for m in markets if market_is_current(m, now)]
    pool = current if current else markets
    def key(m):
        cl = parse_iso(m.get("close_time"))
        return cl.timestamp() if cl else 10**30
    return sorted(pool, key=key)[0] if pool else None


def fair_row(model, distance_bp):
    rows = sorted(model["thresholds"], key=lambda x: x["min_bp"])
    eligible = [r for r in rows if distance_bp >= r["min_bp"]]
    return eligible[-1] if eligible else None


def conservative_fair(row):
    return min(float(row["wilson_low_pct"]), float(row["worst_month_pct"]))


def quality_for_bp(x):
    if x >= 25: return "A+"
    if x >= 20: return "A"
    if x >= 15: return "B"
    if x >= 10: return "C"
    return "—"


def kelly_binary(p, c):
    if c >= 1 or p <= c:
        return 0.0
    return max(0.0, (p - c) / (1.0 - c))


@dataclass
class State:
    updated_at: str = ""
    mode: str = "live"
    status: str = "STARTING"
    status_detail: str = ""
    btc_price: float | None = None
    model_open: float | None = None
    current_minute: int | None = None
    last_completed_minute: int | None = None
    seconds_left: int | None = None
    side: str = "—"
    distance_bp: float | None = None
    last_step_bp: float | None = None
    pullback_confirmed: bool = False
    pullback_forming: bool = False
    never_crossed_open: bool = False
    quality: str = "—"
    fair_pct: float | None = None
    conservative_fair_pct: float | None = None
    sample_n: int | None = None
    market_ticker: str = ""
    market_close_time: str = ""
    kalshi_target: float | None = None
    target_gap_bp: float | None = None
    target_side: str = "—"
    yes_bid_cents: float | None = None
    yes_ask_cents: float | None = None
    no_bid_cents: float | None = None
    no_ask_cents: float | None = None
    selected_contract: str = "—"
    selected_ask_cents: float | None = None
    max_buy_cents: float | None = None
    edge_points: float | None = None
    full_kelly_pct: float | None = None
    fractional_kelly_pct: float | None = None
    risk_pct: float | None = None
    risk_dollars: float | None = None
    contracts_suggested: int | None = None
    basis_ok: bool | None = None
    api_error: str = ""


class Monitor:
    def __init__(self, demo=False):
        self.model = dict(MODEL_DATA)
        self.config = merge_config()
        self.demo = demo
        self.lock = threading.Lock()
        self.state = State(mode="demo" if demo else "live")
        self.stop_event = threading.Event()
        self.kalshi_cache = None
        self.kalshi_last = 0.0
        self.last_alert_key = ""
        self.demo_t0 = time.time()
        self.last_loop_at = time.time()
        self.ensure_journal()

    def ensure_journal(self):
        if JOURNAL_PATH.exists():
            return
        with open(JOURNAL_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "timestamp_utc","status","quality","market_ticker","minute","side","model_open","btc_price",
                "distance_bp","fair_pct","conservative_fair_pct","sample_n","selected_contract","ask_cents",
                "max_buy_cents","edge_points","kalshi_target","target_gap_bp","risk_dollars"
            ])

    def get_state(self):
        with self.lock:
            return asdict(self.state)

    def public_settings(self):
        out = dict(self.config)
        out["telegram_configured"] = bool(telegram_token() and telegram_chat_id(self.config))
        out["telegram_token_present"] = bool(telegram_token())
        out["telegram_chat_id_present"] = bool(telegram_chat_id(self.config))
        out["data_dir"] = str(DATA_DIR)
        out["app_version"] = APP_VERSION
        return out

    def update_settings(self, patch):
        allowed = {
            "min_distance_bp": float, "edge_min_points": float, "fee_buffer_cents": float,
            "bankroll_dollars": float, "kelly_fraction": float, "risk_cap_pct": float,
            "max_basis_gap_bp": float, "poll_seconds": float, "kalshi_poll_seconds": float
        }
        for k, caster in allowed.items():
            if k in patch:
                try:
                    self.config[k] = caster(patch[k])
                except Exception:
                    pass
        save_json(CONFIG_PATH, self.config)
        return self.public_settings()

    def save_telegram_chat_id(self, chat_id):
        self.config["telegram_chat_id"] = str(chat_id)
        save_json(CONFIG_PATH, self.config)

    def fetch_binance(self):
        url = BINANCE_BASE + "/api/v3/klines?" + urlencode({"symbol":"BTCUSDT","interval":"1m","limit":20})
        return http_json(url)

    def fetch_kalshi(self, now):
        if time.time() - self.kalshi_last < float(self.config.get("kalshi_poll_seconds", 2.0)) and self.kalshi_cache:
            return self.kalshi_cache
        url = KALSHI_BASE + "/markets?" + urlencode({"series_ticker":KALSHI_SERIES,"status":"open","limit":20})
        markets = http_json(url).get("markets", [])
        if not markets:
            url2 = KALSHI_BASE + "/markets?" + urlencode({"series_ticker":KALSHI_SERIES,"status":"unopened","limit":20})
            markets = http_json(url2).get("markets", [])
        self.kalshi_cache = choose_kalshi_market(markets, now)
        if self.kalshi_cache and extract_target(self.kalshi_cache) is None and self.kalshi_cache.get("event_ticker"):
            try:
                ev = http_json(KALSHI_BASE + "/events/" + self.kalshi_cache["event_ticker"]).get("event", {})
                self.kalshi_cache["_event_json"] = json.dumps(ev)
            except Exception:
                pass
        self.kalshi_last = time.time()
        return self.kalshi_cache

    def compute(self, klines, market):
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp()*1000)
        if not klines:
            raise ValueError("Binance returned no klines")
        window_start_ms = (now_ms // 900000) * 900000
        window_end_ms = window_start_ms + 900000
        current_minute = int((now_ms-window_start_ms)//60000)+1
        seconds_left = max(0, int((window_end_ms-now_ms)/1000))
        rows=[]
        for k in klines:
            try:
                ot=int(k[0]); op=float(k[1]); cl=float(k[4]); ct=int(k[6])
                if window_start_ms <= ot < window_end_ms:
                    rows.append({"open_time":ot,"open":op,"close":cl,"close_time":ct})
            except Exception:
                continue
        rows=sorted(rows,key=lambda r:r["open_time"])
        if not rows:
            raise ValueError("Current 15m window not present in Binance klines")
        model_open=rows[0]["open"]
        btc_price=rows[-1]["close"]
        completed=[r for r in rows if r["close_time"] < now_ms]
        last_completed_minute=len(completed)
        last_close=completed[-1]["close"] if completed else None
        prev_close=completed[-2]["close"] if len(completed)>=2 else None
        side="UP" if (last_close is not None and last_close>model_open) else ("DOWN" if last_close is not None and last_close<model_open else "—")
        dist=abs(bp(last_close,model_open)) if last_close is not None else None
        step=bp(last_close,prev_close) if last_close is not None and prev_close is not None else None
        pullback=(side=="UP" and step is not None and step<0) or (side=="DOWN" and step is not None and step>0)

        forming=False
        live_dist=abs(bp(btc_price,model_open))
        if completed:
            prev_ref=completed[-1]["close"]
            live_side="UP" if btc_price>model_open else ("DOWN" if btc_price<model_open else "—")
            live_step=bp(btc_price,prev_ref)
            forming=(live_side=="UP" and live_step<0) or (live_side=="DOWN" and live_step>0)

        min_bp=float(self.config.get("min_distance_bp",10))
        confirmed=bool(last_completed_minute in (7,8,9,10) and pullback and dist is not None and dist>=min_bp)
        forming=bool(current_minute in (7,8,9,10) and forming and live_dist>=min_bp)

        row=fair_row(self.model, dist if dist is not None else 0)
        fair=row["fair_pct"] if row else None
        cons=conservative_fair(row) if row else None
        sample_n=row["n"] if row else None
        quality=quality_for_bp(dist or 0)

        never_crossed=False
        if completed and side in ("UP","DOWN"):
            closes=[r["close"] for r in completed]
            never_crossed=all(x>=model_open for x in closes) if side=="UP" else all(x<=model_open for x in closes)

        ticker=""; target=None; close_time=""; yes_bid=yes_ask=no_bid=no_ask=None
        if market:
            ticker=market.get("ticker","")
            target=extract_target(market)
            close_time=market.get("close_time","") or ""
            yes_bid,yes_ask,no_bid,no_ask=get_price_fields(market)

        target_gap=bp(target,model_open) if target else None
        target_side=("UP" if btc_price>target else "DOWN") if target else "—"
        max_basis=float(self.config.get("max_basis_gap_bp",8.0))
        basis_ok=True
        if target_gap is not None:
            basis_ok=abs(target_gap)<=max_basis and (side=="—" or target_side==side)

        selected=side if side in ("UP","DOWN") else "—"
        selected_ask=yes_ask*100 if selected=="UP" and yes_ask is not None else (no_ask*100 if selected=="DOWN" and no_ask is not None else None)
        edge_min=float(self.config.get("edge_min_points",10.0))
        fee_buf=float(self.config.get("fee_buffer_cents",1.0))
        max_buy=(cons-edge_min-fee_buf) if cons is not None else None
        edge=(cons-selected_ask-fee_buf) if cons is not None and selected_ask is not None else None

        full_k=fractional=risk_pct=risk_dollars=contracts_suggested=None
        if cons is not None and selected_ask is not None:
            p=cons/100.0
            c=min(0.999,(selected_ask+fee_buf)/100.0)
            full_k=kelly_binary(p,c)*100
            fractional=full_k*float(self.config.get("kelly_fraction",0.125))
            risk_pct=min(fractional,float(self.config.get("risk_cap_pct",1.0)))
            risk_dollars=float(self.config.get("bankroll_dollars",1000.0))*risk_pct/100.0
            contracts_suggested=max(0,int(risk_dollars/c)) if c>0 else 0

        status="WAIT"; detail="Esperando M7–M10 + pullback confirmado."
        if current_minute<7:
            detail=f"Todavía temprano: M{current_minute}. La ventana estadística empieza en M7."
        elif current_minute>10 and not confirmed:
            status="EXPIRED"; detail="La ventana M7–M10 ya pasó para esta vela."
        elif forming and not confirmed:
            status="FORMING"; detail="Pullback formándose. Es aviso temprano; espera cierre del minuto para confirmación."
        if confirmed:
            status="CONFIRMED"; detail=f"Pullback confirmado en M{last_completed_minute}; quedan {dist:.1f} bp respecto al open Binance."
            if not basis_ok:
                status="BASIS WARNING"
                detail="Patrón Binance confirmado, pero target/side de Kalshi no está suficientemente alineado. No usar el fair como si fuera el mismo evento."
            elif selected_ask is None:
                detail += " Precio Kalshi no disponible."
            elif max_buy is not None and selected_ask<=max_buy:
                status="ENTRY ZONE"; detail=f"Señal confirmada + {selected} por debajo del máximo conservador."
            else:
                status="READY / PRICE HIGH"; detail=f"Señal confirmada, pero {selected} está caro para el edge mínimo configurado."

        return State(
            updated_at=now.isoformat(),mode="demo" if self.demo else "live",status=status,status_detail=detail,
            btc_price=btc_price,model_open=model_open,current_minute=current_minute,last_completed_minute=last_completed_minute,
            seconds_left=seconds_left,side=side,distance_bp=dist,last_step_bp=step,pullback_confirmed=confirmed,
            pullback_forming=forming,never_crossed_open=never_crossed,quality=quality,fair_pct=fair,
            conservative_fair_pct=cons,sample_n=sample_n,market_ticker=ticker,market_close_time=close_time,
            kalshi_target=target,target_gap_bp=target_gap,target_side=target_side,
            yes_bid_cents=yes_bid*100 if yes_bid is not None else None,yes_ask_cents=yes_ask*100 if yes_ask is not None else None,
            no_bid_cents=no_bid*100 if no_bid is not None else None,no_ask_cents=no_ask*100 if no_ask is not None else None,
            selected_contract=selected,selected_ask_cents=selected_ask,max_buy_cents=max_buy,edge_points=edge,
            full_kelly_pct=full_k,fractional_kelly_pct=fractional,risk_pct=risk_pct,risk_dollars=risk_dollars,
            contracts_suggested=contracts_suggested,basis_ok=basis_ok,api_error=""
        )

    def maybe_alert(self, s):
        statuses=set(self.config.get("telegram_alert_statuses", []))
        if s.status not in statuses:
            return
        key=f"{s.status}|{s.market_ticker}|{s.last_completed_minute}|{s.side}|{s.quality}"
        if key==self.last_alert_key:
            return
        self.last_alert_key=key
        if s.status in ("CONFIRMED","ENTRY ZONE","BASIS WARNING"):
            self.write_journal(s)
        if s.selected_ask_cents is not None and s.max_buy_cents is not None and s.edge_points is not None:
            text=(f"BTC15M {s.status} · {s.side} · {s.quality}\n"
                  f"M{s.last_completed_minute} · {s.distance_bp:.1f} bp · fair cons {s.conservative_fair_pct:.1f}%\n"
                  f"Ask {s.selected_ask_cents:.1f}c · max {s.max_buy_cents:.1f}c · edge {s.edge_points:.1f}pt\n"
                  f"Risk cap ${s.risk_dollars:.2f} · {s.contracts_suggested} contratos" if s.risk_dollars is not None else "")
        else:
            m=s.last_completed_minute if s.last_completed_minute else s.current_minute
            text=f"BTC15M {s.status} · {s.side} · {s.quality}\nM{m} · {s.distance_bp or 0:.1f} bp"
        post_telegram(telegram_token(), telegram_chat_id(self.config), text)

    def write_journal(self, s):
        with open(JOURNAL_PATH,"a",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow([s.updated_at,s.status,s.quality,s.market_ticker,s.last_completed_minute,s.side,s.model_open,s.btc_price,
                s.distance_bp,s.fair_pct,s.conservative_fair_pct,s.sample_n,s.selected_contract,s.selected_ask_cents,
                s.max_buy_cents,s.edge_points,s.kalshi_target,s.target_gap_bp,s.risk_dollars])

    def demo_state(self):
        phase=((time.time()-self.demo_t0)*10+360)%900
        minute=max(7,min(10,int(phase//60)+1))
        op=77698.94
        dmap={7:18.0,8:16.0,9:23.0,10:20.0}
        dist=dmap[minute]; side="UP"; price=op*(1+dist/10000); prev_dist=dist+4.0
        step=bp(price,op*(1+prev_dist/10000))
        row=fair_row(self.model,dist); cons=conservative_fair(row); fair=row["fair_pct"]
        ask=60.0; fee=float(self.config.get("fee_buffer_cents",1)); edge=cons-ask-fee; max_buy=cons-float(self.config.get("edge_min_points",10))-fee
        p=cons/100;c=(ask+fee)/100;fk=kelly_binary(p,c)*100;frac=fk*float(self.config.get("kelly_fraction",.125));rp=min(frac,float(self.config.get("risk_cap_pct",1)));rd=float(self.config.get("bankroll_dollars",1000))*rp/100;contracts=int(rd/c) if c else 0
        return State(updated_at=datetime.now(timezone.utc).isoformat(),mode="demo",status="ENTRY ZONE",status_detail="DEMO: señal artificial para comprobar dashboard/Telegram.",btc_price=price,model_open=op,current_minute=minute,last_completed_minute=minute,seconds_left=int(900-phase),side=side,distance_bp=dist,last_step_bp=step,pullback_confirmed=True,pullback_forming=False,never_crossed_open=True,quality=quality_for_bp(dist),fair_pct=fair,conservative_fair_pct=cons,sample_n=row["n"],market_ticker="KXBTC15M-DEMO",market_close_time="",kalshi_target=op,target_gap_bp=0.0,target_side="UP",yes_bid_cents=58.0,yes_ask_cents=60.0,no_bid_cents=39.0,no_ask_cents=41.0,selected_contract="UP",selected_ask_cents=ask,max_buy_cents=max_buy,edge_points=edge,full_kelly_pct=fk,fractional_kelly_pct=frac,risk_pct=rp,risk_dollars=rd,contracts_suggested=contracts,basis_ok=True,api_error="")

    def run(self):
        while not self.stop_event.is_set():
            self.last_loop_at=time.time()
            try:
                if self.demo:
                    s=self.demo_state()
                else:
                    now=datetime.now(timezone.utc);s=self.compute(self.fetch_binance(),self.fetch_kalshi(now))
                with self.lock:self.state=s
                self.maybe_alert(s)
            except Exception as e:
                with self.lock:
                    old=self.state;old.updated_at=datetime.now(timezone.utc).isoformat();old.api_error=str(e)
                    if old.status=="STARTING":old.status="API ERROR"
            time.sleep(max(0.5,float(self.config.get("poll_seconds",1.0))))



# Multiasset live forward-test. BTC remains untouched; these assets run in SHADOW mode.
# Thresholds are deliberately stricter than the first PASS threshold in the research file.
# Fair is fixed at the selected tier to avoid overfitting sparse higher-distance buckets.
ALT_LIVE_MODELS = {
    "ETH":  {"symbol":"ETHUSDT",  "series":"KXETH15M",  "min_bp":30.0, "fair_pct":96.74229203025014, "cons_pct":95.0,              "n":1719, "holdout_n":180, "holdout_pct":95.0},
    "SOL":  {"symbol":"SOLUSDT",  "series":"KXSOL15M",  "min_bp":30.0, "fair_pct":95.96622889305816, "cons_pct":94.11764705882352, "n":2132, "holdout_n":291, "holdout_pct":95.53264604810997},
    "XRP":  {"symbol":"XRPUSDT",  "series":"KXXRP15M",  "min_bp":40.0, "fair_pct":96.8222442899702,  "cons_pct":94.4649446494465,  "n":1007, "holdout_n":271, "holdout_pct":94.4649446494465},
    "DOGE": {"symbol":"DOGEUSDT", "series":"KXDOGE15M", "min_bp":35.0, "fair_pct":96.79043423536817, "cons_pct":94.67213114754098, "n":1589, "holdout_n":244, "holdout_pct":94.67213114754098},
    "BNB":  {"symbol":"BNBUSDT",  "series":"KXBNB15M",  "min_bp":20.0, "fair_pct":95.13888888888889, "cons_pct":93.29268292682927, "n":2160, "holdout_n":268, "holdout_pct":94.40298507462687},
}
ALT_JOURNAL_PATH = DATA_DIR / "multiasset_shadow_journal.csv"


class AltShadowMonitor:
    """Read-only multiasset forward test against live Kalshi prices.

    This engine never places orders. It journals every confirmed underlying setup and
    only sends Telegram when a live Kalshi price is cheap enough to qualify as a
    SHADOW ENTRY ZONE, or when the settlement-basis guard fails.
    """
    def __init__(self, primary_monitor):
        self.primary = primary_monitor
        self.models = ALT_LIVE_MODELS
        self.states = {a: {"asset":a, "status":"STARTING"} for a in self.models}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.last_loop_at = time.time()
        self.kalshi_cache = {}
        self.kalshi_last = {}
        self.last_journal_key = {}
        self.last_alert_key = {}
        self.first_signal = {}
        self.startup_sent = False
        self.ensure_journal()

    def ensure_journal(self):
        if ALT_JOURNAL_PATH.exists():
            return
        with open(ALT_JOURNAL_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "timestamp_utc","asset","window_start_utc","status","market_ticker","minute","side",
                "model_open","live_price","distance_bp","threshold_bp","fair_pct","conservative_fair_pct",
                "sample_n","holdout_n","holdout_pct","selected_contract","ask_cents","max_buy_cents",
                "edge_points","kalshi_target","target_gap_bp","basis_ok"
            ])

    def get_states(self):
        with self.lock:
            return json.loads(json.dumps(self.states))

    def fetch_binance(self, symbol):
        url = BINANCE_BASE + "/api/v3/klines?" + urlencode({"symbol":symbol,"interval":"1m","limit":20})
        return http_json(url)

    def fetch_kalshi(self, asset, series, now):
        last = self.kalshi_last.get(asset, 0.0)
        if time.time() - last < float(os.getenv("ALT_KALSHI_POLL_SECONDS", "3")) and self.kalshi_cache.get(asset):
            return self.kalshi_cache[asset]
        markets = http_json(KALSHI_BASE + "/markets?" + urlencode({"series_ticker":series,"status":"open","limit":20})).get("markets", [])
        if not markets:
            markets = http_json(KALSHI_BASE + "/markets?" + urlencode({"series_ticker":series,"status":"unopened","limit":20})).get("markets", [])
        market = choose_kalshi_market(markets, now)
        if market and extract_target(market) is None and market.get("event_ticker"):
            try:
                ev = http_json(KALSHI_BASE + "/events/" + market["event_ticker"]).get("event", {})
                market["_event_json"] = json.dumps(ev)
            except Exception:
                pass
        self.kalshi_cache[asset] = market
        self.kalshi_last[asset] = time.time()
        return market

    def compute(self, asset, model, klines, market):
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp()*1000)
        window_start_ms = (now_ms // 900000) * 900000
        window_end_ms = window_start_ms + 900000
        current_minute = int((now_ms-window_start_ms)//60000)+1
        seconds_left = max(0, int((window_end_ms-now_ms)/1000))
        rows=[]
        for k in klines or []:
            try:
                ot=int(k[0]); op=float(k[1]); cl=float(k[4]); ct=int(k[6])
                if window_start_ms <= ot < window_end_ms:
                    rows.append({"open_time":ot,"open":op,"close":cl,"close_time":ct})
            except Exception:
                continue
        rows.sort(key=lambda r:r["open_time"])
        if not rows:
            raise ValueError("current 15m window missing")
        model_open = rows[0]["open"]
        live_price = rows[-1]["close"]
        completed = [r for r in rows if r["close_time"] < now_ms]
        last_completed_minute = len(completed)
        last_close = completed[-1]["close"] if completed else None
        prev_close = completed[-2]["close"] if len(completed)>=2 else None
        side = "UP" if last_close is not None and last_close>model_open else ("DOWN" if last_close is not None and last_close<model_open else "—")
        dist = abs(bp(last_close, model_open)) if last_close is not None else None
        step = bp(last_close, prev_close) if last_close is not None and prev_close is not None else None
        pullback = (side=="UP" and step is not None and step<0) or (side=="DOWN" and step is not None and step>0)
        confirmed = bool(last_completed_minute in (7,8,9,10) and pullback and dist is not None and dist >= model["min_bp"])

        ticker=""; target=None; yes_bid=yes_ask=no_bid=no_ask=None
        if market:
            ticker=market.get("ticker","")
            target=extract_target(market)
            yes_bid,yes_ask,no_bid,no_ask=get_price_fields(market)

        target_gap = bp(target, model_open) if target else None
        target_side = ("UP" if live_price>target else "DOWN") if target else "—"
        max_basis=float(self.primary.config.get("max_basis_gap_bp",8.0))
        # If a target cannot be extracted, shadow logging continues but entry status is blocked.
        basis_ok = bool(target is not None and target_gap is not None and abs(target_gap)<=max_basis and (side=="—" or target_side==side))

        selected_ask = yes_ask*100 if side=="UP" and yes_ask is not None else (no_ask*100 if side=="DOWN" and no_ask is not None else None)
        edge_min=float(self.primary.config.get("edge_min_points",10.0))
        fee_buf=float(self.primary.config.get("fee_buffer_cents",1.0))
        max_buy=model["cons_pct"]-edge_min-fee_buf
        edge=(model["cons_pct"]-selected_ask-fee_buf) if selected_ask is not None else None

        status="WAIT"
        detail=f"Waiting for M7-M10 pullback >= {model['min_bp']:.0f} bp."
        if confirmed:
            status="SHADOW CONFIRMED"
            detail=f"Underlying setup confirmed at M{last_completed_minute}; live execution still under forward test."
            if not basis_ok:
                status="BASIS WARNING"
                detail="Underlying setup confirmed, but Kalshi target/basis is missing or not aligned."
            elif selected_ask is not None and selected_ask<=max_buy:
                status="SHADOW ENTRY ZONE"
                detail="Structure + live Kalshi ask satisfy the conservative shadow edge test."
            elif selected_ask is None:
                detail += " Kalshi ask unavailable."
            else:
                status="SHADOW PRICE HIGH"
                detail="Underlying setup confirmed, but the live contract is too expensive for the configured edge."

        return {
            "updated_at":now.isoformat(),"asset":asset,"status":status,"detail":detail,
            "window_start_utc":datetime.fromtimestamp(window_start_ms/1000,tz=timezone.utc).isoformat(),
            "current_minute":current_minute,"last_completed_minute":last_completed_minute,"seconds_left":seconds_left,
            "model_open":model_open,"live_price":live_price,"side":side,"distance_bp":dist,"last_step_bp":step,
            "threshold_bp":model["min_bp"],"fair_pct":model["fair_pct"],"conservative_fair_pct":model["cons_pct"],
            "sample_n":model["n"],"holdout_n":model["holdout_n"],"holdout_pct":model["holdout_pct"],
            "market_ticker":ticker,"kalshi_target":target,"target_gap_bp":target_gap,"target_side":target_side,
            "yes_bid_cents":yes_bid*100 if yes_bid is not None else None,"yes_ask_cents":yes_ask*100 if yes_ask is not None else None,
            "no_bid_cents":no_bid*100 if no_bid is not None else None,"no_ask_cents":no_ask*100 if no_ask is not None else None,
            "selected_contract":side if side in ("UP","DOWN") else "—","selected_ask_cents":selected_ask,
            "max_buy_cents":max_buy,"edge_points":edge,"basis_ok":basis_ok,"api_error":""
        }

    def journal_if_confirmed(self, s):
        if not s["status"].startswith("SHADOW") and s["status"]!="BASIS WARNING":
            return
        if s["status"]=="WAIT":
            return
        key=f"{s['asset']}|{s['window_start_utc']}"
        if self.last_journal_key.get(s["asset"])==key:
            return
        self.last_journal_key[s["asset"]]=key
        with open(ALT_JOURNAL_PATH,"a",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow([
                s["updated_at"],s["asset"],s["window_start_utc"],s["status"],s["market_ticker"],
                s["last_completed_minute"],s["side"],s["model_open"],s["live_price"],s["distance_bp"],
                s["threshold_bp"],s["fair_pct"],s["conservative_fair_pct"],s["sample_n"],s["holdout_n"],
                s["holdout_pct"],s["selected_contract"],s["selected_ask_cents"],s["max_buy_cents"],
                s["edge_points"],s["kalshi_target"],s["target_gap_bp"],s["basis_ok"]
            ])

    def maybe_alert(self, s):
        if s["status"] not in ("SHADOW ENTRY ZONE","BASIS WARNING"):
            return
        key=f"{s['asset']}|{s['status']}|{s['window_start_utc']}|{s['side']}"
        if self.last_alert_key.get(s["asset"])==key:
            return
        self.last_alert_key[s["asset"]]=key
        m=s.get("last_completed_minute") or s.get("current_minute")
        if s["status"]=="SHADOW ENTRY ZONE":
            text=(f"{s['asset']}15M SHADOW ENTRY ZONE · {s['side']}\\n"
                  f"M{m} · {s['distance_bp']:.1f} bp · fair cons {s['conservative_fair_pct']:.1f}%\\n"
                  f"Ask {s['selected_ask_cents']:.1f}c · max {s['max_buy_cents']:.1f}c · edge {s['edge_points']:.1f}pt\\n"
                  f"FORWARD TEST — no ejecutar todavía")
        else:
            gap="—" if s.get("target_gap_bp") is None else f"{s['target_gap_bp']:.1f}bp"
            text=(f"{s['asset']}15M BASIS WARNING · {s['side']}\\n"
                  f"M{m} · {s['distance_bp']:.1f} bp · gap {gap}\\n"
                  f"Setup guardado, pero no usar como entrada.")
        post_telegram(telegram_token(), telegram_chat_id(self.primary.config), text)

    def send_startup(self):
        if self.startup_sent:
            return
        self.startup_sent=True
        lines=["Multiasset SHADOW monitor ✅",
               "ETH ≥30bp · SOL ≥30bp · XRP ≥40bp · DOGE ≥35bp · BNB ≥20bp",
               "BTC sigue igual. HYPE excluido por ahora.",
               "Las alertas SHADOW son forward-test; no órdenes automáticas."]
        post_telegram(telegram_token(), telegram_chat_id(self.primary.config), "\\n".join(lines))

    def run(self):
        # Give the primary BTC monitor time to boot first.
        time.sleep(3)
        self.send_startup()
        while not self.stop_event.is_set():
            self.last_loop_at=time.time()
            for asset,model in self.models.items():
                if self.stop_event.is_set():
                    break
                try:
                    now=datetime.now(timezone.utc)
                    s=self.compute(asset,model,self.fetch_binance(model["symbol"]),self.fetch_kalshi(asset,model["series"],now))
                except Exception as e:
                    s={"updated_at":datetime.now(timezone.utc).isoformat(),"asset":asset,"status":"API ERROR","api_error":str(e)}
                # Match the historical protocol: only the first qualifying pullback per 15m candle counts.
                if s.get("status") in ("SHADOW CONFIRMED","SHADOW ENTRY ZONE","SHADOW PRICE HIGH","BASIS WARNING"):
                    prev=self.first_signal.get(asset)
                    if prev and prev.get("window")==s.get("window_start_utc") and prev.get("minute")!=s.get("last_completed_minute"):
                        s["status"]="SHADOW ALREADY COUNTED"
                        s["detail"]=f"First qualifying setup for this candle was already counted at M{prev.get('minute')}."
                    elif not prev or prev.get("window")!=s.get("window_start_utc"):
                        self.first_signal[asset]={"window":s.get("window_start_utc"),"minute":s.get("last_completed_minute"),"side":s.get("side")}
                with self.lock:
                    self.states[asset]=s
                self.journal_if_confirmed(s)
                self.maybe_alert(s)
                time.sleep(float(os.getenv("ALT_ASSET_STAGGER_SECONDS","0.15")))
            time.sleep(max(1.0,float(os.getenv("ALT_POLL_SECONDS","2.0"))))


ALT_MONITOR=None

RESEARCH_ASSETS = {
    "BTC":  {"symbol":"BTCUSDT",  "market":"spot",    "kalshi_series":"KXBTC15M"},
    "ETH":  {"symbol":"ETHUSDT",  "market":"spot",    "kalshi_series":"KXETH15M"},
    "SOL":  {"symbol":"SOLUSDT",  "market":"spot",    "kalshi_series":"KXSOL15M"},
    "XRP":  {"symbol":"XRPUSDT",  "market":"spot",    "kalshi_series":"KXXRP15M"},
    "DOGE": {"symbol":"DOGEUSDT", "market":"spot",    "kalshi_series":"KXDOGE15M"},
    "BNB":  {"symbol":"BNBUSDT",  "market":"spot",    "kalshi_series":"KXBNB15M"},
    "HYPE": {"symbol":"HYPEUSDT", "market":"futures", "kalshi_series":"KXHYPE15M"},
}
RESEARCH_THRESHOLDS = [5,10,15,20,25,30,35,40,50,60,80,100]
RESEARCH_PATH = DATA_DIR / "v3_multiasset_research.json"
RESEARCH_PARTIAL_PATH = DATA_DIR / "v3_multiasset_research.partial.json"
RESEARCH_VERSION = "2026-08-29-v1"


def wilson_low(wins, n, z=1.959963984540054):
    if not n:
        return None
    p=wins/n
    den=1+z*z/n
    center=(p+z*z/(2*n))/den
    half=z*math.sqrt((p*(1-p)+z*z/(4*n))/n)/den
    return max(0.0, center-half)*100


def pct(wins, n):
    return (wins/n*100.0) if n else None


class ResearchEngine:
    """Background, read-only validation of the BTC pullback family on other crypto assets.

    Methodology intentionally freezes the BTC structure: first qualifying pullback per 15m candle,
    M7-M10, and predeclared bp thresholds. This avoids optimizing a separate magic minute per asset.
    """
    def __init__(self, monitor):
        self.monitor=monitor
        self.lock=threading.Lock()
        self.running=False
        self.done=RESEARCH_PATH.exists()
        self.error=""
        self.asset=""
        self.progress_pct=100.0 if self.done else 0.0
        self.started_at=""
        self.finished_at=""
        self.message="Existing research result found." if self.done else "Not started"
        self.result=None
        if self.done:
            try:self.result=load_json(RESEARCH_PATH)
            except Exception:self.done=False

    def public_status(self):
        with self.lock:
            return {
                "running":self.running,"done":self.done,"error":self.error,"asset":self.asset,
                "progress_pct":round(self.progress_pct,1),"started_at":self.started_at,
                "finished_at":self.finished_at,"message":self.message,
                "result_present":RESEARCH_PATH.exists(),"version":RESEARCH_VERSION
            }

    def start(self, force=False):
        with self.lock:
            if self.running:return False,"Research already running"
            if RESEARCH_PATH.exists() and not force:
                self.done=True
                return False,"Research already completed. Use force=1 to rerun."
            self.running=True;self.done=False;self.error="";self.progress_pct=0.0
            self.started_at=datetime.now(timezone.utc).isoformat();self.finished_at="";self.message="Starting"
        threading.Thread(target=self._run,daemon=True).start()
        return True,"Research started"

    def _fetch_url(self, spec, start_ms, end_ms):
        if spec["market"]=="spot":
            return BINANCE_BASE + "/api/v3/klines?" + urlencode({
                "symbol":spec["symbol"],"interval":"1m","limit":1000,"startTime":start_ms,"endTime":end_ms
            })
        return "https://fapi.binance.com/fapi/v1/klines?" + urlencode({
            "symbol":spec["symbol"],"interval":"1m","limit":1000,"startTime":start_ms,"endTime":end_ms
        })

    def _empty_stats(self):
        return {str(t):{"n":0,"wins":0,"monthly":{},"side":{"UP":[0,0],"DOWN":[0,0]},"minute":{str(m):[0,0] for m in (7,8,9,10)}} for t in RESEARCH_THRESHOLDS}

    def _process_candle(self, rows, stats, coverage):
        if len(rows)!=15:return
        ots=[r[0] for r in rows]
        if any(ots[i+1]-ots[i]!=60000 for i in range(14)):
            coverage["incomplete_candles"]+=1;return
        op=rows[0][1];final=rows[-1][2]
        if not op:return
        coverage["candles_processed"]+=1
        month=datetime.fromtimestamp(rows[0][0]/1000,tz=timezone.utc).strftime("%Y-%m")
        # Exact BTC methodology: for each threshold, count only first qualifying state in M7-M10.
        for t in RESEARCH_THRESHOLDS:
            for m in (7,8,9,10):
                last=rows[m-1][2];prev=rows[m-2][2]
                side="UP" if last>op else ("DOWN" if last<op else "")
                if not side or not prev:continue
                step=(last/prev-1.0)*10000.0
                pullback=(side=="UP" and step<0) or (side=="DOWN" and step>0)
                dist=abs((last/op-1.0)*10000.0)
                if pullback and dist>=t:
                    win=(final>op) if side=="UP" else (final<op)
                    s=stats[str(t)];s["n"]+=1;s["wins"]+=int(win)
                    mo=s["monthly"].setdefault(month,[0,0]);mo[0]+=1;mo[1]+=int(win)
                    sd=s["side"][side];sd[0]+=1;sd[1]+=int(win)
                    mi=s["minute"][str(m)];mi[0]+=1;mi[1]+=int(win)
                    break

    def _research_asset(self, asset, spec, start_ms, end_ms, asset_index, total_assets):
        stats=self._empty_stats()
        coverage={"minutes_fetched":0,"candles_processed":0,"incomplete_candles":0,"first_open_ms":None,"last_open_ms":None,"api_calls":0}
        next_ms=start_ms;group_id=None;group=[];last_ot=None
        total_span=max(1,end_ms-start_ms)
        while next_ms<=end_ms:
            url=self._fetch_url(spec,next_ms,end_ms)
            data=http_json(url,timeout=12)
            coverage["api_calls"]+=1
            if not data:break
            advanced=False
            for k in data:
                try:ot=int(k[0]);op=float(k[1]);cl=float(k[4])
                except Exception:continue
                if ot<start_ms or ot>end_ms:continue
                if last_ot is not None and ot<=last_ot:continue
                last_ot=ot;advanced=True;coverage["minutes_fetched"]+=1
                if coverage["first_open_ms"] is None:coverage["first_open_ms"]=ot
                coverage["last_open_ms"]=ot
                gid=ot//900000
                if group_id is None:group_id=gid
                if gid!=group_id:
                    self._process_candle(group,stats,coverage)
                    group=[];group_id=gid
                group.append((ot,op,cl))
            if not advanced:break
            next_ms=last_ot+60000
            with self.lock:
                frac=min(1.0,max(0.0,(last_ot-start_ms)/total_span))
                self.progress_pct=((asset_index+frac)/total_assets)*100.0
                self.message=f"{asset}: {coverage['minutes_fetched']:,} minutes"
            if len(data)<1000:break
            time.sleep(float(os.getenv("ALT_RESEARCH_SLEEP","0.12")))
        if group:self._process_candle(group,stats,coverage)
        return self._finalize_asset(asset,spec,stats,coverage)

    def _finalize_asset(self, asset, spec, stats, coverage):
        rows=[]
        for t in RESEARCH_THRESHOLDS:
            s=stats[str(t)];n=s["n"];wins=s["wins"]
            monthly={m:{"n":v[0],"wins":v[1],"win_pct":pct(v[1],v[0])} for m,v in sorted(s["monthly"].items())}
            monthly_valid=[v["win_pct"] for v in monthly.values() if v["n"]>=25 and v["win_pct"] is not None]
            worst=min(monthly_valid) if monthly_valid else None
            aug=monthly.get("2026-08",{"n":0,"wins":0,"win_pct":None})
            val_months=[monthly.get("2026-06"),monthly.get("2026-07")]
            vn=sum(x["n"] for x in val_months if x);vw=sum(x["wins"] for x in val_months if x)
            val_pct=pct(vw,vn)
            wl=wilson_low(wins,n)
            cons_candidates=[x for x in (wl,worst,aug.get("win_pct") if aug.get("n",0)>=50 else None,val_pct if vn>=100 else None) if x is not None]
            cons=min(cons_candidates) if cons_candidates else None
            sides={k:{"n":v[0],"wins":v[1],"win_pct":pct(v[1],v[0])} for k,v in s["side"].items()}
            mins={k:{"n":v[0],"wins":v[1],"win_pct":pct(v[1],v[0])} for k,v in s["minute"].items()}
            rows.append({"min_bp":t,"n":n,"wins":wins,"win_pct":pct(wins,n),"wilson_low_pct":wl,
                         "worst_month_pct":worst,"validation_jun_jul_n":vn,"validation_jun_jul_pct":val_pct,
                         "holdout_aug_n":aug.get("n",0),"holdout_aug_pct":aug.get("win_pct"),
                         "conservative_pct":cons,"monthly":monthly,"side":sides,"first_signal_minute":mins})
        candidates=[]
        for r in rows:
            if (r["n"]>=500 and r["holdout_aug_n"]>=100 and r["wilson_low_pct"] is not None and r["wilson_low_pct"]>=88
                and r["holdout_aug_pct"] is not None and r["holdout_aug_pct"]>=88
                and r["worst_month_pct"] is not None and r["worst_month_pct"]>=85):
                candidates.append(r)
        selected=candidates[0] if candidates else None
        status="PASS" if selected else "SHADOW"
        if spec["market"]=="futures":
            status="SHADOW_FUTURES_PROXY" if selected else "SHADOW_FUTURES_PROXY"
        first_iso=datetime.fromtimestamp(coverage["first_open_ms"]/1000,tz=timezone.utc).isoformat() if coverage["first_open_ms"] else None
        last_iso=datetime.fromtimestamp(coverage["last_open_ms"]/1000,tz=timezone.utc).isoformat() if coverage["last_open_ms"] else None
        # If less than ~60 days are available, never promote automatically.
        days=((coverage["last_open_ms"]-coverage["first_open_ms"])/86400000.0) if coverage["first_open_ms"] and coverage["last_open_ms"] else 0
        if days<60:status="INSUFFICIENT_HISTORY";selected=None
        return {"asset":asset,"symbol":spec["symbol"],"market_source":spec["market"],"kalshi_series":spec["kalshi_series"],
                "status":status,"recommended_threshold":selected,"coverage":{**coverage,"first_open":first_iso,"last_open":last_iso,"days":days},
                "thresholds":rows}

    def _run(self):
        try:
            start=datetime(2026,3,1,tzinfo=timezone.utc);end=datetime(2026,8,29,tzinfo=timezone.utc)
            start_ms=int(start.timestamp()*1000);end_ms=int(end.timestamp()*1000)-1
            result={"research_version":RESEARCH_VERSION,"generated_at":datetime.now(timezone.utc).isoformat(),
                    "method":"First qualifying pullback per 15m candle; M7-M10 fixed; direction defined by completed 1m close vs 15m open.",
                    "training_and_validation":"Underlying data Mar 1-Aug 28 2026 UTC. August is treated as a final holdout when coverage is sufficient.",
                    "warning":"Underlying-pattern validation only. Kalshi execution edge must be forward-tested using actual asks/spreads and settlement-basis checks.",
                    "assets":{}}
            if RESEARCH_PARTIAL_PATH.exists():
                try:
                    old=load_json(RESEARCH_PARTIAL_PATH)
                    if old.get("research_version")==RESEARCH_VERSION and isinstance(old.get("assets"),dict):result["assets"].update(old["assets"])
                except Exception:pass
            items=list(RESEARCH_ASSETS.items())
            for i,(asset,spec) in enumerate(items):
                if asset in result["assets"] and result["assets"][asset].get("status") not in ("ERROR",):
                    with self.lock:self.progress_pct=((i+1)/len(items))*100.0;self.message=f"{asset}: cached"
                    continue
                with self.lock:self.asset=asset;self.message=f"Starting {asset}"
                try:res=self._research_asset(asset,spec,start_ms,end_ms,i,len(items))
                except Exception as e:
                    res={"asset":asset,"symbol":spec["symbol"],"market_source":spec["market"],"kalshi_series":spec["kalshi_series"],"status":"ERROR","error":str(e)}
                result["assets"][asset]=res
                save_json(RESEARCH_PARTIAL_PATH,result)
            save_json(RESEARCH_PATH,result)
            try:RESEARCH_PARTIAL_PATH.unlink(missing_ok=True)
            except Exception:pass
            self.result=result
            lines=["Multiasset 15m research finished"]
            for a,res in result["assets"].items():
                sel=res.get("recommended_threshold")
                if sel:
                    lines.append(f"{a}: PASS >= {sel['min_bp']}bp | holdout {sel['holdout_aug_pct']:.1f}% (n={sel['holdout_aug_n']})")
                else:
                    lines.append(f"{a}: {res.get('status','SHADOW')}")
            post_telegram(telegram_token(),telegram_chat_id(self.monitor.config),"\n".join(lines)[:3900])
            with self.lock:
                self.running=False;self.done=True;self.progress_pct=100.0;self.asset="";self.finished_at=datetime.now(timezone.utc).isoformat();self.message="Completed"
        except Exception as e:
            with self.lock:
                self.running=False;self.done=False;self.error=str(e);self.finished_at=datetime.now(timezone.utc).isoformat();self.message="Failed"

MONITOR=None
RESEARCH=None


def check_basic_auth(header):
    password=os.getenv("DASHBOARD_PASSWORD", "")
    if not password:
        return True
    if not header or not header.startswith("Basic "):
        return False
    try:
        raw=base64.b64decode(header.split(" ",1)[1]).decode("utf-8")
        user,pw=raw.split(":",1)
        return user==os.getenv("DASHBOARD_USER","admin") and pw==password
    except Exception:
        return False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"HTTP {self.address_string()} - {fmt%args}")

    def _send(self, code, body, content_type="application/json; charset=utf-8", extra_headers=None):
        data=body.encode("utf-8") if isinstance(body,str) else body
        self.send_response(code)
        self.send_header("Content-Type",content_type)
        self.send_header("Content-Length",str(len(data)))
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("X-Frame-Options","DENY")
        if extra_headers:
            for k,v in extra_headers.items():self.send_header(k,v)
        self.end_headers();self.wfile.write(data)

    def _authorized(self):
        if self.path.startswith("/health"):
            return True
        if check_basic_auth(self.headers.get("Authorization")):
            return True
        self._send(401,"Authentication required","text/plain; charset=utf-8",{"WWW-Authenticate":"Basic realm=BTC15M"})
        return False

    def do_GET(self):
        if not self._authorized():return
        path=urlparse(self.path).path
        if path=="/health":
            stale=time.time()-MONITOR.last_loop_at
            alt_stale=(time.time()-ALT_MONITOR.last_loop_at) if ALT_MONITOR else None
            self._send(200,json.dumps({"ok":True,"version":APP_VERSION,"monitor_loop_age_seconds":round(stale,1),
                                       "multiasset_loop_age_seconds":round(alt_stale,1) if alt_stale is not None else None,
                                       "multiasset_mode":"shadow" if ALT_MONITOR else "off"}))
        elif path=="/":
            self._send(200,DASHBOARD_HTML,"text/html; charset=utf-8")
        elif path=="/api/state":
            self._send(200,json.dumps({"state":MONITOR.get_state(),"settings":MONITOR.public_settings(),"model":MONITOR.model}))
        elif path=="/api/journal":
            if JOURNAL_PATH.exists():self._send(200,JOURNAL_PATH.read_bytes(),"text/csv; charset=utf-8")
            else:self._send(404,b"","text/plain")
        elif path=="/api/telegram/status":
            self._send(200,json.dumps({"token_present":bool(telegram_token()),"chat_id_present":bool(telegram_chat_id(MONITOR.config)),"configured":bool(telegram_token() and telegram_chat_id(MONITOR.config))}))
        elif path=="/api/multiasset":
            self._send(200,json.dumps({"mode":"shadow","version":APP_VERSION,"states":ALT_MONITOR.get_states() if ALT_MONITOR else {}}))
        elif path=="/api/multiasset/journal":
            if ALT_JOURNAL_PATH.exists():self._send(200,ALT_JOURNAL_PATH.read_bytes(),"text/csv; charset=utf-8",{"Content-Disposition":"attachment; filename=multiasset_shadow_journal.csv"})
            else:self._send(404,b"","text/plain")
        elif path=="/api/research/status":
            self._send(200,json.dumps(RESEARCH.public_status()))
        elif path=="/api/research/file":
            if RESEARCH_PATH.exists():self._send(200,RESEARCH_PATH.read_bytes(),"application/json; charset=utf-8",{"Content-Disposition":"attachment; filename=v3_multiasset_research.json"})
            else:self._send(404,json.dumps({"error":"research result not ready"}))
        else:self._send(404,json.dumps({"error":"not found"}))

    def do_POST(self):
        if not self._authorized():return
        path=urlparse(self.path).path
        try:
            if path=="/api/settings":
                n=int(self.headers.get("Content-Length","0"));patch=json.loads(self.rfile.read(n).decode("utf-8")) if n else {}
                self._send(200,json.dumps({"ok":True,"settings":MONITOR.update_settings(patch)}));return
            if path=="/api/telegram/discover":
                cid,msg=discover_telegram_chat(telegram_token())
                if not cid:self._send(400,json.dumps({"ok":False,"error":msg}));return
                MONITOR.save_telegram_chat_id(cid)
                self._send(200,json.dumps({"ok":True,"chat_id":cid,"message":msg}));return
            if path=="/api/telegram/test":
                ok,msg=post_telegram(telegram_token(),telegram_chat_id(MONITOR.config),"BTC15M Monitor ✅\nTelegram conectado correctamente.")
                self._send(200 if ok else 400,json.dumps({"ok":ok,"message":msg}));return
            if path=="/api/research/start":
                ok,msg=RESEARCH.start(force=False)
                self._send(200,json.dumps({"ok":ok,"message":msg,"status":RESEARCH.public_status()}));return
            self._send(404,json.dumps({"error":"not found"}))
        except Exception as e:
            self._send(400,json.dumps({"ok":False,"error":str(e)}))


def main():
    global MONITOR,RESEARCH,ALT_MONITOR
    demo="--demo" in sys.argv or os.getenv("DEMO_MODE","").lower() in ("1","true","yes")
    port=int(os.getenv("PORT","8765"))
    host=os.getenv("HOST","0.0.0.0" if os.getenv("RAILWAY_ENVIRONMENT") else "127.0.0.1")
    for a in sys.argv[1:]:
        if a.startswith("--port="):port=int(a.split("=",1)[1])
        if a.startswith("--host="):host=a.split("=",1)[1]
    MONITOR=Monitor(demo=demo)
    threading.Thread(target=MONITOR.run,daemon=True).start()
    if os.getenv("ALT_SHADOW_ENABLED","1").lower() in ("1","true","yes") and not demo:
        ALT_MONITOR=AltShadowMonitor(MONITOR)
        threading.Thread(target=ALT_MONITOR.run,daemon=True).start()
    RESEARCH=ResearchEngine(MONITOR)
    if os.getenv("ALT_RESEARCH_AUTO","1").lower() in ("1","true","yes") and not RESEARCH.done:
        RESEARCH.start(force=False)
    server=ThreadingHTTPServer((host,port),Handler)
    local_url=f"http://127.0.0.1:{port}"
    print(f"BTC 15M Monitor {APP_VERSION} | {'DEMO' if demo else 'LIVE'} | {host}:{port} | data={DATA_DIR}",flush=True)
    if host in ("127.0.0.1","localhost") and "--no-browser" not in sys.argv:
        threading.Timer(1.0,lambda:webbrowser.open(local_url)).start()
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:
        MONITOR.stop_event.set()
        if ALT_MONITOR:ALT_MONITOR.stop_event.set()
        server.server_close()

if __name__=="__main__":main()
