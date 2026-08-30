#!/usr/bin/env python3
import base64
import csv
import hashlib
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


# v2.4 augments the original BTC dashboard instead of replacing it. This keeps the proven BTC UI intact.
_V23_STYLE = r"""
<style>
.v23-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.v23-card{background:#11151d;border:1px solid #283141;border-radius:16px;padding:15px;min-height:148px}.v23-card.entry{border-color:#46d369;box-shadow:0 0 0 1px #46d369 inset}.v23-card.warn2{border-color:#ffbe44}.v23-title{display:flex;justify-content:space-between;gap:8px;align-items:center}.v23-asset{font-size:21px;font-weight:900}.v23-status{font-size:11px;color:#98a2b3;text-align:right}.v23-main{font-size:23px;font-weight:800;margin-top:11px}.v23-meta{font-size:12px;color:#98a2b3;line-height:1.55;margin-top:7px}.v23-table{width:100%;border-collapse:collapse;font-size:12px}.v23-table th,.v23-table td{padding:8px 6px;border-bottom:1px solid #283141;text-align:right}.v23-table th:first-child,.v23-table td:first-child{text-align:left}.v23-kpi{font-size:25px;font-weight:900;margin-top:6px}.v23-rank{font-size:14px;line-height:1.6}.v23-good{color:#46d369}.v23-bad{color:#ff625d}.v23-warn{color:#ffbe44}
@media(max-width:800px){.v23-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:500px){.v23-grid{grid-template-columns:1fr 1fr}.v23-asset{font-size:18px}.v23-main{font-size:19px}.v23-card{padding:12px;min-height:142px}}
</style>
"""
_V23_HTML = r"""
<div class="section">Multiasset · Execution Lab</div>
<div class="v23-grid" id="v23Assets"></div>
<div class="section">Live research</div>
<div class="grid">
 <div class="card"><div class="label">Setups resueltos</div><div class="v23-kpi" id="v23Resolved">0</div><div class="small">primer setup por vela</div></div>
 <div class="card"><div class="label">Shadow entries</div><div class="v23-kpi" id="v23Entries">0</div><div class="small">precio + estructura + basis</div></div>
 <div class="card"><div class="label">Hit rate live</div><div class="v23-kpi" id="v23Hit">—</div><div class="small">solo entries ya finalizadas</div></div>
 <div class="card"><div class="label">PnL simulado</div><div class="v23-kpi" id="v23Pnl">—</div><div class="small">1 contrato/entry · neto con fee model</div></div>
</div>
<div class="section">Portfolio shadow · riesgo correlacionado</div>
<div class="grid">
 <div class="card"><div class="label">Portfolio entries</div><div class="v23-kpi" id="v24PortfolioEntries">0</div><div class="small">máx. 1 crypto por ventana 15m</div></div>
 <div class="card"><div class="label">Portfolio hit</div><div class="v23-kpi" id="v24PortfolioHit">—</div><div class="small">política no-lookahead</div></div>
 <div class="card"><div class="label">Portfolio net PnL</div><div class="v23-kpi" id="v24PortfolioPnl">—</div><div class="small">1 contrato por ventana seleccionada</div></div>
 <div class="card"><div class="label">Max drawdown</div><div class="v23-kpi" id="v24MaxDD">—</div><div class="small">shadow all-entries</div></div>
</div>
<div class="section">Ranking activo</div>
<div class="card"><div class="v23-rank" id="v23Ranking">No hay ENTRY ZONE activa.</div></div>

<div class="section">Early Entry Lab · M6–M7 experimental</div>
<div class="grid">
 <div class="card"><div class="label">Early captures</div><div class="v23-kpi" id="earlyRecorded">0</div><div class="small">primer precursor M6/M7 por vela</div></div>
 <div class="card"><div class="label">Early hit</div><div class="v23-kpi" id="earlyHit">—</div><div class="small">resultado Kalshi · exploratorio</div></div>
 <div class="card"><div class="label">Ask promedio</div><div class="v23-kpi" id="earlyAsk">—</div><div class="small">precio antes del main setup</div></div>
 <div class="card"><div class="label">Net PnL 1 contrato</div><div class="v23-kpi" id="earlyPnl">—</div><div class="small">todos los early captures con basis OK</div></div>
</div>
<div class="grid" style="margin-top:12px">
 <div class="card"><div class="label">Main follow-through</div><div class="v23-kpi" id="earlyFollow">—</div><div class="small">% que luego llegó al setup estricto</div></div>
 <div class="card"><div class="label">M6</div><div class="v23-kpi" id="earlyM6">—</div><div class="small">n / hit / avg ask</div></div>
 <div class="card"><div class="label">M7</div><div class="v23-kpi" id="earlyM7">—</div><div class="small">n / hit / avg ask</div></div>
 <div class="card"><div class="label">Objetivo</div><div class="v23-kpi">≤90¢</div><div class="small">Telegram avisa solo captures baratos; NO ENTRY</div></div>
</div>
<div class="grid" style="margin-top:12px">
 <div class="card"><div class="label">Data quality VALID</div><div class="v23-kpi" id="earlyValid">0</div><div class="small">capturas v2.6 con fuentes sanas</div></div>
 <div class="card"><div class="label">Captures rechazados</div><div class="v23-kpi" id="earlyRejected">0</div><div class="small">fail-closed · razón en /health</div></div>
 <div class="card"><div class="label">Wilson 95% low</div><div class="v23-kpi" id="earlyWilson">—</div><div class="small">entries resueltas; no es fair</div></div>
 <div class="card"><div class="label">Quality mix</div><div class="v23-kpi" id="earlyQualityMix">—</div><div class="small">V valid · D degraded · L legacy</div></div>
</div>
<div class="card" style="overflow:auto;margin-top:12px"><table class="v23-table"><thead><tr><th>Activo</th><th>Early th.</th><th>Captures</th><th>Hit</th><th>Avg ask</th><th>Net PnL</th><th>Follow-through</th></tr></thead><tbody id="earlyMetrics"></tbody></table></div>
<div style="display:flex;gap:10px;flex-wrap:wrap"><button class="button" onclick="location.href='/api/early/ledger.csv'">Descargar Early Lab CSV</button></div>
<div class="section">Calibración por activo</div>
<div class="card" style="overflow:auto"><table class="v23-table"><thead><tr><th>Activo</th><th>Threshold</th><th>Fair cons.</th><th>Resueltos</th><th>Hit live</th><th>Δ fair</th><th>Entries</th><th>Net PnL</th><th>Wilson</th><th>Bench Δ</th><th>Gate</th></tr></thead><tbody id="v23Metrics"></tbody></table></div>
<div style="display:flex;gap:10px;flex-wrap:wrap"><button class="button" onclick="location.href='/api/multiasset/ledger.csv'">Descargar ledger v2.4</button><button class="button" onclick="location.href='/api/multiasset/journal'">Journal raw v2.2+</button></div>
"""
_V23_JS = r"""
<script>
const v23fmt=(x,d=1)=>x==null?'—':Number(x).toFixed(d);
async function refreshV23(){
 try{
  const [sr,mr]=await Promise.all([fetch('/api/multiasset',{cache:'no-store'}),fetch('/api/multiasset/metrics',{cache:'no-store'})]);
  const sj=await sr.json(),mj=await mr.json();
  const states=sj.states||{}, primary=sj.btc||{};
  const all={BTC:{asset:'BTC',status:primary.status||'—',current_minute:primary.current_minute,distance_bp:primary.distance_bp,threshold_bp:null,side:primary.side,selected_ask_cents:primary.selected_ask_cents,edge_points:primary.edge_points,basis_ok:primary.basis_ok},...states};
  const order=['BTC','ETH','SOL','XRP','DOGE','BNB'];
  document.getElementById('v23Assets').innerHTML=order.map(a=>{let s=all[a]||{};let cl=(s.status||'').includes('ENTRY ZONE')?'entry':((s.status||'').includes('WARNING')?'warn2':'');let th=s.threshold_bp==null?'BTC dynamic':('≥'+v23fmt(s.threshold_bp,0)+'bp');return `<div class="v23-card ${cl}"><div class="v23-title"><div class="v23-asset">${a}</div><div class="v23-status">${s.status||'—'}</div></div><div class="v23-main">${s.side||'—'} · M${s.current_minute??'—'}</div><div class="v23-meta">dist ${v23fmt(s.distance_bp,1)}bp · ${th}<br>ask ${v23fmt(s.selected_ask_cents,1)}¢ · edge net ${v23fmt(s.edge_points,1)}pt<br>spread ${v23fmt(s.spread_cents,1)}¢ · fee est ${v23fmt(s.estimated_taker_fee_cents,2)}¢<br>basis ${s.basis_ok===true?'OK':(s.basis_ok===false?'BLOCK':'—')}</div></div>`}).join('');
  const t=mj.totals||{};
  document.getElementById('v23Resolved').textContent=t.resolved_setups??0;
  document.getElementById('v23Entries').textContent=t.resolved_entries??0;
  document.getElementById('v23Hit').textContent=t.entry_hit_pct==null?'—':v23fmt(t.entry_hit_pct,1)+'%';
  let pnl=t.entry_net_pnl_dollars;document.getElementById('v23Pnl').textContent=pnl==null?'—':((pnl>=0?'+':'')+'$'+v23fmt(pnl,2));
  document.getElementById('v23Pnl').className='v23-kpi '+(pnl>0?'v23-good':pnl<0?'v23-bad':'');
  document.getElementById('v24PortfolioEntries').textContent=t.portfolio_entries??0;
  document.getElementById('v24PortfolioHit').textContent=t.portfolio_hit_pct==null?'—':v23fmt(t.portfolio_hit_pct,1)+'%';
  let pp=t.portfolio_net_pnl_dollars;document.getElementById('v24PortfolioPnl').textContent=pp==null?'—':((pp>=0?'+':'')+'$'+v23fmt(pp,2));
  document.getElementById('v24PortfolioPnl').className='v23-kpi '+(pp>0?'v23-good':pp<0?'v23-bad':'');
  document.getElementById('v24MaxDD').textContent='$'+v23fmt(t.entry_max_drawdown_dollars||0,2);
  let ranks=mj.active_ranking||[];document.getElementById('v23Ranking').innerHTML=ranks.length?ranks.map((r,i)=>`${i+1}. <b>${r.asset} ${r.side}</b> · edge ${v23fmt(r.edge_points,1)}pt · ask ${v23fmt(r.ask_cents,1)}¢ · M${r.minute}`).join('<br>'):'No hay ENTRY ZONE activa.';
  let rows=(mj.by_asset||[]).map(r=>`<tr><td><b>${r.asset}</b></td><td>≥${v23fmt(r.threshold_bp,0)}bp</td><td>${v23fmt(r.conservative_fair_pct,1)}%</td><td>${r.resolved_setups}</td><td>${r.calibration_hit_pct==null?'—':v23fmt(r.calibration_hit_pct,1)+'%'}</td><td>${r.calibration_delta_points==null?'—':(r.calibration_delta_points>=0?'+':'')+v23fmt(r.calibration_delta_points,1)}</td><td>${r.resolved_entries}</td><td>${r.entry_net_pnl_dollars==null?'—':(r.entry_net_pnl_dollars>=0?'+':'')+'$'+v23fmt(r.entry_net_pnl_dollars,2)}</td><td>${r.entry_wilson_low_pct==null?'—':v23fmt(r.entry_wilson_low_pct,1)+'%'}</td><td>${r.benchmark_disagreement_pct==null?'—':v23fmt(r.benchmark_disagreement_pct,1)+'%'}</td><td>${r.promotion_gate||'COLLECTING'}</td></tr>`).join('');document.getElementById('v23Metrics').innerHTML=rows;
 }catch(e){console.log('v2.4 dashboard',e)}
}
setInterval(refreshV23,2000);refreshV23();
</script>
"""
DASHBOARD_HTML = DASHBOARD_HTML.replace("</style>", "</style>" + _V23_STYLE, 1)
DASHBOARD_HTML = DASHBOARD_HTML.replace('<div class="footer">', _V23_HTML + '<div class="footer">', 1)
DASHBOARD_HTML = DASHBOARD_HTML.replace("</body></html>", _V23_JS + "</body></html>", 1)
DASHBOARD_HTML = DASHBOARD_HTML.replace("</body></html>", r'''
<script>
async function refreshEarly(){
 try{
  const r=await fetch('/api/early/metrics',{cache:'no-store'}),j=await r.json(),t=j.totals||{};
  const f=(x,d=1)=>x==null?'—':Number(x).toFixed(d);
  document.getElementById('earlyRecorded').textContent=t.recorded??0;
  document.getElementById('earlyHit').textContent=t.hit_pct==null?'—':f(t.hit_pct,1)+'%';
  document.getElementById('earlyAsk').textContent=t.avg_ask_cents==null?'—':f(t.avg_ask_cents,1)+'¢';
  let p=t.net_pnl_dollars;document.getElementById('earlyPnl').textContent=p==null?'—':((p>=0?'+':'')+'$'+f(p,2));
  document.getElementById('earlyPnl').className='v23-kpi '+(p>0?'v23-good':p<0?'v23-bad':'');
  document.getElementById('earlyFollow').textContent=t.main_followthrough_pct==null?'—':f(t.main_followthrough_pct,1)+'%';
  const op=j.operational||{},qc=t.quality_counts||{};
  document.getElementById('earlyValid').textContent=qc.VALID||0;
  document.getElementById('earlyRejected').textContent=op.captures_rejected||0;
  document.getElementById('earlyWilson').textContent=t.wilson_low_pct==null?'—':f(t.wilson_low_pct,1)+'%';
  document.getElementById('earlyQualityMix').textContent=`${qc.VALID||0}V / ${qc.DEGRADED_OBSERVATION||0}D / ${qc.LEGACY_UNASSESSED||0}L`;
  let mm={};(j.by_minute||[]).forEach(x=>mm[x.minute]=x);for(const m of [6,7]){let x=mm[m]||{};document.getElementById('earlyM'+m).textContent=`${x.recorded||0} / ${x.hit_pct==null?'—':f(x.hit_pct,0)+'%'} / ${x.avg_ask_cents==null?'—':f(x.avg_ask_cents,0)+'¢'}`}
  document.getElementById('earlyMetrics').innerHTML=(j.by_asset||[]).map(x=>`<tr><td><b>${x.asset}</b></td><td>≥${f(x.early_threshold_bp,0)}bp</td><td>${x.recorded}</td><td>${x.hit_pct==null?'—':f(x.hit_pct,1)+'%'}</td><td>${x.avg_ask_cents==null?'—':f(x.avg_ask_cents,1)+'¢'}</td><td>${(x.net_pnl_dollars>=0?'+':'')+'$'+f(x.net_pnl_dollars,2)}</td><td>${x.main_followthrough_pct==null?'—':f(x.main_followthrough_pct,1)+'%'}</td></tr>`).join('');
 }catch(e){console.log('early lab dashboard',e)}
}
setInterval(refreshEarly,2500);refreshEarly();
</script>
''' + "</body></html>", 1)

# Railway exposes RAILWAY_VOLUME_MOUNT_PATH automatically when a volume exists.
# Local fallback keeps data inside ./data.
DATA_DIR = Path(os.getenv("DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or (ROOT / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = DATA_DIR / "config.json"
JOURNAL_PATH = DATA_DIR / "signal_journal.csv"

BINANCE_BASE = "https://data-api.binance.vision"
KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_SERIES = os.getenv("KALSHI_SERIES", "KXBTC15M")
APP_VERSION = "2.6-railway-hardening"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


_SAVE_JSON_LOCK = threading.RLock()


def save_json(path, data):
    """Crash-safer JSON persistence shared by every ledger/config writer.

    A single process-wide lock prevents two threads from racing on the same
    temporary path. Data is flushed and fsynced before the atomic replace.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _SAVE_JSON_LOCK:
        tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            # Persist the directory entry as well when the platform supports it.
            try:
                dfd = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except Exception:
                pass
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


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

def kalshi_trade_fee_cents(price_cents, count=1.0, rate=0.07):
    """Estimated taker trade fee using Kalshi's general quadratic fee model.

    Returns cents. The trade-fee component is rounded up to the nearest centicent
    ($0.0001). Exchange balance-rounding/rebates can make the posted net fee differ
    slightly, so the configured fee_buffer_cents remains a conservative floor.
    """
    try:
        p=max(0.0,min(1.0,float(price_cents)/100.0)); c=max(0.0,float(count))
        raw=float(rate)*c*p*(1.0-p)  # dollars
        return math.ceil(raw*10000.0-1e-12)/100.0  # cents
    except Exception:
        return None


def effective_fee_cents(price_cents, config, count=1.0):
    model=kalshi_trade_fee_cents(price_cents,count=count)
    floor=float(config.get("fee_buffer_cents",1.0))
    return max(floor, model if model is not None else floor)


def max_buy_with_fee(cons_pct, edge_min_points, config):
    if cons_pct is None:return None
    # 15m crypto typically trades on cent ticks in the middle of the range. This
    # display value is conservative; actual entry qualification uses the live ask.
    best=None
    for ask in range(1,100):
        fee=effective_fee_cents(ask,config,1.0)
        if float(cons_pct)-ask-fee >= float(edge_min_points):best=float(ask)
    return best


def parse_orderbook_asks(payload, selected_side):
    """Convert Kalshi bid-only binary orderbook into asks for the side we buy."""
    ob=(payload or {}).get("orderbook_fp") or (payload or {}).get("orderbook") or {}
    # YES ask = 1 - NO bid; NO ask = 1 - YES bid.
    src=ob.get("no_dollars") if selected_side=="UP" else ob.get("yes_dollars")
    levels=[]
    for row in src or []:
        try:
            bid=float(row[0]); qty=float(row[1]); ask=(1.0-bid)*100.0
            if qty>0 and 0<=ask<=100:levels.append((ask,qty))
        except Exception:pass
    levels.sort(key=lambda x:x[0])
    return levels


def orderbook_vwap(levels, qty):
    try:need=float(qty)
    except Exception:return None,None
    if need<=0:return None,0.0
    remain=need;cost=0.0;filled=0.0
    for price,size in levels:
        take=min(remain,float(size))
        if take<=0:continue
        cost += take*float(price);filled += take;remain -= take
        if remain<=1e-9:break
    if filled<=0:return None,0.0
    return (cost/filled if remain<=1e-9 else None),filled


def max_drawdown_dollars(records, pnl_field):
    equity=peak=0.0;maxdd=0.0
    for r in sorted(records,key=lambda x:(x.get("entry_time") or x.get("recorded_at") or "")):
        equity += float(r.get(pnl_field) or 0.0)/100.0
        peak=max(peak,equity);maxdd=max(maxdd,peak-equity)
    return round(maxdd,4)


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
    live_distance_bp: float | None = None
    forming_side: str = "—"
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
        self.first_signal_path = DATA_DIR / "btc_first_signal_v24.json"
        self.first_signal = {}
        try:
            if self.first_signal_path.exists():
                x=load_json(self.first_signal_path)
                if isinstance(x,dict):self.first_signal=x
        except Exception:pass
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
        expected_times=[window_start_ms+i*60000 for i in range(len(rows))]
        actual_times=[r["open_time"] for r in rows]
        if actual_times!=expected_times:
            raise ValueError("Incomplete/non-contiguous Binance 1m sequence; fail-closed")
        model_open=rows[0]["open"]
        btc_price=rows[-1]["close"]
        completed=[r for r in rows if r["close_time"] < now_ms]
        if completed and now_ms-int(completed[-1]["close_time"])>int(float(os.getenv("MAX_BINANCE_COMPLETED_AGE_MS","90000"))):
            raise ValueError("Stale Binance completed candle; fail-closed")
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
        target_side=("UP" if last_close>target else "DOWN") if target and last_close is not None else "—"
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

        # Safety/UI guard: size is actionable only after a CONFIRMED setup also
        # passes basis + max-buy and becomes ENTRY ZONE. Positive Kelly on a
        # forming or overpriced quote must never display a tradable size.
        if status != "ENTRY ZONE":
            risk_pct=0.0
            risk_dollars=0.0
            contracts_suggested=0

        return State(
            updated_at=now.isoformat(),mode="demo" if self.demo else "live",status=status,status_detail=detail,
            btc_price=btc_price,model_open=model_open,current_minute=current_minute,last_completed_minute=last_completed_minute,
            seconds_left=seconds_left,side=side,distance_bp=dist,live_distance_bp=live_dist,forming_side=live_side,last_step_bp=step,pullback_confirmed=confirmed,
            pullback_forming=forming,never_crossed_open=never_crossed,quality=quality,fair_pct=fair,
            conservative_fair_pct=cons,sample_n=sample_n,market_ticker=ticker,market_close_time=close_time,
            kalshi_target=target,target_gap_bp=target_gap,target_side=target_side,
            yes_bid_cents=yes_bid*100 if yes_bid is not None else None,yes_ask_cents=yes_ask*100 if yes_ask is not None else None,
            no_bid_cents=no_bid*100 if no_bid is not None else None,no_ask_cents=no_ask*100 if no_ask is not None else None,
            selected_contract=selected,selected_ask_cents=selected_ask,max_buy_cents=max_buy,edge_points=edge,
            full_kelly_pct=full_k,fractional_kelly_pct=fractional,risk_pct=risk_pct,risk_dollars=risk_dollars,
            contracts_suggested=contracts_suggested,basis_ok=basis_ok,api_error=""
        )

    def apply_first_signal_guard(self,s):
        """Match the historical protocol: first qualifying pullback per 15m candle.

        Status transitions within the same completed minute (e.g. CONFIRMED -> ENTRY
        ZONE as the quote moves) remain allowed. Later qualifying minutes in the same
        candle are displayed but are not treated as a new evidence-backed signal.
        """
        if s.status not in ("CONFIRMED","ENTRY ZONE","READY / PRICE HIGH","BASIS WARNING"):
            return s
        key=s.market_ticker or f"{s.model_open}|{int(time.time()//900)}"
        prev=self.first_signal if self.first_signal.get("window_key")==key else {}
        if prev and prev.get("minute")!=s.last_completed_minute:
            s.status="ALREADY COUNTED"
            s.status_detail=f"Primer pullback calificante de esta vela ya fue contado en M{prev.get('minute')}."
            return s
        if not prev:
            self.first_signal={"window_key":key,"minute":s.last_completed_minute,"side":s.side,"saved_at":s.updated_at}
            try:save_json(self.first_signal_path,self.first_signal)
            except Exception:pass
        return s

    def maybe_alert(self, s):
        statuses=set(self.config.get("telegram_alert_statuses", []))
        if s.status not in statuses:
            return
        # FORMING is intraminute: label the LIVE minute, not the last completed
        # minute. This prevents an M7 preview from being misread as an M6 signal.
        alert_minute=s.current_minute if s.status=="FORMING" else s.last_completed_minute
        alert_side=(s.forming_side if s.status=="FORMING" and s.forming_side in ("UP","DOWN") else s.side)
        key=f"{s.status}|{s.market_ticker}|{alert_minute}|{alert_side}|{s.quality}"
        if key==self.last_alert_key:
            return
        self.last_alert_key=key
        if s.status in ("CONFIRMED","ENTRY ZONE","BASIS WARNING"):
            self.write_journal(s)
        if s.status=="FORMING":
            live_dist=s.live_distance_bp if s.live_distance_bp is not None else 0.0
            text=(f"BTC15M FORMING · {alert_side} · PREVIEW\n"
                  f"LIVE M{alert_minute} · {live_dist:.1f} bp\n"
                  f"NO ENTRY · espera el cierre de M{alert_minute}. La evidencia validada usa cierres M7–M10.")
        elif s.selected_ask_cents is not None and s.max_buy_cents is not None and s.edge_points is not None:
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
                    s=self.apply_first_signal_guard(s)
                    try:
                        _alt=globals().get("ALT_MONITOR")
                        if _alt and getattr(_alt,"early",None):_alt.early.observe_btc_state(s)
                    except Exception as _ee:
                        print("early BTC observe:",_ee,flush=True)
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
        self.ledger = ShadowLedger(self)
        self.early = EarlyEntryLab(self)

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

    def fetch_orderbook(self,ticker):
        if not ticker:return {}
        try:
            return http_json(KALSHI_BASE+"/markets/"+ticker+"/orderbook?"+urlencode({"depth":100}),timeout=5)
        except Exception as e:
            return {"_error":str(e)}

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
        expected_times=[window_start_ms+i*60000 for i in range(len(rows))]
        actual_times=[r["open_time"] for r in rows]
        if actual_times!=expected_times:
            raise ValueError("incomplete/non-contiguous Binance 1m sequence; fail-closed")
        model_open = rows[0]["open"]
        live_price = rows[-1]["close"]
        completed = [r for r in rows if r["close_time"] < now_ms]
        if completed and now_ms-int(completed[-1]["close_time"])>int(float(os.getenv("MAX_BINANCE_COMPLETED_AGE_MS","90000"))):
            raise ValueError("stale Binance completed candle; fail-closed")
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
        target_side = ("UP" if last_close>target else "DOWN") if target and last_close is not None else "—"
        max_basis=float(self.primary.config.get("max_basis_gap_bp",8.0))
        # If a target cannot be extracted, shadow logging continues but entry status is blocked.
        basis_ok = bool(target is not None and target_gap is not None and abs(target_gap)<=max_basis and (side=="—" or target_side==side))

        selected_ask = yes_ask*100 if side=="UP" and yes_ask is not None else (no_ask*100 if side=="DOWN" and no_ask is not None else None)
        selected_bid = yes_bid*100 if side=="UP" and yes_bid is not None else (no_bid*100 if side=="DOWN" and no_bid is not None else None)
        spread=(selected_ask-selected_bid) if selected_ask is not None and selected_bid is not None else None
        edge_min=float(self.primary.config.get("edge_min_points",10.0))
        fee_est=kalshi_trade_fee_cents(selected_ask,1.0) if selected_ask is not None else None
        eff_fee=effective_fee_cents(selected_ask,self.primary.config,1.0) if selected_ask is not None else None
        max_buy=max_buy_with_fee(model["cons_pct"],edge_min,self.primary.config)
        edge=(model["cons_pct"]-selected_ask-eff_fee) if selected_ask is not None and eff_fee is not None else None

        status="WAIT"
        detail=f"Waiting for M7-M10 pullback >= {model['min_bp']:.0f} bp."
        if confirmed:
            status="SHADOW CONFIRMED"
            detail=f"Underlying setup confirmed at M{last_completed_minute}; live execution still under forward test."
            if not basis_ok:
                status="BASIS WARNING"
                detail="Underlying setup confirmed, but Kalshi target/basis is missing or not aligned."
            elif edge is not None and edge>=edge_min:
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
            "selected_contract":side if side in ("UP","DOWN") else "—","selected_ask_cents":selected_ask,"selected_bid_cents":selected_bid,"spread_cents":spread,
            "estimated_taker_fee_cents":fee_est,"effective_fee_cents":eff_fee,"kalshi_distance_bp":abs(bp(last_close,target)) if target and last_close is not None else None,
            "max_buy_cents":max_buy,"edge_points":edge,"basis_ok":basis_ok,"api_error":""
        }

    def journal_if_confirmed(self, s):
        if not s["status"].startswith("SHADOW") and s["status"]!="BASIS WARNING":
            return
        if s["status"]=="WAIT":
            return
        key=f"{s['asset']}|{s['window_start_utc']}"
        if self.last_journal_key.get(s["asset"])==key or self.ledger.has_setup(key):
            self.last_journal_key[s["asset"]]=key
            self.ledger.observe_state(s)
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
        self.ledger.record_setup(s)
        self.ledger.observe_state(s)

    def maybe_alert(self, s):
        if s["status"] not in ("SHADOW ENTRY ZONE","BASIS WARNING"):
            return
        key=f"{s['asset']}|{s['status']}|{s['window_start_utc']}|{s['side']}"
        if self.last_alert_key.get(s["asset"])==key or self.ledger.alert_was_sent(s):
            self.last_alert_key[s["asset"]]=key
            return
        self.last_alert_key[s["asset"]]=key
        m=s.get("last_completed_minute") or s.get("current_minute")
        if s["status"]=="SHADOW ENTRY ZONE":
            text=(f"{s['asset']}15M SHADOW ENTRY ZONE · {s['side']}\n"
                  f"M{m} · {s['distance_bp']:.1f} bp · fair cons {s['conservative_fair_pct']:.1f}%\n"
                  f"Ask {s['selected_ask_cents']:.1f}c · max {s['max_buy_cents']:.1f}c · edge {s['edge_points']:.1f}pt\n"
                  f"FORWARD TEST — no ejecutar todavía")
        else:
            gap="—" if s.get("target_gap_bp") is None else f"{s['target_gap_bp']:.1f}bp"
            text=(f"{s['asset']}15M BASIS WARNING · {s['side']}\n"
                  f"M{m} · {s['distance_bp']:.1f} bp · gap {gap}\n"
                  f"Setup guardado, pero no usar como entrada.")
        ok,_=post_telegram(telegram_token(), telegram_chat_id(self.primary.config), text)
        if ok:self.ledger.mark_alert_sent(s)

    def send_startup(self):
        if self.startup_sent:
            return
        self.startup_sent=True
        lines=["Multiasset EXECUTION LAB v2.6 HARDENING ✅",
               "MAIN: ETH ≥30bp · SOL ≥30bp · XRP ≥40bp · DOGE ≥35bp · BNB ≥20bp",
               "EARLY LAB: M6-M7 · BTC ≥10bp · ETH/SOL ≥25bp · XRP ≥35bp · DOGE ≥30bp · BNB ≥15bp",
               "Early Lab es experimental: captura precio antes; NO es señal operativa.",
               "BTC sigue igual. HYPE excluido. Sin órdenes automáticas."]
        post_telegram(telegram_token(), telegram_chat_id(self.primary.config), "\n".join(lines))

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
                    _klines=self.fetch_binance(model["symbol"])
                    _market=self.fetch_kalshi(asset,model["series"],now)
                    s=self.compute(asset,model,_klines,_market)
                    try:
                        self.early.observe(asset,_klines,_market)
                    except Exception as _ee:
                        print("early lab observe:",_ee,flush=True)
                except Exception as e:
                    s={"updated_at":datetime.now(timezone.utc).isoformat(),"asset":asset,"status":"API ERROR","api_error":str(e)}
                # Match the historical protocol: only the first qualifying pullback per 15m candle counts.
                if s.get("status") in ("SHADOW CONFIRMED","SHADOW ENTRY ZONE","SHADOW PRICE HIGH","BASIS WARNING"):
                    prev=self.first_signal.get(asset)
                    if not prev:
                        prior=self.ledger.get_setup(asset,s.get("window_start_utc"))
                        if prior:
                            prev={"window":prior.get("window_start_utc"),"minute":prior.get("minute"),"side":prior.get("side")}
                            self.first_signal[asset]=prev
                    if prev and prev.get("window")==s.get("window_start_utc") and prev.get("minute")!=s.get("last_completed_minute"):
                        s["status"]="SHADOW ALREADY COUNTED"
                        s["detail"]=f"First qualifying setup for this candle was already counted at M{prev.get('minute')}."
                    elif not prev or prev.get("window")!=s.get("window_start_utc"):
                        self.first_signal[asset]={"window":s.get("window_start_utc"),"minute":s.get("last_completed_minute"),"side":s.get("side")}
                try:
                    self.early.observe_main_state(s)
                except Exception as _ee:
                    print("early lab main followthrough:",_ee,flush=True)
                with self.lock:
                    self.states[asset]=s
                self.journal_if_confirmed(s)
                self.maybe_alert(s)
                time.sleep(float(os.getenv("ALT_ASSET_STAGGER_SECONDS","0.15")))
            try:
                self.ledger.observe_portfolio(self.get_states())
            except Exception as e:
                print("portfolio shadow:",e,flush=True)
            try:
                self.ledger.maybe_resolve()
            except Exception as e:
                print("shadow ledger resolver:",e,flush=True)
            try:
                self.early.maybe_resolve()
                self.early.maybe_daily_report()
            except Exception as e:
                print("early lab resolver:",e,flush=True)
            time.sleep(max(1.0,float(os.getenv("ALT_POLL_SECONDS","2.0"))))


EARLY_LAB_MODELS = {
    "BTC":  {"min_bp":10.0, "main_bp":10.0},
    # Exploratory thresholds only. They intentionally sit one step below the main
    # Execution Lab thresholds. No historical fair is assigned to these signals.
    "ETH":  {"min_bp":25.0, "main_bp":30.0},
    "SOL":  {"min_bp":25.0, "main_bp":30.0},
    "XRP":  {"min_bp":35.0, "main_bp":40.0},
    "DOGE": {"min_bp":30.0, "main_bp":35.0},
    "BNB":  {"min_bp":15.0, "main_bp":20.0},
}
EARLY_LEDGER_PATH = DATA_DIR / "early_entry_lab_v25.json"
EARLY_LEDGER_CSV_NAME = "early_entry_lab_v25.csv"
EARLY_LEDGER_SCHEMA_VERSION = "2.6"
EARLY_EXPERIMENT_RULE = "early_m6_m7"


def deterministic_capture_id(asset, window_start_utc, minute, side, rule=EARLY_EXPERIMENT_RULE):
    raw = f"{asset}|{window_start_utc}|{minute}|{side}|{rule}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class EarlyEntryLab:
    """Exploratory M6-M7 price-discovery forward test.

    This deliberately does NOT inherit the main model's historical fair. It asks a
    narrower empirical question: when an early pullback precursor appears, what price
    is Kalshi offering, what ultimately settles, and does the stricter M7-M10 main
    setup appear later in the same candle?

    Every eligible observation is simulated as exactly one contract at the observed
    ask (plus the fee model) for research accounting only. No order is ever placed.
    """
    def __init__(self, alt_monitor):
        self.alt=alt_monitor
        self.lock=threading.RLock()
        self.last_resolve_check=0.0
        now_iso=datetime.now(timezone.utc).isoformat()
        self.data={
            "version":EARLY_LEDGER_SCHEMA_VERSION,
            "schema_version":EARLY_LEDGER_SCHEMA_VERSION,
            "mode":"early-entry-exploratory",
            "experiments":{},
            "rejected_candidates":{},
            "counters":{
                "captures_received":0,
                "captures_persisted":0,
                "captures_rejected":0,
                "duplicates_suppressed":0,
                "persistence_errors":0,
            },
            "rejections":{
                "stale_binance":0,
                "stale_kalshi_book":0,
                "missing_book":0,
                "missing_bid_or_ask":0,
                "incomplete_minute_data":0,
                "basis_invalid":0,
                "duplicate_capture":0,
                "settlement_unresolved":0,
                "other":0,
            },
            "last_capture_utc":None,
            "last_resolution_utc":None,
            "last_persist_utc":None,
            "last_daily_report_date":"",
            "created_at":now_iso,
        }
        if EARLY_LEDGER_PATH.exists():
            try:
                old=load_json(EARLY_LEDGER_PATH)
                if isinstance(old,dict):
                    if str(old.get("schema_version") or old.get("version") or "")!="2.6":
                        backup_path=DATA_DIR/"early_entry_lab_v25.pre_v26.json"
                        if not backup_path.exists():
                            save_json(backup_path,old)
                    had_counters=isinstance(old.get("counters"),dict)
                    self.data.update(old)
                    self.data.setdefault("experiments",{})
                    self.data.setdefault("rejected_candidates",{})
                    self.data.setdefault("last_daily_report_date","")
                    self.data.setdefault("rejections",{})
                    for key in ("stale_binance","stale_kalshi_book","missing_book","missing_bid_or_ask","incomplete_minute_data","basis_invalid","duplicate_capture","settlement_unresolved","other"):
                        self.data["rejections"].setdefault(key,0)
                    self.data.setdefault("counters",{})
                    existing_count=len(self.data.get("experiments",{}))
                    defaults={
                        "captures_received":existing_count if not had_counters else 0,
                        "captures_persisted":existing_count if not had_counters else 0,
                        "captures_rejected":0,
                        "duplicates_suppressed":0,
                        "persistence_errors":0,
                    }
                    for key,value in defaults.items():
                        self.data["counters"].setdefault(key,value)
            except Exception as e:
                print("early ledger load:",e,flush=True)
        self.data["version"]=EARLY_LEDGER_SCHEMA_VERSION
        self.data["schema_version"]=EARLY_LEDGER_SCHEMA_VERSION
        self.data["mode"]="early-entry-exploratory"
        self.data.setdefault("last_capture_utc",None)
        self.data.setdefault("last_resolution_utc",None)
        self.data.setdefault("last_persist_utc",None)
        # Non-destructive migration: v2.5 observations remain in the same file.
        for rec in self.data.get("experiments",{}).values():
            rec.setdefault("schema_version","2.5-legacy")
            rec.setdefault("model_version","2.5-railway-early-entry-lab")
            rec.setdefault("experimental_rule",EARLY_EXPERIMENT_RULE)
            rec.setdefault("predicted_probability",None)
            rec.setdefault("predicted_edge",None)
            rec.setdefault("capture_id",deterministic_capture_id(
                rec.get("asset",""),
                rec.get("window_start_utc",""),
                rec.get("early_minute"),
                rec.get("side",""),
            ))
            rec.setdefault("data_quality",{
                "status":"LEGACY_UNASSESSED",
                "score":None,
                "reasons":["quality vector not recorded before v2.6"],
                "binance_age_ms":None,
                "kalshi_book_age_ms":None,
                "settlement_basis_ok":rec.get("basis_ok"),
                "minute_closes_complete":None,
                "gap_count":None,
                "largest_gap_seconds":None,
                "book_present":bool(rec.get("market_ticker")),
                "bid_present":rec.get("bid_cents") is not None,
                "ask_present":rec.get("ask_cents") is not None,
                "spread_cents":rec.get("spread_cents"),
                "source_errors_recent":None,
                "reconnects_recent":None,
            })
            rec.setdefault("audit",{
                "created_by_version":rec.get("model_version","2.5-railway-early-entry-lab"),
                "last_updated_by_version":APP_VERSION,
            })
        if not self.data.get("last_capture_utc") and self.data.get("experiments"):
            self.data["last_capture_utc"]=max((str(r.get("recorded_at") or "") for r in self.data["experiments"].values()),default=None) or None
        self.save()

    def save(self):
        with self.lock:
            self.data["last_persist_utc"]=datetime.now(timezone.utc).isoformat()
            try:
                save_json(EARLY_LEDGER_PATH,self.data)
            except Exception:
                self.data.setdefault("counters",{}).setdefault("persistence_errors",0)
                self.data["counters"]["persistence_errors"]+=1
                raise

    def _inc_rejection(self, reason):
        reason=reason if reason in self.data.get("rejections",{}) else "other"
        self.data["rejections"][reason]=int(self.data["rejections"].get(reason,0))+1

    def _record_invalid_candidate(self, capture_id, asset, window_iso, minute, side, reasons):
        now_iso=datetime.now(timezone.utc).isoformat()
        primary_reason=(reasons or ["other"])[0]
        with self.lock:
            rejected=self.data.setdefault("rejected_candidates",{})
            if capture_id in rejected:
                return
            self.data["counters"]["captures_received"]+=1
            self.data["counters"]["captures_rejected"]+=1
            self._inc_rejection(primary_reason)
            rejected[capture_id]={
                "capture_id":capture_id,
                "asset":asset,
                "window_start_utc":window_iso,
                "early_minute":minute,
                "side":side,
                "reasons":list(reasons or ["other"]),
                "rejected_at":now_iso,
            }
            try:
                self.save()
            except Exception:
                rejected.pop(capture_id,None)
                self.data["counters"]["captures_received"]-=1
                self.data["counters"]["captures_rejected"]-=1
                self.data["rejections"][primary_reason]=max(0,int(self.data["rejections"].get(primary_reason,0))-1)
                raise

    def _data_quality(self, asset, ws, completed, minute, market, side, model_open, last, ask, bid, basis_ok):
        reasons=[]
        invalid=[]
        gap_count=0
        largest_gap=0
        expected=[ws+i*60000 for i in range(minute)]
        actual=[int(r["open_time"]) for r in completed[:minute]]
        complete=(len(actual)==minute and actual==expected)
        if not complete:
            invalid.append("incomplete_minute_data")
            if len(actual)>=2:
                gaps=[max(0,int((actual[i+1]-actual[i])/1000)-60) for i in range(len(actual)-1)]
                gap_count=sum(1 for g in gaps if g>0)
                largest_gap=max(gaps,default=0)
            elif len(actual)<minute:
                gap_count=max(1,minute-len(actual))
                largest_gap=None
        now_ms=int(time.time()*1000)
        binance_age=(max(0,now_ms-int(completed[-1]["close_time"])) if completed else None)
        max_binance_age=int(float(os.getenv("EARLY_MAX_BINANCE_AGE_MS","90000")))
        if binance_age is None or binance_age>max_binance_age:
            invalid.append("stale_binance")
        if asset=="BTC":
            last_fetch=float(getattr(self.alt.primary,"kalshi_last",0.0) or 0.0)
            poll=float(self.alt.primary.config.get("kalshi_poll_seconds",2.0))
        else:
            last_fetch=float(self.alt.kalshi_last.get(asset,0.0) or 0.0)
            poll=float(os.getenv("ALT_KALSHI_POLL_SECONDS","3"))
        kalshi_age=(max(0.0,(time.time()-last_fetch)*1000.0) if last_fetch else None)
        default_kalshi_age=max(15000,int(poll*5000))
        max_kalshi_age=int(float(os.getenv("EARLY_MAX_KALSHI_AGE_MS",str(default_kalshi_age))))
        if kalshi_age is None or kalshi_age>max_kalshi_age:
            reasons.append("stale_kalshi_book")
        if not market or not market.get("ticker"):
            reasons.append("missing_book")
        if ask is None or bid is None:
            reasons.append("missing_bid_or_ask")
        if basis_ok is not True:
            reasons.append("basis_invalid")
        all_reasons=invalid+[r for r in reasons if r not in invalid]
        status="INVALID" if invalid else ("DEGRADED_OBSERVATION" if reasons else "VALID")
        score=0.0 if status=="INVALID" else (0.5 if status=="DEGRADED_OBSERVATION" else 1.0)
        return {
            "status":status,
            "score":score,
            "reasons":all_reasons,
            "binance_age_ms":round(binance_age,1) if binance_age is not None else None,
            "kalshi_book_age_ms":round(kalshi_age,1) if kalshi_age is not None else None,
            "settlement_basis_ok":basis_ok,
            "minute_closes_complete":complete,
            "gap_count":gap_count,
            "largest_gap_seconds":largest_gap,
            "book_present":bool(market and market.get("ticker")),
            "bid_present":bid is not None,
            "ask_present":ask is not None,
            "spread_cents":(ask-bid) if ask is not None and bid is not None else None,
            "source_errors_recent":None,
            "reconnects_recent":None,
        }

    @staticmethod
    def _window_rows(klines,now_ms):
        ws=(now_ms//900000)*900000;we=ws+900000;rows=[]
        for k in klines or []:
            try:
                ot=int(k[0]);op=float(k[1]);cl=float(k[4]);ct=int(k[6])
                if ws<=ot<we:rows.append({"open_time":ot,"open":op,"close":cl,"close_time":ct})
            except Exception:pass
        rows.sort(key=lambda r:r["open_time"])
        return ws,we,rows

    def observe(self,asset,klines,market):
        cfg=EARLY_LAB_MODELS.get(asset)
        if not cfg:return None
        now=datetime.now(timezone.utc);now_ms=int(now.timestamp()*1000)
        ws,we,rows=self._window_rows(klines,now_ms)
        if not rows:return None
        completed=[r for r in rows if r["close_time"]<now_ms]
        m=len(completed)
        if m not in (6,7) or len(completed)<2:return None
        model_open=rows[0]["open"];last=completed[-1]["close"];prev=completed[-2]["close"]
        side="UP" if last>model_open else ("DOWN" if last<model_open else "—")
        if side=="—":return None
        dist=abs(bp(last,model_open));step=bp(last,prev)
        pullback=(side=="UP" and step<0) or (side=="DOWN" and step>0)
        if not pullback or dist<float(cfg["min_bp"]):return None

        window_iso=datetime.fromtimestamp(ws/1000,tz=timezone.utc).isoformat()
        eid=f"{asset}|{window_iso}"
        capture_id=deterministic_capture_id(asset,window_iso,m,side)
        with self.lock:
            existing=self.data.get("experiments",{}).get(eid)
            if existing:
                return dict(existing)

        ticker="";target=None;yb=ya=nb=na=None
        if market:
            ticker=market.get("ticker","");target=extract_target(market);yb,ya,nb,na=get_price_fields(market)
        ask=ya*100 if side=="UP" and ya is not None else (na*100 if side=="DOWN" and na is not None else None)
        bid=yb*100 if side=="UP" and yb is not None else (nb*100 if side=="DOWN" and nb is not None else None)
        spread=(ask-bid) if ask is not None and bid is not None else None
        target_gap=bp(target,model_open) if target else None
        target_side=("UP" if last>target else "DOWN") if target and last is not None else "—"
        max_basis=float(self.alt.primary.config.get("max_basis_gap_bp",8.0))
        basis_ok=bool(target is not None and target_gap is not None and abs(target_gap)<=max_basis and target_side==side)
        dq=self._data_quality(asset,ws,completed,m,market,side,model_open,last,ask,bid,basis_ok)
        if dq["status"]=="INVALID":
            self._record_invalid_candidate(capture_id,asset,window_iso,m,side,dq.get("reasons") or ["other"])
            return None

        fee=effective_fee_cents(ask,self.alt.primary.config,1.0) if ask is not None else None
        simulated=bool(dq["status"]=="VALID" and basis_ok and ask is not None and 0<float(ask)<100)
        rec={
            "schema_version":EARLY_LEDGER_SCHEMA_VERSION,
            "capture_id":capture_id,
            "experiment_id":eid,
            "model_version":APP_VERSION,
            "experimental_rule":EARLY_EXPERIMENT_RULE,
            "predicted_probability":None,
            "predicted_edge":None,
            "recorded_at":now.isoformat(),"asset":asset,"window_start_utc":window_iso,"window_start_ms":ws,"window_end_ms":we,
            "early_minute":m,"side":side,"model_open":model_open,"signal_binance_price":last,"distance_bp":dist,"last_step_bp":step,
            "early_threshold_bp":cfg["min_bp"],"main_threshold_bp":cfg["main_bp"],"market_ticker":ticker,"kalshi_target":target,"target_gap_bp":target_gap,
            "basis_ok":basis_ok,"ask_cents":ask,"bid_cents":bid,"spread_cents":spread,"entry_simulated":simulated,"entry_ask_cents":ask if simulated else None,
            "entry_fee_cents":fee if simulated else None,"main_followthrough":False,"main_followthrough_minute":None,"main_followthrough_side":"",
            "resolved":False,"kalshi_status":"","kalshi_result":"","kalshi_win":None,"settlement_ts":"","underlying_final_close":None,"underlying_model_win":None,
            "benchmark_disagreement":None,"gross_pnl_cents":None,"net_pnl_cents":None,"resolution_error":"",
            "data_quality":dq,
            "audit":{"created_by_version":APP_VERSION,"last_updated_by_version":APP_VERSION},
        }
        with self.lock:
            # Re-check after quality computation so concurrent observers remain idempotent.
            if eid in self.data.get("experiments",{}):
                self.data["counters"]["duplicates_suppressed"]+=1
                self.data["rejections"]["duplicate_capture"]+=1
                return dict(self.data["experiments"][eid])
            old_last_capture=self.data.get("last_capture_utc")
            self.data["counters"]["captures_received"]+=1
            self.data["experiments"][eid]=rec
            self.data["counters"]["captures_persisted"]+=1
            self.data["last_capture_utc"]=rec["recorded_at"]
            try:
                self.save()
            except Exception:
                self.data["experiments"].pop(eid,None)
                self.data["counters"]["captures_received"]-=1
                self.data["counters"]["captures_persisted"]-=1
                self.data["last_capture_utc"]=old_last_capture
                raise
        self.maybe_alert(rec)
        return dict(rec)

    def observe_btc_state(self,s):
        cfg=EARLY_LAB_MODELS["BTC"]
        try:m=int(s.last_completed_minute or 0)
        except Exception:return None
        # BTC reaches this method only after Monitor.compute() has passed the
        # fail-closed contiguous/fresh Binance checks.
        if m in (6,7) and s.side in ("UP","DOWN") and s.distance_bp is not None and s.last_step_bp is not None:
            pullback=(s.side=="UP" and float(s.last_step_bp)<0) or (s.side=="DOWN" and float(s.last_step_bp)>0)
            if pullback and float(s.distance_bp)>=cfg["min_bp"]:
                dt=parse_iso(s.updated_at) or datetime.now(timezone.utc)
                ws_ms=(int(dt.timestamp()*1000)//900000)*900000;we_ms=ws_ms+900000
                window_iso=datetime.fromtimestamp(ws_ms/1000,tz=timezone.utc).isoformat()
                eid=f"BTC|{window_iso}"
                capture_id=deterministic_capture_id("BTC",window_iso,m,s.side)
                with self.lock:
                    exists=eid in self.data.get("experiments",{})
                if not exists:
                    ask=s.selected_ask_cents
                    bid=(s.yes_bid_cents if s.side=="UP" else s.no_bid_cents)
                    spread=(float(ask)-float(bid)) if ask is not None and bid is not None else None
                    reasons=[]
                    k_last=float(getattr(self.alt.primary,"kalshi_last",0.0) or 0.0)
                    k_age=max(0.0,(time.time()-k_last)*1000.0) if k_last else None
                    k_poll=float(self.alt.primary.config.get("kalshi_poll_seconds",2.0))
                    k_max=int(float(os.getenv("EARLY_MAX_KALSHI_AGE_MS",str(max(15000,int(k_poll*5000))))))
                    if k_age is None or k_age>k_max:reasons.append("stale_kalshi_book")
                    if not s.market_ticker:reasons.append("missing_book")
                    if ask is None or bid is None:reasons.append("missing_bid_or_ask")
                    if s.basis_ok is not True:reasons.append("basis_invalid")
                    dq={
                        "status":"DEGRADED_OBSERVATION" if reasons else "VALID",
                        "score":0.5 if reasons else 1.0,
                        "reasons":reasons,
                        "binance_age_ms":None,
                        "kalshi_book_age_ms":round(k_age,1) if k_age is not None else None,
                        "settlement_basis_ok":s.basis_ok,
                        "minute_closes_complete":True,
                        "gap_count":0,
                        "largest_gap_seconds":0,
                        "book_present":bool(s.market_ticker),
                        "bid_present":bid is not None,
                        "ask_present":ask is not None,
                        "spread_cents":spread,
                        "source_errors_recent":None,
                        "reconnects_recent":None,
                    }
                    fee=effective_fee_cents(ask,self.alt.primary.config,1.0) if ask is not None else None
                    simulated=bool(dq["status"]=="VALID" and s.basis_ok is True and ask is not None and 0<float(ask)<100)
                    rec={
                        "schema_version":EARLY_LEDGER_SCHEMA_VERSION,
                        "capture_id":capture_id,
                        "experiment_id":eid,
                        "model_version":APP_VERSION,
                        "experimental_rule":EARLY_EXPERIMENT_RULE,
                        "predicted_probability":None,
                        "predicted_edge":None,
                        "recorded_at":s.updated_at,"asset":"BTC","window_start_utc":window_iso,"window_start_ms":ws_ms,"window_end_ms":we_ms,
                        "early_minute":m,"side":s.side,"model_open":s.model_open,"signal_binance_price":s.btc_price,"distance_bp":s.distance_bp,"last_step_bp":s.last_step_bp,
                        "early_threshold_bp":cfg["min_bp"],"main_threshold_bp":cfg["main_bp"],"market_ticker":s.market_ticker,"kalshi_target":s.kalshi_target,"target_gap_bp":s.target_gap_bp,
                        "basis_ok":bool(s.basis_ok),"ask_cents":ask,"bid_cents":bid,"spread_cents":spread,"entry_simulated":simulated,"entry_ask_cents":ask if simulated else None,"entry_fee_cents":fee if simulated else None,
                        "main_followthrough":False,"main_followthrough_minute":None,"main_followthrough_side":"","resolved":False,"kalshi_status":"","kalshi_result":"","kalshi_win":None,"settlement_ts":"",
                        "underlying_final_close":None,"underlying_model_win":None,"benchmark_disagreement":None,"gross_pnl_cents":None,"net_pnl_cents":None,"resolution_error":"",
                        "data_quality":dq,
                        "audit":{"created_by_version":APP_VERSION,"last_updated_by_version":APP_VERSION},
                    }
                    with self.lock:
                        if eid not in self.data["experiments"]:
                            old_last_capture=self.data.get("last_capture_utc")
                            self.data["counters"]["captures_received"]+=1
                            self.data["experiments"][eid]=rec
                            self.data["counters"]["captures_persisted"]+=1
                            self.data["last_capture_utc"]=rec["recorded_at"]
                            try:
                                self.save()
                            except Exception:
                                self.data["experiments"].pop(eid,None)
                                self.data["counters"]["captures_received"]-=1
                                self.data["counters"]["captures_persisted"]-=1
                                self.data["last_capture_utc"]=old_last_capture
                                raise
                            created=True
                        else:
                            self.data["counters"]["duplicates_suppressed"]+=1
                            self.data["rejections"]["duplicate_capture"]+=1
                            created=False
                    if created:self.maybe_alert(rec)
        # Record whether the later evidence-aligned BTC setup appeared.
        if s.status in ("CONFIRMED","ENTRY ZONE","READY / PRICE HIGH","BASIS WARNING","ALREADY COUNTED") and m in (7,8,9,10):
            dt=parse_iso(s.updated_at) or datetime.now(timezone.utc);ws_ms=(int(dt.timestamp()*1000)//900000)*900000;eid=f"BTC|{datetime.fromtimestamp(ws_ms/1000,tz=timezone.utc).isoformat()}"
            with self.lock:
                r=self.data.get("experiments",{}).get(eid)
                if r and not r.get("main_followthrough"):
                    r["main_followthrough"]=True
                    r["main_followthrough_minute"]=m
                    r["main_followthrough_side"]=s.side
                    r.setdefault("audit",{})["last_updated_by_version"]=APP_VERSION
                    self.save()
        return None

    def observe_main_state(self,s):
        if s.get("status") not in ("SHADOW CONFIRMED","SHADOW ENTRY ZONE","SHADOW PRICE HIGH","BASIS WARNING","SHADOW ALREADY COUNTED"):return
        eid=f"{s.get('asset')}|{s.get('window_start_utc')}"
        changed=False
        with self.lock:
            r=self.data.get("experiments",{}).get(eid)
            if not r:return
            if not r.get("main_followthrough") and s.get("last_completed_minute") in (7,8,9,10):
                r["main_followthrough"]=True
                r["main_followthrough_minute"]=s.get("last_completed_minute")
                r["main_followthrough_side"]=s.get("side")
                r.setdefault("audit",{})["last_updated_by_version"]=APP_VERSION
                changed=True
            if changed:self.save()

    def maybe_alert(self,rec):
        if os.getenv("EARLY_LAB_ALERTS","1").lower() not in ("1","true","yes"):return
        ask=rec.get("ask_cents")
        maxask=float(os.getenv("EARLY_LAB_TELEGRAM_MAX_ASK","90"))
        if ask is None or float(ask)>maxask or rec.get("basis_ok") is not True:return
        text=(f"{rec['asset']}15M EARLY LAB CAPTURE · {rec['side']}\n"
              f"M{rec['early_minute']} · {rec['distance_bp']:.1f}bp · exploratory threshold {rec['early_threshold_bp']:.0f}bp\n"
              f"Kalshi ask {float(ask):.1f}c · spread {float(rec.get('spread_cents') or 0):.1f}c\n"
              f"EXPERIMENTAL · NO ENTRY · 1-contract shadow only")
        post_telegram(telegram_token(),telegram_chat_id(self.alt.primary.config),text)

    def maybe_resolve(self):
        interval=max(10.0,float(os.getenv("EARLY_RESOLVE_SECONDS","20")))
        if time.time()-self.last_resolve_check<interval:return
        self.last_resolve_check=time.time();now_ms=int(time.time()*1000);changed=False
        with self.lock:
            ids=[k for k,r in self.data.get("experiments",{}).items() if not r.get("resolved") and r.get("window_end_ms") and now_ms>r["window_end_ms"]+15000]
        for eid in ids[:20]:
            with self.lock: rec=dict(self.data["experiments"].get(eid,{}) or {})
            if not rec:continue
            try:
                done=self.alt.ledger._resolve_one(rec)
                if rec.get("asset")=="BTC" and rec.get("underlying_final_close") is None:
                    try:
                        start=rec.get("window_start_ms")
                        url=BINANCE_BASE+"/api/v3/klines?"+urlencode({"symbol":"BTCUSDT","interval":"1m","startTime":start,"endTime":start+899999,"limit":15})
                        _rows=http_json(url);_cl=sorted((int(k[0]),float(k[4])) for k in (_rows or []) if start<=int(k[0])<start+900000)
                        if len(_cl)>=15:
                            rec["underlying_final_close"]=_cl[-1][1];_ms="UP" if _cl[-1][1]>float(rec.get("model_open")) else ("DOWN" if _cl[-1][1]<float(rec.get("model_open")) else "TIE")
                            rec["underlying_model_win"]=( _ms==rec.get("side") )
                            if rec.get("kalshi_win") is not None:rec["benchmark_disagreement"]=(bool(rec.get("underlying_model_win"))!=bool(rec.get("kalshi_win")))
                    except Exception:pass
                rec["resolution_error"]=""
                rec.setdefault("audit",{})["last_updated_by_version"]=APP_VERSION
                with self.lock:
                    self.data["experiments"][eid]=rec
                    if rec.get("resolved"):
                        self.data["last_resolution_utc"]=rec.get("resolved_at") or datetime.now(timezone.utc).isoformat()
                changed=True
            except Exception as e:
                rec["resolution_error"]=str(e)[:300]
                rec.setdefault("audit",{})["last_updated_by_version"]=APP_VERSION
                with self.lock:self.data["experiments"][eid]=rec
                changed=True
            time.sleep(0.05)
        if changed:self.save()

    @staticmethod
    def _cohort(rows):
        resolved=[r for r in rows if r.get("resolved") and r.get("kalshi_win") is not None]
        quoted=[r for r in rows if r.get("entry_simulated") and r.get("entry_ask_cents") is not None]
        sim=[r for r in resolved if r.get("entry_simulated") and r.get("entry_ask_cents") is not None]
        wins=sum(1 for r in sim if r.get("kalshi_win"))
        pnl=sum(float(r.get("net_pnl_cents") or 0) for r in sim)/100.0
        asks=sorted(float(r.get("entry_ask_cents")) for r in quoted)
        avgask=(sum(asks)/len(asks)) if asks else None
        if asks:
            mid=len(asks)//2
            medianask=asks[mid] if len(asks)%2 else (asks[mid-1]+asks[mid])/2.0
        else:
            medianask=None
        # Follow-through is evaluated only after settlement so fresh captures are not
        # temporarily counted as failures before M7-M10 has had a chance to occur.
        follow_resolved=[r for r in resolved if ((r.get("data_quality") or {}).get("status") or "LEGACY_UNASSESSED")!="DEGRADED_OBSERVATION"]
        fcount=sum(1 for r in follow_resolved if r.get("main_followthrough"))
        bench=[r for r in resolved if r.get("benchmark_disagreement") is not None]
        qcounts={}
        for r in rows:
            qs=((r.get("data_quality") or {}).get("status") or "LEGACY_UNASSESSED")
            qcounts[qs]=qcounts.get(qs,0)+1
        return {
            "recorded":len(rows),
            "resolved":len(resolved),
            "simulated_entries":len(quoted),
            "resolved_entries":len(sim),
            "wins":wins,
            "losses":len(sim)-wins,
            "hit_pct":(100*wins/len(sim) if sim else None),
            "wilson_low_pct":(wilson_low(wins,len(sim)) if sim else None),
            "net_pnl_dollars":pnl,
            "avg_net_pnl_per_entry_dollars":(pnl/len(sim) if sim else None),
            "avg_ask_cents":avgask,
            "median_ask_cents":medianask,
            "main_followthrough_pct":(100*fcount/len(follow_resolved) if follow_resolved else None),
            "benchmark_disagreement_pct":(100*sum(1 for r in bench if r.get("benchmark_disagreement"))/len(bench) if bench else None),
            "quality_counts":qcounts,
        }

    def metrics(self):
        with self.lock:
            rows=[dict(r) for r in self.data.get("experiments",{}).values()]
            counters=dict(self.data.get("counters",{}))
            rejections=dict(self.data.get("rejections",{}))
            last_capture=self.data.get("last_capture_utc")
            last_resolution=self.data.get("last_resolution_utc")
            last_persist=self.data.get("last_persist_utc")
        by=[]
        for asset,cfg in EARLY_LAB_MODELS.items():
            aa=[r for r in rows if r.get("asset")==asset]
            x=self._cohort(aa)
            x.update({"asset":asset,"early_threshold_bp":cfg["min_bp"],"main_threshold_bp":cfg["main_bp"]})
            by.append(x)
        minute=[]
        for m in (6,7):
            mm=[r for r in rows if r.get("early_minute")==m]
            x=self._cohort(mm);x["minute"]=m;minute.append(x)
        side=[]
        for s in ("UP","DOWN"):
            ss=[r for r in rows if r.get("side")==s]
            x=self._cohort(ss);x["side"]=s;side.append(x)
        # Preserve the v2.5 cumulative view for continuity.
        bands=[]
        for cap in (70,75,80,85,90,95,99):
            rr=[r for r in rows if r.get("entry_ask_cents") is not None and float(r.get("entry_ask_cents"))<=cap]
            x=self._cohort(rr);x["max_ask_cents"]=cap;bands.append(x)
        ask_bands=[]
        for lo,hi,label in ((0,49.999,"0-49"),(50,59.999,"50-59"),(60,69.999,"60-69"),(70,79.999,"70-79"),(80,89.999,"80-89"),(90,99.999,"90-99")):
            rr=[r for r in rows if r.get("entry_ask_cents") is not None and lo<=float(r.get("entry_ask_cents"))<=hi]
            x=self._cohort(rr);x.update({"ask_band":label,"min_ask_cents":lo,"max_ask_cents":hi});ask_bands.append(x)
        distance_bands=[]
        for lo,hi,label in ((10,14.999,"10-14"),(15,24.999,"15-24"),(25,34.999,"25-34"),(35,10**9,"35+")):
            rr=[r for r in rows if r.get("distance_bp") is not None and lo<=float(r.get("distance_bp"))<=hi]
            x=self._cohort(rr);x.update({"distance_bp_band":label,"min_distance_bp":lo,"max_distance_bp":None if hi>=10**9 else hi});distance_bands.append(x)
        quality=[]
        qnames=sorted({((r.get("data_quality") or {}).get("status") or "LEGACY_UNASSESSED") for r in rows} | {"VALID","DEGRADED_OBSERVATION","LEGACY_UNASSESSED"})
        for q in qnames:
            qq=[r for r in rows if ((r.get("data_quality") or {}).get("status") or "LEGACY_UNASSESSED")==q]
            x=self._cohort(qq);x["data_quality_status"]=q;quality.append(x)
        totals=self._cohort(rows)
        received=int(counters.get("captures_received",0))
        persisted=int(counters.get("captures_persisted",0))
        rejected=int(counters.get("captures_rejected",0))
        quality_reason_counts={}
        for r in rows:
            for reason in ((r.get("data_quality") or {}).get("reasons") or []):
                quality_reason_counts[reason]=quality_reason_counts.get(reason,0)+1
        operational={
            **counters,
            "pending_capture_writes":0,
            "counter_invariant_ok":received==(persisted+rejected),
            "last_capture_utc":last_capture,
            "last_resolution_utc":last_resolution,
            "last_persist_utc":last_persist,
            "rejections":rejections,
            "quality_reason_counts":quality_reason_counts,
        }
        return {
            "version":APP_VERSION,
            "ledger_schema":EARLY_LEDGER_SCHEMA_VERSION,
            "mode":"early-entry-exploratory",
            "generated_at":datetime.now(timezone.utc).isoformat(),
            "rule":"first qualifying completed M6 or M7 pullback per asset/window; relaxed threshold; no historical fair assigned",
            "probability_model":"NOT_ASSIGNED",
            "totals":totals,
            "by_asset":by,
            "by_minute":minute,
            "by_side":side,
            "price_bands":bands,
            "ask_bands":ask_bands,
            "distance_bands":distance_bands,
            "by_data_quality_status":quality,
            "operational":operational,
        }

    def csv_bytes(self):
        import io
        fields=[
            "schema_version","capture_id","experiment_id","model_version","experimental_rule","predicted_probability","predicted_edge",
            "recorded_at","asset","window_start_utc","early_minute","side","model_open","signal_binance_price","distance_bp","last_step_bp","early_threshold_bp","main_threshold_bp",
            "market_ticker","kalshi_target","target_gap_bp","basis_ok","ask_cents","bid_cents","spread_cents","entry_simulated","entry_ask_cents","entry_fee_cents",
            "data_quality_status","data_quality_score","data_quality_reasons","binance_age_ms","kalshi_book_age_ms","minute_closes_complete","gap_count","largest_gap_seconds",
            "book_present","bid_present","ask_present","main_followthrough","main_followthrough_minute","main_followthrough_side",
            "resolved","kalshi_status","kalshi_result","kalshi_win","settlement_ts","underlying_final_close","underlying_model_win","benchmark_disagreement",
            "gross_pnl_cents","net_pnl_cents","resolution_error"
        ]
        buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=fields,extrasaction="ignore");w.writeheader()
        with self.lock: vals=sorted((dict(r) for r in self.data.get("experiments",{}).values()),key=lambda r:r.get("recorded_at") or "")
        for r in vals:
            dq=dict(r.get("data_quality") or {})
            row=dict(r)
            row.update({
                "data_quality_status":dq.get("status"),
                "data_quality_score":dq.get("score"),
                "data_quality_reasons":"|".join(dq.get("reasons") or []),
                "binance_age_ms":dq.get("binance_age_ms"),
                "kalshi_book_age_ms":dq.get("kalshi_book_age_ms"),
                "minute_closes_complete":dq.get("minute_closes_complete"),
                "gap_count":dq.get("gap_count"),
                "largest_gap_seconds":dq.get("largest_gap_seconds"),
                "book_present":dq.get("book_present"),
                "bid_present":dq.get("bid_present"),
                "ask_present":dq.get("ask_present"),
            })
            w.writerow(row)
        return buf.getvalue().encode("utf-8")

    def maybe_daily_report(self):
        now=datetime.now(timezone.utc);h=int(os.getenv("EARLY_DAILY_REPORT_UTC_HOUR","23"));m=int(os.getenv("EARLY_DAILY_REPORT_UTC_MINUTE","57"));today=now.date().isoformat()
        with self.lock:last=self.data.get("last_daily_report_date","")
        if not (now.hour>h or (now.hour==h and now.minute>=m)) or last==today:return
        with self.lock: day=[dict(r) for r in self.data.get("experiments",{}).values() if str(r.get("recorded_at") or "").startswith(today)]
        x=self._cohort(day)
        hit_txt="—" if x.get("hit_pct") is None else f"{x['hit_pct']:.1f}%"
        ask_txt="—" if x.get("avg_ask_cents") is None else f"{x['avg_ask_cents']:.1f}c"
        follow_txt="—" if x.get("main_followthrough_pct") is None else f"{x['main_followthrough_pct']:.1f}%"
        text=(f"EARLY ENTRY LAB DAILY · {today} UTC\n"
              f"Captures {x['recorded']} · resolved {x['resolved']} · hit {hit_txt}\n"
              f"Avg ask {ask_txt} · net 1-contract {x['net_pnl_dollars']:+.2f}$\n"
              f"Main follow-through {follow_txt}\n"
              f"Exploratory only · no orders")
        post_telegram(telegram_token(),telegram_chat_id(self.alt.primary.config),text)
        with self.lock:self.data["last_daily_report_date"]=today;save_json(EARLY_LEDGER_PATH,self.data)


ALT_MONITOR=None


SHADOW_LEDGER_PATH = DATA_DIR / "multiasset_shadow_ledger_v23.json"
SHADOW_LEDGER_CSV_NAME = "multiasset_shadow_ledger_v24.csv"


def iso_to_ms(x):
    try:
        return int(parse_iso(x).timestamp()*1000)
    except Exception:
        return None


class ShadowLedger:
    """Persistent forward-test ledger.

    Ground truth for simulated PnL is Kalshi's public settled/determined `result` field,
    not a Binance proxy. Binance final close is also saved separately for model diagnostics.
    No order is ever placed.
    """
    def __init__(self, alt_monitor):
        self.alt = alt_monitor
        self.lock = threading.Lock()
        self.data = {"version":"2.4","setups":{},"portfolio_windows":{},"last_daily_report_date":"","created_at":datetime.now(timezone.utc).isoformat()}
        if SHADOW_LEDGER_PATH.exists():
            try:
                old=load_json(SHADOW_LEDGER_PATH)
                if isinstance(old,dict):
                    self.data.update(old)
                    self.data.setdefault("setups",{})
                    self.data.setdefault("portfolio_windows",{})
                    self.data["version"]="2.4"
            except Exception as e:
                print("ledger load:",e,flush=True)
        # Migrate v2.3 rows in-place without losing forward-test history.
        for rec in self.data.get("setups",{}).values():
            rec.setdefault("portfolio_selected",False);rec.setdefault("benchmark_disagreement",None)
            if rec.get("entry_simulated") and rec.get("entry_ask_cents") is None and rec.get("ask_cents") is not None:
                rec["entry_ask_cents"]=rec.get("ask_cents");rec["entry_time"]=rec.get("recorded_at") or "";rec["entry_minute"]=rec.get("minute")
                rec["entry_fee_cents"]=effective_fee_cents(rec.get("ask_cents"),self.alt.primary.config,1.0);rec["entry_net_edge_points"]=rec.get("edge_points")
            if rec.get("resolved") and rec.get("entry_simulated") and rec.get("gross_pnl_cents") is not None and rec.get("net_pnl_cents") is None:
                fee=float(rec.get("entry_fee_cents") or effective_fee_cents(rec.get("entry_ask_cents") or rec.get("ask_cents"),self.alt.primary.config,1.0))
                rec["estimated_fee_pnl_cents"]=fee;rec["net_pnl_cents"]=float(rec.get("gross_pnl_cents"))-fee
            if rec.get("underlying_model_win") is not None and rec.get("kalshi_win") is not None:
                rec["benchmark_disagreement"]=(bool(rec.get("underlying_model_win"))!=bool(rec.get("kalshi_win")))
        self.last_resolve_check=0.0
        self.save()

    def save(self):
        with self.lock:
            save_json(SHADOW_LEDGER_PATH,self.data)

    def record_setup(self,s):
        setup_id=f"{s.get('asset')}|{s.get('window_start_utc')}"
        with self.lock:
            if setup_id in self.data["setups"]:
                return False
            wms=iso_to_ms(s.get("window_start_utc"))
            self.data["setups"][setup_id]={
                "setup_id":setup_id,"recorded_at":s.get("updated_at"),"asset":s.get("asset"),
                "window_start_utc":s.get("window_start_utc"),"window_start_ms":wms,"window_end_ms":wms+900000 if wms else None,
                "signal_status":s.get("status"),"minute":s.get("last_completed_minute"),"side":s.get("side"),
                "model_open":s.get("model_open"),"signal_binance_price":s.get("live_price"),"distance_bp":s.get("distance_bp"),
                "threshold_bp":s.get("threshold_bp"),"fair_pct":s.get("fair_pct"),"conservative_fair_pct":s.get("conservative_fair_pct"),
                "sample_n":s.get("sample_n"),"holdout_n":s.get("holdout_n"),"holdout_pct":s.get("holdout_pct"),
                "market_ticker":s.get("market_ticker"),"kalshi_target":s.get("kalshi_target"),"target_gap_bp":s.get("target_gap_bp"),
                "basis_ok":s.get("basis_ok"),"ask_cents":s.get("selected_ask_cents"),"max_buy_cents":s.get("max_buy_cents"),
                "edge_points":s.get("edge_points"),"setup_bid_cents":s.get("selected_bid_cents"),"setup_spread_cents":s.get("spread_cents"),
                "setup_fee_est_cents":s.get("estimated_taker_fee_cents"),"entry_simulated":False,"entry_alert_sent":False,"basis_alert_sent":False,
                "entry_time":"","entry_minute":None,"entry_ask_cents":None,"entry_bid_cents":None,"entry_spread_cents":None,"entry_fee_cents":None,"entry_net_edge_points":None,
                "book_available":None,"book_error":"","book_depth_at_max":None,"book_vwap_1_cents":None,"book_vwap_risk_cents":None,"book_risk_contracts":None,
                "portfolio_selected":False,"resolved":False,"kalshi_result":"","kalshi_status":"","settlement_ts":"","settlement_value_dollars":None,"underlying_final_close":None,
                "underlying_model_win":None,"kalshi_win":None,"benchmark_disagreement":None,"gross_pnl_cents":None,"estimated_fee_pnl_cents":None,"net_pnl_cents":None,"buffer_adjusted_pnl_cents":None,
                "resolution_error":""
            }
            save_json(SHADOW_LEDGER_PATH,self.data)
        return True

    def observe_state(self,s):
        """Capture the first actually executable quote for the already-counted setup.

        This fixes the v2.3 distinction between *setup detection* and *execution*:
        a setup can be confirmed while price is high and become cheap later within
        the same completed signal minute. We preserve one statistical setup per candle
        while recording the first real SHADOW ENTRY ZONE quote separately.
        """
        setup_id=f"{s.get('asset')}|{s.get('window_start_utc')}"
        with self.lock:
            rec=self.data.get("setups",{}).get(setup_id)
            if not rec:return False
            if rec.get("entry_simulated"):return False
            if s.get("status")!="SHADOW ENTRY ZONE" or s.get("basis_ok") is not True:return False
            if rec.get("minute")!=s.get("last_completed_minute"):return False
        ask=s.get("selected_ask_cents")
        if ask is None:return False
        book=self.alt.fetch_orderbook(s.get("market_ticker"))
        levels=parse_orderbook_asks(book,s.get("side")) if not book.get("_error") else []
        risk_dollars=float(self.alt.primary.config.get("bankroll_dollars",1000.0))*float(self.alt.primary.config.get("risk_cap_pct",1.0))/100.0
        per_contract=(float(ask)+effective_fee_cents(ask,self.alt.primary.config,1.0))/100.0
        risk_contracts=max(1,int(risk_dollars/per_contract)) if per_contract>0 else 1
        vwap1,fill1=orderbook_vwap(levels,1.0)
        vwapr,fillr=orderbook_vwap(levels,risk_contracts)
        maxbuy=s.get("max_buy_cents")
        depth=sum(q for p,q in levels if maxbuy is not None and p<=float(maxbuy)) if levels else None
        with self.lock:
            rec=self.data.get("setups",{}).get(setup_id)
            if not rec or rec.get("entry_simulated"):return False
            rec.update({
                "entry_simulated":True,"entry_time":s.get("updated_at"),"entry_minute":s.get("last_completed_minute"),
                "entry_ask_cents":float(ask),"entry_bid_cents":s.get("selected_bid_cents"),"entry_spread_cents":s.get("spread_cents"),
                "entry_fee_cents":effective_fee_cents(ask,self.alt.primary.config,1.0),"entry_net_edge_points":s.get("edge_points"),
                "book_available":bool(levels),"book_error":book.get("_error","") if isinstance(book,dict) else "",
                "book_depth_at_max":depth,"book_vwap_1_cents":vwap1,"book_vwap_risk_cents":vwapr,"book_risk_contracts":risk_contracts,
                "book_fill_1":fill1,"book_fill_risk":fillr,
            })
            save_json(SHADOW_LEDGER_PATH,self.data)
        return True

    def observe_portfolio(self,states):
        """Non-lookahead correlated-crypto shadow portfolio: max one pick per 15m window.

        On the first polling cycle where one or more assets are simultaneously in an
        executable ENTRY ZONE, pick the highest net edge. Later opportunities cannot
        replace it; this avoids hindsight selection.
        """
        cands=[]
        for asset,s in (states or {}).items():
            if s.get("status")!="SHADOW ENTRY ZONE":continue
            sid=f"{asset}|{s.get('window_start_utc')}"
            with self.lock:r=dict(self.data.get("setups",{}).get(sid,{}) or {})
            if r.get("entry_simulated"):
                cands.append((float(r.get("entry_net_edge_points") or -999),sid,r))
        if not cands:return None
        cands.sort(reverse=True,key=lambda x:x[0]);_,sid,best=cands[0]
        window=best.get("window_start_utc")
        with self.lock:
            pw=self.data.setdefault("portfolio_windows",{})
            if window in pw:return pw[window]
            pw[window]={"window_start_utc":window,"setup_id":sid,"asset":best.get("asset"),"selected_at":datetime.now(timezone.utc).isoformat(),"entry_net_edge_points":best.get("entry_net_edge_points")}
            if sid in self.data.get("setups",{}):self.data["setups"][sid]["portfolio_selected"]=True
            save_json(SHADOW_LEDGER_PATH,self.data)
            return dict(pw[window])

    def has_setup(self,setup_id):
        with self.lock:return setup_id in self.data.get("setups",{})

    def get_setup(self,asset,window_start_utc):
        setup_id=f"{asset}|{window_start_utc}"
        with self.lock:
            r=self.data.get("setups",{}).get(setup_id)
            return dict(r) if r else None

    def alert_was_sent(self,s):
        setup_id=f"{s.get('asset')}|{s.get('window_start_utc')}"
        field="entry_alert_sent" if s.get("status")=="SHADOW ENTRY ZONE" else "basis_alert_sent"
        with self.lock:return bool(self.data.get("setups",{}).get(setup_id,{}).get(field))

    def mark_alert_sent(self,s):
        setup_id=f"{s.get('asset')}|{s.get('window_start_utc')}"
        field="entry_alert_sent" if s.get("status")=="SHADOW ENTRY ZONE" else "basis_alert_sent"
        with self.lock:
            if setup_id in self.data.get("setups",{}):
                self.data["setups"][setup_id][field]=True
                save_json(SHADOW_LEDGER_PATH,self.data)

    def _final_binance_close(self,rec):
        model=self.alt.models.get(rec.get("asset"),{})
        symbol=model.get("symbol")
        start=rec.get("window_start_ms")
        if not symbol or start is None:return None
        url=BINANCE_BASE+"/api/v3/klines?"+urlencode({"symbol":symbol,"interval":"1m","startTime":start,"endTime":start+899999,"limit":15})
        rows=http_json(url)
        closes=[]
        for k in rows or []:
            try:
                if start<=int(k[0])<start+900000:closes.append((int(k[0]),float(k[4])))
            except Exception:pass
        closes.sort()
        return closes[-1][1] if len(closes)>=15 else None

    def _resolve_one(self,rec):
        ticker=rec.get("market_ticker")
        if not ticker:return False
        market=http_json(KALSHI_BASE+"/markets/"+ticker).get("market",{})
        result=str(market.get("result") or "").lower()
        status=str(market.get("status") or "")
        rec["kalshi_status"]=status
        rec["settlement_ts"]=market.get("settlement_ts") or rec.get("settlement_ts") or ""
        try:rec["settlement_value_dollars"]=float(market.get("settlement_value_dollars")) if market.get("settlement_value_dollars") not in (None,"") else rec.get("settlement_value_dollars")
        except Exception:pass
        final_close=self._final_binance_close(rec)
        if final_close is not None:
            rec["underlying_final_close"]=final_close
            mo=rec.get("model_open");side=rec.get("side")
            if mo is not None and side in ("UP","DOWN"):
                model_side="UP" if final_close>float(mo) else ("DOWN" if final_close<float(mo) else "TIE")
                rec["underlying_model_win"]=(model_side==side)
        final_status=(status.lower() in ("finalized","settled") or bool(market.get("settlement_ts")))
        if result not in ("yes","no") or not final_status:
            return False
        side=rec.get("side")
        win=(result=="yes" and side=="UP") or (result=="no" and side=="DOWN")
        rec["kalshi_result"]=result
        rec["kalshi_win"]=bool(win)
        rec["resolved"]=True
        rec["resolved_at"]=datetime.now(timezone.utc).isoformat()
        if rec.get("underlying_model_win") is not None:
            rec["benchmark_disagreement"]=(bool(rec.get("underlying_model_win"))!=bool(rec.get("kalshi_win")))
        if rec.get("entry_simulated") and rec.get("entry_ask_cents") is not None:
            ask=float(rec["entry_ask_cents"])
            gross=(100.0-ask) if win else -ask
            fee=float(rec.get("entry_fee_cents") or effective_fee_cents(ask,self.alt.primary.config,1.0))
            rec["gross_pnl_cents"]=gross
            rec["estimated_fee_pnl_cents"]=fee
            rec["net_pnl_cents"]=gross-fee
            rec["buffer_adjusted_pnl_cents"]=gross-float(self.alt.primary.config.get("fee_buffer_cents",1.0))
        return True

    def maybe_resolve(self):
        interval=max(10.0,float(os.getenv("SHADOW_RESOLVE_SECONDS","20")))
        if time.time()-self.last_resolve_check<interval:return
        self.last_resolve_check=time.time()
        now_ms=int(time.time()*1000)
        changed=False
        with self.lock:
            ids=[k for k,r in self.data["setups"].items() if not r.get("resolved") and r.get("window_end_ms") and now_ms>r["window_end_ms"]+15000]
        for setup_id in ids[:20]:
            with self.lock: rec=dict(self.data["setups"].get(setup_id,{}) )
            if not rec:continue
            try:
                done=self._resolve_one(rec)
                rec["resolution_error"]=""
                with self.lock:self.data["setups"][setup_id]=rec
                changed=True
                if done and rec.get("entry_simulated"):
                    self.send_resolution(rec)
            except Exception as e:
                rec["resolution_error"]=str(e)[:300]
                with self.lock:self.data["setups"][setup_id]=rec
                changed=True
            time.sleep(0.08)
        if changed:self.save()

    def send_resolution(self,rec):
        if os.getenv("SHADOW_RESOLUTION_ALERTS","1").lower() not in ("1","true","yes"):return
        icon="✅" if rec.get("kalshi_win") else "❌"
        pnl=(rec.get("net_pnl_cents") or 0)/100.0
        text=(f"{rec['asset']}15M SHADOW RESULT {icon}\n"
              f"{rec['side']} · Kalshi result {str(rec.get('kalshi_result')).upper()}\n"
              f"Entry {float(rec.get('entry_ask_cents') or 0):.1f}c · net fee-model 1 contract {pnl:+.2f}$\n"
              f"Forward-test only")
        post_telegram(telegram_token(),telegram_chat_id(self.alt.primary.config),text)

    def metrics(self,states=None):
        with self.lock: setups=[dict(x) for x in self.data.get("setups",{}).values()]
        rows=[]
        totals={"recorded_setups":len(setups),"resolved_setups":0,"resolved_entries":0,"entry_wins":0,"entry_losses":0,"entry_hit_pct":None,
                "entry_gross_pnl_dollars":0.0,"entry_net_pnl_dollars":0.0,"entry_max_drawdown_dollars":0.0,"pending_setups":0,
                "portfolio_entries":0,"portfolio_wins":0,"portfolio_losses":0,"portfolio_hit_pct":None,"portfolio_net_pnl_dollars":0.0,"portfolio_max_drawdown_dollars":0.0}
        all_entries=[];portfolio_entries=[]
        for asset,model in self.alt.models.items():
            aa=[r for r in setups if r.get("asset")==asset]
            rr=[r for r in aa if r.get("resolved")]
            cal=[r for r in rr if r.get("basis_ok") is True and r.get("kalshi_win") is not None]
            ent=[r for r in rr if r.get("entry_simulated") and r.get("kalshi_win") is not None]
            all_entries.extend(ent);portfolio_entries.extend([r for r in ent if r.get("portfolio_selected")])
            wins=sum(1 for r in ent if r.get("kalshi_win"));losses=len(ent)-wins
            calwins=sum(1 for r in cal if r.get("kalshi_win"))
            gross=sum(float(r.get("gross_pnl_cents") or 0) for r in ent)/100.0
            net=sum(float(r.get("net_pnl_cents") or 0) for r in ent)/100.0
            hit=(100*wins/len(ent)) if ent else None;calhit=(100*calwins/len(cal)) if cal else None
            wil=wilson_low(wins,len(ent)) if ent else None
            dis=[r for r in rr if r.get("benchmark_disagreement") is not None]
            disagree=(100*sum(1 for r in dis if r.get("benchmark_disagreement"))/len(dis)) if dis else None
            basis_fail=(100*sum(1 for r in aa if r.get("basis_ok") is False)/len(aa)) if aa else None
            if len(ent)<100:gate=f"COLLECTING {len(ent)}/100"
            else:
                caldelta=(calhit-model["cons_pct"]) if calhit is not None else None
                ok=(wil is not None and wil>=90 and net>0 and caldelta is not None and caldelta>=-5 and (disagree is None or disagree<=5))
                gate="REVIEW CANDIDATE" if ok else "HOLD"
            rows.append({"asset":asset,"threshold_bp":model["min_bp"],"conservative_fair_pct":model["cons_pct"],
                         "recorded_setups":len(aa),"resolved_setups":len(rr),"calibration_n":len(cal),"calibration_hit_pct":calhit,
                         "calibration_delta_points":(calhit-model["cons_pct"]) if calhit is not None else None,
                         "resolved_entries":len(ent),"entry_wins":wins,"entry_losses":losses,"entry_hit_pct":hit,"entry_wilson_low_pct":wil,
                         "entry_gross_pnl_dollars":round(gross,4),"entry_net_pnl_dollars":round(net,4),"entry_max_drawdown_dollars":max_drawdown_dollars(ent,"net_pnl_cents"),
                         "benchmark_disagreement_pct":disagree,"basis_fail_pct":basis_fail,"promotion_gate":gate})
            totals["resolved_setups"]+=len(rr);totals["resolved_entries"]+=len(ent);totals["entry_wins"]+=wins;totals["entry_losses"]+=losses
            totals["entry_gross_pnl_dollars"]+=gross;totals["entry_net_pnl_dollars"]+=net
        totals["pending_setups"]=sum(1 for r in setups if not r.get("resolved"))
        if totals["resolved_entries"]:totals["entry_hit_pct"]=100*totals["entry_wins"]/totals["resolved_entries"]
        totals["entry_max_drawdown_dollars"]=max_drawdown_dollars(all_entries,"net_pnl_cents")
        pwins=sum(1 for r in portfolio_entries if r.get("kalshi_win"));ploss=len(portfolio_entries)-pwins
        totals["portfolio_entries"]=len(portfolio_entries);totals["portfolio_wins"]=pwins;totals["portfolio_losses"]=ploss
        totals["portfolio_hit_pct"]=(100*pwins/len(portfolio_entries)) if portfolio_entries else None
        totals["portfolio_net_pnl_dollars"]=round(sum(float(r.get("net_pnl_cents") or 0) for r in portfolio_entries)/100.0,4)
        totals["portfolio_max_drawdown_dollars"]=max_drawdown_dollars(portfolio_entries,"net_pnl_cents")
        active=[]
        for asset,st in (states or self.alt.get_states()).items():
            if st.get("status")=="SHADOW ENTRY ZONE":
                active.append({"asset":asset,"side":st.get("side"),"edge_points":st.get("edge_points"),"ask_cents":st.get("selected_ask_cents"),"minute":st.get("last_completed_minute"),"spread_cents":st.get("spread_cents")})
        active.sort(key=lambda r:(r.get("edge_points") if r.get("edge_points") is not None else -999),reverse=True)
        totals["entry_gross_pnl_dollars"]=round(totals["entry_gross_pnl_dollars"],4);totals["entry_net_pnl_dollars"]=round(totals["entry_net_pnl_dollars"],4)
        return {"version":APP_VERSION,"generated_at":datetime.now(timezone.utc).isoformat(),"fee_model":"general quadratic 0.07; configured buffer is a conservative floor",
                "promotion_rule":"review only; never auto-promotes or places orders","totals":totals,"by_asset":rows,"active_ranking":active}

    def csv_bytes(self):
        import io
        fields=["setup_id","recorded_at","asset","window_start_utc","signal_status","minute","side","distance_bp","threshold_bp","fair_pct","conservative_fair_pct","market_ticker","kalshi_target","target_gap_bp","basis_ok",
                "ask_cents","setup_bid_cents","setup_spread_cents","setup_fee_est_cents","max_buy_cents","edge_points","entry_simulated","entry_time","entry_minute","entry_ask_cents","entry_bid_cents","entry_spread_cents","entry_fee_cents","entry_net_edge_points",
                "book_available","book_error","book_depth_at_max","book_vwap_1_cents","book_vwap_risk_cents","book_risk_contracts","portfolio_selected","entry_alert_sent","basis_alert_sent","resolved","kalshi_status","kalshi_result","kalshi_win","settlement_ts",
                "underlying_final_close","underlying_model_win","benchmark_disagreement","gross_pnl_cents","estimated_fee_pnl_cents","net_pnl_cents","buffer_adjusted_pnl_cents","resolution_error"]
        buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=fields,extrasaction="ignore");w.writeheader()
        with self.lock: vals=sorted(self.data.get("setups",{}).values(),key=lambda r:r.get("recorded_at") or "")
        for r in vals:w.writerow(r)
        return buf.getvalue().encode("utf-8")



class DailyShadowReporter:
    def __init__(self,ledger,monitor):
        self.ledger=ledger;self.monitor=monitor;self.stop_event=threading.Event()
    def run(self):
        while not self.stop_event.is_set():
            try:
                now=datetime.now(timezone.utc);h=int(os.getenv("DAILY_REPORT_UTC_HOUR","23"));m=int(os.getenv("DAILY_REPORT_UTC_MINUTE","55"));today=now.date().isoformat()
                if (now.hour>h or (now.hour==h and now.minute>=m)) and self.ledger.data.get("last_daily_report_date")!=today:
                    with self.ledger.lock:
                        day=[dict(r) for r in self.ledger.data.get("setups",{}).values() if str(r.get("recorded_at") or "").startswith(today)]
                    resolved=[r for r in day if r.get("resolved")]
                    entries=[r for r in resolved if r.get("entry_simulated") and r.get("kalshi_win") is not None]
                    wins=sum(1 for r in entries if r.get("kalshi_win"));losses=len(entries)-wins
                    hit="—" if not entries else f"{100*wins/len(entries):.1f}%"
                    pnl=sum(float(r.get("net_pnl_cents") or 0) for r in entries)/100.0
                    pending=sum(1 for r in day if not r.get("resolved"))
                    pentries=[r for r in entries if r.get("portfolio_selected")]
                    ppnl=sum(float(r.get("net_pnl_cents") or 0) for r in pentries)/100.0
                    text=(f"BTC15M DAILY EXECUTION LAB · {today} UTC\n"
                          f"Setups {len(day)} · resolved {len(resolved)} · entries {len(entries)}\n"
                          f"Wins {wins} · losses {losses} · hit {hit}\n"
                          f"Net simulated PnL fee-model (1 contract/entry): ${pnl:+.2f}\n"
                          f"Portfolio 1/window: {len(pentries)} entries · net ${ppnl:+.2f}\n"
                          f"Pending settlement: {pending}")
                    post_telegram(telegram_token(),telegram_chat_id(self.monitor.config),text)
                    with self.ledger.lock:self.ledger.data["last_daily_report_date"]=today
                    self.ledger.save()
            except Exception as e:print("daily reporter:",e,flush=True)
            self.stop_event.wait(30)


class HealthWatchdog:
    def __init__(self,primary,alt):
        self.primary=primary;self.alt=alt;self.stop_event=threading.Event();self.bad=False;self.last_alert=0;self.api_error_since=None
    def run(self):
        while not self.stop_event.is_set():
            try:
                now_ts=time.time()
                stale=float(os.getenv("WATCHDOG_STALE_SECONDS","60"))
                api_hold=float(os.getenv("WATCHDOG_API_HOLD_SECONDS","90"))
                rollover_grace=float(os.getenv("WATCHDOG_ROLLOVER_GRACE_SECONDS","75"))
                p=now_ts-self.primary.last_loop_at;a=now_ts-self.alt.last_loop_at
                ps=self.primary.get_state();alts=self.alt.get_states()
                bad_assets=[name for name,s in alts.items() if s.get("status")=="API ERROR" or bool(s.get("api_error"))]
                primary_api=(ps.get("status")=="API ERROR" or bool(ps.get("api_error")))
                any_api=bool(primary_api or bad_assets)
                if any_api:
                    if self.api_error_since is None:self.api_error_since=now_ts
                else:
                    self.api_error_since=None
                api_age=(now_ts-self.api_error_since) if self.api_error_since is not None else 0.0
                # Exchanges/market discovery can briefly roll between 15m contracts exactly at :00/:15/:30/:45.
                # If both loops are alive, suppress transient API warnings during the opening grace period.
                sec_into_window=int(now_ts)%900
                in_rollover_grace=(sec_into_window < rollover_grace and p<=stale and a<=stale)
                sustained_api=bool(any_api and api_age>=api_hold and not in_rollover_grace)
                is_bad=(p>stale or a>stale or sustained_api)
                if is_bad and (not self.bad or now_ts-self.last_alert>3600):
                    self.bad=True;self.last_alert=now_ts
                    parts=[]
                    if p>stale or a>stale:parts.append(f"loop stale BTC {p:.0f}s / multi {a:.0f}s")
                    if sustained_api:
                        names=(["BTC"] if primary_api else [])+bad_assets
                        parts.append("API errors >"+str(int(api_hold))+"s: "+(", ".join(names) or "unknown"))
                    post_telegram(telegram_token(),telegram_chat_id(self.primary.config),"BTC15M WATCHDOG ⚠️\n"+" · ".join(parts)+"\nPersistent condition; inspect if it continues.")
                elif not is_bad and self.bad:
                    self.bad=False;post_telegram(telegram_token(),telegram_chat_id(self.primary.config),"BTC15M WATCHDOG ✅\nData/API loops recovered.")
            except Exception as e:print("watchdog:",e,flush=True)
            self.stop_event.wait(15)


DAILY_REPORTER=None
WATCHDOG=None

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
            now_ts=time.time()
            stale=now_ts-MONITOR.last_loop_at
            alt_stale=(now_ts-ALT_MONITOR.last_loop_at) if ALT_MONITOR else None
            met=ALT_MONITOR.ledger.metrics()["totals"] if ALT_MONITOR else {}
            early_metrics=ALT_MONITOR.early.metrics() if ALT_MONITOR else {}
            early_totals=early_metrics.get("totals",{})
            early_op=early_metrics.get("operational",{})
            kalshi_age_ms=(max(0.0,(now_ts-float(getattr(MONITOR,"kalshi_last",0.0) or 0.0))*1000.0) if getattr(MONITOR,"kalshi_last",0.0) else None)
            stale_limit=float(os.getenv("WATCHDOG_STALE_SECONDS","60"))
            health_ok=bool(stale<=stale_limit and (alt_stale is None or alt_stale<=stale_limit))
            payload={
                "ok":health_ok,
                "version":APP_VERSION,
                "monitor_loop_age_seconds":round(stale,1),
                "multiasset_loop_age_seconds":round(alt_stale,1) if alt_stale is not None else None,
                "multiasset_mode":"shadow" if ALT_MONITOR else "off",
                "shadow_resolved_setups":met.get("resolved_setups"),
                "shadow_resolved_entries":met.get("resolved_entries"),
                "shadow_pending_setups":met.get("pending_setups"),
                "shadow_net_pnl_dollars":met.get("entry_net_pnl_dollars"),
                "portfolio_shadow_entries":met.get("portfolio_entries"),
                "portfolio_shadow_net_pnl_dollars":met.get("portfolio_net_pnl_dollars"),
                # Backward-compatible flat fields.
                "early_lab_recorded":early_totals.get("recorded",0),
                "early_lab_resolved":early_totals.get("resolved",0),
                "early_lab_net_pnl_dollars":early_totals.get("net_pnl_dollars",0.0),
                # v2.6 operational view.
                "early_lab":{
                    "mode":"experimental-no-entry",
                    "ledger_schema":early_metrics.get("ledger_schema"),
                    "captures_received":early_op.get("captures_received",0),
                    "captures_persisted":early_op.get("captures_persisted",0),
                    "captures_rejected":early_op.get("captures_rejected",0),
                    "duplicates_suppressed":early_op.get("duplicates_suppressed",0),
                    "pending_resolutions":max(0,int(early_totals.get("recorded",0))-int(early_totals.get("resolved",0))),
                    "last_capture_utc":early_op.get("last_capture_utc"),
                    "last_resolution_utc":early_op.get("last_resolution_utc"),
                    "last_persist_utc":early_op.get("last_persist_utc"),
                    "counter_invariant_ok":early_op.get("counter_invariant_ok"),
                    "rejections":early_op.get("rejections",{}),
                    "probability_model":"NOT_ASSIGNED",
                },
                "sources":{
                    "binance":{
                        "healthy":stale<=stale_limit,
                        "monitor_loop_age_ms":round(stale*1000.0,1),
                    },
                    "kalshi":{
                        "healthy":kalshi_age_ms is not None and kalshi_age_ms<=max(15000,float(MONITOR.config.get("kalshi_poll_seconds",2.0))*5000),
                        "book_age_ms":round(kalshi_age_ms,1) if kalshi_age_ms is not None else None,
                    },
                },
            }
            self._send(200,json.dumps(payload))
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
            self._send(200,json.dumps({"mode":"shadow-execution-lab","version":APP_VERSION,"btc":MONITOR.get_state(),"states":ALT_MONITOR.get_states() if ALT_MONITOR else {},"ranking":ALT_MONITOR.ledger.metrics().get("active_ranking",[]) if ALT_MONITOR else []}))
        elif path=="/api/multiasset/metrics":
            self._send(200,json.dumps(ALT_MONITOR.ledger.metrics() if ALT_MONITOR else {"error":"multiasset off"}))
        elif path=="/api/multiasset/ledger.csv":
            if ALT_MONITOR:self._send(200,ALT_MONITOR.ledger.csv_bytes(),"text/csv; charset=utf-8",{"Content-Disposition":f"attachment; filename={SHADOW_LEDGER_CSV_NAME}"})
            else:self._send(404,b"","text/plain")
        elif path=="/api/multiasset/journal":
            if ALT_JOURNAL_PATH.exists():self._send(200,ALT_JOURNAL_PATH.read_bytes(),"text/csv; charset=utf-8",{"Content-Disposition":"attachment; filename=multiasset_shadow_journal.csv"})
            else:self._send(404,b"","text/plain")
        elif path=="/api/early":
            self._send(200,json.dumps(ALT_MONITOR.early.metrics() if ALT_MONITOR else {"error":"early lab off"}))
        elif path=="/api/early/metrics":
            self._send(200,json.dumps(ALT_MONITOR.early.metrics() if ALT_MONITOR else {"error":"early lab off"}))
        elif path=="/api/early/ledger.csv":
            if ALT_MONITOR:self._send(200,ALT_MONITOR.early.csv_bytes(),"text/csv; charset=utf-8",{"Content-Disposition":f"attachment; filename={EARLY_LEDGER_CSV_NAME}"})
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
    global MONITOR,RESEARCH,ALT_MONITOR,DAILY_REPORTER,WATCHDOG
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
        DAILY_REPORTER=DailyShadowReporter(ALT_MONITOR.ledger,MONITOR)
        WATCHDOG=HealthWatchdog(MONITOR,ALT_MONITOR)
        threading.Thread(target=DAILY_REPORTER.run,daemon=True).start()
        threading.Thread(target=WATCHDOG.run,daemon=True).start()
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
        if DAILY_REPORTER:DAILY_REPORTER.stop_event.set()
        if WATCHDOG:WATCHDOG.stop_event.set()
        server.server_close()

if __name__=="__main__":main()
