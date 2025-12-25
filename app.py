import os
import time
import math
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, render_template_string

from core.storage import CryptoDB
from core.graph import CryptoGraph

app = Flask(__name__)

db = CryptoDB()
g = CryptoGraph()

STATE: Dict[str, Any] = {"csv_path": None, "db_loaded": False, "graph_built": False}

DEFAULT_CSV_PATH = os.environ.get("CRYPTO_CSV_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "dataset.csv"))


def ok(data=None, **extra):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return jsonify(payload)

def err(message: str, status: int = 400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status

def require_db():
    if not STATE["db_loaded"]:
        return err("DB not loaded (startup ingestion failed). Check CSV path.", 400)
    return None

def require_graph():
    if not STATE["db_loaded"]:
        return err("DB not loaded (startup ingestion failed).", 400)
    if not STATE["graph_built"]:
        return err("Graph not built (startup build failed).", 400)
    return None

def warm_start():
    """Ingest CSV + build graph at startup."""
    csv_path = (DEFAULT_CSV_PATH or "").strip()
    STATE["csv_path"] = csv_path

    if not csv_path:
        print("[warm_start] No CSV path provided.")
        return
    if not os.path.exists(csv_path):
        print(f"[warm_start] CSV not found: {csv_path}")
        return

    print(f"[warm_start] Ingesting CSV: {csv_path}")
    t0 = time.time()
    db.ingest_data(csv_path)
    t1 = time.time()
    STATE["db_loaded"] = True
    print(f"[warm_start] DB loaded: {len(db.data_store):,} records in {t1 - t0:.2f}s")

    print("[warm_start] Building graph...")
    t2 = time.time()
    g.build_graph_from_db(db)
    t3 = time.time()
    STATE["graph_built"] = True
    print(f"[warm_start] Graph built: {len(g.graph):,} nodes in {t3 - t2:.2f}s")

def parse_timestamp_str_only(ts_raw: str) -> Optional[int]:
    """
    ACCEPTED INPUT ONLY:
      - datetime string: "YYYY-MM-DD HH:MM:SS"

    Returns unix timestamp int (seconds) or None if invalid.

    NOTE: Unix timestamp numeric strings are NOT allowed.
    """
    s = (ts_raw or "").strip()
    if not s:
        return None

    if s.isdigit():
        return None

    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except ValueError:
        return None


def _iter_amounts(
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    sender_wallet: Optional[str] = None,
    receiver_wallet: Optional[str] = None,
) -> float:
    """Yield price values with optional filters. Skips deleted/None."""
    sym = (symbol or "").strip()
    st = (status or "").strip()
    sw = (sender_wallet or "").strip()
    rw = (receiver_wallet or "").strip()

    for rec in db.data_store:
        if rec is None or rec.get("__deleted__", False):
            continue

        if sym and rec.get("token") != sym:
            continue
        if st and rec.get("status") != st:
            continue
        if sw and rec.get("wallet_from") != sw:
            continue
        if rw and rec.get("wallet_to") != rw:
            continue

        v = rec.get("price", None)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        yield fv

def amount_usd_analytics(
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    sender_wallet: Optional[str] = None,
    receiver_wallet: Optional[str] = None,
    sample_size: int = 50000,
) -> Dict[str, Any]:
    """
    Streaming stats using Welford + reservoir sample for approximate quantiles.
    Avoids sorting all 1M rows.
    """
    n = 0
    mean = 0.0
    M2 = 0.0
    s = 0.0
    vmin = None
    vmax = None

    sample: List[float] = []
    rng = random.Random(1337)

    for x in _iter_amounts(symbol=symbol, status=status, sender_wallet=sender_wallet, receiver_wallet=receiver_wallet):
        n += 1
        s += x
        vmin = x if vmin is None else min(vmin, x)
        vmax = x if vmax is None else max(vmax, x)

        delta = x - mean
        mean += delta / n
        delta2 = x - mean
        M2 += delta * delta2

        if len(sample) < sample_size:
            sample.append(x)
        else:
            j = rng.randint(1, n)
            if j <= sample_size:
                sample[j - 1] = x

    if n == 0:
        return {
            "count": 0,
            "filters": {
                "crypto_symbol": symbol or "",
                "status": status or "",
                "sender_wallet": sender_wallet or "",
                "receiver_wallet": receiver_wallet or "",
            },
            "message": "No matching records."
        }

    variance_pop = (M2 / n) if n > 0 else 0.0
    std_pop = math.sqrt(variance_pop)

    sample_sorted = sorted(sample)

    def q(p: float) -> float:
        if not sample_sorted:
            return 0.0
        idx = int(round((len(sample_sorted) - 1) * p))
        idx = max(0, min(len(sample_sorted) - 1, idx))
        return sample_sorted[idx]

    return {
        "count": n,
        "sum": s,
        "min": vmin,
        "max": vmax,
        "mean": mean,
        "std_pop": std_pop,
        "approx_percentiles": {
            "p50": q(0.50),
            "p90": q(0.90),
            "p95": q(0.95),
            "p99": q(0.99),
        },
        "approx_note": f"Percentiles are estimated from a reservoir sample (size={len(sample_sorted):,})",
        "filters": {
            "crypto_symbol": symbol or "",
            "status": status or "",
            "sender_wallet": sender_wallet or "",
            "receiver_wallet": receiver_wallet or "",
        }
    }


@app.get("/api/status")
def api_status():
    return ok({
        "csv_path": STATE["csv_path"],
        "db_loaded": STATE["db_loaded"],
        "graph_built": STATE["graph_built"],
        "records_in_store": len(db.data_store),
        "graph_nodes_with_outgoing": len(g.graph),
    })


@app.get("/api/db/by_timestamp/<ts>")
def api_db_by_timestamp(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    rec = db.get_record_by_timestamp(ts_int)
    if rec is None:
        return err("record not found", 404)
    return ok(rec)

@app.get("/api/db/by_token/<symbol>")
def api_db_by_token(symbol: str):
    r = require_db()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.search_by_token(symbol):
        rows.append(rec)
        if len(rows) >= limit:
            break
    return ok({"count_returned": len(rows), "rows": rows})

@app.get("/api/db/by_sender/<wallet>")
def api_db_by_sender(wallet: str):
    r = require_db()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.search_by_sender_wallet(wallet):
        rows.append(rec)
        if len(rows) >= limit:
            break
    return ok({"count_returned": len(rows), "rows": rows})

@app.get("/api/db/range")
def api_db_range():
    r = require_db()
    if r is not None:
        return r

    start = request.args.get("start")
    end = request.args.get("end")
    if start is None or end is None:
        return err("start and end are required: /api/db/range?start=...&end=...")

    start_ts = parse_timestamp_str_only(start)
    end_ts = parse_timestamp_str_only(end)
    if start_ts is None or end_ts is None:
        return err("start/end must be 'YYYY-MM-DD HH:MM:SS'")

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.range_query_by_timestamp(start_ts, end_ts):
        rows.append(rec)
        if len(rows) >= limit:
            break

    return ok({"count_returned": len(rows), "rows": rows})


@app.post("/api/db/insert")
def api_db_insert():
    r = require_db()
    if r is not None:
        return r

    data = request.get_json(silent=True) or {}
    required = [
        "tx_id", "timestamp_str", "crypto_symbol", "amount_usd", "fee_usd",
        "sender_wallet", "receiver_wallet", "status"
    ]
    missing = [k for k in required if k not in data]
    if missing:
        return err(f"missing fields: {missing}")

    ts_int = parse_timestamp_str_only(str(data.get("timestamp_str", "")))
    if ts_int is None:
        return err("timestamp_str must be 'YYYY-MM-DD HH:MM:SS' (unix not allowed)")

    new_id = db.insert_record(data)
    if new_id is None:
        return err("insert rejected (timestamp already exists)", 409)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"record_id": new_id, "record": db.data_store[new_id]})

@app.post("/api/db/update_by_timestamp/<ts>")
def api_db_update(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    updates = request.get_json(silent=True) or {}
    if not updates:
        return err("updates JSON body required")

    updated = db.update_record_by_timestamp(ts_int, updates)
    if not updated:
        return err("record not found / deleted", 404)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"updated": True, "record": db.get_record_by_timestamp(ts_int)})

@app.post("/api/db/delete_by_timestamp/<ts>")
def api_db_delete(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    deleted = db.delete_record_by_timestamp(ts_int)
    if not deleted:
        return err("record not found / deleted", 404)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"deleted": True, "timestamp": ts_int})


@app.get("/api/graph/wallet_summary/<wallet>")
def api_wallet_summary(wallet: str):
    r = require_graph()
    if r is not None:
        return r
    return ok(g.wallet_summary(wallet))

@app.get("/api/graph/neighbors/<wallet>")
def api_neighbors(wallet: str):
    r = require_graph()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    nbrs = g.neighbors(wallet)
    preview = list(nbrs.items())[:limit]
    return ok({
        "wallet": wallet,
        "neighbors_count": len(nbrs),
        "neighbors_preview": [{"neighbor": k, "stats": v} for k, v in preview],
    })

@app.get("/api/graph/top_counterparties/<wallet>")
def api_top_counterparties(wallet: str):
    r = require_graph()
    if r is not None:
        return r

    k = request.args.get("k", "10")
    by = request.args.get("by", "total_amt")
    direction = request.args.get("direction", "both")

    try:
        k = max(1, min(50, int(k)))
    except ValueError:
        k = 10

    rows = g.top_counterparties(wallet, k=k, by=by, direction=direction)
    return ok({"wallet": wallet, "rows": rows})

@app.get("/api/graph/shortest_path")
def api_shortest_path():
    r = require_graph()
    if r is not None:
        return r

    src = (request.args.get("src") or "").strip()
    dst = (request.args.get("dst") or "").strip()
    if not src or not dst:
        return err("src and dst required: /api/graph/shortest_path?src=...&dst=...")

    path = g.shortest_path(src, dst)
    if path is None:
        return err("no path found (or src not in graph)", 404)

    return ok({"length": len(path) - 1, "path": path})


@app.get("/api/stats/pair_stats")
def api_pair_stats():
    r = require_graph()
    if r is not None:
        return r

    a = (request.args.get("a") or "").strip()
    b = (request.args.get("b") or "").strip()
    directed = (request.args.get("directed") or "false").strip().lower() in {"1", "true", "yes", "y"}

    if not a or not b:
        return err("a and b required: /api/stats/pair_stats?a=W0001&b=W0002&directed=false")

    out = None
    if hasattr(g, "pair_stats"):
        out = g.pair_stats(a, b, directed=directed)

    if out is None:
        return err("no relationship found for these wallets", 404)

    if isinstance(out, dict):
        out.pop("a", None)
        out.pop("b", None)
        out.pop("directed", None)

    return ok(out)

@app.get("/api/stats/wallet_currency_breakdown/<wallet>")
def api_wallet_currency_breakdown(wallet: str):
    r = require_graph()
    if r is not None:
        return r
    out = g.wallet_currency_breakdown(wallet)
    return ok(out)

@app.get("/api/stats/top_wallet_by_currency")
def api_top_wallet_by_currency():
    r = require_graph()
    if r is not None:
        return r

    symbol = (request.args.get("symbol") or "").strip()
    by = (request.args.get("by") or "count").strip().lower()

    if not symbol:
        return err("symbol required: /api/stats/top_wallet_by_currency?symbol=BTC&by=count")

    out = g.top_wallet_by_currency(symbol, by=by)
    if out is None:
        return err("no wallet found for this symbol (or symbol not present)", 404)

    return ok(out)

@app.get("/api/stats/amount_usd_analytics")
def api_amount_usd_analytics():
    r = require_db()
    if r is not None:
        return r

    symbol = (request.args.get("symbol") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    sender_wallet = (request.args.get("sender_wallet") or "").strip() or None
    receiver_wallet = (request.args.get("receiver_wallet") or "").strip() or None

    out = amount_usd_analytics(
        symbol=symbol,
        status=status,
        sender_wallet=sender_wallet,
        receiver_wallet=receiver_wallet,
    )
    return ok(out)


HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>CryptoDB UI</title>
  <style>
    :root{
      --bg:#041a3a;
      --white:#ffffff;
      --muted:rgba(255,255,255,.75);
      --line:rgba(255,255,255,.18);
      --shadow:0 18px 60px rgba(0,0,0,.35);
      --ink:#0b1b33;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      min-height:100vh;
      background: radial-gradient(1200px 900px at 20% 10%, #0b2c66 0%, var(--bg) 55%) fixed;
      color:var(--white);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
      display:flex;
      align-items:center;
      justify-content:center;
      padding:24px;
    }
    .stage{width:min(860px, 96vw);display:flex;align-items:center;justify-content:center;}
    .panel{
      width:min(720px, 96vw);
      background: rgba(255,255,255,.06);
      border:1px solid rgba(255,255,255,.14);
      border-radius:22px;
      box-shadow: var(--shadow);
      padding:22px;
      backdrop-filter: blur(10px);
    }
    .center{text-align:center;}
    .btnGrid{margin-top:18px;display:grid;gap:12px;}
    .btn{
      width:100%;
      border:none;
      border-radius:16px;
      padding:14px 16px;
      background:var(--white);
      color:var(--ink);
      font-weight:900;
      font-size:15px;
      cursor:pointer;
      transition: transform .08s ease, filter .08s ease;
    }
    .btn:hover{ filter: brightness(.96); }
    .btn:active{ transform: translateY(1px); }
    .btn.secondary{
      background: rgba(255,255,255,.12);
      color: var(--white);
      border: 1px solid rgba(255,255,255,.22);
    }
    .pill{
      display:inline-block;
      padding:6px 12px;
      border-radius:999px;
      border:1px solid rgba(255,255,255,.20);
      background: rgba(255,255,255,.08);
      color: var(--muted);
      font-size:12px;
      font-weight:800;
    }
    .smallBtnRow{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      justify-content:center;
      margin-top:14px;
    }
    .smallBtnRow .btn{ width:auto; padding:10px 12px; border-radius:14px; font-size:13px; }

    .form{margin-top:14px;display:grid;gap:12px;text-align:left;}
    label{font-size:12px;color:var(--muted);font-weight:800;}
    input, select{
      width:100%;
      padding:12px 12px;
      border-radius:14px;
      border:1px solid rgba(255,255,255,.18);
      background: rgba(2,10,25,.55);
      color: var(--white);
      outline:none;
      font-size:14px;
    }
    input::placeholder{ color: rgba(255,255,255,.38); }
    input:focus, select:focus{
      border-color: rgba(255,255,255,.40);
      box-shadow: 0 0 0 4px rgba(255,255,255,.08);
    }
    .two{display:grid;gap:12px;grid-template-columns: 1fr 1fr;}
    @media (max-width: 620px){ .two{ grid-template-columns: 1fr; } }

    .results{
      margin-top:16px;
      border-top:1px solid rgba(255,255,255,.18);
      padding-top:14px;
    }
    .msg{color:var(--muted);font-size:13px;line-height:1.5;margin-top:8px;}
    .alert{
      margin-top:12px;
      padding:12px 12px;
      border-radius:14px;
      font-weight:900;
      font-size:13px;
      border:1px solid rgba(255,255,255,.20);
      background: rgba(255,255,255,.08);
    }
    .alert.err{ border-color: rgba(255,107,107,.55); background: rgba(255,107,107,.12); color:#ffd4d4; }
    .alert.ok{ border-color: rgba(46,229,157,.55); background: rgba(46,229,157,.10); color:#caffea; }

    table{
      width:100%;
      border-collapse: collapse;
      margin-top:10px;
      overflow:hidden;
      border-radius:14px;
      border:1px solid rgba(255,255,255,.14);
    }
    th, td{
      padding:10px 10px;
      border-bottom:1px solid rgba(255,255,255,.12);
      text-align:left;
      font-size:13px;
      color: rgba(255,255,255,.92);
    }
    th{ color: var(--muted); font-size:12px; }
    tr:last-child td{ border-bottom:none; }
  </style>
</head>

<body>
  <div class="stage">
    <div class="panel" id="screen"></div>
  </div>

<script>
  function el(id){ return document.getElementById(id); }

  function esc(s){
    return (s ?? "").toString().replace(/[&<>"']/g, m => ({
      "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
    }[m]));
  }

  function renderKV(title, obj){
    const rows = Object.entries(obj || {}).map(([k,v]) => {
      const val = (typeof v === "number")
        ? v.toLocaleString(undefined,{maximumFractionDigits:2})
        : (v ?? "");
      return `<tr><th>${esc(k)}</th><td>${esc(val)}</td></tr>`;
    }).join("");
    return `
      <div class="results">
        <div class="pill">${esc(title)}</div>
        <table><tbody>${rows || `<tr><td colspan="2">No data</td></tr>`}</tbody></table>
      </div>
    `;
  }

  function renderTable(title, rows, cols){
    const head = cols.map(c => `<th>${esc(c)}</th>`).join("");
    const body = (rows || []).map(r => {
      const tds = cols.map(c => {
        const v = r[c];
        const val = (typeof v === "number")
          ? v.toLocaleString(undefined,{maximumFractionDigits:2})
          : (v ?? "");
        return `<td>${esc(val)}</td>`;
      }).join("");
      return `<tr>${tds}</tr>`;
    }).join("");

    return `
      <div class="results">
        <div class="pill">${esc(title)}</div>
        <table>
          <thead><tr>${head}</tr></thead>
          <tbody>${body || `<tr><td colspan="${cols.length}">No rows</td></tr>`}</tbody>
        </table>
      </div>
    `;
  }

  function alertBox(kind, text){
    const cls = kind === "ok" ? "ok" : "err";
    return `<div class="alert ${cls}">${esc(text)}</div>`;
  }

  async function apiGet(url){
    const res = await fetch(url);
    const data = await res.json().catch(()=>({}));
    return { http: res.status, ...data };
  }

  async function apiPost(url, body){
    const res = await fetch(url, {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(body||{})
    });
    const data = await res.json().catch(()=>({}));
    return { http: res.status, ...data };
  }

  let MODE = "home";
  let CATEGORY = null;
  let FUNC = null;

  function goHome(){ MODE="home"; CATEGORY=null; FUNC=null; draw(); }
  function goSubmenu(cat){ MODE="submenu"; CATEGORY=cat; FUNC=null; draw(); }
  function goFunction(cat, fn){ MODE="function"; CATEGORY=cat; FUNC=fn; draw(); }

  function draw(){
    if (MODE === "home") return drawHome();
    if (MODE === "submenu") return drawSubmenu(CATEGORY);
    return drawFunction(CATEGORY, FUNC);
  }

  function drawHome(){
    el("screen").innerHTML = `
      <div class="center">
        <div class="pill">Choose a category</div>
        <div class="btnGrid" style="margin-top:18px;">
          <button class="btn" onclick="goSubmenu('graph')">Wallet Interactions</button>
          <button class="btn" onclick="goFunction('stats','amount_usd_analytics')">Statistics</button>
          <button class="btn" onclick="goSubmenu('search')">DB Search</button>
          <button class="btn" onclick="goSubmenu('edit')">DB Edit</button>
        </div>
      </div>
    `;
  }

  function drawSubmenu(cat){
    let title = "";
    let buttons = "";

    if (cat === "graph"){
      title = "Wallet Interactions";
      buttons = `
        <button class="btn" onclick="goFunction('graph','wallet_summary')">Wallet Summary</button>
        <button class="btn" onclick="goFunction('graph','neighbors')">Neighbors</button>
        <button class="btn" onclick="goFunction('graph','top_counterparties')">Top Counterparties</button>
        <button class="btn" onclick="goFunction('graph','shortest_path')">Shortest Path</button>

        <button class="btn" onclick="goFunction('graph','pair_stats')">Pair Stats</button>
        <button class="btn" onclick="goFunction('graph','wallet_currency_breakdown')">Wallet Currency Breakdown</button>
        <button class="btn" onclick="goFunction('graph','top_wallet_by_currency')">Top Wallet by Currency</button>
      `;
    } else if (cat === "search"){
      title = "DB Search";
      buttons = `
        <button class="btn" onclick="goFunction('search','by_timestamp')">Get by Timestamp</button>
        <button class="btn" onclick="goFunction('search','by_token')">Search by Token</button>
        <button class="btn" onclick="goFunction('search','by_sender')">Search by Sender</button>
        <button class="btn" onclick="goFunction('search','range')">Range Query</button>
      `;
    } else {
      title = "DB Edit";
      buttons = `
        <button class="btn" onclick="goFunction('edit','insert')">Insert Record</button>
        <button class="btn" onclick="goFunction('edit','update')">Update by Timestamp</button>
        <button class="btn" onclick="goFunction('edit','delete')">Delete by Timestamp</button>
      `;
    }

    el("screen").innerHTML = `
      <div class="center">
        <div class="pill">${esc(title)}</div>
        <div class="btnGrid">${buttons}</div>
        <div class="smallBtnRow">
          <button class="btn secondary" onclick="goHome()">← Back</button>
        </div>
      </div>
    `;
  }

  function drawFunction(cat, fn){
    const title = (cat === "graph") ? "WALLET INTERACTIONS" : cat.toUpperCase();
    const header = `
      <div class="center">
        <div class="pill">${esc(title)} • ${esc(fn.replaceAll('_',' '))}</div>
        <div class="smallBtnRow">
          <button class="btn secondary" onclick="${cat==='stats' ? 'goHome()' : `goSubmenu('${esc(cat)}')`}">← Back</button>
          <button class="btn secondary" onclick="goHome()">Home</button>
        </div>
      </div>
    `;

    let form = "";
    let runBtn = "";
    let results = `<div class="results"><div class="msg">Run the operation to see results.</div></div>`;

    // ---- NOTE: analytics is ONLY under stats now (not graph) ----

    if (cat === "graph" && fn === "pair_stats"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>Wallet A</label>
              <input id="a" placeholder="W000001"/>
            </div>
            <div>
              <label>Wallet B</label>
              <input id="b" placeholder="W000002"/>
            </div>
          </div>
          <div>
            <label>Direction</label>
            <select id="directed">
              <option value="false" selected>A ↔ B</option>
              <option value="true">A → B</option>
            </select>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_pair_stats()">Run</button>`;
    }

    if (cat === "graph" && fn === "wallet_currency_breakdown"){
      form = `
        <div class="form">
          <div>
            <label>Wallet</label>
            <input id="wallet" placeholder="W001817"/>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_wallet_currency_breakdown()">Run</button>`;
    }

    if (cat === "graph" && fn === "top_wallet_by_currency"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>Symbol</label>
              <input id="symbol" placeholder="BTC"/>
            </div>
            <div>
              <label>By</label>
              <select id="by">
                <option value="count" selected>count</option>
                <option value="amount">amount</option>
              </select>
            </div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_top_wallet_by_currency()">Run</button>`;
    }

    // analytics ONLY here:
    if (cat === "stats" && fn === "amount_usd_analytics"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>crypto_symbol (optional)</label>
              <input id="symbol" placeholder="BTC"/>
            </div>
            <div>
              <label>status (optional)</label>
              <input id="status" placeholder="pending"/>
            </div>
          </div>
          <div class="two">
            <div>
              <label>sender_wallet (optional)</label>
              <input id="sender_wallet" placeholder="W001817"/>
            </div>
            <div>
              <label>receiver_wallet (optional)</label>
              <input id="receiver_wallet" placeholder="W000999"/>
            </div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_amount_usd_analytics()">Run</button>`;
    }

    if (cat === "graph" && fn === "wallet_summary"){
      form = `<div class="form"><div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_wallet_summary()">Run</button>`;
    }

    if (cat === "graph" && fn === "neighbors"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div>
            <div><label>Limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_neighbors()">Run</button>`;
    }

    if (cat === "graph" && fn === "top_counterparties"){
      form = `
        <div class="form">
          <div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div>
          <div class="two">
            <div><label>k</label><input id="k" placeholder="10"/></div>
            <div>
              <label>Sort by</label>
              <select id="by">
                <option value="total_amt">total_amt</option>
                <option value="count">count</option>
                <option value="total_fee">total_fee</option>
              </select>
            </div>
          </div>
          <div>
            <label>Direction</label>
            <select id="dir">
              <option value="both">both</option>
              <option value="out">out</option>
              <option value="in">in</option>
            </select>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_top_counterparties()">Run</button>`;
    }

    if (cat === "graph" && fn === "shortest_path"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>Source wallet</label><input id="src" placeholder="W000001"/></div>
            <div><label>Destination wallet</label><input id="dst" placeholder="W000999"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_shortest_path()">Run</button>`;
    }

    if (cat === "search" && fn === "by_timestamp"){
      form = `<div class="form"><div><label>timestamp</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_by_timestamp()">Run</button>`;
    }

    if (cat === "search" && fn === "by_token"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>crypto_symbol</label><input id="sym" placeholder="BTC"/></div>
            <div><label>limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_by_token()">Run</button>`;
    }

    if (cat === "search" && fn === "by_sender"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>sender_wallet</label><input id="wallet" placeholder="W001817"/></div>
            <div><label>limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_by_sender()">Run</button>`;
    }

    if (cat === "search" && fn === "range"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>start</label><input id="start" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
            <div><label>end</label><input id="end" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          </div>
          <div><label>limit</label><input id="limit" placeholder="50"/></div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_range()">Run</button>`;
    }

    if (cat === "edit" && fn === "insert"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>tx_id</label><input id="tx_id" placeholder="999999999"/></div>
            <div><label>timestamp_str</label><input id="timestamp_str" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          </div>
          <div class="two">
            <div><label>crypto_symbol</label><input id="crypto_symbol" placeholder="BTC"/></div>
            <div><label>status</label><input id="status" placeholder="pending"/></div>
          </div>
          <div class="two">
            <div><label>amount_usd</label><input id="amount_usd" placeholder="1234.56"/></div>
            <div><label>fee_usd</label><input id="fee_usd" placeholder="2.34"/></div>
          </div>
          <div class="two">
            <div><label>sender_wallet</label><input id="sender_wallet" placeholder="WTEST_FROM"/></div>
            <div><label>receiver_wallet</label><input id="receiver_wallet" placeholder="WTEST_TO"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_insert()">Run</button>`;
    }

    if (cat === "edit" && fn === "update"){
      form = `
        <div class="form">
          <div><label>timestamp (record key)</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          <div class="two">
            <div><label>amount_usd (optional)</label><input id="amount_usd" placeholder=""/></div>
            <div><label>fee_usd (optional)</label><input id="fee_usd" placeholder=""/></div>
          </div>
          <div class="two">
            <div><label>status (optional)</label><input id="status" placeholder=""/></div>
            <div><label>crypto_symbol (optional)</label><input id="crypto_symbol" placeholder=""/></div>
          </div>
          <div class="two">
            <div><label>sender_wallet (optional)</label><input id="sender_wallet" placeholder=""/></div>
            <div><label>receiver_wallet (optional)</label><input id="receiver_wallet" placeholder=""/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_update()">Run</button>`;
    }

    if (cat === "edit" && fn === "delete"){
      form = `<div class="form"><div><label>timestamp</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_delete()">Run</button>`;
    }

    el("screen").innerHTML = `
      ${header}
      ${form}
      <div class="smallBtnRow" style="margin-top:14px;">
        ${runBtn}
        <button class="btn secondary" onclick="clearResults()">Clear</button>
      </div>
      <div id="resultsHost">${results}</div>
    `;
  }

  function clearResults(){
    const host = el("resultsHost");
    if(host) host.innerHTML = `<div class="results"><div class="msg">Run the operation to see results.</div></div>`;
  }

  function setResults(html){
    el("resultsHost").innerHTML = html;
  }

  function numOrDefault(s, d){
    const n = parseInt((s||"").trim(), 10);
    return Number.isFinite(n) ? n : d;
  }

  async function run_wallet_summary(){
    const w = (el("wallet").value || "").trim();
    const r = await apiGet(`/api/graph/wallet_summary/${encodeURIComponent(w)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Wallet summary", r.data));
  }

  async function run_neighbors(){
    const w = (el("wallet").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/graph/neighbors/${encodeURIComponent(w)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    const rows = (r.data.neighbors_preview || []).map(x => ({
      neighbor: x.neighbor,
      count: x.stats.count,
      total_amt: x.stats.total_amt,
      total_fee: x.stats.total_fee
    }));
    setResults(renderTable(`Neighbors (total=${r.data.neighbors_count})`, rows, ["neighbor","count","total_amt","total_fee"]));
  }

  async function run_top_counterparties(){
    const w = (el("wallet").value || "").trim();
    const k = numOrDefault(el("k").value, 10);
    const by = el("by").value;
    const dir = el("dir").value;

    const r = await apiGet(`/api/graph/top_counterparties/${encodeURIComponent(w)}?k=${encodeURIComponent(k)}&by=${encodeURIComponent(by)}&direction=${encodeURIComponent(dir)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      wallet: x.wallet,
      count: x.count,
      total_amt: x.total_amt,
      total_fee: x.total_fee,
      avg_amt: x.avg_amt
    }));
    setResults(renderTable("Top counterparties", rows, ["wallet","count","total_amt","total_fee","avg_amt"]));
  }

  async function run_shortest_path(){
    const src = (el("src").value || "").trim();
    const dst = (el("dst").value || "").trim();
    const r = await apiGet(`/api/graph/shortest_path?src=${encodeURIComponent(src)}&dst=${encodeURIComponent(dst)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const path = (r.data.path || []).join(" → ");
    setResults(`
      <div class="results">
        <div class="pill">Shortest path</div>
        <div class="msg">Length: <b>${esc(r.data.length)}</b></div>
        <div class="msg" style="margin-top:10px;color:rgba(255,255,255,.92);font-weight:900;">${esc(path)}</div>
      </div>
    `);
  }

  async function run_pair_stats(){
    const a = (el("a").value || "").trim();
    const b = (el("b").value || "").trim();
    const directed = (el("directed").value || "false").trim();

    const r = await apiGet(`/api/stats/pair_stats?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}&directed=${encodeURIComponent(directed)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Pair stats", r.data));
  }

  async function run_wallet_currency_breakdown(){
    const w = (el("wallet").value || "").trim();
    const r = await apiGet(`/api/stats/wallet_currency_breakdown/${encodeURIComponent(w)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const counts = r.data.counts || {};
    const amounts = r.data.amounts || {};

    const syms = new Set([...Object.keys(counts), ...Object.keys(amounts)]);
    const rows = Array.from(syms).map(sym => ({
      symbol: sym,
      count: counts[sym] ?? 0,
      total_amount: amounts[sym] ?? 0
    })).sort((x,y) => (y.total_amount - x.total_amount));

    setResults(renderTable("Wallet currency breakdown", rows, ["symbol","count","total_amount"]));
  }

  async function run_top_wallet_by_currency(){
    const symbol = (el("symbol").value || "").trim();
    const by = (el("by").value || "count").trim();

    const r = await apiGet(`/api/stats/top_wallet_by_currency?symbol=${encodeURIComponent(symbol)}&by=${encodeURIComponent(by)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Top wallet by currency", r.data));
  }

  async function run_amount_usd_analytics(){
    const symbol = (el("symbol").value || "").trim();
    const status = (el("status").value || "").trim();
    const sender_wallet = (el("sender_wallet").value || "").trim();
    const receiver_wallet = (el("receiver_wallet").value || "").trim();

    const qs = new URLSearchParams();
    if(symbol) qs.set("symbol", symbol);
    if(status) qs.set("status", status);
    if(sender_wallet) qs.set("sender_wallet", sender_wallet);
    if(receiver_wallet) qs.set("receiver_wallet", receiver_wallet);

    const r = await apiGet(`/api/stats/amount_usd_analytics?${qs.toString()}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const data = r.data || {};
    const main = {
      count: data.count,
      sum: data.sum,
      min: data.min,
      max: data.max,
      mean: data.mean,
      std_pop: data.std_pop
    };

    const p = (data.approx_percentiles || {});
    const pct = {
      p50: p.p50, p90: p.p90, p95: p.p95, p99: p.p99,
      approx_note: data.approx_note || ""
    };

    setResults(
      renderKV("amount_usd analytics", main) +
      renderKV("approx percentiles", pct) +
      renderKV("filters", data.filters || {})
    );
  }

  async function run_by_timestamp(){
    const ts = (el("ts").value || "").trim();
    const r = await apiGet(`/api/db/by_timestamp/${encodeURIComponent(ts)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Record", r.data));
  }

  async function run_by_token(){
    const sym = (el("sym").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/db/by_token/${encodeURIComponent(sym)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      sender_wallet: x.sender_wallet,
      receiver_wallet: x.receiver_wallet,
      status: x.status
    }));
    setResults(renderTable(`Token: ${sym} (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","sender_wallet","receiver_wallet","status"]));
  }

  async function run_by_sender(){
    const w = (el("wallet").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/db/by_sender/${encodeURIComponent(w)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      receiver_wallet: x.receiver_wallet,
      status: x.status
    }));
    setResults(renderTable(`Sender: ${w} (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","receiver_wallet","status"]));
  }

  async function run_range(){
    const start = (el("start").value || "").trim();
    const end = (el("end").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);

    const r = await apiGet(`/api/db/range?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      sender_wallet: x.sender_wallet,
      receiver_wallet: x.receiver_wallet,
    }));
    setResults(renderTable(`Range (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","sender_wallet","receiver_wallet"]));
  }

  async function run_insert(){
    const body = {
      tx_id: Number((el("tx_id").value || "").trim()),
      timestamp_str: (el("timestamp_str").value || "").trim(),
      crypto_symbol: (el("crypto_symbol").value || "").trim(),
      amount_usd: Number((el("amount_usd").value || "").trim()),
      fee_usd: Number((el("fee_usd").value || "").trim()),
      sender_wallet: (el("sender_wallet").value || "").trim(),
      receiver_wallet: (el("receiver_wallet").value || "").trim(),
      status: (el("status").value || "").trim(),
    };
    const r = await apiPost("/api/db/insert", body);
    if(!r.ok) return setResults(alertBox("err", r.error || "Insert failed"));
    setResults(alertBox("ok", "Inserted successfully") + renderKV("Inserted record", r.data.record));
  }

  async function run_update(){
    const ts = (el("ts").value || "").trim();
    const updates = {};

    const a = (el("amount_usd").value || "").trim();
    const f = (el("fee_usd").value || "").trim();
    const st = (el("status").value || "").trim();
    const cs = (el("crypto_symbol").value || "").trim();
    const sw = (el("sender_wallet").value || "").trim();
    const rw = (el("receiver_wallet").value || "").trim();

    if(a !== "") updates["amount_usd"] = Number(a);
    if(f !== "") updates["fee_usd"] = Number(f);
    if(st !== "") updates["status"] = st;
    if(cs !== "") updates["crypto_symbol"] = cs;
    if(sw !== "") updates["sender_wallet"] = sw;
    if(rw !== "") updates["receiver_wallet"] = rw;

    const r = await apiPost(`/api/db/update_by_timestamp/${encodeURIComponent(ts)}`, updates);
    if(!r.ok) return setResults(alertBox("err", r.error || "Update failed"));
    setResults(alertBox("ok", "Updated successfully") + renderKV("Updated record", r.data.record));
  }

  async function run_delete(){
    const ts = (el("ts").value || "").trim();
    const r = await apiPost(`/api/db/delete_by_timestamp/${encodeURIComponent(ts)}`, {});
    if(!r.ok) return setResults(alertBox("err", r.error || "Delete failed"));
    setResults(alertBox("ok", `Deleted timestamp=${r.data.timestamp}`));
  }

  goHome();
</script>
</body>
</html>
"""

@app.get("/")
def home():
    return render_template_string(HTML)

if __name__ == "__mimport os
import time
import math
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, render_template_string

from cryptodb import CryptoDB
from cryptograph import CryptoGraph

app = Flask(__name__)

db = CryptoDB()
g = CryptoGraph()

STATE: Dict[str, Any] = {"csv_path": None, "db_loaded": False, "graph_built": False}

DEFAULT_CSV_PATH = os.environ.get(
    "CRYPTO_CSV_PATH",
    "/Users/linamejlumyan/Desktop/crypto_transactions_1M_pairs_max50_wallet220.csv",
)


def ok(data=None, **extra):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return jsonify(payload)

def err(message: str, status: int = 400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status

def require_db():
    if not STATE["db_loaded"]:
        return err("DB not loaded (startup ingestion failed). Check CSV path.", 400)
    return None

def require_graph():
    if not STATE["db_loaded"]:
        return err("DB not loaded (startup ingestion failed).", 400)
    if not STATE["graph_built"]:
        return err("Graph not built (startup build failed).", 400)
    return None

def warm_start():
    """Ingest CSV + build graph at startup."""
    csv_path = (DEFAULT_CSV_PATH or "").strip()
    STATE["csv_path"] = csv_path

    if not csv_path:
        print("[warm_start] No CSV path provided.")
        return
    if not os.path.exists(csv_path):
        print(f"[warm_start] CSV not found: {csv_path}")
        return

    print(f"[warm_start] Ingesting CSV: {csv_path}")
    t0 = time.time()
    db.ingest_data(csv_path)
    t1 = time.time()
    STATE["db_loaded"] = True
    print(f"[warm_start] DB loaded: {len(db.data_store):,} records in {t1 - t0:.2f}s")

    print("[warm_start] Building graph...")
    t2 = time.time()
    g.build_graph_from_db(db)
    t3 = time.time()
    STATE["graph_built"] = True
    print(f"[warm_start] Graph built: {len(g.graph):,} nodes in {t3 - t2:.2f}s")

def parse_timestamp_str_only(ts_raw: str) -> Optional[int]:
    """
    ACCEPTED INPUT ONLY:
      - datetime string: "YYYY-MM-DD HH:MM:SS"

    Returns unix timestamp int (seconds) or None if invalid.

    NOTE: Unix timestamp numeric strings are NOT allowed.
    """
    s = (ts_raw or "").strip()
    if not s:
        return None

    if s.isdigit():
        return None

    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp())
    except ValueError:
        return None


def _iter_amounts(
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    sender_wallet: Optional[str] = None,
    receiver_wallet: Optional[str] = None,
) -> float:
    """Yield amount_usd values with optional filters. Skips deleted/None."""
    sym = (symbol or "").strip()
    st = (status or "").strip()
    sw = (sender_wallet or "").strip()
    rw = (receiver_wallet or "").strip()

    for rec in db.data_store:
        if rec is None or rec.get("__deleted__", False):
            continue

        if sym and rec.get("crypto_symbol") != sym:
            continue
        if st and rec.get("status") != st:
            continue
        if sw and rec.get("sender_wallet") != sw:
            continue
        if rw and rec.get("receiver_wallet") != rw:
            continue

        v = rec.get("amount_usd", None)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        yield fv

def amount_usd_analytics(
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    sender_wallet: Optional[str] = None,
    receiver_wallet: Optional[str] = None,
    sample_size: int = 50000,
) -> Dict[str, Any]:
    """
    Streaming stats using Welford + reservoir sample for approximate quantiles.
    Avoids sorting all 1M rows.
    """
    n = 0
    mean = 0.0
    M2 = 0.0
    s = 0.0
    vmin = None
    vmax = None

    sample: List[float] = []
    rng = random.Random(1337)

    for x in _iter_amounts(symbol=symbol, status=status, sender_wallet=sender_wallet, receiver_wallet=receiver_wallet):
        n += 1
        s += x
        vmin = x if vmin is None else min(vmin, x)
        vmax = x if vmax is None else max(vmax, x)

        delta = x - mean
        mean += delta / n
        delta2 = x - mean
        M2 += delta * delta2

        if len(sample) < sample_size:
            sample.append(x)
        else:
            j = rng.randint(1, n)
            if j <= sample_size:
                sample[j - 1] = x

    if n == 0:
        return {
            "count": 0,
            "filters": {
                "crypto_symbol": symbol or "",
                "status": status or "",
                "sender_wallet": sender_wallet or "",
                "receiver_wallet": receiver_wallet or "",
            },
            "message": "No matching records."
        }

    variance_pop = (M2 / n) if n > 0 else 0.0
    std_pop = math.sqrt(variance_pop)

    sample_sorted = sorted(sample)

    def q(p: float) -> float:
        if not sample_sorted:
            return 0.0
        idx = int(round((len(sample_sorted) - 1) * p))
        idx = max(0, min(len(sample_sorted) - 1, idx))
        return sample_sorted[idx]

    return {
        "count": n,
        "sum": s,
        "min": vmin,
        "max": vmax,
        "mean": mean,
        "std_pop": std_pop,
        "approx_percentiles": {
            "p50": q(0.50),
            "p90": q(0.90),
            "p95": q(0.95),
            "p99": q(0.99),
        },
        "approx_note": f"Percentiles are estimated from a reservoir sample (size={len(sample_sorted):,})",
        "filters": {
            "crypto_symbol": symbol or "",
            "status": status or "",
            "sender_wallet": sender_wallet or "",
            "receiver_wallet": receiver_wallet or "",
        }
    }


@app.get("/api/status")
def api_status():
    return ok({
        "csv_path": STATE["csv_path"],
        "db_loaded": STATE["db_loaded"],
        "graph_built": STATE["graph_built"],
        "records_in_store": len(db.data_store),
        "graph_nodes_with_outgoing": len(g.graph),
    })


@app.get("/api/db/by_timestamp/<ts>")
def api_db_by_timestamp(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    rec = db.get_record_by_timestamp(ts_int)
    if rec is None:
        return err("record not found", 404)
    return ok(rec)

@app.get("/api/db/by_token/<symbol>")
def api_db_by_token(symbol: str):
    r = require_db()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.search_by_token(symbol):
        rows.append(rec)
        if len(rows) >= limit:
            break
    return ok({"count_returned": len(rows), "rows": rows})

@app.get("/api/db/by_sender/<wallet>")
def api_db_by_sender(wallet: str):
    r = require_db()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.search_by_sender_wallet(wallet):
        rows.append(rec)
        if len(rows) >= limit:
            break
    return ok({"count_returned": len(rows), "rows": rows})

@app.get("/api/db/range")
def api_db_range():
    r = require_db()
    if r is not None:
        return r

    start = request.args.get("start")
    end = request.args.get("end")
    if start is None or end is None:
        return err("start and end are required: /api/db/range?start=...&end=...")

    start_ts = parse_timestamp_str_only(start)
    end_ts = parse_timestamp_str_only(end)
    if start_ts is None or end_ts is None:
        return err("start/end must be 'YYYY-MM-DD HH:MM:SS'")

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    rows: List[Dict[str, Any]] = []
    for rec in db.range_query_by_timestamp(start_ts, end_ts):
        rows.append(rec)
        if len(rows) >= limit:
            break

    return ok({"count_returned": len(rows), "rows": rows})


@app.post("/api/db/insert")
def api_db_insert():
    r = require_db()
    if r is not None:
        return r

    data = request.get_json(silent=True) or {}
    required = [
        "tx_id", "timestamp_str", "crypto_symbol", "amount_usd", "fee_usd",
        "sender_wallet", "receiver_wallet", "status"
    ]
    missing = [k for k in required if k not in data]
    if missing:
        return err(f"missing fields: {missing}")

    ts_int = parse_timestamp_str_only(str(data.get("timestamp_str", "")))
    if ts_int is None:
        return err("timestamp_str must be 'YYYY-MM-DD HH:MM:SS' (unix not allowed)")

    new_id = db.insert_record(data)
    if new_id is None:
        return err("insert rejected (timestamp already exists)", 409)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"record_id": new_id, "record": db.data_store[new_id]})

@app.post("/api/db/update_by_timestamp/<ts>")
def api_db_update(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    updates = request.get_json(silent=True) or {}
    if not updates:
        return err("updates JSON body required")

    updated = db.update_record_by_timestamp(ts_int, updates)
    if not updated:
        return err("record not found / deleted", 404)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"updated": True, "record": db.get_record_by_timestamp(ts_int)})

@app.post("/api/db/delete_by_timestamp/<ts>")
def api_db_delete(ts: str):
    r = require_db()
    if r is not None:
        return r

    ts_int = parse_timestamp_str_only(ts)
    if ts_int is None:
        return err("timestamp must be 'YYYY-MM-DD HH:MM:SS'")

    deleted = db.delete_record_by_timestamp(ts_int)
    if not deleted:
        return err("record not found / deleted", 404)

    g.build_graph_from_db(db)
    STATE["graph_built"] = True

    return ok({"deleted": True, "timestamp": ts_int})


@app.get("/api/graph/wallet_summary/<wallet>")
def api_wallet_summary(wallet: str):
    r = require_graph()
    if r is not None:
        return r
    return ok(g.wallet_summary(wallet))

@app.get("/api/graph/neighbors/<wallet>")
def api_neighbors(wallet: str):
    r = require_graph()
    if r is not None:
        return r

    limit = request.args.get("limit", "50")
    try:
        limit = max(1, min(200, int(limit)))
    except ValueError:
        limit = 50

    nbrs = g.neighbors(wallet)
    preview = list(nbrs.items())[:limit]
    return ok({
        "wallet": wallet,
        "neighbors_count": len(nbrs),
        "neighbors_preview": [{"neighbor": k, "stats": v} for k, v in preview],
    })

@app.get("/api/graph/top_counterparties/<wallet>")
def api_top_counterparties(wallet: str):
    r = require_graph()
    if r is not None:
        return r

    k = request.args.get("k", "10")
    by = request.args.get("by", "total_amt")
    direction = request.args.get("direction", "both")

    try:
        k = max(1, min(50, int(k)))
    except ValueError:
        k = 10

    rows = g.top_counterparties(wallet, k=k, by=by, direction=direction)
    return ok({"wallet": wallet, "rows": rows})

@app.get("/api/graph/shortest_path")
def api_shortest_path():
    r = require_graph()
    if r is not None:
        return r

    src = (request.args.get("src") or "").strip()
    dst = (request.args.get("dst") or "").strip()
    if not src or not dst:
        return err("src and dst required: /api/graph/shortest_path?src=...&dst=...")

    path = g.shortest_path(src, dst)
    if path is None:
        return err("no path found (or src not in graph)", 404)

    return ok({"length": len(path) - 1, "path": path})


@app.get("/api/stats/pair_stats")
def api_pair_stats():
    r = require_graph()
    if r is not None:
        return r

    a = (request.args.get("a") or "").strip()
    b = (request.args.get("b") or "").strip()
    directed = (request.args.get("directed") or "false").strip().lower() in {"1", "true", "yes", "y"}

    if not a or not b:
        return err("a and b required: /api/stats/pair_stats?a=W0001&b=W0002&directed=false")

    out = None
    if hasattr(g, "pair_stats"):
        out = g.pair_stats(a, b, directed=directed)

    if out is None:
        return err("no relationship found for these wallets", 404)

    if isinstance(out, dict):
        out.pop("a", None)
        out.pop("b", None)
        out.pop("directed", None)

    return ok(out)

@app.get("/api/stats/wallet_currency_breakdown/<wallet>")
def api_wallet_currency_breakdown(wallet: str):
    r = require_graph()
    if r is not None:
        return r
    out = g.wallet_currency_breakdown(wallet)
    return ok(out)

@app.get("/api/stats/top_wallet_by_currency")
def api_top_wallet_by_currency():
    r = require_graph()
    if r is not None:
        return r

    symbol = (request.args.get("symbol") or "").strip()
    by = (request.args.get("by") or "count").strip().lower()

    if not symbol:
        return err("symbol required: /api/stats/top_wallet_by_currency?symbol=BTC&by=count")

    out = g.top_wallet_by_currency(symbol, by=by)
    if out is None:
        return err("no wallet found for this symbol (or symbol not present)", 404)

    return ok(out)

@app.get("/api/stats/amount_usd_analytics")
def api_amount_usd_analytics():
    r = require_db()
    if r is not None:
        return r

    symbol = (request.args.get("symbol") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    sender_wallet = (request.args.get("sender_wallet") or "").strip() or None
    receiver_wallet = (request.args.get("receiver_wallet") or "").strip() or None

    out = amount_usd_analytics(
        symbol=symbol,
        status=status,
        sender_wallet=sender_wallet,
        receiver_wallet=receiver_wallet,
    )
    return ok(out)


HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>CryptoDB UI</title>
  <style>
    :root{
      --bg:#041a3a;
      --white:#ffffff;
      --muted:rgba(255,255,255,.75);
      --line:rgba(255,255,255,.18);
      --shadow:0 18px 60px rgba(0,0,0,.35);
      --ink:#0b1b33;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      min-height:100vh;
      background: radial-gradient(1200px 900px at 20% 10%, #0b2c66 0%, var(--bg) 55%) fixed;
      color:var(--white);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
      display:flex;
      align-items:center;
      justify-content:center;
      padding:24px;
    }
    .stage{width:min(860px, 96vw);display:flex;align-items:center;justify-content:center;}
    .panel{
      width:min(720px, 96vw);
      background: rgba(255,255,255,.06);
      border:1px solid rgba(255,255,255,.14);
      border-radius:22px;
      box-shadow: var(--shadow);
      padding:22px;
      backdrop-filter: blur(10px);
    }
    .center{text-align:center;}
    .btnGrid{margin-top:18px;display:grid;gap:12px;}
    .btn{
      width:100%;
      border:none;
      border-radius:16px;
      padding:14px 16px;
      background:var(--white);
      color:var(--ink);
      font-weight:900;
      font-size:15px;
      cursor:pointer;
      transition: transform .08s ease, filter .08s ease;
    }
    .btn:hover{ filter: brightness(.96); }
    .btn:active{ transform: translateY(1px); }
    .btn.secondary{
      background: rgba(255,255,255,.12);
      color: var(--white);
      border: 1px solid rgba(255,255,255,.22);
    }
    .pill{
      display:inline-block;
      padding:6px 12px;
      border-radius:999px;
      border:1px solid rgba(255,255,255,.20);
      background: rgba(255,255,255,.08);
      color: var(--muted);
      font-size:12px;
      font-weight:800;
    }
    .smallBtnRow{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      justify-content:center;
      margin-top:14px;
    }
    .smallBtnRow .btn{ width:auto; padding:10px 12px; border-radius:14px; font-size:13px; }

    .form{margin-top:14px;display:grid;gap:12px;text-align:left;}
    label{font-size:12px;color:var(--muted);font-weight:800;}
    input, select{
      width:100%;
      padding:12px 12px;
      border-radius:14px;
      border:1px solid rgba(255,255,255,.18);
      background: rgba(2,10,25,.55);
      color: var(--white);
      outline:none;
      font-size:14px;
    }
    input::placeholder{ color: rgba(255,255,255,.38); }
    input:focus, select:focus{
      border-color: rgba(255,255,255,.40);
      box-shadow: 0 0 0 4px rgba(255,255,255,.08);
    }
    .two{display:grid;gap:12px;grid-template-columns: 1fr 1fr;}
    @media (max-width: 620px){ .two{ grid-template-columns: 1fr; } }

    .results{
      margin-top:16px;
      border-top:1px solid rgba(255,255,255,.18);
      padding-top:14px;
    }
    .msg{color:var(--muted);font-size:13px;line-height:1.5;margin-top:8px;}
    .alert{
      margin-top:12px;
      padding:12px 12px;
      border-radius:14px;
      font-weight:900;
      font-size:13px;
      border:1px solid rgba(255,255,255,.20);
      background: rgba(255,255,255,.08);
    }
    .alert.err{ border-color: rgba(255,107,107,.55); background: rgba(255,107,107,.12); color:#ffd4d4; }
    .alert.ok{ border-color: rgba(46,229,157,.55); background: rgba(46,229,157,.10); color:#caffea; }

    table{
      width:100%;
      border-collapse: collapse;
      margin-top:10px;
      overflow:hidden;
      border-radius:14px;
      border:1px solid rgba(255,255,255,.14);
    }
    th, td{
      padding:10px 10px;
      border-bottom:1px solid rgba(255,255,255,.12);
      text-align:left;
      font-size:13px;
      color: rgba(255,255,255,.92);
    }
    th{ color: var(--muted); font-size:12px; }
    tr:last-child td{ border-bottom:none; }
  </style>
</head>

<body>
  <div class="stage">
    <div class="panel" id="screen"></div>
  </div>

<script>
  function el(id){ return document.getElementById(id); }

  function esc(s){
    return (s ?? "").toString().replace(/[&<>"']/g, m => ({
      "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
    }[m]));
  }

  function renderKV(title, obj){
    const rows = Object.entries(obj || {}).map(([k,v]) => {
      const val = (typeof v === "number")
        ? v.toLocaleString(undefined,{maximumFractionDigits:2})
        : (v ?? "");
      return `<tr><th>${esc(k)}</th><td>${esc(val)}</td></tr>`;
    }).join("");
    return `
      <div class="results">
        <div class="pill">${esc(title)}</div>
        <table><tbody>${rows || `<tr><td colspan="2">No data</td></tr>`}</tbody></table>
      </div>
    `;
  }

  function renderTable(title, rows, cols){
    const head = cols.map(c => `<th>${esc(c)}</th>`).join("");
    const body = (rows || []).map(r => {
      const tds = cols.map(c => {
        const v = r[c];
        const val = (typeof v === "number")
          ? v.toLocaleString(undefined,{maximumFractionDigits:2})
          : (v ?? "");
        return `<td>${esc(val)}</td>`;
      }).join("");
      return `<tr>${tds}</tr>`;
    }).join("");

    return `
      <div class="results">
        <div class="pill">${esc(title)}</div>
        <table>
          <thead><tr>${head}</tr></thead>
          <tbody>${body || `<tr><td colspan="${cols.length}">No rows</td></tr>`}</tbody>
        </table>
      </div>
    `;
  }

  function alertBox(kind, text){
    const cls = kind === "ok" ? "ok" : "err";
    return `<div class="alert ${cls}">${esc(text)}</div>`;
  }

  async function apiGet(url){
    const res = await fetch(url);
    const data = await res.json().catch(()=>({}));
    return { http: res.status, ...data };
  }

  async function apiPost(url, body){
    const res = await fetch(url, {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify(body||{})
    });
    const data = await res.json().catch(()=>({}));
    return { http: res.status, ...data };
  }

  let MODE = "home";
  let CATEGORY = null;
  let FUNC = null;

  function goHome(){ MODE="home"; CATEGORY=null; FUNC=null; draw(); }
  function goSubmenu(cat){ MODE="submenu"; CATEGORY=cat; FUNC=null; draw(); }
  function goFunction(cat, fn){ MODE="function"; CATEGORY=cat; FUNC=fn; draw(); }

  function draw(){
    if (MODE === "home") return drawHome();
    if (MODE === "submenu") return drawSubmenu(CATEGORY);
    return drawFunction(CATEGORY, FUNC);
  }

  function drawHome(){
    el("screen").innerHTML = `
      <div class="center">
        <div class="pill">Choose a category</div>
        <div class="btnGrid" style="margin-top:18px;">
          <button class="btn" onclick="goSubmenu('graph')">Wallet Interactions</button>
          <button class="btn" onclick="goFunction('stats','amount_usd_analytics')">Statistics</button>
          <button class="btn" onclick="goSubmenu('search')">DB Search</button>
          <button class="btn" onclick="goSubmenu('edit')">DB Edit</button>
        </div>
      </div>
    `;
  }

  function drawSubmenu(cat){
    let title = "";
    let buttons = "";

    if (cat === "graph"){
      title = "Wallet Interactions";
      buttons = `
        <button class="btn" onclick="goFunction('graph','wallet_summary')">Wallet Summary</button>
        <button class="btn" onclick="goFunction('graph','neighbors')">Neighbors</button>
        <button class="btn" onclick="goFunction('graph','top_counterparties')">Top Counterparties</button>
        <button class="btn" onclick="goFunction('graph','shortest_path')">Shortest Path</button>

        <button class="btn" onclick="goFunction('graph','pair_stats')">Pair Stats</button>
        <button class="btn" onclick="goFunction('graph','wallet_currency_breakdown')">Wallet Currency Breakdown</button>
        <button class="btn" onclick="goFunction('graph','top_wallet_by_currency')">Top Wallet by Currency</button>
      `;
    } else if (cat === "search"){
      title = "DB Search";
      buttons = `
        <button class="btn" onclick="goFunction('search','by_timestamp')">Get by Timestamp</button>
        <button class="btn" onclick="goFunction('search','by_token')">Search by Token</button>
        <button class="btn" onclick="goFunction('search','by_sender')">Search by Sender</button>
        <button class="btn" onclick="goFunction('search','range')">Range Query</button>
      `;
    } else {
      title = "DB Edit";
      buttons = `
        <button class="btn" onclick="goFunction('edit','insert')">Insert Record</button>
        <button class="btn" onclick="goFunction('edit','update')">Update by Timestamp</button>
        <button class="btn" onclick="goFunction('edit','delete')">Delete by Timestamp</button>
      `;
    }

    el("screen").innerHTML = `
      <div class="center">
        <div class="pill">${esc(title)}</div>
        <div class="btnGrid">${buttons}</div>
        <div class="smallBtnRow">
          <button class="btn secondary" onclick="goHome()">← Back</button>
        </div>
      </div>
    `;
  }

  function drawFunction(cat, fn){
    const title = (cat === "graph") ? "WALLET INTERACTIONS" : cat.toUpperCase();
    const header = `
      <div class="center">
        <div class="pill">${esc(title)} • ${esc(fn.replaceAll('_',' '))}</div>
        <div class="smallBtnRow">
          <button class="btn secondary" onclick="${cat==='stats' ? 'goHome()' : `goSubmenu('${esc(cat)}')`}">← Back</button>
          <button class="btn secondary" onclick="goHome()">Home</button>
        </div>
      </div>
    `;

    let form = "";
    let runBtn = "";
    let results = `<div class="results"><div class="msg">Run the operation to see results.</div></div>`;

    // ---- NOTE: analytics is ONLY under stats now (not graph) ----

    if (cat === "graph" && fn === "pair_stats"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>Wallet A</label>
              <input id="a" placeholder="W000001"/>
            </div>
            <div>
              <label>Wallet B</label>
              <input id="b" placeholder="W000002"/>
            </div>
          </div>
          <div>
            <label>Direction</label>
            <select id="directed">
              <option value="false" selected>A ↔ B</option>
              <option value="true">A → B</option>
            </select>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_pair_stats()">Run</button>`;
    }

    if (cat === "graph" && fn === "wallet_currency_breakdown"){
      form = `
        <div class="form">
          <div>
            <label>Wallet</label>
            <input id="wallet" placeholder="W001817"/>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_wallet_currency_breakdown()">Run</button>`;
    }

    if (cat === "graph" && fn === "top_wallet_by_currency"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>Symbol</label>
              <input id="symbol" placeholder="BTC"/>
            </div>
            <div>
              <label>By</label>
              <select id="by">
                <option value="count" selected>count</option>
                <option value="amount">amount</option>
              </select>
            </div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_top_wallet_by_currency()">Run</button>`;
    }

    // analytics ONLY here:
    if (cat === "stats" && fn === "amount_usd_analytics"){
      form = `
        <div class="form">
          <div class="two">
            <div>
              <label>crypto_symbol (optional)</label>
              <input id="symbol" placeholder="BTC"/>
            </div>
            <div>
              <label>status (optional)</label>
              <input id="status" placeholder="pending"/>
            </div>
          </div>
          <div class="two">
            <div>
              <label>sender_wallet (optional)</label>
              <input id="sender_wallet" placeholder="W001817"/>
            </div>
            <div>
              <label>receiver_wallet (optional)</label>
              <input id="receiver_wallet" placeholder="W000999"/>
            </div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_amount_usd_analytics()">Run</button>`;
    }

    if (cat === "graph" && fn === "wallet_summary"){
      form = `<div class="form"><div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_wallet_summary()">Run</button>`;
    }

    if (cat === "graph" && fn === "neighbors"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div>
            <div><label>Limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_neighbors()">Run</button>`;
    }

    if (cat === "graph" && fn === "top_counterparties"){
      form = `
        <div class="form">
          <div><label>Wallet</label><input id="wallet" placeholder="W001817"/></div>
          <div class="two">
            <div><label>k</label><input id="k" placeholder="10"/></div>
            <div>
              <label>Sort by</label>
              <select id="by">
                <option value="total_amt">total_amt</option>
                <option value="count">count</option>
                <option value="total_fee">total_fee</option>
              </select>
            </div>
          </div>
          <div>
            <label>Direction</label>
            <select id="dir">
              <option value="both">both</option>
              <option value="out">out</option>
              <option value="in">in</option>
            </select>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_top_counterparties()">Run</button>`;
    }

    if (cat === "graph" && fn === "shortest_path"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>Source wallet</label><input id="src" placeholder="W000001"/></div>
            <div><label>Destination wallet</label><input id="dst" placeholder="W000999"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_shortest_path()">Run</button>`;
    }

    if (cat === "search" && fn === "by_timestamp"){
      form = `<div class="form"><div><label>timestamp</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_by_timestamp()">Run</button>`;
    }

    if (cat === "search" && fn === "by_token"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>crypto_symbol</label><input id="sym" placeholder="BTC"/></div>
            <div><label>limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_by_token()">Run</button>`;
    }

    if (cat === "search" && fn === "by_sender"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>sender_wallet</label><input id="wallet" placeholder="W001817"/></div>
            <div><label>limit</label><input id="limit" placeholder="50"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_by_sender()">Run</button>`;
    }

    if (cat === "search" && fn === "range"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>start</label><input id="start" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
            <div><label>end</label><input id="end" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          </div>
          <div><label>limit</label><input id="limit" placeholder="50"/></div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_range()">Run</button>`;
    }

    if (cat === "edit" && fn === "insert"){
      form = `
        <div class="form">
          <div class="two">
            <div><label>tx_id</label><input id="tx_id" placeholder="999999999"/></div>
            <div><label>timestamp_str</label><input id="timestamp_str" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          </div>
          <div class="two">
            <div><label>crypto_symbol</label><input id="crypto_symbol" placeholder="BTC"/></div>
            <div><label>status</label><input id="status" placeholder="pending"/></div>
          </div>
          <div class="two">
            <div><label>amount_usd</label><input id="amount_usd" placeholder="1234.56"/></div>
            <div><label>fee_usd</label><input id="fee_usd" placeholder="2.34"/></div>
          </div>
          <div class="two">
            <div><label>sender_wallet</label><input id="sender_wallet" placeholder="WTEST_FROM"/></div>
            <div><label>receiver_wallet</label><input id="receiver_wallet" placeholder="WTEST_TO"/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_insert()">Run</button>`;
    }

    if (cat === "edit" && fn === "update"){
      form = `
        <div class="form">
          <div><label>timestamp (record key)</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div>
          <div class="two">
            <div><label>amount_usd (optional)</label><input id="amount_usd" placeholder=""/></div>
            <div><label>fee_usd (optional)</label><input id="fee_usd" placeholder=""/></div>
          </div>
          <div class="two">
            <div><label>status (optional)</label><input id="status" placeholder=""/></div>
            <div><label>crypto_symbol (optional)</label><input id="crypto_symbol" placeholder=""/></div>
          </div>
          <div class="two">
            <div><label>sender_wallet (optional)</label><input id="sender_wallet" placeholder=""/></div>
            <div><label>receiver_wallet (optional)</label><input id="receiver_wallet" placeholder=""/></div>
          </div>
        </div>
      `;
      runBtn = `<button class="btn" onclick="run_update()">Run</button>`;
    }

    if (cat === "edit" && fn === "delete"){
      form = `<div class="form"><div><label>timestamp</label><input id="ts" placeholder="YYYY-MM-DD HH:MM:SS"/></div></div>`;
      runBtn = `<button class="btn" onclick="run_delete()">Run</button>`;
    }

    el("screen").innerHTML = `
      ${header}
      ${form}
      <div class="smallBtnRow" style="margin-top:14px;">
        ${runBtn}
        <button class="btn secondary" onclick="clearResults()">Clear</button>
      </div>
      <div id="resultsHost">${results}</div>
    `;
  }

  function clearResults(){
    const host = el("resultsHost");
    if(host) host.innerHTML = `<div class="results"><div class="msg">Run the operation to see results.</div></div>`;
  }

  function setResults(html){
    el("resultsHost").innerHTML = html;
  }

  function numOrDefault(s, d){
    const n = parseInt((s||"").trim(), 10);
    return Number.isFinite(n) ? n : d;
  }

  async function run_wallet_summary(){
    const w = (el("wallet").value || "").trim();
    const r = await apiGet(`/api/graph/wallet_summary/${encodeURIComponent(w)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Wallet summary", r.data));
  }

  async function run_neighbors(){
    const w = (el("wallet").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/graph/neighbors/${encodeURIComponent(w)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    const rows = (r.data.neighbors_preview || []).map(x => ({
      neighbor: x.neighbor,
      count: x.stats.count,
      total_amt: x.stats.total_amt,
      total_fee: x.stats.total_fee
    }));
    setResults(renderTable(`Neighbors (total=${r.data.neighbors_count})`, rows, ["neighbor","count","total_amt","total_fee"]));
  }

  async function run_top_counterparties(){
    const w = (el("wallet").value || "").trim();
    const k = numOrDefault(el("k").value, 10);
    const by = el("by").value;
    const dir = el("dir").value;

    const r = await apiGet(`/api/graph/top_counterparties/${encodeURIComponent(w)}?k=${encodeURIComponent(k)}&by=${encodeURIComponent(by)}&direction=${encodeURIComponent(dir)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      wallet: x.wallet,
      count: x.count,
      total_amt: x.total_amt,
      total_fee: x.total_fee,
      avg_amt: x.avg_amt
    }));
    setResults(renderTable("Top counterparties", rows, ["wallet","count","total_amt","total_fee","avg_amt"]));
  }

  async function run_shortest_path(){
    const src = (el("src").value || "").trim();
    const dst = (el("dst").value || "").trim();
    const r = await apiGet(`/api/graph/shortest_path?src=${encodeURIComponent(src)}&dst=${encodeURIComponent(dst)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const path = (r.data.path || []).join(" → ");
    setResults(`
      <div class="results">
        <div class="pill">Shortest path</div>
        <div class="msg">Length: <b>${esc(r.data.length)}</b></div>
        <div class="msg" style="margin-top:10px;color:rgba(255,255,255,.92);font-weight:900;">${esc(path)}</div>
      </div>
    `);
  }

  async function run_pair_stats(){
    const a = (el("a").value || "").trim();
    const b = (el("b").value || "").trim();
    const directed = (el("directed").value || "false").trim();

    const r = await apiGet(`/api/stats/pair_stats?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}&directed=${encodeURIComponent(directed)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Pair stats", r.data));
  }

  async function run_wallet_currency_breakdown(){
    const w = (el("wallet").value || "").trim();
    const r = await apiGet(`/api/stats/wallet_currency_breakdown/${encodeURIComponent(w)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const counts = r.data.counts || {};
    const amounts = r.data.amounts || {};

    const syms = new Set([...Object.keys(counts), ...Object.keys(amounts)]);
    const rows = Array.from(syms).map(sym => ({
      symbol: sym,
      count: counts[sym] ?? 0,
      total_amount: amounts[sym] ?? 0
    })).sort((x,y) => (y.total_amount - x.total_amount));

    setResults(renderTable("Wallet currency breakdown", rows, ["symbol","count","total_amount"]));
  }

  async function run_top_wallet_by_currency(){
    const symbol = (el("symbol").value || "").trim();
    const by = (el("by").value || "count").trim();

    const r = await apiGet(`/api/stats/top_wallet_by_currency?symbol=${encodeURIComponent(symbol)}&by=${encodeURIComponent(by)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Top wallet by currency", r.data));
  }

  async function run_amount_usd_analytics(){
    const symbol = (el("symbol").value || "").trim();
    const status = (el("status").value || "").trim();
    const sender_wallet = (el("sender_wallet").value || "").trim();
    const receiver_wallet = (el("receiver_wallet").value || "").trim();

    const qs = new URLSearchParams();
    if(symbol) qs.set("symbol", symbol);
    if(status) qs.set("status", status);
    if(sender_wallet) qs.set("sender_wallet", sender_wallet);
    if(receiver_wallet) qs.set("receiver_wallet", receiver_wallet);

    const r = await apiGet(`/api/stats/amount_usd_analytics?${qs.toString()}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const data = r.data || {};
    const main = {
      count: data.count,
      sum: data.sum,
      min: data.min,
      max: data.max,
      mean: data.mean,
      std_pop: data.std_pop
    };

    const p = (data.approx_percentiles || {});
    const pct = {
      p50: p.p50, p90: p.p90, p95: p.p95, p99: p.p99,
      approx_note: data.approx_note || ""
    };

    setResults(
      renderKV("amount_usd analytics", main) +
      renderKV("approx percentiles", pct) +
      renderKV("filters", data.filters || {})
    );
  }

  async function run_by_timestamp(){
    const ts = (el("ts").value || "").trim();
    const r = await apiGet(`/api/db/by_timestamp/${encodeURIComponent(ts)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));
    setResults(renderKV("Record", r.data));
  }

  async function run_by_token(){
    const sym = (el("sym").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/db/by_token/${encodeURIComponent(sym)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      sender_wallet: x.sender_wallet,
      receiver_wallet: x.receiver_wallet,
      status: x.status
    }));
    setResults(renderTable(`Token: ${sym} (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","sender_wallet","receiver_wallet","status"]));
  }

  async function run_by_sender(){
    const w = (el("wallet").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);
    const r = await apiGet(`/api/db/by_sender/${encodeURIComponent(w)}?limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      receiver_wallet: x.receiver_wallet,
      status: x.status
    }));
    setResults(renderTable(`Sender: ${w} (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","receiver_wallet","status"]));
  }

  async function run_range(){
    const start = (el("start").value || "").trim();
    const end = (el("end").value || "").trim();
    const limit = numOrDefault(el("limit").value, 50);

    const r = await apiGet(`/api/db/range?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=${encodeURIComponent(limit)}`);
    if(!r.ok) return setResults(alertBox("err", r.error || "Error"));

    const rows = (r.data.rows || []).map(x => ({
      tx_id: x.tx_id,
      timestamp: x.timestamp_str || x.timestamp,
      crypto_symbol: x.crypto_symbol,
      amount_usd: x.amount_usd,
      sender_wallet: x.sender_wallet,
      receiver_wallet: x.receiver_wallet,
    }));
    setResults(renderTable(`Range (returned=${r.data.count_returned})`, rows,
      ["tx_id","timestamp","crypto_symbol","amount_usd","sender_wallet","receiver_wallet"]));
  }

  async function run_insert(){
    const body = {
      tx_id: Number((el("tx_id").value || "").trim()),
      timestamp_str: (el("timestamp_str").value || "").trim(),
      crypto_symbol: (el("crypto_symbol").value || "").trim(),
      amount_usd: Number((el("amount_usd").value || "").trim()),
      fee_usd: Number((el("fee_usd").value || "").trim()),
      sender_wallet: (el("sender_wallet").value || "").trim(),
      receiver_wallet: (el("receiver_wallet").value || "").trim(),
      status: (el("status").value || "").trim(),
    };
    const r = await apiPost("/api/db/insert", body);
    if(!r.ok) return setResults(alertBox("err", r.error || "Insert failed"));
    setResults(alertBox("ok", "Inserted successfully") + renderKV("Inserted record", r.data.record));
  }

  async function run_update(){
    const ts = (el("ts").value || "").trim();
    const updates = {};

    const a = (el("amount_usd").value || "").trim();
    const f = (el("fee_usd").value || "").trim();
    const st = (el("status").value || "").trim();
    const cs = (el("crypto_symbol").value || "").trim();
    const sw = (el("sender_wallet").value || "").trim();
    const rw = (el("receiver_wallet").value || "").trim();

    if(a !== "") updates["amount_usd"] = Number(a);
    if(f !== "") updates["fee_usd"] = Number(f);
    if(st !== "") updates["status"] = st;
    if(cs !== "") updates["crypto_symbol"] = cs;
    if(sw !== "") updates["sender_wallet"] = sw;
    if(rw !== "") updates["receiver_wallet"] = rw;

    const r = await apiPost(`/api/db/update_by_timestamp/${encodeURIComponent(ts)}`, updates);
    if(!r.ok) return setResults(alertBox("err", r.error || "Update failed"));
    setResults(alertBox("ok", "Updated successfully") + renderKV("Updated record", r.data.record));
  }

  async function run_delete(){
    const ts = (el("ts").value || "").trim();
    const r = await apiPost(`/api/db/delete_by_timestamp/${encodeURIComponent(ts)}`, {});
    if(!r.ok) return setResults(alertBox("err", r.error || "Delete failed"));
    setResults(alertBox("ok", `Deleted timestamp=${r.data.timestamp}`));
  }

  goHome();
</script>
</body>
</html>
"""

@app.get("/")
def home():
    return render_template_string(HTML)

if __name__ == "__main__":
    warm_start()
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
ain__":
    warm_start()
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
