#!/usr/bin/env python3
"""
MANJIT JAGOAR OI DATA SCANNER - backend server.

Tabs:
  1. OI SCANNER  — live option chain, section B/C/D, DB-logged every 3 min
  2. BACKTEST    — ORB historical backtest, candle cache
  3. TRENDING    — 5 stocks, LTP-entry at 09:15, SL/TGT %-based, logged forever
  4. FNO TRADES  — 5 stocks, futures, ORB 30-min breakout, 1 lot per stock,
                   balance tracker, lot-size from Upstox (fallback hardcoded),
                   re-entry on fund-add if within 0.5% of breakout level
"""

import gzip, http.server, json, os, sqlite3, threading, time, urllib.parse
from datetime import datetime

try:
    import requests
except ImportError:
    raise SystemExit("Run: pip install requests")

PORT          = 6180
HTML_FILE     = "jagoar_oi_scanner.html"
DB_FILE       = "jagoar_oi.db"
BASE          = "https://api.upstox.com/v2"
POLL_INTERVAL = 3 * 60
TEN_LAC       = 1_000_000

ACCESS_TOKEN = os.environ.get(
    "UPSTOX_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1OTUzNjIiLCJqdGkiOiI2OWZmMzBhMjU0NzlhOTZjOWM4MmJkYzQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaXNFeHRlbmRlZCI6dHJ1ZSwiaWF0IjoxNzc4MzMxODEwLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE4MDk5MDAwMDB9.luATCCj9PL3Rz3xx0_-wXGuJjH2i1MfvAN0JL534WHQ"
)

HEADERS = {
    "Accept":        "application/json",
    "Content-Type":  "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
}

INDEX_KEYS = {
    "NIFTY 50":   "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "SENSEX":     "BSE_INDEX|SENSEX",
}
STRIKE_STEP = {
    "NIFTY 50": 50, "BANK NIFTY": 100, "FINNIFTY": 50, "SENSEX": 100,
}

# ── Stock configs ──────────────────────────────────────────────────────────────
FNO_STOCKS = [
    {"symbol": "DIXON",      "eq_key": "NSE_EQ|INE935N01020",  "fut_key": None, "lot_size": 25},
    {"symbol": "FORCEMOT",   "eq_key": "NSE_EQ|INE451H01013",  "fut_key": None, "lot_size": 50},
    {"symbol": "POWERINDIA", "eq_key": "NSE_EQ|INE195N01010",  "fut_key": None, "lot_size": 100},
    {"symbol": "BSE",        "eq_key": "NSE_EQ|INE118H01025",  "fut_key": None, "lot_size": 150},
    {"symbol": "MCX",        "eq_key": "NSE_EQ|INE745G01035",  "fut_key": None, "lot_size": 125},
]

TRENDING_STOCKS = [
    {"symbol": "DIXON",      "instrument_key": "NSE_EQ|INE935N01020"},
    {"symbol": "FORCEMOT",   "instrument_key": "NSE_EQ|INE451H01013"},
    {"symbol": "POWERINDIA", "instrument_key": "NSE_EQ|INE195N01010"},
    {"symbol": "BSE",        "instrument_key": "NSE_EQ|INE118H01025"},
    {"symbol": "MCX",        "instrument_key": "NSE_EQ|INE745G01035"},
]

# ── FnO trade defaults ─────────────────────────────────────────────────────────
FNO_DEFAULT_SL_PCT     = 1.0    # 1% of entry price
FNO_DEFAULT_TARGET_PCT = 1.0    # 1% of entry price
FNO_MARGIN_PCT         = 0.15
FNO_REENTRY_TOLERANCE  = 0.005
FNO_OR_START           = 9  * 60 + 15
FNO_OR_END             = 9  * 60 + 45
FNO_SQUAREOFF          = 15 * 60 + 25
INITIAL_BALANCE        = 500_000.0

# ── Thread-safe shared state for FnO engine ────────────────────────────────────
_fno_lock   = threading.Lock()
_fno_state  = {
    "balance":       INITIAL_BALANCE,
    "sl_pct":        FNO_DEFAULT_SL_PCT,
    "target_pct":    FNO_DEFAULT_TARGET_PCT,
    "or_data":       {},
    "pending_funds": [],
}

# ── Time helpers ───────────────────────────────────────────────────────────────
def now_ist():
    return datetime.now()

def ist_date_str():
    return now_ist().strftime("%Y-%m-%d")

def cur_min_ist():
    t = now_ist()
    return t.hour * 60 + t.minute

# ── Market status ──────────────────────────────────────────────────────────────
def is_market_open():
    try:
        r = requests.get(f"{BASE}/market/status/NSE", headers=HEADERS, timeout=(5, 10))
        r.raise_for_status()
        return r.json().get("data", {}).get("status", "") == "NORMAL_OPEN"
    except Exception:
        t = now_ist()
        if t.weekday() >= 5:
            return False
        cm = t.hour * 60 + t.minute
        return (9 * 60 + 15) <= cm <= (15 * 60 + 30)

# ── SQLite ─────────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def db_init():
    with _db_lock, get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS section_d_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date TEXT NOT NULL, scrip TEXT NOT NULL, expiry TEXT NOT NULL,
                ts TEXT NOT NULL, call_oi INTEGER NOT NULL, put_oi INTEGER NOT NULL,
                diff INTEGER NOT NULL, change_diff INTEGER NOT NULL, action TEXT NOT NULL DEFAULT ''
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scrip_expiry_date ON section_d_logs(log_date,scrip,expiry)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date TEXT NOT NULL, symbol TEXT NOT NULL,
                entry_price REAL NOT NULL, entry_time TEXT NOT NULL,
                sl_pct REAL NOT NULL, target_pct REAL NOT NULL,
                sl_price REAL NOT NULL, target_price REAL NOT NULL,
                exit_price REAL, exit_time TEXT, result TEXT NOT NULL DEFAULT 'OPEN',
                pnl_pts REAL, pnl_pct REAL
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trending_date_symbol ON trending_logs(log_date,symbol)")

        # fno_trades — sl_pct / target_pct (% of entry price)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fno_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date     TEXT    NOT NULL,
                symbol       TEXT    NOT NULL,
                side         TEXT    NOT NULL,
                lot_size     INTEGER NOT NULL,
                entry_price  REAL    NOT NULL,
                entry_time   TEXT    NOT NULL,
                sl_pct       REAL    NOT NULL,
                target_pct   REAL    NOT NULL,
                sl_price     REAL    NOT NULL,
                target_price REAL    NOT NULL,
                margin_used  REAL    NOT NULL,
                or_high      REAL    NOT NULL,
                or_low       REAL    NOT NULL,
                exit_price   REAL,
                exit_time    TEXT,
                result       TEXT    NOT NULL DEFAULT 'OPEN',
                pnl_pts      REAL,
                pnl_inr      REAL
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fno_date_sym ON fno_trades(log_date,symbol)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fno_balance_log (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date TEXT  NOT NULL,
                ts       TEXT  NOT NULL,
                event    TEXT  NOT NULL,
                amount   REAL  NOT NULL,
                balance  REAL  NOT NULL,
                note     TEXT
            )""")
        conn.commit()

# ── OI scanner DB helpers ──────────────────────────────────────────────────────
def db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO section_d_logs (log_date,scrip,expiry,ts,call_oi,put_oi,diff,change_diff,action) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ist_date_str(), scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action))
        conn.commit()

def db_get_last_diff(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "SELECT diff FROM section_d_logs WHERE log_date=? AND scrip=? AND expiry=? ORDER BY id DESC LIMIT 1",
            (today, scrip, expiry))
        row = cur.fetchone()
    return row[0] if row else None

def db_get_logs(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT ts,call_oi,put_oi,diff,change_diff,action FROM section_d_logs "
            "WHERE log_date=? AND scrip=? AND expiry=? ORDER BY id ASC",
            (today, scrip, expiry)).fetchall()
    return [{"ts":r[0],"callOI":r[1],"putOI":r[2],"diff":r[3],"changeDiff":r[4],"action":r[5]} for r in rows]

def db_wipe_today():
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        conn.execute("DELETE FROM section_d_logs WHERE log_date=?", (today,))
        conn.commit()
    print(f"[{now_ist().strftime('%H:%M:%S')} IST] DB wiped (section_d) for {today}")

# ── Trending DB helpers ────────────────────────────────────────────────────────
def trending_already_entered(symbol, log_date):
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price,"
            "exit_price,exit_time,result,pnl_pts,pnl_pct "
            "FROM trending_logs WHERE log_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
            (log_date, symbol)).fetchone()
    if not row: return None
    return {"id":row[0],"entry_price":row[1],"entry_time":row[2],"sl_pct":row[3],"target_pct":row[4],
            "sl_price":row[5],"target_price":row[6],"exit_price":row[7],"exit_time":row[8],
            "result":row[9],"pnl_pts":row[10],"pnl_pct":row[11]}

def trending_insert(symbol, log_date, entry_price, entry_time, sl_pct, target_pct, sl_price, target_price):
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO trending_logs (log_date,symbol,entry_price,entry_time,sl_pct,target_pct,"
            "sl_price,target_price,result) VALUES (?,?,?,?,?,?,?,?,'OPEN')",
            (log_date,symbol,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price))
        conn.commit()
        return cur.lastrowid

def trending_update_exit(row_id, exit_price, exit_time, result, pnl_pts, pnl_pct):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE trending_logs SET exit_price=?,exit_time=?,result=?,pnl_pts=?,pnl_pct=? WHERE id=?",
            (exit_price, exit_time, result, pnl_pts, pnl_pct, row_id))
        conn.commit()

def trending_get_history(symbol=None, days=30):
    with _db_lock, get_conn() as conn:
        if symbol:
            rows = conn.execute(
                "SELECT log_date,symbol,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price,"
                "exit_price,exit_time,result,pnl_pts,pnl_pct FROM trending_logs WHERE symbol=? "
                "ORDER BY log_date DESC,id DESC LIMIT ?", (symbol, days*5)).fetchall()
        else:
            rows = conn.execute(
                "SELECT log_date,symbol,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price,"
                "exit_price,exit_time,result,pnl_pts,pnl_pct FROM trending_logs "
                "ORDER BY log_date DESC,id DESC LIMIT ?", (days*5,)).fetchall()
    return [{"log_date":r[0],"symbol":r[1],"entry_price":r[2],"entry_time":r[3],
             "sl_pct":r[4],"target_pct":r[5],"sl_price":r[6],"target_price":r[7],
             "exit_price":r[8],"exit_time":r[9],"result":r[10],"pnl_pts":r[11],"pnl_pct":r[12]} for r in rows]

# ── FnO trades DB helpers ──────────────────────────────────────────────────────
def fno_trade_today(symbol, log_date):
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id,side,lot_size,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price,"
            "margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
            "FROM fno_trades WHERE log_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
            (log_date, symbol)).fetchone()
    if not row: return None
    return {"id":row[0],"side":row[1],"lot_size":row[2],"entry_price":row[3],"entry_time":row[4],
            "sl_pct":row[5],"target_pct":row[6],"sl_price":row[7],"target_price":row[8],
            "margin_used":row[9],"or_high":row[10],"or_low":row[11],
            "exit_price":row[12],"exit_time":row[13],"result":row[14],"pnl_pts":row[15],"pnl_inr":row[16]}

def fno_trade_insert(log_date, symbol, side, lot_size, entry_price, entry_time,
                     sl_pct, target_pct, sl_price, target_price, margin_used, or_high, or_low):
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO fno_trades (log_date,symbol,side,lot_size,entry_price,entry_time,"
            "sl_pct,target_pct,sl_price,target_price,margin_used,or_high,or_low,result) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')",
            (log_date,symbol,side,lot_size,entry_price,entry_time,
             sl_pct,target_pct,sl_price,target_price,margin_used,or_high,or_low))
        conn.commit()
        return cur.lastrowid

def fno_trade_update_exit(trade_id, exit_price, exit_time, result, pnl_pts, pnl_inr):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE fno_trades SET exit_price=?,exit_time=?,result=?,pnl_pts=?,pnl_inr=? WHERE id=?",
            (exit_price, exit_time, result, pnl_pts, pnl_inr, trade_id))
        conn.commit()

def fno_trades_history(days=30, symbol=None):
    with _db_lock, get_conn() as conn:
        if symbol:
            rows = conn.execute(
                "SELECT log_date,symbol,side,lot_size,entry_price,entry_time,sl_pct,target_pct,"
                "sl_price,target_price,margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
                "FROM fno_trades WHERE symbol=? ORDER BY log_date DESC,id DESC LIMIT ?",
                (symbol, days*5)).fetchall()
        else:
            rows = conn.execute(
                "SELECT log_date,symbol,side,lot_size,entry_price,entry_time,sl_pct,target_pct,"
                "sl_price,target_price,margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
                "FROM fno_trades ORDER BY log_date DESC,id DESC LIMIT ?",
                (days*5,)).fetchall()
    return [{"log_date":r[0],"symbol":r[1],"side":r[2],"lot_size":r[3],"entry_price":r[4],
             "entry_time":r[5],"sl_pct":r[6],"target_pct":r[7],"sl_price":r[8],"target_price":r[9],
             "margin_used":r[10],"or_high":r[11],"or_low":r[12],"exit_price":r[13],
             "exit_time":r[14],"result":r[15],"pnl_pts":r[16],"pnl_inr":r[17]} for r in rows]

def fno_balance_log_insert(event, amount, balance, note=""):
    ts = now_ist().strftime("%H:%M:%S")
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO fno_balance_log (log_date,ts,event,amount,balance,note) VALUES (?,?,?,?,?,?)",
            (ist_date_str(), ts, event, amount, balance, note))
        conn.commit()

def fno_balance_history(days=30):
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT log_date,ts,event,amount,balance,note FROM fno_balance_log "
            "ORDER BY log_date DESC,id DESC LIMIT ?", (days*50,)).fetchall()
    return [{"log_date":r[0],"ts":r[1],"event":r[2],"amount":r[3],"balance":r[4],"note":r[5]} for r in rows]

# ── Upstox API helpers ─────────────────────────────────────────────────────────
def upstox_get(path, params):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, params=params, timeout=(5, 10))
    r.raise_for_status()
    return r.json()

def get_ltp(instrument_key):
    data = upstox_get("/market-quote/ltp", {"instrument_key": instrument_key})
    for k, v in (data.get("data") or {}).items():
        ltp = v.get("last_price")
        if ltp:
            return float(ltp)
    return None

def get_ltp_multi(keys_list):
    if not keys_list:
        return {}
    combined = ",".join(keys_list)
    try:
        data = upstox_get("/market-quote/ltp", {"instrument_key": combined})
        result = {}
        for k, v in (data.get("data") or {}).items():
            sym = k.split(":")[-1].split("|")[-1]
            result[sym] = v.get("last_price")
        return result
    except Exception:
        return {}

def get_nearest_expiry(instrument_key):
    data = upstox_get("/option/contract", {"instrument_key": instrument_key})
    expiries = sorted({d.get("expiry") for d in data.get("data", []) if d.get("expiry")})
    today = ist_date_str()
    for exp in expiries:
        if exp >= today:
            return exp
    return expiries[-1] if expiries else None

def get_option_chain(instrument_key, expiry_date):
    data = upstox_get("/option/chain", {"instrument_key": instrument_key, "expiry_date": expiry_date})
    rows, pcr_vals, spot = [], [], None
    for item in data.get("data", []):
        spot = item.get("underlying_spot_price")
        if item.get("pcr") is not None: pcr_vals.append(item["pcr"])
        call = item.get("call_options", {}).get("market_data", {}) or {}
        put  = item.get("put_options",  {}).get("market_data", {}) or {}
        rows.append({
            "strike": item.get("strike_price"),
            "call_oi_change": (call.get("oi",0) or 0) - (call.get("prev_oi",0) or 0),
            "put_oi_change":  (put.get("oi",0) or 0)  - (put.get("prev_oi",0) or 0),
            "call_oi": call.get("oi",0) or 0, "put_oi": put.get("oi",0) or 0,
            "call_volume": call.get("volume",0) or 0, "put_volume": put.get("volume",0) or 0,
            "call_ltp": call.get("ltp",0) or 0, "put_ltp": put.get("ltp",0) or 0,
        })
    rows.sort(key=lambda x: (x["strike"] is None, x["strike"]))
    return {"rows": rows, "spot": spot, "pcr": pcr_vals[0] if pcr_vals else None, "ts": now_ist().strftime("%H:%M:%S")}

def atm_index(rows, spot):
    best, bd = 0, float("inf")
    for i, r in enumerate(rows):
        d = abs((r["strike"] or 0) - (spot or 0))
        if d < bd: bd = d; best = i
    return best

def compute_section_d(rows, atm, scrip, expiry):
    start, end = max(0, atm - 1), min(len(rows), atm + 2)
    call_oi = sum(rows[i]["call_oi_change"] for i in range(start, end))
    put_oi  = sum(rows[i]["put_oi_change"]  for i in range(start, end))
    diff = call_oi - put_oi
    last_diff   = db_get_last_diff(scrip, expiry)
    change_diff = 0 if last_diff is None else diff - last_diff
    action = ""
    if call_oi - put_oi >  TEN_LAC: action = "BUY PUT"
    if put_oi  - call_oi > TEN_LAC: action = "BUY CALL"
    ts = now_ist().strftime("%H:%M:%S")
    db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action)
    print(f"[{ts}] Logged {scrip} | diff={diff} action={action or '-'}")

# ── FnO: lot size and futures instrument key ───────────────────────────────────
_lot_cache    = {}
_futkey_cache = {}

def _load_nse_instruments():
    local = "NSE.json.gz"
    if os.path.exists(local):
        with open(local, "rb") as f:
            return json.loads(gzip.decompress(f.read()))
    try:
        r = requests.get(
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            timeout=(10, 30))
        r.raise_for_status()
        return json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"  NSE instruments load failed: {e}")
        return []

def resolve_lot_sizes_and_futures():
    print(f"[{now_ist().strftime('%H:%M:%S')}] Resolving lot sizes and futures keys...")
    today = ist_date_str()
    instruments = _load_nse_instruments()
    if not instruments:
        print("  Using hardcoded lot sizes (instrument list unavailable)")
        for s in FNO_STOCKS:
            _lot_cache[s["symbol"]] = s["lot_size"]
        return

    fo_map = {}
    for d in instruments:
        if d.get("segment") != "NSE_FO":
            continue
        sym = d.get("underlying_symbol", "")
        if not sym:
            continue
        fo_map.setdefault(sym, []).append(d)

    for s in FNO_STOCKS:
        sym = s["symbol"]
        rows = fo_map.get(sym, [])
        if not rows:
            _lot_cache[sym]   = s["lot_size"]
            print(f"  {sym}: not found in instruments, using hardcoded lot_size={s['lot_size']}")
            continue

        lot = rows[0].get("lot_size") or s["lot_size"]
        _lot_cache[sym] = int(lot)

        fut_rows = [d for d in rows
                    if d.get("instrument_type") == "FUT"
                    and d.get("expiry") and d.get("expiry") >= today
                    and d.get("instrument_key")]
        if fut_rows:
            fut_rows.sort(key=lambda x: x["expiry"])
            best = fut_rows[0]
            _futkey_cache[sym] = best["instrument_key"]
            print(f"  {sym}: lot={lot}, fut_key={best['instrument_key']}, expiry={best['expiry']}")
        else:
            _futkey_cache[sym] = s["eq_key"]
            print(f"  {sym}: lot={lot}, no futures found, using eq_key for LTP")

def get_lot_size(symbol):
    return _lot_cache.get(symbol) or next(
        (s["lot_size"] for s in FNO_STOCKS if s["symbol"] == symbol), 100)

def get_fut_ltp(symbol):
    ik = _futkey_cache.get(symbol)
    if not ik:
        ik = next((s["eq_key"] for s in FNO_STOCKS if s["symbol"] == symbol), None)
    if not ik:
        return None
    return get_ltp(ik)

def estimate_margin(symbol, entry_price, lot_size):
    notional = entry_price * lot_size
    try:
        fut_key = _futkey_cache.get(symbol)
        if fut_key:
            payload = {"instruments": [{"instrument_token": fut_key, "transaction_type": "BUY",
                                         "quantity": lot_size, "price": entry_price, "product": "D"}]}
            r = requests.post(f"{BASE}/charges/margin", headers=HEADERS, json=payload, timeout=(5, 10))
            if r.ok:
                margin = r.json().get("data", {}).get("required_margin")
                if margin:
                    return float(margin)
    except Exception:
        pass
    return round(notional * FNO_MARGIN_PCT, 2)

# ── FnO paper-trade engine ─────────────────────────────────────────────────────
def _try_enter_fno(symbol, side, or_high, or_low, entry_level, today, note=""):
    existing = fno_trade_today(symbol, today)
    if existing:
        return "ALREADY_ENTERED"

    ltp = get_fut_ltp(symbol)
    if ltp is None:
        return "LTP_FAIL"

    lot_size = get_lot_size(symbol)
    margin   = estimate_margin(symbol, ltp, lot_size)

    with _fno_lock:
        balance    = _fno_state["balance"]
        sl_pct     = _fno_state["sl_pct"]
        target_pct = _fno_state["target_pct"]

    if balance < margin:
        with _fno_lock:
            already_pending = any(p["symbol"] == symbol for p in _fno_state["pending_funds"])
            if not already_pending:
                _fno_state["pending_funds"].append({
                    "symbol": symbol, "side": side,
                    "or_high": or_high, "or_low": or_low,
                    "entry_level": entry_level,
                })
        print(f"  {symbol}: INSUFFICIENT FUNDS (need ₹{margin:.0f}, have ₹{balance:.0f})")
        return "INSUFFICIENT_FUNDS"

    # Compute SL / target as % of entry LTP
    if side == "LONG":
        sl_price     = round(ltp * (1 - sl_pct / 100), 2)
        target_price = round(ltp * (1 + target_pct / 100), 2)
    else:
        sl_price     = round(ltp * (1 + sl_pct / 100), 2)
        target_price = round(ltp * (1 - target_pct / 100), 2)

    trade_id = fno_trade_insert(
        today, symbol, side, lot_size, ltp, now_ist().strftime("%H:%M:%S"),
        sl_pct, target_pct, sl_price, target_price, margin, or_high, or_low
    )
    with _fno_lock:
        _fno_state["balance"] -= margin
    fno_balance_log_insert("TRADE_ENTRY", -margin, _fno_state["balance"],
                           f"{symbol} {side} 1lot@{ltp:.2f} SL={sl_pct}% TGT={target_pct}% margin={margin:.0f} {note}")
    print(f"  [{now_ist().strftime('%H:%M:%S')}] FnO ENTRY {symbol} {side} @ {ltp} | SL={sl_price} TGT={target_price} | lot={lot_size} | margin={margin:.0f}")
    return "ENTERED"

def _check_fno_exits(today):
    cm = cur_min_ist()
    for s in FNO_STOCKS:
        sym = s["symbol"]
        trade = fno_trade_today(sym, today)
        if not trade or trade["result"] != "OPEN":
            continue
        ltp = get_fut_ltp(sym)
        if ltp is None:
            continue

        side         = trade["side"]
        lot_size     = trade["lot_size"]
        sl_price     = trade["sl_price"]
        target_price = trade["target_price"]
        entry_price  = trade["entry_price"]
        margin_used  = trade["margin_used"]
        exit_p = result = None

        hit_sl  = (ltp <= sl_price)     if side == "LONG" else (ltp >= sl_price)
        hit_tgt = (ltp >= target_price) if side == "LONG" else (ltp <= target_price)
        sq_off  = cm >= FNO_SQUAREOFF

        if hit_tgt and hit_sl:
            exit_p, result = sl_price, "SL HIT"
        elif hit_tgt:
            exit_p, result = target_price, "TGT HIT"
        elif hit_sl:
            exit_p, result = sl_price, "SL HIT"
        elif sq_off:
            exit_p, result = ltp, "SQUAREOFF"

        if result:
            pnl_pts = (exit_p - entry_price) if side == "LONG" else (entry_price - exit_p)
            pnl_inr = round(pnl_pts * lot_size, 2)
            exit_time = now_ist().strftime("%H:%M:%S")
            fno_trade_update_exit(trade["id"], exit_p, exit_time, result, round(pnl_pts,2), pnl_inr)
            returned = margin_used + pnl_inr
            with _fno_lock:
                _fno_state["balance"] += returned
                bal = _fno_state["balance"]
            fno_balance_log_insert("TRADE_EXIT", returned, bal,
                                   f"{sym} {side} exit@{exit_p} {result} P&L=₹{pnl_inr:+.0f}")
            print(f"  [{exit_time}] FnO EXIT {sym} {side} @ {exit_p} | {result} | P&L=₹{pnl_inr:+.2f}")

def _build_or_and_scan(today):
    cm = cur_min_ist()
    with _fno_lock:
        sl_pct     = _fno_state["sl_pct"]
        target_pct = _fno_state["target_pct"]

    for s in FNO_STOCKS:
        sym = s["symbol"]
        if fno_trade_today(sym, today):
            continue

        with _fno_lock:
            od = _fno_state["or_data"].setdefault(sym, {
                "or_high": None, "or_low": None, "or_done": False})

        ltp = get_fut_ltp(sym)
        if ltp is None:
            continue

        if FNO_OR_START <= cm < FNO_OR_END:
            with _fno_lock:
                od = _fno_state["or_data"][sym]
                od["or_high"] = ltp if od["or_high"] is None else max(od["or_high"], ltp)
                od["or_low"]  = ltp if od["or_low"]  is None else min(od["or_low"],  ltp)
                od["or_done"] = False
            print(f"  [{now_ist().strftime('%H:%M:%S')}] OR build {sym}: H={od['or_high']} L={od['or_low']}")

        elif cm >= FNO_OR_END and not od.get("or_done"):
            or_high = od.get("or_high")
            or_low  = od.get("or_low")
            if or_high is None or or_low is None:
                continue

            side = entry_level = None
            if ltp > or_high:
                side, entry_level = "LONG", or_high
            elif ltp < or_low:
                side, entry_level = "SHORT", or_low

            if side:
                res = _try_enter_fno(sym, side, or_high, or_low, entry_level, today, "OR breakout")
                if res == "ENTERED":
                    with _fno_lock:
                        _fno_state["or_data"][sym]["or_done"] = True

def _retry_pending_on_funds(today):
    with _fno_lock:
        pending = list(_fno_state["pending_funds"])

    still_pending = []
    for p in pending:
        sym         = p["symbol"]
        side        = p["side"]
        or_high     = p["or_high"]
        or_low      = p["or_low"]
        entry_level = p["entry_level"]

        ltp = get_fut_ltp(sym)
        if ltp is None:
            still_pending.append(p)
            continue

        tolerance    = entry_level * FNO_REENTRY_TOLERANCE
        near_entry   = abs(ltp - entry_level) <= tolerance
        correct_side = (ltp > or_high) if side == "LONG" else (ltp < or_low)

        if near_entry and correct_side:
            res = _try_enter_fno(sym, side, or_high, or_low, entry_level, today, "re-entry after funds")
            if res != "INSUFFICIENT_FUNDS":
                print(f"  {sym}: re-entry {res} after funds added")
                continue
        else:
            print(f"  {sym}: skipping re-entry, LTP {ltp} too far from entry level {entry_level} or wrong side")

        still_pending.append(p)

    with _fno_lock:
        _fno_state["pending_funds"] = still_pending

# ── FnO scheduler ─────────────────────────────────────────────────────────────
def fno_scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] FnO scheduler started.")
    try:
        resolve_lot_sizes_and_futures()
    except Exception as e:
        print(f"  lot-size resolve error: {e}")

    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT balance FROM fno_balance_log ORDER BY id DESC LIMIT 1").fetchone()
    if row:
        with _fno_lock:
            _fno_state["balance"] = row[0]
        print(f"  Balance restored from DB: ₹{row[0]:,.2f}")
    else:
        fno_balance_log_insert("FUND_ADD", INITIAL_BALANCE, INITIAL_BALANCE, "Initial capital")

    while True:
        time.sleep(180)
        if not is_market_open():
            with _fno_lock:
                _fno_state["or_data"] = {}
            continue
        today = ist_date_str()
        try:
            _build_or_and_scan(today)
            _check_fno_exits(today)
        except Exception as e:
            print(f"  FnO scheduler error: {e}")

# ── Trending scheduler ─────────────────────────────────────────────────────────
def trending_scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] Trending scheduler started.")
    _params = {"sl_pct": 2.0, "target_pct": 0.6}
    _params_lock = threading.Lock()

    def get_params():
        with _params_lock:
            return dict(_params)

    trending_scheduler._params      = _params
    trending_scheduler._params_lock = _params_lock

    while True:
        t   = now_ist()
        cm  = t.hour * 60 + t.minute

        if cm in (9*60+15, 9*60+16):
            if is_market_open():
                today = ist_date_str()
                p = get_params()
                for stock in TRENDING_STOCKS:
                    sym, ik = stock["symbol"], stock["instrument_key"]
                    if trending_already_entered(sym, today):
                        continue
                    try:
                        ltp = get_ltp(ik)
                        if not ltp: continue
                        sl_price     = round(ltp * (1 - p["sl_pct"]    / 100), 2)
                        target_price = round(ltp * (1 + p["target_pct"] / 100), 2)
                        trending_insert(sym, today, ltp, now_ist().strftime("%H:%M:%S"),
                                        p["sl_pct"], p["target_pct"], sl_price, target_price)
                        print(f"  Trending ENTRY {sym} @ {ltp}")
                    except Exception as e:
                        print(f"  Trending entry {sym}: {e}")
            time.sleep(90)
            continue

        if is_market_open() and (9*60+15) <= cm <= (15*60+15):
            today = ist_date_str()
            for stock in TRENDING_STOCKS:
                sym, ik = stock["symbol"], stock["instrument_key"]
                row = trending_already_entered(sym, today)
                if not row or row["result"] != "OPEN": continue
                try:
                    ltp = get_ltp(ik)
                    if not ltp: continue
                    entry, result, exit_p = row["entry_price"], None, None
                    if ltp <= row["sl_price"]:       exit_p, result = row["sl_price"],     "SL HIT"
                    elif ltp >= row["target_price"]: exit_p, result = row["target_price"], "TGT HIT"
                    elif cm >= 15*60:                exit_p, result = ltp, "SQUAREOFF"
                    if result:
                        pnl_pts = exit_p - entry
                        pnl_pct = round((pnl_pts / entry) * 100, 2)
                        trending_update_exit(row["id"], exit_p, now_ist().strftime("%H:%M:%S"),
                                             result, round(pnl_pts, 2), pnl_pct)
                        print(f"  Trending EXIT {sym} @ {exit_p} {result}")
                except Exception as e:
                    print(f"  Trending check {sym}: {e}")

        time.sleep(180)

# ── OI Scheduler ───────────────────────────────────────────────────────────────
def scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] OI Scheduler started.")
    while True:
        time.sleep(POLL_INTERVAL)
        if not is_market_open():
            continue
        for scrip, ik in INDEX_KEYS.items():
            try:
                expiry = get_nearest_expiry(ik)
                if not expiry: continue
                chain = get_option_chain(ik, expiry)
                rows, spot = chain["rows"], chain["spot"]
                if not rows or spot is None: continue
                compute_section_d(rows, atm_index(rows, spot), scrip, expiry)
            except Exception as e:
                print(f"  OI {scrip}: {e}")

def midnight_wiper():
    while True:
        t = now_ist()
        secs = (23 - t.hour)*3600 + (59 - t.minute)*60 + (60 - t.second)
        time.sleep(secs + 5)
        db_wipe_today()
        with _fno_lock:
            _fno_state["or_data"]       = {}
            _fno_state["pending_funds"] = []

# ── Backtest helpers ───────────────────────────────────────────────────────────
def get_fno_stocks():
    try:
        instruments = _load_nse_instruments()
        seen, result = set(), []
        for d in instruments:
            if (d.get("segment") == "NSE_FO" and d.get("instrument_type") == "CE"
                    and d.get("underlying_type") == "EQUITY"
                    and d.get("underlying_symbol") and d.get("underlying_key")):
                sym = d["underlying_symbol"]
                if sym not in seen:
                    seen.add(sym)
                    result.append({"symbol": sym, "instrument_key": d["underlying_key"]})
        return sorted(result, key=lambda x: x["symbol"])
    except Exception:
        return []

def candle_cache_init():
    with _db_lock, get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candle_cache (
                instrument_key TEXT NOT NULL, interval TEXT NOT NULL,
                ts TEXT NOT NULL, open REAL NOT NULL, high REAL NOT NULL,
                low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL,
                PRIMARY KEY (instrument_key, interval, ts))""")
        conn.commit()

def candle_cache_get(instrument_key, interval, from_date, to_date):
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT ts,open,high,low,close,volume FROM candle_cache "
            "WHERE instrument_key=? AND interval=? AND substr(ts,1,10) BETWEEN ? AND ? ORDER BY ts ASC",
            (instrument_key, interval, from_date, to_date)).fetchall()
    return [{"ts":r[0],"open":r[1],"high":r[2],"low":r[3],"close":r[4],"volume":r[5]} for r in rows]

def candle_cache_put(instrument_key, interval, candles):
    if not candles: return
    with _db_lock, get_conn() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO candle_cache (instrument_key,interval,ts,open,high,low,close,volume) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [(instrument_key,interval,c["ts"],c["open"],c["high"],c["low"],c["close"],c["volume"]) for c in candles])
        conn.commit()

TIMEFRAMES = {
    "1min":  ("minutes","1",20), "3min":  ("minutes","3",20),
    "5min":  ("minutes","5",25), "15min": ("minutes","15",90),
    "day":   ("days","1",365),
}
V3_BASE = "https://api.upstox.com/v3"

def _date_chunks(from_date, to_date, max_days):
    from datetime import timedelta
    d0 = datetime.strptime(from_date,"%Y-%m-%d").date()
    d1 = datetime.strptime(to_date,  "%Y-%m-%d").date()
    chunks, cur = [], d0
    while cur <= d1:
        end = min(cur + timedelta(days=max_days-1), d1)
        chunks.append((cur.isoformat(), end.isoformat()))
        cur = end + timedelta(days=1)
    return chunks

def _fetch_v3(instrument_key, unit, interval, chunk_from, chunk_to):
    ik_enc = instrument_key.replace("|","%7C")
    url = f"{V3_BASE}/historical-candle/{ik_enc}/{unit}/{interval}/{chunk_to}/{chunk_from}"
    r = requests.get(url, headers=HEADERS, timeout=(10,30))
    r.raise_for_status()
    return [{"ts":c[0],"open":c[1],"high":c[2],"low":c[3],"close":c[4],"volume":c[5]}
            for c in r.json().get("data",{}).get("candles",[])]

def get_candles(instrument_key, timeframe, from_date, to_date):
    unit, interval, max_days = TIMEFRAMES[timeframe]
    cache_key = f"{unit}/{interval}"
    cached = candle_cache_get(instrument_key, cache_key, from_date, to_date)
    if cached: return cached
    all_candles = []
    for cf, ct in _date_chunks(from_date, to_date, max_days):
        try:
            all_candles.extend(_fetch_v3(instrument_key, unit, interval, cf, ct))
            time.sleep(0.25)
        except requests.HTTPError:
            time.sleep(0.5)
    seen = {}
    for c in all_candles: seen[c["ts"]] = c
    merged = sorted(seen.values(), key=lambda x: x["ts"])
    candle_cache_put(instrument_key, cache_key, merged)
    return merged

def _group_by_day(candles):
    days = {}
    for c in candles:
        days.setdefault(c["ts"][:10],[]).append(c)
    for d in days: days[d].sort(key=lambda x: x["ts"])
    return days

def _minutes_of(ts):
    return int(ts[11:13])*60 + int(ts[14:16])

OR_START = 9*60+15; OR_END = 9*60+45; SQUARE_OFF = 15*60

def run_orb_backtest(instrument_key, symbol, timeframe, from_date, to_date, x_pts, y_pts):
    candles = get_candles(instrument_key, timeframe, from_date, to_date)
    trades = []
    for day, bars in sorted(_group_by_day(candles).items()):
        or_bars = [b for b in bars if OR_START <= _minutes_of(b["ts"]) < OR_END]
        if not or_bars: continue
        or_high = max(b["high"] for b in or_bars)
        or_low  = min(b["low"]  for b in or_bars)
        post = [b for b in bars if _minutes_of(b["ts"]) >= OR_END]
        side = entry = entry_idx = None
        for i, b in enumerate(post):
            bu, bd = b["high"] > or_high, b["low"] < or_low
            if bu and bd:
                side = "LONG" if abs(b["open"]-or_high) <= abs(b["open"]-or_low) else "SHORT"
                entry = or_high if side=="LONG" else or_low
            elif bu: side, entry = "LONG", or_high
            elif bd: side, entry = "SHORT", or_low
            if side: entry_idx=i; break
        if side is None: continue
        target = round(entry + x_pts,2) if side=="LONG" else round(entry - x_pts,2)
        sl     = round(entry - y_pts,2) if side=="LONG" else round(entry + y_pts,2)
        exit_p = result = exit_time = None
        for b in post[entry_idx:]:
            ht = (b["high"]>=target) if side=="LONG" else (b["low"]<=target)
            hs = (b["low"]<=sl)      if side=="LONG" else (b["high"]>=sl)
            if ht and hs:  exit_p,result = sl,"LOSS"
            elif ht:       exit_p,result = target,"WIN"
            elif hs:       exit_p,result = sl,"LOSS"
            if result: exit_time=b["ts"][11:16]; break
            if _minutes_of(b["ts"])>=SQUARE_OFF: exit_p=b["close"]; exit_time=b["ts"][11:16]; break
        if exit_p is None:
            exit_p=post[-1]["close"]; exit_time=post[-1]["ts"][11:16]
        pnl = round((exit_p-entry if side=="LONG" else entry-exit_p),2)
        if result is None: result="WIN" if pnl>0 else "LOSS"
        trades.append({"symbol":symbol,"entry_date":day,"side":side,"entry":round(entry,2),
                       "sl":sl,"target":target,"exit":round(exit_p,2),"exit_time":exit_time,
                       "result":result,"pnl":pnl,"pnl_pct":round((pnl/entry)*100,2) if entry else 0})
    return trades

# ── HTTP handler ───────────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Content-Length",str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a): pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs     = urllib.parse.parse_qs(parsed.query)
        try:
            p = parsed.path

            # ── OI scanner ────────────────────────────────────────────────────
            if p == "/indices":
                self._send(200, upstox_get("/market-quote/ltp", {"instrument_key": ",".join(INDEX_KEYS.values())}))
            elif p == "/expiries":
                ik   = qs.get("instrument_key",[""])[0]
                data = upstox_get("/option/contract",{"instrument_key":ik})
                self._send(200,{"expiries": sorted({d.get("expiry") for d in data.get("data",[]) if d.get("expiry")})})
            elif p == "/chain":
                chain = get_option_chain(qs.get("instrument_key",[""])[0], qs.get("expiry_date",[""])[0])
                chain["market_open"] = is_market_open()
                self._send(200, chain)
            elif p == "/logs":
                self._send(200,{"logs": db_get_logs(qs.get("scrip",[""])[0], qs.get("expiry",[""])[0])})
            elif p == "/log":
                scrip = qs.get("scrip",[""])[0]; exp = qs.get("expiry",[""])[0]
                call_oi = qs.get("call_oi",[None])[0]
                if scrip and exp and call_oi is not None and is_market_open():
                    db_insert_log(scrip, exp,
                                  qs.get("ts",[now_ist().strftime("%H:%M:%S")])[0],
                                  int(call_oi), int(qs.get("put_oi",[0])[0]),
                                  int(qs.get("diff",[0])[0]), int(qs.get("change_diff",[0])[0]),
                                  qs.get("action",[""])[0])
                self._send(200,{"ok":True})
            elif p in ("/","/index.html"):
                try:
                    body = open(HTML_FILE,"rb").read()
                    self.send_response(200); self.send_header("Content-Type","text/html")
                    self.send_header("Content-Length",str(len(body))); self.end_headers()
                    self.wfile.write(body)
                except FileNotFoundError:
                    self._send(404,{"error":f"{HTML_FILE} not found"})
            elif p == "/instruments":
                self._send(200,{"indices":INDEX_KEYS,"strike_step":STRIKE_STEP})
            elif p == "/fno_stocks":
                self._send(200,{"stocks": get_fno_stocks()})

            # ── Backtest ──────────────────────────────────────────────────────
            elif p == "/backtest":
                ik   = qs.get("instrument_key",[""])[0]
                sym  = qs.get("symbol",[""])[0]
                tf   = qs.get("timeframe",["3min"])[0]
                fd   = qs.get("from_date",[""])[0]
                td   = qs.get("to_date",[ist_date_str()])[0]
                xp   = float(qs.get("x_pts",["50"])[0])
                yp   = float(qs.get("y_pts",["30"])[0])
                if not ik or not fd:
                    self._send(400,{"error":"instrument_key and from_date required"})
                else:
                    trades = run_orb_backtest(ik,sym,tf,fd,td,xp,yp)
                    wins   = sum(1 for t in trades if t["result"]=="WIN")
                    longs  = sum(1 for t in trades if t["side"]=="LONG")
                    pnl    = round(sum(t["pnl"] for t in trades),2)
                    self._send(200,{"trades":trades,"summary":{
                        "total":len(trades),"wins":wins,"losses":len(trades)-wins,
                        "longs":longs,"shorts":len(trades)-longs,
                        "win_rate":round(wins/len(trades)*100,1) if trades else 0,"total_pnl":pnl}})

            # ── Trending ──────────────────────────────────────────────────────
            elif p == "/trending_stocks":
                today = ist_date_str()
                self._send(200,{"stocks":[{"symbol":s["symbol"],"instrument_key":s["instrument_key"],
                    "today": trending_already_entered(s["symbol"],today)} for s in TRENDING_STOCKS],"date":today})
            elif p == "/trending_params":
                sl  = float(qs.get("sl_pct",["-1"])[0])
                tgt = float(qs.get("target_pct",["-1"])[0])
                if sl > 0 and tgt > 0:
                    with trending_scheduler._params_lock:
                        trending_scheduler._params.update({"sl_pct":sl,"target_pct":tgt})
                with trending_scheduler._params_lock:
                    pp = dict(trending_scheduler._params)
                self._send(200,pp)
            elif p == "/trending_history":
                self._send(200,{"history": trending_get_history(
                    qs.get("symbol",[None])[0], int(qs.get("days",["30"])[0]))})
            elif p == "/trending_ltp":
                keys = ",".join(s["instrument_key"] for s in TRENDING_STOCKS)
                data = upstox_get("/market-quote/ltp",{"instrument_key":keys})
                ltps = {}
                for k,v in (data.get("data") or {}).items():
                    sym = k.split(":")[-1].split("|")[-1]
                    ltps[sym] = v.get("last_price")
                self._send(200,{"ltps":ltps})

            # ── FnO Trade endpoints ───────────────────────────────────────────
            elif p == "/fno_state":
                today = ist_date_str()
                with _fno_lock:
                    balance    = _fno_state["balance"]
                    sl_pct     = _fno_state["sl_pct"]
                    target_pct = _fno_state["target_pct"]
                    pending    = list(_fno_state["pending_funds"])
                fut_keys = [_futkey_cache.get(s["symbol"], s["eq_key"]) for s in FNO_STOCKS]
                ltps_raw = get_ltp_multi(fut_keys)
                stocks_status = []
                for s in FNO_STOCKS:
                    sym    = s["symbol"]
                    trade  = fno_trade_today(sym, today)
                    lot    = get_lot_size(sym)
                    fut_k  = _futkey_cache.get(sym, s["eq_key"])
                    ltp    = None
                    for k,v in ltps_raw.items():
                        if sym.upper() in k.upper() or k.upper() in sym.upper():
                            ltp = v; break
                    with _fno_lock:
                        or_d = dict(_fno_state["or_data"].get(sym, {}))
                    stocks_status.append({
                        "symbol": sym, "lot_size": lot,
                        "fut_key": fut_k, "ltp": ltp,
                        "trade": trade,
                        "or_high": or_d.get("or_high"), "or_low": or_d.get("or_low"),
                        "or_done": or_d.get("or_done", False),
                        "is_pending": any(pp["symbol"]==sym for pp in pending),
                    })
                self._send(200, {
                    "balance": round(balance,2), "sl_pct": sl_pct, "target_pct": target_pct,
                    "stocks": stocks_status, "pending": pending, "date": today,
                    "market_open": is_market_open(),
                })

            elif p == "/fno_params":
                sl  = float(qs.get("sl_pct",["-1"])[0])
                tgt = float(qs.get("target_pct",["-1"])[0])
                if sl > 0 and tgt > 0:
                    with _fno_lock:
                        _fno_state["sl_pct"]     = sl
                        _fno_state["target_pct"] = tgt
                    print(f"  FnO params updated: SL={sl}% TGT={tgt}%")
                with _fno_lock:
                    self._send(200,{"sl_pct":_fno_state["sl_pct"],"target_pct":_fno_state["target_pct"]})

            elif p == "/fno_add_funds":
                amount = float(qs.get("amount",["0"])[0])
                if amount <= 0:
                    self._send(400,{"error":"amount must be positive"})
                else:
                    with _fno_lock:
                        _fno_state["balance"] += amount
                        new_bal = _fno_state["balance"]
                    fno_balance_log_insert("FUND_ADD", amount, new_bal,
                                          f"Manual top-up ₹{amount:,.0f}")
                    print(f"  Funds added: ₹{amount:,.0f} → balance ₹{new_bal:,.2f}")
                    _retry_pending_on_funds(ist_date_str())
                    with _fno_lock:
                        pending = list(_fno_state["pending_funds"])
                    self._send(200,{"balance": round(new_bal,2),
                                    "pending_count": len(pending)})

            elif p == "/fno_history":
                sym  = qs.get("symbol",[None])[0]
                days = int(qs.get("days",["30"])[0])
                self._send(200,{
                    "trades":  fno_trades_history(days, sym),
                    "balance_log": fno_balance_history(days),
                })

            elif p == "/fno_lot_sizes":
                self._send(200,{s["symbol"]: get_lot_size(s["symbol"]) for s in FNO_STOCKS})

            else:
                self._send(404,{"error":"unknown path"})

        except requests.HTTPError as e:
            self._send(502,{"error":"upstox","detail":str(e),"body":getattr(e.response,"text","")})
        except Exception as e:
            import traceback
            self._send(500,{"error":str(e),"trace":traceback.format_exc()[-500:]})

# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    db_init()
    candle_cache_init()
    threading.Thread(target=midnight_wiper,     daemon=True).start()
    threading.Thread(target=scheduler,          daemon=True).start()
    threading.Thread(target=trending_scheduler, daemon=True).start()
    threading.Thread(target=fno_scheduler,      daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"JAGOAR OI server → http://0.0.0.0:{PORT}")
    print(f"Market open: {is_market_open()} | Balance: ₹{INITIAL_BALANCE:,.0f}")
    server.serve_forever()
