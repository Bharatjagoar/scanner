#!/usr/bin/env python3
"""
MANJIT JAGOAR OI DATA SCANNER - backend server.

Why this exists: Upstox API cannot be called directly from a browser (CORS).
All Upstox calls happen here, server-side. The HTML frontend talks ONLY to
this local server on http://localhost:6180.

SQLite DB (jagoar_oi.db) sits beside this script.
- Server-side scheduler polls all 4 indices every 3 min during market hours.
- Rows logged whether browser is open or not.
- Table wiped every day at 00:00 IST by a background thread.
- /logs endpoint returns today's rows for a given scrip+expiry.
- Market open/closed determined by Upstox /market/status/NSE API (handles holidays).
"""

import gzip
import http.server
import json
import os
import sqlite3
import threading
import time
import urllib.parse
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
    "NIFTY 50":   50,
    "BANK NIFTY": 100,
    "FINNIFTY":   50,
    "SENSEX":     100,
}

# ── Time helpers ───────────────────────────────────────────────────────────────
def now_ist():
    return datetime.now()

def ist_date_str():
    return now_ist().strftime("%Y-%m-%d")

# ── Market status ──────────────────────────────────────────────────────────────
def is_market_open():
    try:
        r = requests.get(f"{BASE}/market/status/NSE", headers=HEADERS, timeout=(5, 10))
        r.raise_for_status()
        status = r.json().get("data", {}).get("status", "")
        return status == "NORMAL_OPEN"
    except Exception:
        t = now_ist()
        if t.weekday() >= 5:
            return False
        cur_min = t.hour * 60 + t.minute
        return (9 * 60 + 15) <= cur_min <= (15 * 60 + 15)

# ── SQLite ─────────────────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def db_init():
    with _db_lock, get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS section_d_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date    TEXT    NOT NULL,
                scrip       TEXT    NOT NULL,
                expiry      TEXT    NOT NULL,
                ts          TEXT    NOT NULL,
                call_oi     INTEGER NOT NULL,
                put_oi      INTEGER NOT NULL,
                diff        INTEGER NOT NULL,
                change_diff INTEGER NOT NULL,
                action      TEXT    NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scrip_expiry_date "
            "ON section_d_logs(log_date, scrip, expiry)"
        )
        conn.commit()

def db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO section_d_logs "
            "(log_date, scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (ist_date_str(), scrip, expiry, ts,
             call_oi, put_oi, diff, change_diff, action)
        )
        conn.commit()

def db_get_last_diff(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "SELECT diff FROM section_d_logs "
            "WHERE log_date=? AND scrip=? AND expiry=? "
            "ORDER BY id DESC LIMIT 1",
            (today, scrip, expiry)
        )
        row = cur.fetchone()
    return row[0] if row else None

def db_get_logs(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "SELECT ts, call_oi, put_oi, diff, change_diff, action "
            "FROM section_d_logs "
            "WHERE log_date=? AND scrip=? AND expiry=? "
            "ORDER BY id ASC",
            (today, scrip, expiry)
        )
        rows = cur.fetchall()
    return [
        {"ts": r[0], "callOI": r[1], "putOI": r[2],
         "diff": r[3], "changeDiff": r[4], "action": r[5]}
        for r in rows
    ]

def db_wipe_today():
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        conn.execute("DELETE FROM section_d_logs WHERE log_date=?", (today,))
        conn.commit()
    print(f"[{now_ist().strftime('%H:%M:%S')} IST] DB wiped for {today}")

# ── Upstox helpers ─────────────────────────────────────────────────────────────
def upstox_get(path, params):
    r = requests.get(f"{BASE}{path}", headers=HEADERS,
                     params=params, timeout=(5, 10))
    r.raise_for_status()
    return r.json()

def get_nearest_expiry(instrument_key):
    data = upstox_get("/option/contract", {"instrument_key": instrument_key})
    expiries = sorted({d.get("expiry") for d in data.get("data", []) if d.get("expiry")})
    today = ist_date_str()
    for exp in expiries:
        if exp >= today:
            return exp
    return expiries[-1] if expiries else None

def get_option_chain(instrument_key, expiry_date):
    data = upstox_get("/option/chain", {
        "instrument_key": instrument_key,
        "expiry_date":    expiry_date,
    })
    rows, pcr_vals, spot = [], [], None
    for item in data.get("data", []):
        spot = item.get("underlying_spot_price")
        if item.get("pcr") is not None:
            pcr_vals.append(item["pcr"])
        call = item.get("call_options", {}).get("market_data", {}) or {}
        put  = item.get("put_options",  {}).get("market_data", {}) or {}
        call_oi   = call.get("oi", 0) or 0
        call_prev = call.get("prev_oi", 0) or 0
        put_oi    = put.get("oi", 0) or 0
        put_prev  = put.get("prev_oi", 0) or 0
        rows.append({
            "strike":         item.get("strike_price"),
            "call_oi_change": call_oi - call_prev,
            "put_oi_change":  put_oi  - put_prev,
            "call_oi":        call_oi,
            "put_oi":         put_oi,
            "call_volume":    call.get("volume", 0) or 0,
            "put_volume":     put.get("volume", 0) or 0,
            "call_ltp":       call.get("ltp", 0) or 0,
            "put_ltp":        put.get("ltp", 0) or 0,
        })
    rows.sort(key=lambda x: (x["strike"] is None, x["strike"]))
    return {
        "rows": rows,
        "spot": spot,
        "pcr":  pcr_vals[0] if pcr_vals else None,
        "ts":   now_ist().strftime("%H:%M:%S"),
    }

def atm_index(rows, spot):
    best, bd = 0, float("inf")
    for i, r in enumerate(rows):
        d = abs((r["strike"] or 0) - (spot or 0))
        if d < bd:
            bd = d; best = i
    return best

def compute_section_d(rows, atm, scrip, expiry):
    start   = max(0, atm - 1)
    end     = min(len(rows), atm + 2)
    call_oi = sum(rows[i]["call_oi_change"] for i in range(start, end))
    put_oi  = sum(rows[i]["put_oi_change"]  for i in range(start, end))
    diff    = call_oi - put_oi
    last_diff   = db_get_last_diff(scrip, expiry)
    change_diff = 0 if last_diff is None else diff - last_diff
    action = ""
    if call_oi - put_oi >  TEN_LAC: action = "BUY PUT"
    if put_oi  - call_oi > TEN_LAC: action = "BUY CALL"
    ts = now_ist().strftime("%H:%M:%S")
    db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action)
    print(f"[{ts} IST] Logged {scrip} | expiry={expiry} | callOI={call_oi} putOI={put_oi} diff={diff} action={action or '-'}")

# ── Backtest helpers ───────────────────────────────────────────────────────────
def get_fno_stocks():
    r = requests.get(
        "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
        timeout=(10, 30)
    )
    data = json.loads(gzip.decompress(r.content))
    seen, result = set(), []
    for d in data:
        if (d.get("segment") == "NSE_FO"
                and d.get("instrument_type") == "CE"
                and d.get("underlying_type") == "EQUITY"
                and d.get("underlying_symbol")
                and d.get("underlying_key")):
            sym = d["underlying_symbol"]
            if sym not in seen:
                seen.add(sym)
                result.append({
                    "symbol":         sym,
                    "instrument_key": d["underlying_key"],
                })
    return sorted(result, key=lambda x: x["symbol"])

# ── Candle cache (SQLite) ────────────────────────────────────────────────────
# Separate table from section_d_logs so the midnight wiper never touches it.
# Cached candles are immutable history, so repeat backtests are instant.
def candle_cache_init():
    with _db_lock, get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candle_cache (
                instrument_key TEXT    NOT NULL,
                interval       TEXT    NOT NULL,   -- e.g. 'minutes/3', 'day'
                ts             TEXT    NOT NULL,   -- full ISO timestamp from Upstox
                open           REAL    NOT NULL,
                high           REAL    NOT NULL,
                low            REAL    NOT NULL,
                close          REAL    NOT NULL,
                volume         REAL    NOT NULL,
                PRIMARY KEY (instrument_key, interval, ts)
            )
        """)
        conn.commit()

def candle_cache_get(instrument_key, interval, from_date, to_date):
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT ts, open, high, low, close, volume FROM candle_cache "
            "WHERE instrument_key=? AND interval=? AND substr(ts,1,10) BETWEEN ? AND ? "
            "ORDER BY ts ASC",
            (instrument_key, interval, from_date, to_date),
        ).fetchall()
    return [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5]} for r in rows]

def candle_cache_put(instrument_key, interval, candles):
    if not candles:
        return
    with _db_lock, get_conn() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO candle_cache "
            "(instrument_key, interval, ts, open, high, low, close, volume) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [(instrument_key, interval, c["ts"], c["open"], c["high"],
              c["low"], c["close"], c["volume"]) for c in candles],
        )
        conn.commit()

# ── V3 candle fetcher with chunking ──────────────────────────────────────────
# Timeframe map → (unit, interval, max_days_per_request).
# Sub-15-min intervals have a small per-request window (Upstox caps ~1 month and
# has been seen as low as ~6 days), so we chunk conservatively. 'day' is one shot.
TIMEFRAMES = {
    "1min":  ("minutes", "1",  20),
    "3min":  ("minutes", "3",  20),
    "5min":  ("minutes", "5",  25),
    "15min": ("minutes", "15", 90),
    "day":   ("days",    "1",  365),
}
V3_BASE = "https://api.upstox.com/v3"

def _date_chunks(from_date, to_date, max_days):
    from datetime import timedelta
    d0 = datetime.strptime(from_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(to_date,   "%Y-%m-%d").date()
    chunks = []
    cur = d0
    while cur <= d1:
        end = min(cur + timedelta(days=max_days - 1), d1)
        chunks.append((cur.isoformat(), end.isoformat()))
        cur = end + timedelta(days=1)
    return chunks

def _fetch_v3(instrument_key, unit, interval, chunk_from, chunk_to):
    ik_encoded = instrument_key.replace("|", "%7C")
    url = f"{V3_BASE}/historical-candle/{ik_encoded}/{unit}/{interval}/{chunk_to}/{chunk_from}"
    r = requests.get(url, headers=HEADERS, timeout=(10, 30))
    r.raise_for_status()
    raw = r.json().get("data", {}).get("candles", [])
    out = []
    for c in raw:
        out.append({"ts": c[0], "open": c[1], "high": c[2],
                    "low": c[3], "close": c[4], "volume": c[5]})
    return out

def get_candles(instrument_key, timeframe, from_date, to_date):
    """Cache-first. Fetches only what's missing, chunked, then returns the full range."""
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"bad timeframe: {timeframe}")
    unit, interval, max_days = TIMEFRAMES[timeframe]
    cache_key = f"{unit}/{interval}"

    cached = candle_cache_get(instrument_key, cache_key, from_date, to_date)
    if cached:
        return cached  # whole range present (or partial — accepted; history is append-only)

    all_candles = []
    for cf, ct in _date_chunks(from_date, to_date, max_days):
        try:
            chunk = _fetch_v3(instrument_key, unit, interval, cf, ct)
            all_candles.extend(chunk)
            time.sleep(0.25)  # gentle on rate limits
        except requests.HTTPError:
            time.sleep(0.5)   # skip a bad window rather than abort the whole run
            continue
    # dedupe by ts, sort
    seen = {}
    for c in all_candles:
        seen[c["ts"]] = c
    merged = sorted(seen.values(), key=lambda x: x["ts"])
    candle_cache_put(instrument_key, cache_key, merged)
    return merged

# ── Opening Range Breakout backtest ───────────────────────────────────────────
# Strategy:
#   Opening range = 09:15–09:45 (first 30 min). Record OR_high / OR_low.
#   After 09:45, first candle whose high > OR_high → LONG at OR_high;
#                first candle whose low  < OR_low  → SHORT at OR_low.
#   Whichever breaks first that day takes the single trade.
#   Exit (first to occur): target hit (entry ± X), SL hit (entry ∓ Y),
#   or 15:00 force square-off at that candle's close.
#   Same-candle target+SL → counted as SL (pessimistic).
def _group_by_day(candles):
    days = {}
    for c in candles:
        day = c["ts"][:10]
        days.setdefault(day, []).append(c)
    for d in days:
        days[d].sort(key=lambda x: x["ts"])
    return days

def _minutes_of(ts):
    # ts like '2025-01-12T09:45:00+05:30' → minutes since midnight
    hh = int(ts[11:13]); mm = int(ts[14:16])
    return hh * 60 + mm

OR_START = 9 * 60 + 15   # 09:15
OR_END   = 9 * 60 + 45   # 09:45 (opening range is candles starting < 09:45)
SQUARE_OFF = 15 * 60     # 15:00

def run_orb_backtest(instrument_key, symbol, timeframe, from_date, to_date, x_pts, y_pts):
    candles = get_candles(instrument_key, timeframe, from_date, to_date)
    days = _group_by_day(candles)
    trades = []

    for day in sorted(days.keys()):
        bars = days[day]
        # Opening range: bars that start within 09:15–09:45
        or_bars = [b for b in bars if OR_START <= _minutes_of(b["ts"]) < OR_END]
        if not or_bars:
            continue
        or_high = max(b["high"] for b in or_bars)
        or_low  = min(b["low"]  for b in or_bars)

        # Scan bars after the opening range for the first breakout
        post = [b for b in bars if _minutes_of(b["ts"]) >= OR_END]
        entry = side = None
        entry_idx = None
        for i, b in enumerate(post):
            broke_up   = b["high"] > or_high
            broke_down = b["low"]  < or_low
            if broke_up and broke_down:
                # both in same bar — take the side closer to bar open (pessimistic ambiguity)
                side  = "LONG" if abs(b["open"] - or_high) <= abs(b["open"] - or_low) else "SHORT"
                entry = or_high if side == "LONG" else or_low
            elif broke_up:
                side, entry = "LONG", or_high
            elif broke_down:
                side, entry = "SHORT", or_low
            if side:
                entry_idx = i
                break
        if side is None:
            continue  # no breakout that day → no trade

        if side == "LONG":
            target = round(entry + x_pts, 2)
            sl     = round(entry - y_pts, 2)
        else:
            target = round(entry - x_pts, 2)
            sl     = round(entry + y_pts, 2)

        # Walk forward from the breakout bar to find exit
        exit_price = result = exit_time = None
        for b in post[entry_idx:]:
            tmin = _minutes_of(b["ts"])
            hit_target = (b["high"] >= target) if side == "LONG" else (b["low"]  <= target)
            hit_sl     = (b["low"]  <= sl)     if side == "LONG" else (b["high"] >= sl)
            if hit_target and hit_sl:
                exit_price, result = sl, "LOSS"          # pessimistic same-bar tiebreak
            elif hit_target:
                exit_price, result = target, "WIN"
            elif hit_sl:
                exit_price, result = sl, "LOSS"
            if result:
                exit_time = b["ts"][11:16]
                break
            if tmin >= SQUARE_OFF:                        # force square-off at 15:00
                exit_price = b["close"]
                exit_time  = b["ts"][11:16]
                break
        if exit_price is None:                            # ran out of bars before 15:00
            last = post[-1]
            exit_price = last["close"]
            exit_time  = last["ts"][11:16]

        if side == "LONG":
            pnl = round(exit_price - entry, 2)
        else:
            pnl = round(entry - exit_price, 2)
        if result is None:
            result = "WIN" if pnl > 0 else "LOSS"
        pnl_pct = round((pnl / entry) * 100, 2) if entry else 0

        trades.append({
            "symbol":     symbol,
            "entry_date": day,
            "side":       side,
            "entry":      round(entry, 2),
            "sl":         sl,
            "target":     target,
            "exit":       round(exit_price, 2),
            "exit_time":  exit_time,
            "result":     result,
            "pnl":        pnl,
            "pnl_pct":    pnl_pct,
        })
    return trades

# ── Scheduler ──────────────────────────────────────────────────────────────────
def scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')} IST] Scheduler started.")
    while True:
        time.sleep(POLL_INTERVAL)
        try:
            if not is_market_open():
                print(f"[{now_ist().strftime('%H:%M:%S')} IST] Market closed — skipping poll.")
                continue
            print(f"[{now_ist().strftime('%H:%M:%S')} IST] Polling all indices...")
            for scrip, ik in INDEX_KEYS.items():
                try:
                    expiry = get_nearest_expiry(ik)
                    if not expiry:
                        continue
                    chain = get_option_chain(ik, expiry)
                    rows  = chain["rows"]
                    spot  = chain["spot"]
                    if not rows or spot is None:
                        continue
                    atm = atm_index(rows, spot)
                    compute_section_d(rows, atm, scrip, expiry)
                except Exception as e:
                    print(f"  {scrip}: error — {e}")
        except Exception as e:
            print(f"[{now_ist().strftime('%H:%M:%S')} IST] Scheduler error: {e}")

# ── Midnight wiper ─────────────────────────────────────────────────────────────
def midnight_wiper():
    while True:
        t = now_ist()
        secs_until = ((23 - t.hour) * 3600
                      + (59 - t.minute) * 60
                      + (60 - t.second))
        time.sleep(secs_until + 5)
        db_wipe_today()

# ── HTTP handler ───────────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type",                "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length",              str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs     = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path == "/indices":
                keys = ",".join(INDEX_KEYS.values())
                self._send(200, upstox_get("/market-quote/ltp", {"instrument_key": keys}))

            elif parsed.path == "/expiries":
                ik = qs.get("instrument_key", [""])[0]
                data = upstox_get("/option/contract", {"instrument_key": ik})
                expiries = sorted({d.get("expiry") for d in data.get("data", []) if d.get("expiry")})
                self._send(200, {"expiries": expiries})

            elif parsed.path == "/chain":
                ik  = qs.get("instrument_key", [""])[0]
                exp = qs.get("expiry_date",    [""])[0]
                chain = get_option_chain(ik, exp)
                chain["market_open"] = is_market_open()
                self._send(200, chain)

            elif parsed.path == "/logs":
                scrip = qs.get("scrip",  [""])[0]
                exp   = qs.get("expiry", [""])[0]
                self._send(200, {"logs": db_get_logs(scrip, exp)})

            elif parsed.path == "/log":
                scrip     = qs.get("scrip",       [""])[0]
                exp       = qs.get("expiry",      [""])[0]
                ts        = qs.get("ts",          [now_ist().strftime("%H:%M:%S")])[0]
                call_oi   = qs.get("call_oi",     [None])[0]
                put_oi_p  = qs.get("put_oi",      [None])[0]
                diff_p    = qs.get("diff",        [None])[0]
                chgdiff_p = qs.get("change_diff", [None])[0]
                action_p  = qs.get("action",      [""])[0]
                if scrip and exp and call_oi is not None and is_market_open():
                    db_insert_log(
                        scrip, exp, ts,
                        int(call_oi), int(put_oi_p or 0),
                        int(diff_p or 0), int(chgdiff_p or 0),
                        action_p
                    )
                self._send(200, {"ok": True})

            elif parsed.path in ("/", "/index.html"):
                try:
                    with open(HTML_FILE, "rb") as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type",   "text/html")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except FileNotFoundError:
                    self._send(404, {"error": f"{HTML_FILE} not found"})

            elif parsed.path == "/instruments":
                self._send(200, {"indices": INDEX_KEYS, "strike_step": STRIKE_STEP})

            elif parsed.path == "/fno_stocks":
                self._send(200, {"stocks": get_fno_stocks()})

            elif parsed.path == "/backtest":
                ik        = qs.get("instrument_key", [""])[0]
                symbol    = qs.get("symbol",         [""])[0]
                timeframe = qs.get("timeframe",      ["3min"])[0]
                from_date = qs.get("from_date",      [""])[0]
                to_date   = qs.get("to_date",        [ist_date_str()])[0]
                x_pts     = float(qs.get("x_pts",    ["50"])[0])   # target in points
                y_pts     = float(qs.get("y_pts",    ["30"])[0])   # stoploss in points
                if not ik or not from_date:
                    self._send(400, {"error": "instrument_key and from_date required"})
                else:
                    trades    = run_orb_backtest(ik, symbol, timeframe, from_date, to_date, x_pts, y_pts)
                    wins      = sum(1 for t in trades if t["result"] == "WIN")
                    losses    = len(trades) - wins
                    longs     = sum(1 for t in trades if t["side"] == "LONG")
                    shorts    = len(trades) - longs
                    total_pnl = round(sum(t["pnl"] for t in trades), 2)
                    self._send(200, {
                        "trades": trades,
                        "summary": {
                            "total":     len(trades),
                            "wins":      wins,
                            "losses":    losses,
                            "longs":     longs,
                            "shorts":    shorts,
                            "win_rate":  round(wins / len(trades) * 100, 1) if trades else 0,
                            "total_pnl": total_pnl,
                        }
                    })

            else:
                self._send(404, {"error": "unknown path"})

        except requests.HTTPError as e:
            self._send(502, {"error": "upstox", "detail": str(e),
                             "body": getattr(e.response, "text", "")})
        except Exception as e:
            self._send(500, {"error": str(e)})


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    db_init()
    candle_cache_init()
    threading.Thread(target=midnight_wiper, daemon=True).start()
    print(f"[{now_ist().strftime('%H:%M:%S')} IST] Midnight wiper started.")
    threading.Thread(target=scheduler, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"JAGOAR OI server → http://0.0.0.0:{PORT}")
    print(f"Market open now  : {is_market_open()}")
    server.serve_forever()