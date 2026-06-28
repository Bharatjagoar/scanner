#!/usr/bin/env python3
"""
MANJIT JAGOAR OI DATA SCANNER - backend server.

Tabs:
  1. OI SCANNER  — live option chain, section B/C/D, DB-logged every 3 min
  2. BACKTEST    — ORB historical backtest, candle cache
  3. TRENDING    — top-N rising + falling NSE FnO stocks (3-day consecutive
                   close filter), 30-min ORB entry (9:15-9:45), futures breakout
                   at 9:45, capital 20L, multiple lots within capital budget.
                   Square-off 15:25 IST.
  4. TODAY'S TRADES — Equity cash tab (top 4 rising + falling)
  5. FNO TRADES  — 5 stocks, futures ORB 30-min breakout, balance tracker
"""

import gzip, http.server, json, os, sqlite3, threading, time, urllib.parse
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    raise SystemExit("Run: pip install requests")

PORT = 6180
HTML_FILE = "jagoar_oi_scanner.html"
DB_FILE = "jagoar_oi.db"
BASE = "https://api.upstox.com/v2"
V3_BASE = "https://api.upstox.com/v3"
POLL_INTERVAL = 3 * 60
TEN_LAC = 1_000_000

ACCESS_TOKEN = os.environ.get(
    "UPSTOX_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0QUNFVUwiLCJqdGkiOiI2OWY3ODAzMmJmYWU5ODAyMjEzYWJjZDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc3NzgyNzg5MCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODA5MzgxNjAwfQ.DyDCLRfFvCDG59gRL96XUTow-66vUmP45cVdAZZQIfI",
)

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
}

INDEX_KEYS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "SENSEX": "BSE_INDEX|SENSEX",
}
STRIKE_STEP = {
    "NIFTY 50": 50,
    "BANK NIFTY": 100,
    "FINNIFTY": 50,
    "SENSEX": 100,
}

# ── FnO stocks (for FNO TRADES tab only) ──────────────────────────────────────
FNO_STOCKS = [
    {
        "symbol": "DIXON",
        "eq_key": "NSE_EQ|INE935N01020",
        "fut_key": None,
        "lot_size": 25,
    },
    {
        "symbol": "FORCEMOT",
        "eq_key": "NSE_EQ|INE451H01013",
        "fut_key": None,
        "lot_size": 50,
    },
    {
        "symbol": "POWERINDIA",
        "eq_key": "NSE_EQ|INE195N01010",
        "fut_key": None,
        "lot_size": 100,
    },
    {
        "symbol": "BSE",
        "eq_key": "NSE_EQ|INE118H01025",
        "fut_key": None,
        "lot_size": 150,
    },
    {
        "symbol": "MCX",
        "eq_key": "NSE_EQ|INE745G01035",
        "fut_key": None,
        "lot_size": 125,
    },
]

# ── FnO trade defaults ─────────────────────────────────────────────────────────
FNO_DEFAULT_SL_PCT = 1.0
FNO_DEFAULT_TARGET_PCT = 1.0
FNO_MARGIN_PCT = 0.15
FNO_REENTRY_TOLERANCE = 0.005
FNO_OR_START = 9 * 60 + 15
FNO_OR_END = 9 * 60 + 45
FNO_SQUAREOFF = 15 * 60 + 25
INITIAL_BALANCE = 500_000.0

# ── Trending config ────────────────────────────────────────────────────────────
TR_TOP_N = 10
TR_DAYS_CONSEC = 3
TR_SCAN_HOUR = 9  # scan triggers at >= 9:00
TR_SCAN_MIN = 0
TR_OR_START = 9 * 60 + 15  # start building OR
TR_OR_END = 9 * 60 + 45  # OR locks, breakout watch begins
TR_ENTRY_END = 14 * 60 + 30  # no new entries after 14:30
TR_SQUAREOFF = 15 * 60 + 25  # square off at 15:25
TR_DEFAULT_SL_PCT = 1.0
TR_DEFAULT_TGT_PCT = 1.0
TR_CAPITAL = 20_00_000  # 20 lac total capital for trending tab

# Trending shared state
_tr_lock = threading.Lock()
_tr_state = {
    "rising": [],
    "falling": [],
    "scanned_date": None,
    "sl_pct": TR_DEFAULT_SL_PCT,
    "target_pct": TR_DEFAULT_TGT_PCT,
    "intraday": {},  # sym -> {or_high, or_low, or_locked, entered}
    "capital_deployed": 0.0,  # notional currently in open trades
    "capital": TR_CAPITAL,  # effective capital (can be set next-day)
    "capital_next": TR_CAPITAL,  # pending capital for next trading day
}

# Futures key + lot size cache for trending stocks
_tr_fut_cache = {}
_tr_fut_lock = threading.Lock()

# ── Equity Cash (Tab 4) shared state ──────────────────────────────────────────
_eq_lock = threading.Lock()
_eq_state = {
    "capital": 10_000.0,
    "sl_pct": 1.0,
    "target_pct": 1.0,
    "intraday": {},
}
EQ_TOP_N = 4
EQ_ENTRY_START = 9 * 60 + 15
EQ_ENTRY_END = 14 * 60 + 30
EQ_SQUAREOFF = 15 * 60 + 25

# ── FnO shared state ───────────────────────────────────────────────────────────
_fno_lock = threading.Lock()
_fno_state = {
    "balance": INITIAL_BALANCE,
    "sl_pct": FNO_DEFAULT_SL_PCT,
    "target_pct": FNO_DEFAULT_TARGET_PCT,
    "or_data": {},
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


# ── Expiry normaliser ──────────────────────────────────────────────────────────
def _expiry_str(val):
    if isinstance(val, int):
        return datetime.fromtimestamp(val / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return str(val)[:10]


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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scrip_expiry_date ON section_d_logs(log_date,scrip,expiry)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS trending_logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date     TEXT    NOT NULL,
                symbol       TEXT    NOT NULL,
                fut_key      TEXT,
                lot_size     INTEGER NOT NULL DEFAULT 1,
                lots_taken   INTEGER NOT NULL DEFAULT 1,
                side         TEXT    NOT NULL DEFAULT 'LONG',
                entry_price  REAL    NOT NULL,
                entry_time   TEXT    NOT NULL,
                sl_pct       REAL    NOT NULL,
                target_pct   REAL    NOT NULL,
                sl_price     REAL    NOT NULL,
                target_price REAL    NOT NULL,
                notional     REAL,
                exit_price   REAL,
                exit_time    TEXT,
                result       TEXT    NOT NULL DEFAULT 'OPEN',
                pnl_pts      REAL,
                pnl_pct      REAL,
                pnl_inr      REAL
            )""")
        # Safe column additions for existing DBs
        for col, defn in [
            ("fut_key", "TEXT"),
            ("lot_size", "INTEGER NOT NULL DEFAULT 1"),
            ("lots_taken", "INTEGER NOT NULL DEFAULT 1"),
            ("side", "TEXT NOT NULL DEFAULT 'LONG'"),
            ("pnl_inr", "REAL"),
            ("notional", "REAL"),
        ]:
            try:
                conn.execute(f"ALTER TABLE trending_logs ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trending_date_symbol ON trending_logs(log_date,symbol)"
        )

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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fno_date_sym ON fno_trades(log_date,symbol)"
        )

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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS eq_cash_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                log_date     TEXT    NOT NULL,
                symbol       TEXT    NOT NULL,
                eq_key       TEXT    NOT NULL,
                side         TEXT    NOT NULL,
                qty          INTEGER NOT NULL,
                capital_used REAL    NOT NULL,
                entry_price  REAL    NOT NULL,
                entry_time   TEXT    NOT NULL,
                sl_pct       REAL    NOT NULL,
                target_pct   REAL    NOT NULL,
                sl_price     REAL    NOT NULL,
                target_price REAL    NOT NULL,
                exit_price   REAL,
                exit_time    TEXT,
                result       TEXT    NOT NULL DEFAULT 'OPEN',
                pnl_pts      REAL,
                pnl_pct      REAL,
                pnl_inr      REAL
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_eq_date_sym ON eq_cash_trades(log_date,symbol)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                tr_sl           REAL    DEFAULT 1.0,
                tr_tgt          REAL    DEFAULT 1.0,
                tr_capital      REAL    DEFAULT 2000000.0,
                tr_capital_next REAL    DEFAULT 2000000.0,
                eq_cap          REAL    DEFAULT 10000.0,
                eq_sl           REAL    DEFAULT 1.0,
                eq_tgt          REAL    DEFAULT 1.0,
                fno_sl          REAL    DEFAULT 1.0,
                fno_tgt         REAL    DEFAULT 1.0
            )""")
        # Safe column additions for existing user_settings
        for col, defn in [
            ("tr_capital", "REAL DEFAULT 2000000.0"),
            ("tr_capital_next", "REAL DEFAULT 2000000.0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE user_settings ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.execute("INSERT OR IGNORE INTO user_settings (id) VALUES (1)")
        conn.commit()


def db_update_setting(column, value):
    with _db_lock, get_conn() as conn:
        conn.execute(f"UPDATE user_settings SET {column}=? WHERE id=1", (value,))
        conn.commit()


def db_load_settings():
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT tr_sl, tr_tgt, tr_capital, tr_capital_next, "
            "eq_cap, eq_sl, eq_tgt, fno_sl, fno_tgt FROM user_settings WHERE id=1"
        ).fetchone()
        if row:
            with _tr_lock:
                _tr_state["sl_pct"] = row[0]
                _tr_state["target_pct"] = row[1]
                _tr_state["capital"] = row[2] if row[2] else TR_CAPITAL
                _tr_state["capital_next"] = row[3] if row[3] else TR_CAPITAL
            with _eq_lock:
                _eq_state["capital"] = row[4]
                _eq_state["sl_pct"] = row[5]
                _eq_state["target_pct"] = row[6]
            with _fno_lock:
                _fno_state["sl_pct"] = row[7]
                _fno_state["target_pct"] = row[8]
            print(f"[{now_ist().strftime('%H:%M:%S')}] Loaded user settings from DB.")


def _load_deployed_capital():
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT notional FROM trending_logs WHERE result='OPEN'"
        ).fetchall()
    total = sum(r[0] for r in rows if r[0])
    with _tr_lock:
        _tr_state["capital_deployed"] = total
    print(f"  [TR] Restored capital_deployed: ₹{total:,.0f}")


# ── OI scanner DB helpers ──────────────────────────────────────────────────────
def db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO section_d_logs (log_date,scrip,expiry,ts,call_oi,put_oi,diff,change_diff,action) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                ist_date_str(),
                scrip,
                expiry,
                ts,
                call_oi,
                put_oi,
                diff,
                change_diff,
                action,
            ),
        )
        conn.commit()


def db_get_last_diff(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "SELECT diff FROM section_d_logs WHERE log_date=? AND scrip=? AND expiry=? ORDER BY id DESC LIMIT 1",
            (today, scrip, expiry),
        )
        row = cur.fetchone()
    return row[0] if row else None


def db_get_logs(scrip, expiry):
    today = ist_date_str()
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT ts,call_oi,put_oi,diff,change_diff,action FROM section_d_logs "
            "WHERE log_date=? AND scrip=? AND expiry=? ORDER BY id ASC",
            (today, scrip, expiry),
        ).fetchall()
    return [
        {
            "ts": r[0],
            "callOI": r[1],
            "putOI": r[2],
            "diff": r[3],
            "changeDiff": r[4],
            "action": r[5],
        }
        for r in rows
    ]


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
            "SELECT id,fut_key,lot_size,lots_taken,side,entry_price,entry_time,"
            "sl_pct,target_pct,sl_price,target_price,notional,"
            "exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
            "FROM trending_logs WHERE log_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
            (log_date, symbol),
        ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "fut_key": row[1],
        "lot_size": row[2],
        "lots_taken": row[3],
        "side": row[4],
        "entry_price": row[5],
        "entry_time": row[6],
        "sl_pct": row[7],
        "target_pct": row[8],
        "sl_price": row[9],
        "target_price": row[10],
        "notional": row[11],
        "exit_price": row[12],
        "exit_time": row[13],
        "result": row[14],
        "pnl_pts": row[15],
        "pnl_pct": row[16],
        "pnl_inr": row[17],
    }


def trending_insert(
    symbol,
    log_date,
    fut_key,
    lot_size,
    lots_taken,
    side,
    entry_price,
    entry_time,
    sl_pct,
    target_pct,
    sl_price,
    target_price,
    notional,
):
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO trending_logs "
            "(log_date,symbol,fut_key,lot_size,lots_taken,side,entry_price,entry_time,"
            "sl_pct,target_pct,sl_price,target_price,notional,result) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')",
            (
                log_date,
                symbol,
                fut_key,
                lot_size,
                lots_taken,
                side,
                entry_price,
                entry_time,
                sl_pct,
                target_pct,
                sl_price,
                target_price,
                notional,
            ),
        )
        conn.commit()
        return cur.lastrowid


def trending_update_exit(
    row_id, exit_price, exit_time, result, pnl_pts, pnl_pct, pnl_inr
):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE trending_logs SET exit_price=?,exit_time=?,result=?,"
            "pnl_pts=?,pnl_pct=?,pnl_inr=? WHERE id=?",
            (exit_price, exit_time, result, pnl_pts, pnl_pct, pnl_inr, row_id),
        )
        conn.commit()


def trending_get_history(symbol=None, days=30):
    with _db_lock, get_conn() as conn:
        if symbol:
            rows = conn.execute(
                "SELECT log_date,symbol,fut_key,lot_size,lots_taken,side,entry_price,entry_time,"
                "sl_pct,target_pct,sl_price,target_price,notional,"
                "exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
                "FROM trending_logs WHERE symbol=? "
                "ORDER BY log_date DESC,id DESC LIMIT ?",
                (symbol, days * 5),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT log_date,symbol,fut_key,lot_size,lots_taken,side,entry_price,entry_time,"
                "sl_pct,target_pct,sl_price,target_price,notional,"
                "exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
                "FROM trending_logs ORDER BY log_date DESC,id DESC LIMIT ?",
                (days * 20,),
            ).fetchall()
    return [
        {
            "log_date": r[0],
            "symbol": r[1],
            "fut_key": r[2],
            "lot_size": r[3],
            "lots_taken": r[4],
            "side": r[5],
            "entry_price": r[6],
            "entry_time": r[7],
            "sl_pct": r[8],
            "target_pct": r[9],
            "sl_price": r[10],
            "target_price": r[11],
            "notional": r[12],
            "exit_price": r[13],
            "exit_time": r[14],
            "result": r[15],
            "pnl_pts": r[16],
            "pnl_pct": r[17],
            "pnl_inr": r[18],
        }
        for r in rows
    ]


def trending_daily_summary():
    """Per-day aggregated P&L for history view."""
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT log_date, COUNT(*) as trades, "
            "SUM(CASE WHEN result='TGT HIT' THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN result='SL HIT' THEN 1 ELSE 0 END) as losses, "
            "ROUND(SUM(COALESCE(pnl_inr,0)),2) as total_pnl "
            "FROM trending_logs GROUP BY log_date ORDER BY log_date DESC LIMIT 60"
        ).fetchall()
    return [
        {"date": r[0], "trades": r[1], "wins": r[2], "losses": r[3], "pnl_inr": r[4]}
        for r in rows
    ]


# ── NSE instruments loader ─────────────────────────────────────────────────────
def _load_nse_instruments():
    local = "NSE.json.gz"
    if os.path.exists(local):
        with open(local, "rb") as f:
            return json.loads(gzip.decompress(f.read()))
    try:
        r = requests.get(
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            timeout=(10, 30),
        )
        r.raise_for_status()
        return json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"  NSE instruments load failed: {e}")
        return []


# ── Trending: resolve futures key + lot size for a symbol ─────────────────────
def _resolve_tr_fut(symbol, instruments, today):
    with _tr_fut_lock:
        if symbol in _tr_fut_cache:
            return _tr_fut_cache[symbol]

    rows = [
        d
        for d in instruments
        if d.get("underlying_symbol") == symbol and d.get("segment") == "NSE_FO"
    ]
    if not rows:
        return None

    lot_size = int(rows[0].get("lot_size") or 1)
    fut_rows = [
        d
        for d in rows
        if d.get("instrument_type") == "FUT"
        and _expiry_str(d.get("expiry", 0)) >= today
        and d.get("instrument_key")
    ]
    if not fut_rows:
        return None

    fut_rows.sort(key=lambda x: _expiry_str(x["expiry"]))
    fut_key = fut_rows[0]["instrument_key"]
    lot_size = int(fut_rows[0].get("lot_size") or lot_size)

    result = {"fut_key": fut_key, "lot_size": lot_size}
    print(f"  [TR] RESOLVED {symbol}: fut={fut_key} lot_size={lot_size}")
    with _tr_fut_lock:
        _tr_fut_cache[symbol] = result
    return result


# ── Trending: 3-day scan ───────────────────────────────────────────────────────
def _fetch_daily_data(instrument_key, n_days=5):
    from datetime import timedelta

    today = datetime.now().date()
    from_date = (today - timedelta(days=n_days * 3)).isoformat()
    to_date = (today - timedelta(days=1)).isoformat()
    try:
        ik_enc = instrument_key.replace("|", "%7C")
        url = f"{V3_BASE}/historical-candle/{ik_enc}/days/1/{to_date}/{from_date}"
        r = requests.get(url, headers=HEADERS, timeout=(8, 20))
        r.raise_for_status()
        candles = r.json().get("data", {}).get("candles", [])
        data = [{"close": c[4], "volume": c[5]} for c in candles if len(c) >= 6]
        data.reverse()
        return data[-n_days:] if len(data) >= n_days else []
    except Exception:
        return []


def _scan_trending_stocks():
    """
    Scan all NSE FnO equity stocks for 3-day consecutive rising/falling.
    Volume SMA-5 filter: yesterday's volume must be above 5-day average.
    """
    print(f"[{now_ist().strftime('%H:%M:%S')}] Trending scan started...")
    instruments = _load_nse_instruments()
    today = ist_date_str()

    stocks = []
    seen = set()
    for d in instruments:
        if (
            d.get("segment") == "NSE_FO"
            and d.get("instrument_type") == "CE"
            and d.get("underlying_type") == "EQUITY"
            and d.get("underlying_symbol")
            and d.get("underlying_key")
        ):
            sym = d["underlying_symbol"]
            if sym not in seen:
                seen.add(sym)
                stocks.append({"symbol": sym, "instrument_key": d["underlying_key"]})

    rising, falling = [], []

    for s in stocks:
        daily_data = _fetch_daily_data(s["instrument_key"], n_days=5)
        if len(daily_data) < 5:
            time.sleep(0.05)
            continue

        closes = [d["close"] for d in daily_data]
        volumes = [d["volume"] for d in daily_data]

        d1, d2, d3 = closes[-1], closes[-2], closes[-3]
        v1 = volumes[-1]
        vol_sma5 = sum(volumes) / 5
        gain3 = round((d1 - closes[-4]) / closes[-4] * 100, 3) if closes[-4] else 0
        vol_ok = v1 > vol_sma5

        qualifies_rising = (d1 > d2 > d3) and vol_ok
        qualifies_falling = (d1 < d2 < d3) and vol_ok

        if not qualifies_rising and not qualifies_falling:
            time.sleep(0.05)
            continue

        fut_info = _resolve_tr_fut(s["symbol"], instruments, today)
        if not fut_info:
            time.sleep(0.05)
            continue

        entry = {
            "symbol": s["symbol"],
            "instrument_key": s["instrument_key"],
            "fut_key": fut_info["fut_key"],
            "lot_size": fut_info["lot_size"],
            "gain3d_pct": gain3,
            "close_d1": d1,
            "close_d2": d2,
            "close_d3": d3,
        }

        if qualifies_rising:
            rising.append(entry)
        else:
            falling.append(entry)

        time.sleep(0.05)

    rising.sort(key=lambda x: x["gain3d_pct"], reverse=True)
    falling.sort(key=lambda x: x["gain3d_pct"])
    rising = rising[:TR_TOP_N]
    falling = falling[:TR_TOP_N]

    print(f"  Trending scan done: {len(rising)} rising, {len(falling)} falling")
    return rising, falling


# ── Trending: enter a futures trade ───────────────────────────────────────────
def _tr_enter(symbol, fut_key, lot_size, side, ltp, today):
    """
    Calculate how many lots we can take within the remaining capital budget,
    then insert the trade. Returns True if entered.
    """
    if trending_already_entered(symbol, today):
        return False

    with _tr_lock:
        sl_pct = _tr_state["sl_pct"]
        tgt_pct = _tr_state["target_pct"]
        capital = _tr_state["capital"]
        deployed = _tr_state["capital_deployed"]

    remaining = capital - deployed
    notional_per_lot = ltp * lot_size

    if notional_per_lot <= 0 or remaining < notional_per_lot:
        print(
            f"  [TR] {symbol}: no room. remaining=₹{remaining:,.0f} need=₹{notional_per_lot:,.0f}"
        )
        return False

    # Take as many lots as remaining capital allows
    lots_taken = 1
    total_notional = round(lots_taken * notional_per_lot, 2)

    if side == "LONG":
        sl_price = round(ltp * (1 - sl_pct / 100), 2)
        target_price = round(ltp * (1 + tgt_pct / 100), 2)
    else:
        sl_price = round(ltp * (1 + sl_pct / 100), 2)
        target_price = round(ltp * (1 - tgt_pct / 100), 2)

    entry_time = now_ist().strftime("%H:%M:%S")
    trending_insert(
        symbol,
        today,
        fut_key,
        lot_size,
        lots_taken,
        side,
        ltp,
        entry_time,
        sl_pct,
        tgt_pct,
        sl_price,
        target_price,
        total_notional,
    )

    with _tr_lock:
        _tr_state["capital_deployed"] += total_notional

    print(
        f"  [TR] ENTRY {symbol} {side} @ ₹{ltp} | lots={lots_taken} "
        f"| lot_size={lot_size} | notional=₹{total_notional:,.0f} "
        f"| SL={sl_price} TGT={target_price}"
    )
    return True


# ── Trending: check exits ──────────────────────────────────────────────────────
def _tr_all_active():
    with _tr_lock:
        return list(_tr_state["rising"]) + list(_tr_state["falling"])


def _tr_check_exits(today):
    cm = cur_min_ist()
    for s in _tr_all_active():
        sym = s["symbol"]
        row = trending_already_entered(sym, today)
        if not row or row["result"] != "OPEN":
            continue
        try:
            fut_key = row.get("fut_key") or s.get("fut_key")
            lot_size = row.get("lot_size") or s.get("lot_size") or 1
            lots_taken = row.get("lots_taken") or 1
            if not fut_key:
                continue

            ltp = get_ltp(fut_key)
            if not ltp:
                continue

            entry = row["entry_price"]
            side = row.get("side", "LONG")
            is_long = side == "LONG"
            exit_p = result = None

            hit_sl = (ltp <= row["sl_price"]) if is_long else (ltp >= row["sl_price"])
            hit_tgt = (
                (ltp >= row["target_price"])
                if is_long
                else (ltp <= row["target_price"])
            )
            sq_off = cm >= TR_SQUAREOFF

            if hit_tgt and hit_sl:
                exit_p, result = row["sl_price"], "SL HIT"
            elif hit_tgt:
                exit_p, result = row["target_price"], "TGT HIT"
            elif hit_sl:
                exit_p, result = row["sl_price"], "SL HIT"
            elif sq_off:
                exit_p, result = ltp, "SQUAREOFF"

            if result:
                pnl_pts = (exit_p - entry) if is_long else (entry - exit_p)
                pnl_inr = round(pnl_pts * lot_size * lots_taken, 2)
                pnl_pct = round((pnl_pts / entry) * 100, 2)
                trending_update_exit(
                    row["id"],
                    exit_p,
                    now_ist().strftime("%H:%M:%S"),
                    result,
                    round(pnl_pts, 2),
                    pnl_pct,
                    pnl_inr,
                )
                # Release capital
                notional = row.get("notional") or (entry * lot_size * lots_taken)
                with _tr_lock:
                    _tr_state["capital_deployed"] = max(
                        0.0, _tr_state["capital_deployed"] - notional
                    )
                print(
                    f"  [TR] EXIT {sym} {side} @ {exit_p} {result} "
                    f"| lots={lots_taken} P&L=₹{pnl_inr:+.0f} ({pnl_pct:+.2f}%)"
                )
        except Exception as e:
            print(f"  [TR] exit check {sym}: {e}")


def _tr_check_exits_batched(today, ltps):
    cm = cur_min_ist()
    for s in _tr_all_active():
        sym = s["symbol"]
        row = trending_already_entered(sym, today)
        if not row or row["result"] != "OPEN":
            continue
        ltp = ltps.get(sym)
        if not ltp:
            continue
        lot_size = row.get("lot_size") or s.get("lot_size") or 1
        lots_taken = row.get("lots_taken") or 1
        entry = row["entry_price"]
        side = row.get("side", "LONG")
        is_long = side == "LONG"
        hit_sl = (ltp <= row["sl_price"]) if is_long else (ltp >= row["sl_price"])
        hit_tgt = (
            (ltp >= row["target_price"]) if is_long else (ltp <= row["target_price"])
        )
        exit_p = result = None
        if hit_tgt and hit_sl:
            exit_p, result = row["sl_price"], "SL HIT"
        elif hit_tgt:
            exit_p, result = row["target_price"], "TGT HIT"
        elif hit_sl:
            exit_p, result = row["sl_price"], "SL HIT"
        elif cm >= TR_SQUAREOFF:
            exit_p, result = ltp, "SQUAREOFF"
        if result:
            pnl_pts = (exit_p - entry) if is_long else (entry - exit_p)
            pnl_inr = round(pnl_pts * lot_size * lots_taken, 2)
            pnl_pct = round((pnl_pts / entry) * 100, 2)
            trending_update_exit(
                row["id"],
                exit_p,
                now_ist().strftime("%H:%M:%S"),
                result,
                round(pnl_pts, 2),
                pnl_pct,
                pnl_inr,
            )
            notional = row.get("notional") or (entry * lot_size * lots_taken)
            with _tr_lock:
                _tr_state["capital_deployed"] = max(
                    0.0, _tr_state["capital_deployed"] - notional
                )
            print(
                f"  [TR] EXIT {sym} {side} @ {exit_p} {result} | lots={lots_taken} P&L=₹{pnl_inr:+.0f} ({pnl_pct:+.2f}%)"
            )


# ── Trending scheduler ─────────────────────────────────────────────────────────
_scanned_today = [None]


def new_trending_scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] Trending scheduler started.")

    while True:
        t = now_ist()
        cm = t.hour * 60 + t.minute
        today = ist_date_str()

        # ── Scan: triggers at >= 9:00, once per day ────────────────────────
        # Using >= so a missed tick (server restart, sleep overshoot) still runs
        if cm >= TR_SCAN_HOUR * 60 + TR_SCAN_MIN and _scanned_today[0] != today:
            try:
                rising, falling = _scan_trending_stocks()
                with _tr_lock:
                    _tr_state["rising"] = rising
                    _tr_state["falling"] = falling
                    _tr_state["scanned_date"] = today
                    _tr_state["intraday"] = {}
                _scanned_today[0] = today
                print(f"  [TR] Scan done. Watching OR from 9:15...")
            except Exception as e:
                print(f"  [TR] scan error: {e}")
            time.sleep(60)
            continue

        # ── Batch LTP fetch ────────────────────────────────────────────────
        active = _tr_all_active()
        fut_keys = [s["fut_key"] for s in active if s.get("fut_key")]
        key_to_sym = {s["fut_key"]: s["symbol"] for s in active if s.get("fut_key")}
        ltps = get_ltp_multi(fut_keys, key_to_sym) if fut_keys else {}

        # ── OR build phase: 9:15 to 9:44 ──────────────────────────────────
        if TR_OR_START <= cm < TR_OR_END:
            for s in active:
                sym = s["symbol"]
                ltp = ltps.get(sym)
                if not ltp:
                    continue
                with _tr_lock:
                    iday = _tr_state["intraday"].setdefault(
                        sym,
                        {
                            "or_high": None,
                            "or_low": None,
                            "or_locked": False,
                            "entered": False,
                        },
                    )
                    if not iday["or_locked"]:
                        iday["or_high"] = (
                            ltp
                            if iday["or_high"] is None
                            else max(iday["or_high"], ltp)
                        )
                        iday["or_low"] = (
                            ltp if iday["or_low"] is None else min(iday["or_low"], ltp)
                        )

        # ── Entry + exit phase: 9:45 onwards ──────────────────────────────
        elif cm >= TR_OR_END:
            with _tr_lock:
                for sym2, iday2 in _tr_state["intraday"].items():
                    if not iday2["or_locked"] and iday2["or_high"] is not None:
                        iday2["or_locked"] = True
                        print(
                            f"  [TR] OR LOCKED {sym2}: H=₹{iday2['or_high']} L=₹{iday2['or_low']}"
                        )

            for s in active:
                sym = s["symbol"]
                lot_size = s.get("lot_size", 1)
                fut_key = s.get("fut_key")
                if not fut_key:
                    continue
                with _tr_lock:
                    iday = _tr_state["intraday"].get(sym)
                if not iday or not iday["or_locked"]:
                    continue
                ltp = ltps.get(sym)
                if not ltp:
                    continue
                is_rising = any(r["symbol"] == sym for r in _tr_state.get("rising", []))
                side = "LONG" if is_rising else "SHORT"
                if not iday["entered"] and cm <= TR_ENTRY_END:
                    already = trending_already_entered(sym, today)
                    if not already:
                        trigger = (side == "LONG" and ltp > iday["or_high"]) or (
                            side == "SHORT" and ltp < iday["or_low"]
                        )
                        if trigger:
                            ok = _tr_enter(sym, fut_key, lot_size, side, ltp, today)
                            if ok:
                                with _tr_lock:
                                    _tr_state["intraday"][sym]["entered"] = True
            try:
                _tr_check_exits_batched(today, ltps)
            except Exception as e:
                print(f"  [TR] exit error: {e}")

        # ── Sleep ──────────────────────────────────────────────────────────
        cm = cur_min_ist()
        if TR_OR_START <= cm <= TR_SQUAREOFF:
            time.sleep(10)
        else:
            time.sleep(60)


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


def get_ltp_multi(keys_list, key_to_sym=None):
    if not keys_list:
        return {}
    combined = ",".join(keys_list)
    try:
        data = upstox_get("/market-quote/ltp", {"instrument_key": combined})
        result = {}
        for raw_key, v in (data.get("data") or {}).items():
            ltp = v.get("last_price")
            if key_to_sym:
                name_part = raw_key.split(":")[-1]
                for fut_key, sym in key_to_sym.items():
                    if name_part.startswith(sym):
                        result[sym] = ltp
                        break
            else:
                # fallback: symbol name is in the raw key e.g. "NSE_FO:DIXON26JUNFUT"
                result[raw_key] = ltp
        return result
    except Exception:
        return {}


def get_nearest_expiry(instrument_key):
    data = upstox_get("/option/contract", {"instrument_key": instrument_key})
    expiries = sorted(
        {d.get("expiry") for d in data.get("data", []) if d.get("expiry")}
    )
    today = ist_date_str()
    for exp in expiries:
        if exp >= today:
            return exp
    return expiries[-1] if expiries else None


def get_option_chain(instrument_key, expiry_date):
    data = upstox_get(
        "/option/chain", {"instrument_key": instrument_key, "expiry_date": expiry_date}
    )
    rows, pcr_vals, spot = [], [], None
    for item in data.get("data", []):
        spot = item.get("underlying_spot_price")
        if item.get("pcr") is not None:
            pcr_vals.append(item["pcr"])
        call = item.get("call_options", {}).get("market_data", {}) or {}
        put = item.get("put_options", {}).get("market_data", {}) or {}
        rows.append(
            {
                "strike": item.get("strike_price"),
                "call_oi_change": (call.get("oi", 0) or 0)
                - (call.get("prev_oi", 0) or 0),
                "put_oi_change": (put.get("oi", 0) or 0) - (put.get("prev_oi", 0) or 0),
                "call_oi": call.get("oi", 0) or 0,
                "put_oi": put.get("oi", 0) or 0,
                "call_volume": call.get("volume", 0) or 0,
                "put_volume": put.get("volume", 0) or 0,
                "call_ltp": call.get("ltp", 0) or 0,
                "put_ltp": put.get("ltp", 0) or 0,
            }
        )
    rows.sort(key=lambda x: (x["strike"] is None, x["strike"]))
    return {
        "rows": rows,
        "spot": spot,
        "pcr": pcr_vals[0] if pcr_vals else None,
        "ts": now_ist().strftime("%H:%M:%S"),
    }


def atm_index(rows, spot):
    best, bd = 0, float("inf")
    for i, r in enumerate(rows):
        d = abs((r["strike"] or 0) - (spot or 0))
        if d < bd:
            bd = d
            best = i
    return best


def compute_section_d(rows, atm, scrip, expiry):
    start, end = max(0, atm - 1), min(len(rows), atm + 2)
    call_oi = sum(rows[i]["call_oi_change"] for i in range(start, end))
    put_oi = sum(rows[i]["put_oi_change"] for i in range(start, end))
    diff = call_oi - put_oi
    last_diff = db_get_last_diff(scrip, expiry)
    change_diff = 0 if last_diff is None else diff - last_diff
    action = ""
    if call_oi - put_oi > TEN_LAC:
        action = "BUY PUT"
    if put_oi - call_oi > TEN_LAC:
        action = "BUY CALL"
    ts = now_ist().strftime("%H:%M:%S")
    db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action)
    print(
        f"[{ts} IST] Logged {scrip} | expiry={expiry} | callOI={call_oi} putOI={put_oi} diff={diff} action={action or '-'}"
    )


# ── Equity Cash DB helpers ─────────────────────────────────────────────────────
def eq_trade_today(symbol, log_date):
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id,side,qty,capital_used,entry_price,entry_time,sl_pct,target_pct,"
            "sl_price,target_price,exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
            "FROM eq_cash_trades WHERE log_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
            (log_date, symbol),
        ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "side": row[1],
        "qty": row[2],
        "capital_used": row[3],
        "entry_price": row[4],
        "entry_time": row[5],
        "sl_pct": row[6],
        "target_pct": row[7],
        "sl_price": row[8],
        "target_price": row[9],
        "exit_price": row[10],
        "exit_time": row[11],
        "result": row[12],
        "pnl_pts": row[13],
        "pnl_pct": row[14],
        "pnl_inr": row[15],
    }


def eq_trade_insert(
    log_date,
    symbol,
    eq_key,
    side,
    qty,
    capital_used,
    entry_price,
    entry_time,
    sl_pct,
    target_pct,
    sl_price,
    target_price,
):
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO eq_cash_trades "
            "(log_date,symbol,eq_key,side,qty,capital_used,entry_price,entry_time,"
            "sl_pct,target_pct,sl_price,target_price,result) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')",
            (
                log_date,
                symbol,
                eq_key,
                side,
                qty,
                capital_used,
                entry_price,
                entry_time,
                sl_pct,
                target_pct,
                sl_price,
                target_price,
            ),
        )
        conn.commit()
        return cur.lastrowid


def eq_trade_update_exit(
    trade_id, exit_price, exit_time, result, pnl_pts, pnl_pct, pnl_inr
):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE eq_cash_trades SET exit_price=?,exit_time=?,result=?,"
            "pnl_pts=?,pnl_pct=?,pnl_inr=? WHERE id=?",
            (exit_price, exit_time, result, pnl_pts, pnl_pct, pnl_inr, trade_id),
        )
        conn.commit()


def eq_trades_history(from_date, to_date, symbol=None):
    with _db_lock, get_conn() as conn:
        if symbol:
            rows = conn.execute(
                "SELECT id,log_date,symbol,eq_key,side,qty,capital_used,entry_price,entry_time,"
                "sl_pct,target_pct,sl_price,target_price,exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
                "FROM eq_cash_trades WHERE log_date BETWEEN ? AND ? AND symbol=? "
                "ORDER BY log_date DESC,id DESC",
                (from_date, to_date, symbol),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id,log_date,symbol,eq_key,side,qty,capital_used,entry_price,entry_time,"
                "sl_pct,target_pct,sl_price,target_price,exit_price,exit_time,result,pnl_pts,pnl_pct,pnl_inr "
                "FROM eq_cash_trades WHERE log_date BETWEEN ? AND ? "
                "ORDER BY log_date DESC,id DESC",
                (from_date, to_date),
            ).fetchall()
    return [
        {
            "id": r[0],
            "log_date": r[1],
            "symbol": r[2],
            "eq_key": r[3],
            "side": r[4],
            "qty": r[5],
            "capital_used": r[6],
            "entry_price": r[7],
            "entry_time": r[8],
            "sl_pct": r[9],
            "target_pct": r[10],
            "sl_price": r[11],
            "target_price": r[12],
            "exit_price": r[13],
            "exit_time": r[14],
            "result": r[15],
            "pnl_pts": r[16],
            "pnl_pct": r[17],
            "pnl_inr": r[18],
        }
        for r in rows
    ]


# ── FnO lot size + futures key ─────────────────────────────────────────────────
_lot_cache = {}
_futkey_cache = {}


def resolve_lot_sizes_and_futures():
    print(
        f"[{now_ist().strftime('%H:%M:%S')}] Resolving FnO tab lot sizes and futures keys..."
    )
    today = ist_date_str()
    instruments = _load_nse_instruments()
    if not instruments:
        for s in FNO_STOCKS:
            _lot_cache[s["symbol"]] = s["lot_size"]
        return

    fo_map = {}
    for d in instruments:
        if d.get("segment") != "NSE_FO":
            continue
        sym = d.get("underlying_symbol", "")
        if sym:
            fo_map.setdefault(sym, []).append(d)

    for s in FNO_STOCKS:
        sym = s["symbol"]
        rows = fo_map.get(sym, [])
        if not rows:
            _lot_cache[sym] = s["lot_size"]
            continue
        lot = rows[0].get("lot_size") or s["lot_size"]
        _lot_cache[sym] = int(lot)
        fut_rows = [
            d
            for d in rows
            if d.get("instrument_type") == "FUT"
            and _expiry_str(d.get("expiry", 0)) >= today
            and d.get("instrument_key")
        ]
        if fut_rows:
            fut_rows.sort(key=lambda x: _expiry_str(x["expiry"]))
            _futkey_cache[sym] = fut_rows[0]["instrument_key"]
        else:
            _futkey_cache[sym] = s["eq_key"]


def get_lot_size(symbol):
    return _lot_cache.get(symbol) or next(
        (s["lot_size"] for s in FNO_STOCKS if s["symbol"] == symbol), 100
    )


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
            payload = {
                "instruments": [
                    {
                        "instrument_token": fut_key,
                        "transaction_type": "BUY",
                        "quantity": lot_size,
                        "price": entry_price,
                        "product": "D",
                    }
                ]
            }
            r = requests.post(
                f"{BASE}/charges/margin", headers=HEADERS, json=payload, timeout=(5, 10)
            )
            if r.ok:
                margin = r.json().get("data", {}).get("required_margin")
                if margin:
                    return float(margin)
    except Exception:
        pass
    return round(notional * FNO_MARGIN_PCT, 2)


# ── FnO paper-trade engine ─────────────────────────────────────────────────────
def fno_trade_today(symbol, log_date):
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id,side,lot_size,entry_price,entry_time,sl_pct,target_pct,sl_price,target_price,"
            "margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
            "FROM fno_trades WHERE log_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
            (log_date, symbol),
        ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "side": row[1],
        "lot_size": row[2],
        "entry_price": row[3],
        "entry_time": row[4],
        "sl_pct": row[5],
        "target_pct": row[6],
        "sl_price": row[7],
        "target_price": row[8],
        "margin_used": row[9],
        "or_high": row[10],
        "or_low": row[11],
        "exit_price": row[12],
        "exit_time": row[13],
        "result": row[14],
        "pnl_pts": row[15],
        "pnl_inr": row[16],
    }


def fno_trade_insert(
    log_date,
    symbol,
    side,
    lot_size,
    entry_price,
    entry_time,
    sl_pct,
    target_pct,
    sl_price,
    target_price,
    margin_used,
    or_high,
    or_low,
):
    with _db_lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO fno_trades (log_date,symbol,side,lot_size,entry_price,entry_time,"
            "sl_pct,target_pct,sl_price,target_price,margin_used,or_high,or_low,result) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')",
            (
                log_date,
                symbol,
                side,
                lot_size,
                entry_price,
                entry_time,
                sl_pct,
                target_pct,
                sl_price,
                target_price,
                margin_used,
                or_high,
                or_low,
            ),
        )
        conn.commit()
        return cur.lastrowid


def fno_trade_update_exit(trade_id, exit_price, exit_time, result, pnl_pts, pnl_inr):
    with _db_lock, get_conn() as conn:
        conn.execute(
            "UPDATE fno_trades SET exit_price=?,exit_time=?,result=?,pnl_pts=?,pnl_inr=? WHERE id=?",
            (exit_price, exit_time, result, pnl_pts, pnl_inr, trade_id),
        )
        conn.commit()


def fno_trades_history(days=30, symbol=None):
    with _db_lock, get_conn() as conn:
        if symbol:
            rows = conn.execute(
                "SELECT log_date,symbol,side,lot_size,entry_price,entry_time,sl_pct,target_pct,"
                "sl_price,target_price,margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
                "FROM fno_trades WHERE symbol=? ORDER BY log_date DESC,id DESC LIMIT ?",
                (symbol, days * 5),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT log_date,symbol,side,lot_size,entry_price,entry_time,sl_pct,target_pct,"
                "sl_price,target_price,margin_used,or_high,or_low,exit_price,exit_time,result,pnl_pts,pnl_inr "
                "FROM fno_trades ORDER BY log_date DESC,id DESC LIMIT ?",
                (days * 5,),
            ).fetchall()
    return [
        {
            "log_date": r[0],
            "symbol": r[1],
            "side": r[2],
            "lot_size": r[3],
            "entry_price": r[4],
            "entry_time": r[5],
            "sl_pct": r[6],
            "target_pct": r[7],
            "sl_price": r[8],
            "target_price": r[9],
            "margin_used": r[10],
            "or_high": r[11],
            "or_low": r[12],
            "exit_price": r[13],
            "exit_time": r[14],
            "result": r[15],
            "pnl_pts": r[16],
            "pnl_inr": r[17],
        }
        for r in rows
    ]


def fno_balance_log_insert(event, amount, balance, note=""):
    ts = now_ist().strftime("%H:%M:%S")
    with _db_lock, get_conn() as conn:
        conn.execute(
            "INSERT INTO fno_balance_log (log_date,ts,event,amount,balance,note) VALUES (?,?,?,?,?,?)",
            (ist_date_str(), ts, event, amount, balance, note),
        )
        conn.commit()


def fno_balance_history(days=30):
    with _db_lock, get_conn() as conn:
        rows = conn.execute(
            "SELECT log_date,ts,event,amount,balance,note FROM fno_balance_log "
            "ORDER BY log_date DESC,id DESC LIMIT ?",
            (days * 50,),
        ).fetchall()
    return [
        {
            "log_date": r[0],
            "ts": r[1],
            "event": r[2],
            "amount": r[3],
            "balance": r[4],
            "note": r[5],
        }
        for r in rows
    ]


def _try_enter_fno(symbol, side, or_high, or_low, entry_level, today, note=""):
    existing = fno_trade_today(symbol, today)
    if existing:
        return "ALREADY_ENTERED"
    ltp = get_fut_ltp(symbol)
    if ltp is None:
        return "LTP_FAIL"
    lot_size = get_lot_size(symbol)
    margin = estimate_margin(symbol, ltp, lot_size)
    with _fno_lock:
        balance = _fno_state["balance"]
        sl_pct = _fno_state["sl_pct"]
        target_pct = _fno_state["target_pct"]
    if balance < margin:
        with _fno_lock:
            already_pending = any(
                p["symbol"] == symbol for p in _fno_state["pending_funds"]
            )
            if not already_pending:
                _fno_state["pending_funds"].append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "or_high": or_high,
                        "or_low": or_low,
                        "entry_level": entry_level,
                    }
                )
        return "INSUFFICIENT_FUNDS"
    if side == "LONG":
        sl_price = round(ltp * (1 - sl_pct / 100), 2)
        target_price = round(ltp * (1 + target_pct / 100), 2)
    else:
        sl_price = round(ltp * (1 + sl_pct / 100), 2)
        target_price = round(ltp * (1 - target_pct / 100), 2)
    fno_trade_insert(
        today,
        symbol,
        side,
        lot_size,
        ltp,
        now_ist().strftime("%H:%M:%S"),
        sl_pct,
        target_pct,
        sl_price,
        target_price,
        margin,
        or_high,
        or_low,
    )
    with _fno_lock:
        _fno_state["balance"] -= margin
    fno_balance_log_insert(
        "TRADE_ENTRY",
        -margin,
        _fno_state["balance"],
        f"{symbol} {side} 1lot@{ltp:.2f} margin={margin:.0f} {note}",
    )
    print(f"  FnO ENTRY {symbol} {side} @ {ltp} | SL={sl_price} TGT={target_price}")
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
        side, lot_size = trade["side"], trade["lot_size"]
        sl_price, target_price = trade["sl_price"], trade["target_price"]
        entry_price, margin_used = trade["entry_price"], trade["margin_used"]
        exit_p = result = None
        hit_sl = (ltp <= sl_price) if side == "LONG" else (ltp >= sl_price)
        hit_tgt = (ltp >= target_price) if side == "LONG" else (ltp <= target_price)
        if hit_tgt and hit_sl:
            exit_p, result = sl_price, "SL HIT"
        elif hit_tgt:
            exit_p, result = target_price, "TGT HIT"
        elif hit_sl:
            exit_p, result = sl_price, "SL HIT"
        elif cm >= FNO_SQUAREOFF:
            exit_p, result = ltp, "SQUAREOFF"
        if result:
            pnl_pts = (
                (exit_p - entry_price) if side == "LONG" else (entry_price - exit_p)
            )
            pnl_inr = round(pnl_pts * lot_size, 2)
            fno_trade_update_exit(
                trade["id"],
                exit_p,
                now_ist().strftime("%H:%M:%S"),
                result,
                round(pnl_pts, 2),
                pnl_inr,
            )
            returned = margin_used + pnl_inr
            with _fno_lock:
                _fno_state["balance"] += returned
                bal = _fno_state["balance"]
            fno_balance_log_insert(
                "TRADE_EXIT",
                returned,
                bal,
                f"{sym} {side} exit@{exit_p} {result} P&L=Rs{pnl_inr:+.0f}",
            )


def _build_or_and_scan(today):
    cm = cur_min_ist()
    for s in FNO_STOCKS:
        sym = s["symbol"]
        if fno_trade_today(sym, today):
            continue
        with _fno_lock:
            od = _fno_state["or_data"].setdefault(
                sym, {"or_high": None, "or_low": None, "or_done": False}
            )
        ltp = get_fut_ltp(sym)
        if ltp is None:
            continue
        if FNO_OR_START <= cm < FNO_OR_END:
            with _fno_lock:
                od = _fno_state["or_data"][sym]
                od["or_high"] = (
                    ltp if od["or_high"] is None else max(od["or_high"], ltp)
                )
                od["or_low"] = ltp if od["or_low"] is None else min(od["or_low"], ltp)
                od["or_done"] = False
        elif cm >= FNO_OR_END and not od.get("or_done"):
            or_high, or_low = od.get("or_high"), od.get("or_low")
            if or_high is None or or_low is None:
                continue
            side = entry_level = None
            if ltp > or_high:
                side, entry_level = "LONG", or_high
            elif ltp < or_low:
                side, entry_level = "SHORT", or_low
            if side:
                res = _try_enter_fno(
                    sym, side, or_high, or_low, entry_level, today, "OR breakout"
                )
                if res == "ENTERED":
                    with _fno_lock:
                        _fno_state["or_data"][sym]["or_done"] = True


def _retry_pending_on_funds(today):
    with _fno_lock:
        pending = list(_fno_state["pending_funds"])
    still_pending = []
    for p in pending:
        sym, side = p["symbol"], p["side"]
        or_high, or_low, entry_level = p["or_high"], p["or_low"], p["entry_level"]
        ltp = get_fut_ltp(sym)
        if ltp is None:
            still_pending.append(p)
            continue
        tolerance = entry_level * FNO_REENTRY_TOLERANCE
        near_entry = abs(ltp - entry_level) <= tolerance
        correct_side = (ltp > or_high) if side == "LONG" else (ltp < or_low)
        if near_entry and correct_side:
            res = _try_enter_fno(
                sym, side, or_high, or_low, entry_level, today, "re-entry after funds"
            )
            if res != "INSUFFICIENT_FUNDS":
                continue
        still_pending.append(p)
    with _fno_lock:
        _fno_state["pending_funds"] = still_pending


def fno_scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] FnO scheduler started.")
    try:
        resolve_lot_sizes_and_futures()
    except Exception as e:
        print(f"  lot-size resolve error: {e}")
    with _db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT balance FROM fno_balance_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row:
        with _fno_lock:
            _fno_state["balance"] = row[0]
    else:
        fno_balance_log_insert(
            "FUND_ADD", INITIAL_BALANCE, INITIAL_BALANCE, "Initial capital"
        )
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


# ── Equity Cash Scheduler ─────────────────────────────────────────────────────
def eq_cash_scheduler():
    print(f"[{now_ist().strftime('%H:%M:%S')}] Equity cash scheduler started.")
    while True:
        time.sleep(180)
        if not is_market_open():
            continue
        today = ist_date_str()
        cm = cur_min_ist()

        with _tr_lock:
            rising = list(_tr_state["rising"])[:EQ_TOP_N]
            falling = list(_tr_state["falling"])[:EQ_TOP_N]

        if not rising and not falling:
            continue

        with _eq_lock:
            capital = _eq_state["capital"]
            sl_pct = _eq_state["sl_pct"]
            target_pct = _eq_state["target_pct"]

        all_stocks = [(s, "LONG") for s in rising] + [(s, "SHORT") for s in falling]

        for s, side in all_stocks:
            sym = s["symbol"]
            eq_key = s.get("instrument_key")
            if not eq_key:
                continue
            try:
                ltp = get_ltp(eq_key)
                if not ltp:
                    continue
                if ltp > capital:
                    continue
                qty = int(capital / ltp)
                if qty < 1:
                    continue

                with _eq_lock:
                    iday = _eq_state["intraday"].setdefault(
                        sym,
                        {
                            "day_high": ltp,
                            "day_low": ltp,
                            "entered": False,
                            "side": side,
                            "eq_key": eq_key,
                            "qty": qty,
                        },
                    )
                    prev_high = iday["day_high"]
                    prev_low = iday["day_low"]
                    iday["day_high"] = max(iday["day_high"], ltp)
                    iday["day_low"] = min(iday["day_low"], ltp)
                    entered = iday["entered"]

                if not entered and EQ_ENTRY_START <= cm <= EQ_ENTRY_END:
                    existing = eq_trade_today(sym, today)
                    if not existing:
                        trigger = (side == "LONG" and ltp > prev_high) or (
                            side == "SHORT" and ltp < prev_low
                        )
                        if trigger:
                            capital_used = round(qty * ltp, 2)
                            sl_price = (
                                round(ltp * (1 - sl_pct / 100), 2)
                                if side == "LONG"
                                else round(ltp * (1 + sl_pct / 100), 2)
                            )
                            target_price = (
                                round(ltp * (1 + target_pct / 100), 2)
                                if side == "LONG"
                                else round(ltp * (1 - target_pct / 100), 2)
                            )
                            eq_trade_insert(
                                today,
                                sym,
                                eq_key,
                                side,
                                qty,
                                capital_used,
                                ltp,
                                now_ist().strftime("%H:%M:%S"),
                                sl_pct,
                                target_pct,
                                sl_price,
                                target_price,
                            )
                            with _eq_lock:
                                _eq_state["intraday"][sym]["entered"] = True
                            print(f"  [EQ] ENTRY {sym} {side} @ Rs{ltp} qty={qty}")

                trade = eq_trade_today(sym, today)
                if trade and trade["result"] == "OPEN":
                    is_long = trade["side"] == "LONG"
                    hit_sl = (
                        (ltp <= trade["sl_price"])
                        if is_long
                        else (ltp >= trade["sl_price"])
                    )
                    hit_tgt = (
                        (ltp >= trade["target_price"])
                        if is_long
                        else (ltp <= trade["target_price"])
                    )
                    sq_off = cm >= EQ_SQUAREOFF
                    exit_p = result = None
                    if hit_tgt and hit_sl:
                        exit_p, result = trade["sl_price"], "SL HIT"
                    elif hit_tgt:
                        exit_p, result = trade["target_price"], "TGT HIT"
                    elif hit_sl:
                        exit_p, result = trade["sl_price"], "SL HIT"
                    elif sq_off:
                        exit_p, result = ltp, "SQUAREOFF"
                    if result:
                        pnl_pts = (
                            (exit_p - trade["entry_price"])
                            if is_long
                            else (trade["entry_price"] - exit_p)
                        )
                        pnl_inr = round(pnl_pts * trade["qty"], 2)
                        pnl_pct = round((pnl_pts / trade["entry_price"]) * 100, 2)
                        eq_trade_update_exit(
                            trade["id"],
                            exit_p,
                            now_ist().strftime("%H:%M:%S"),
                            result,
                            round(pnl_pts, 2),
                            pnl_pct,
                            pnl_inr,
                        )
                        print(
                            f"  [EQ] EXIT {sym} @ Rs{exit_p} {result} P&L=Rs{pnl_inr:+.2f}"
                        )
            except Exception as e:
                print(f"  [EQ] {sym}: {e}")


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
                if not expiry:
                    continue
                chain = get_option_chain(ik, expiry)
                rows, spot = chain["rows"], chain["spot"]
                if not rows or spot is None:
                    continue
                compute_section_d(rows, atm_index(rows, spot), scrip, expiry)
            except Exception as e:
                print(f"  OI {scrip}: {e}")


def midnight_wiper():
    while True:
        t = now_ist()
        secs = (23 - t.hour) * 3600 + (59 - t.minute) * 60 + (60 - t.second)
        time.sleep(secs + 5)
        db_wipe_today()

        # Apply pending capital for next trading day
        with _db_lock, get_conn() as conn:
            row = conn.execute(
                "SELECT tr_capital_next FROM user_settings WHERE id=1"
            ).fetchone()
        if row and row[0]:
            new_cap = row[0]
            with _tr_lock:
                _tr_state["capital"] = new_cap
            db_update_setting("tr_capital", new_cap)
            print(f"  [TR] Capital updated for new day: ₹{new_cap:,.0f}")

        with _fno_lock:
            _fno_state["or_data"] = {}
            _fno_state["pending_funds"] = []
        with _tr_lock:
            _tr_state["intraday"] = {}
            _tr_state["scanned_date"] = None
            _tr_state["capital_deployed"] = 0.0
        with _eq_lock:
            _eq_state["intraday"] = {}
        with _tr_fut_lock:
            _tr_fut_cache.clear()
        print(f"[{now_ist().strftime('%H:%M:%S')}] Midnight reset done.")


# ── Backtest helpers ───────────────────────────────────────────────────────────
def get_fno_stocks():
    """
    Returns only stocks that have an active futures contract.
    instrument_key is the FUTURES key (not equity spot).
    Also includes indices.
    """
    today = ist_date_str()
    try:
        instruments = _load_nse_instruments()
        # Build a set of symbols that have a valid future
        fut_map = {}  # symbol -> {instrument_key: fut_key, lot_size: n}
        for d in instruments:
            if (
                d.get("segment") == "NSE_FO"
                and d.get("instrument_type") == "FUT"
                and d.get("underlying_type") == "EQUITY"
                and d.get("underlying_symbol")
                and d.get("instrument_key")
                and _expiry_str(d.get("expiry", 0)) >= today
            ):
                sym = d["underlying_symbol"]
                exp = _expiry_str(d["expiry"])
                # Keep nearest expiry only
                if sym not in fut_map or exp < fut_map[sym]["expiry"]:
                    fut_map[sym] = {
                        "symbol": sym,
                        "instrument_key": d["instrument_key"],  # FUTURES key
                        "lot_size": int(d.get("lot_size") or 1),
                        "expiry": exp,
                        "type": "stock",
                    }

        stocks = sorted(fut_map.values(), key=lambda x: x["symbol"])

        # Add indices — futures keys fetched from instruments
        index_fut_keys = {
            "NIFTY 50": ("NSE_INDEX|Nifty 50", "NIFTY", 65, "index"),
            "BANK NIFTY": ("NSE_INDEX|Nifty Bank", "BANKNIFTY", 30, "index"),
            "FINNIFTY": ("NSE_INDEX|Nifty Fin Service", "FINNIFTY", 65, "index"),
            "SENSEX": ("BSE_INDEX|SENSEX", "SENSEX", 20, "index"),
        }
        indices = []
        for label, (eq_key, sym, lot, typ) in index_fut_keys.items():
            # Try to find index futures in instruments
            idx_futs = [
                d
                for d in instruments
                if d.get("segment") == "NSE_FO"
                and d.get("instrument_type") == "FUT"
                and d.get("underlying_symbol") == sym
                and d.get("instrument_key")
                and _expiry_str(d.get("expiry", 0)) >= today
            ]
            if idx_futs:
                idx_futs.sort(key=lambda x: _expiry_str(x["expiry"]))
                indices.append(
                    {
                        "symbol": label,
                        "instrument_key": idx_futs[0]["instrument_key"],
                        "lot_size": int(idx_futs[0].get("lot_size") or lot),
                        "expiry": _expiry_str(idx_futs[0]["expiry"]),
                        "type": typ,
                    }
                )
            else:
                # Fallback: use spot key (candles still available)
                indices.append(
                    {
                        "symbol": label,
                        "instrument_key": eq_key,
                        "lot_size": lot,
                        "expiry": "",
                        "type": typ,
                    }
                )

        return indices + stocks  # indices first
    except Exception as e:
        print(f"  get_fno_stocks error: {e}")
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
            (instrument_key, interval, from_date, to_date),
        ).fetchall()
    return [
        {
            "ts": r[0],
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        }
        for r in rows
    ]


def candle_cache_put(instrument_key, interval, candles):
    if not candles:
        return
    with _db_lock, get_conn() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO candle_cache (instrument_key,interval,ts,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    instrument_key,
                    interval,
                    c["ts"],
                    c["open"],
                    c["high"],
                    c["low"],
                    c["close"],
                    c["volume"],
                )
                for c in candles
            ],
        )
        conn.commit()


TIMEFRAMES = {
    "1min": ("minutes", "1", 20),
    "3min": ("minutes", "3", 20),
    "5min": ("minutes", "5", 25),
    "15min": ("minutes", "15", 90),
    "day": ("days", "1", 365),
}


def _date_chunks(from_date, to_date, max_days):
    from datetime import timedelta

    d0 = datetime.strptime(from_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(to_date, "%Y-%m-%d").date()
    chunks, cur = [], d0
    while cur <= d1:
        end = min(cur + timedelta(days=max_days - 1), d1)
        chunks.append((cur.isoformat(), end.isoformat()))
        cur = end + timedelta(days=1)
    return chunks


def _fetch_v3(instrument_key, unit, interval, chunk_from, chunk_to):
    ik_enc = instrument_key.replace("|", "%7C")
    url = f"{V3_BASE}/historical-candle/{ik_enc}/{unit}/{interval}/{chunk_to}/{chunk_from}"
    print(f"  [BT] {url}")
    r = requests.get(url, headers=HEADERS, timeout=(10, 30))
    print(
        f"  [BT] status={r.status_code} candles={len(r.json().get('data',{}).get('candles',[]))}"
    )
    r.raise_for_status()
    return [
        {
            "ts": c[0],
            "open": c[1],
            "high": c[2],
            "low": c[3],
            "close": c[4],
            "volume": c[5],
        }
        for c in r.json().get("data", {}).get("candles", [])
    ]


def get_candles(instrument_key, timeframe, from_date, to_date):
    unit, interval, max_days = TIMEFRAMES[timeframe]
    cache_key = f"{unit}/{interval}"
    cached = candle_cache_get(instrument_key, cache_key, from_date, to_date)
    if cached:
        return cached
    all_candles = []
    for cf, ct in _date_chunks(from_date, to_date, max_days):
        try:
            all_candles.extend(_fetch_v3(instrument_key, unit, interval, cf, ct))
            time.sleep(0.25)
        except requests.HTTPError:
            time.sleep(0.5)
    seen = {}
    for c in all_candles:
        seen[c["ts"]] = c
    merged = sorted(seen.values(), key=lambda x: x["ts"])
    candle_cache_put(instrument_key, cache_key, merged)
    return merged


def _group_by_day(candles):
    days = {}
    for c in candles:
        days.setdefault(c["ts"][:10], []).append(c)
    for d in days:
        days[d].sort(key=lambda x: x["ts"])
    return days


def _minutes_of(ts):
    return int(ts[11:13]) * 60 + int(ts[14:16])


OR_START = 9 * 60 + 15
OR_END = 9 * 60 + 45
SQUARE_OFF = 15 * 60


def run_orb_backtest(
    instrument_key, symbol, timeframe, from_date, to_date, x_pts, y_pts, lot_size=1
):
    candles = get_candles(instrument_key, timeframe, from_date, to_date)
    trades = []
    for day, bars in sorted(_group_by_day(candles).items()):
        or_bars = [b for b in bars if OR_START <= _minutes_of(b["ts"]) < OR_END]
        if not or_bars:
            continue
        or_high = max(b["high"] for b in or_bars)
        or_low = min(b["low"] for b in or_bars)
        post = [b for b in bars if _minutes_of(b["ts"]) >= OR_END]
        side = entry = entry_idx = None
        for i, b in enumerate(post):
            bu, bd = b["high"] > or_high, b["low"] < or_low
            if bu and bd:
                side = (
                    "LONG"
                    if abs(b["open"] - or_high) <= abs(b["open"] - or_low)
                    else "SHORT"
                )
                entry = or_high if side == "LONG" else or_low
            elif bu:
                side, entry = "LONG", or_high
            elif bd:
                side, entry = "SHORT", or_low
            if side:
                entry_idx = i
                break
        if side is None:
            continue
        target = round(entry + x_pts, 2) if side == "LONG" else round(entry - x_pts, 2)
        sl = round(entry - y_pts, 2) if side == "LONG" else round(entry + y_pts, 2)
        exit_p = result = exit_time = None
        for b in post[entry_idx:]:
            ht = (b["high"] >= target) if side == "LONG" else (b["low"] <= target)
            hs = (b["low"] <= sl) if side == "LONG" else (b["high"] >= sl)
            if ht and hs:
                exit_p, result = sl, "LOSS"
            elif ht:
                exit_p, result = target, "WIN"
            elif hs:
                exit_p, result = sl, "LOSS"
            if result:
                exit_time = b["ts"][11:16]
                break
            if _minutes_of(b["ts"]) >= SQUARE_OFF:
                exit_p = b["close"]
                exit_time = b["ts"][11:16]
                break
        if exit_p is None:
            exit_p = post[-1]["close"]
            exit_time = post[-1]["ts"][11:16]
        pnl = round((exit_p - entry if side == "LONG" else entry - exit_p), 2)
        if result is None:
            result = "WIN" if pnl > 0 else "LOSS"
        trades.append(
            {
                "symbol": symbol,
                "entry_date": day,
                "side": side,
                "entry": round(entry, 2),
                "sl": sl,
                "target": target,
                "exit": round(exit_p, 2),
                "exit_time": exit_time,
                "result": result,
                "pnl": pnl,
                "pnl_pct": round((pnl / entry) * 100, 2) if entry else 0,
                "pnl_inr": round(pnl * lot_size, 2),
            }
        )
    return trades


# ── Momentum backtest helpers ──────────────────────────────────────────────────
def _calc_ema(values, period):
    ema = []
    k = 2 / (period + 1)
    for i, v in enumerate(values):
        if i < period - 1:
            ema.append(None)
        elif i == period - 1:
            ema.append(sum(values[:period]) / period)
        else:
            ema.append(v * k + ema[-1] * (1 - k))
    return ema


def _calc_rsi(closes, period=14):
    rsi = [None] * period
    rsi_gain_prev = rsi_loss_prev = 0.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
        if i >= period:
            if i == period:
                avg_gain = sum(gains[-period:]) / period
                avg_loss = sum(losses[-period:]) / period
            else:
                avg_gain = (rsi_gain_prev * (period - 1) + gains[-1]) / period
                avg_loss = (rsi_loss_prev * (period - 1) + losses[-1]) / period
            rsi_gain_prev = avg_gain
            rsi_loss_prev = avg_loss
            rsi.append(
                100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
            )
    return rsi


def _calc_adx(highs, lows, closes, period=14):
    n = len(closes)
    adx = [None] * n
    if n < period * 2 + 1:
        return adx
    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        pdm_list.append(max(up, 0) if up > down else 0)
        ndm_list.append(max(down, 0) if down > up else 0)
        tr_list.append(tr)

    def smooth(lst, p):
        out = [sum(lst[:p])]
        for v in lst[p:]:
            out.append(out[-1] - out[-1] / p + v)
        return out

    atr_s = smooth(tr_list, period)
    pdm_s = smooth(pdm_list, period)
    ndm_s = smooth(ndm_list, period)
    di_plus = [100 * p / a if a else 0 for p, a in zip(pdm_s, atr_s)]
    di_minus = [100 * m / a if a else 0 for m, a in zip(ndm_s, atr_s)]
    dx_list = [
        100 * abs(p - m) / (p + m) if (p + m) else 0 for p, m in zip(di_plus, di_minus)
    ]
    adx_vals = [sum(dx_list[:period]) / period]
    for v in dx_list[period:]:
        adx_vals.append((adx_vals[-1] * (period - 1) + v) / period)
    offset = 2 * period - 1
    for i, val in enumerate(adx_vals):
        if offset + i < n:
            adx[offset + i] = val
    return adx


def run_momentum_backtest(
    instrument_key,
    symbol,
    timeframe,
    from_date,
    to_date,
    target_pct,
    sl_pct,
    lot_size=1,
):
    candles = get_candles(instrument_key, timeframe, from_date, to_date)
    if not candles:
        return []
    days_map = _group_by_day(candles)
    trades = []
    for day, bars in sorted(days_map.items()):
        if len(bars) < 22:
            continue
        closes = [b["close"] for b in bars]
        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        volumes = [b["volume"] for b in bars]
        ema9 = _calc_ema(closes, 9)
        ema21 = _calc_ema(closes, 21)
        rsi = _calc_rsi(closes, 14)
        adx = _calc_adx(highs, lows, closes, 14)
        # VWAP — resets per day
        vwap, cum_tpv, cum_vol = [], 0.0, 0.0
        for b in bars:
            tp = (b["high"] + b["low"] + b["close"]) / 3
            cum_tpv += tp * b["volume"]
            cum_vol += b["volume"]
            vwap.append(cum_tpv / cum_vol if cum_vol else b["close"])
        # Volume SMA-20
        vol_sma20 = [None] * len(bars)
        for i in range(19, len(bars)):
            vol_sma20[i] = sum(volumes[i - 19 : i + 1]) / 20
        in_trade = False
        for i in range(20, len(bars)):
            if in_trade:
                continue
            e9, e21, r, dx, vw, vs = (
                ema9[i],
                ema21[i],
                rsi[i],
                adx[i],
                vwap[i],
                vol_sma20[i],
            )
            if None in (e9, e21, r, dx, vw, vs):
                continue
            cl, vol = closes[i], volumes[i]
            # Relax filters for larger timeframes
            adx_threshold = 15 if timeframe in ("15min", "day") else 20
            vol_threshold = 1.2 if timeframe in ("15min", "day") else 1.5
            adx_threshold = 15 if timeframe in ("15min", "day") else 20
            vol_threshold = 1.2 if timeframe in ("15min", "day") else 1.5
            adx_ok = dx > adx_threshold
            vol_ok = (vol > vol_threshold * vs) if vs else True
            # For 15min/day, volume filter is optional — skip if SMA not ready
            if timeframe in ("15min", "day") and not vol_ok:
                vol_ok = True
            long_sig = (e9 > e21) and (r > 50) and adx_ok and (cl > vw) and vol_ok
            short_sig = (e9 < e21) and (r < 50) and adx_ok and (cl < vw) and vol_ok
            if not long_sig and not short_sig:
                continue
            side = "LONG" if long_sig else "SHORT"
            entry_price = cl
            tgt_price = (
                round(entry_price * (1 + target_pct / 100), 2)
                if side == "LONG"
                else round(entry_price * (1 - target_pct / 100), 2)
            )
            sl_price = (
                round(entry_price * (1 - sl_pct / 100), 2)
                if side == "LONG"
                else round(entry_price * (1 + sl_pct / 100), 2)
            )
            exit_p = exit_time = result = None
            for j in range(i + 1, len(bars)):
                b = bars[j]
                hit_tgt = (
                    (b["high"] >= tgt_price)
                    if side == "LONG"
                    else (b["low"] <= tgt_price)
                )
                hit_sl = (
                    (b["low"] <= sl_price)
                    if side == "LONG"
                    else (b["high"] >= sl_price)
                )
                if hit_tgt and hit_sl:
                    exit_p, result = sl_price, "LOSS"
                elif hit_tgt:
                    exit_p, result = tgt_price, "WIN"
                elif hit_sl:
                    exit_p, result = sl_price, "LOSS"
                if result:
                    exit_time = b["ts"][11:16]
                    break
                if _minutes_of(b["ts"]) >= SQUARE_OFF:
                    exit_p = b["close"]
                    exit_time = b["ts"][11:16]
                    break
            if exit_p is None:
                exit_p = bars[-1]["close"]
                exit_time = bars[-1]["ts"][11:16]
            pnl = round(
                (exit_p - entry_price) if side == "LONG" else (entry_price - exit_p), 2
            )
            if result is None:
                result = "WIN" if pnl > 0 else "LOSS"
            trades.append(
                {
                    "symbol": symbol,
                    "entry_date": day,
                    "side": side,
                    "entry": round(entry_price, 2),
                    "sl": sl_price,
                    "target": tgt_price,
                    "exit": round(exit_p, 2),
                    "exit_time": exit_time,
                    "result": result,
                    "pnl": pnl,
                    "pnl_pct": (
                        round((pnl / entry_price) * 100, 2) if entry_price else 0
                    ),
                    "ema9": round(e9, 2),
                    "ema21": round(e21, 2),
                    "rsi": round(r, 2),
                    "adx": round(dx, 2),
                    "vwap": round(vw, 2),
                    "pnl_inr": round(pnl * lot_size, 2),
                }
            )
            in_trade = True
    return trades


# ── HTTP handler ───────────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        try:
            p = parsed.path

            if p == "/indices":
                self._send(
                    200,
                    upstox_get(
                        "/market-quote/ltp",
                        {"instrument_key": ",".join(INDEX_KEYS.values())},
                    ),
                )

            elif p == "/expiries":
                ik = qs.get("instrument_key", [""])[0]
                data = upstox_get("/option/contract", {"instrument_key": ik})
                self._send(
                    200,
                    {
                        "expiries": sorted(
                            {
                                d.get("expiry")
                                for d in data.get("data", [])
                                if d.get("expiry")
                            }
                        )
                    },
                )

            elif p == "/chain":
                chain = get_option_chain(
                    qs.get("instrument_key", [""])[0], qs.get("expiry_date", [""])[0]
                )
                chain["market_open"] = is_market_open()
                self._send(200, chain)

            elif p == "/logs":
                self._send(
                    200,
                    {
                        "logs": db_get_logs(
                            qs.get("scrip", [""])[0], qs.get("expiry", [""])[0]
                        )
                    },
                )

            elif p == "/log":
                scrip = qs.get("scrip", [""])[0]
                exp = qs.get("expiry", [""])[0]
                call_oi = qs.get("call_oi", [None])[0]
                if scrip and exp and call_oi is not None and is_market_open():
                    db_insert_log(
                        scrip,
                        exp,
                        qs.get("ts", [now_ist().strftime("%H:%M:%S")])[0],
                        int(call_oi),
                        int(qs.get("put_oi", [0])[0]),
                        int(qs.get("diff", [0])[0]),
                        int(qs.get("change_diff", [0])[0]),
                        qs.get("action", [""])[0],
                    )
                self._send(200, {"ok": True})

            elif p in ("/", "/index.html"):
                try:
                    body = open(HTML_FILE, "rb").read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except FileNotFoundError:
                    self._send(404, {"error": f"{HTML_FILE} not found"})

            elif p == "/instruments":
                self._send(200, {"indices": INDEX_KEYS, "strike_step": STRIKE_STEP})

            elif p == "/fno_stocks":
                self._send(200, {"stocks": get_fno_stocks()})

            elif p == "/backtest":
                ik = qs.get("instrument_key", [""])[0]
                sym = qs.get("symbol", [""])[0]
                tf = qs.get("timeframe", ["3min"])[0]
                fd = qs.get("from_date", [""])[0]
                td = qs.get("to_date", [ist_date_str()])[0]
                strategy = qs.get("strategy", ["orb"])[0]
                lot_size = int(qs.get("lot_size", ["1"])[0])
                if not ik or not fd:
                    self._send(400, {"error": "instrument_key and from_date required"})
                else:
                    if strategy == "momentum":
                        tgt_pct = float(qs.get("target_pct", ["1.5"])[0])
                        sl_pct = float(qs.get("sl_pct", ["1.0"])[0])
                        trades = run_momentum_backtest(
                            ik, sym, tf, fd, td, tgt_pct, sl_pct, lot_size
                        )
                    else:
                        xp = float(qs.get("x_pts", ["50"])[0])
                        yp = float(qs.get("y_pts", ["30"])[0])
                        trades = run_orb_backtest(ik, sym, tf, fd, td, xp, yp, lot_size)
                    wins = sum(1 for t in trades if t["result"] == "WIN")
                    longs = sum(1 for t in trades if t["side"] == "LONG")
                    pnl = round(sum(t["pnl"] for t in trades), 2)
                    pnl_inr = round(sum(t.get("pnl_inr", 0) for t in trades), 2)
                    self._send(
                        200,
                        {
                            "trades": trades,
                            "strategy": strategy,
                            "lot_size": lot_size,
                            "summary": {
                                "total": len(trades),
                                "wins": wins,
                                "losses": len(trades) - wins,
                                "longs": longs,
                                "shorts": len(trades) - longs,
                                "win_rate": (
                                    round(wins / len(trades) * 100, 1) if trades else 0
                                ),
                                "total_pnl": pnl,
                                "total_pnl_inr": pnl_inr,
                            },
                        },
                    )

            # ── TRENDING endpoints ─────────────────────────────────────────
            elif p == "/trending_state":
                today = ist_date_str()
                with _tr_lock:
                    rising = list(_tr_state["rising"])
                    falling = list(_tr_state["falling"])
                    scanned = _tr_state["scanned_date"]
                    sl_pct = _tr_state["sl_pct"]
                    tgt_pct = _tr_state["target_pct"]
                    intraday = dict(_tr_state["intraday"])
                    deployed = _tr_state["capital_deployed"]
                    capital = _tr_state["capital"]

                def enrich(stocks, direction):
                    out = []
                    for s in stocks:
                        sym = s["symbol"]
                        iday = intraday.get(sym, {})
                        trade = trending_already_entered(sym, today)
                        out.append(
                            {
                                **s,
                                "direction": direction,
                                "or_high": iday.get("or_high"),
                                "or_low": iday.get("or_low"),
                                "or_locked": iday.get("or_locked", False),
                                "entered": iday.get("entered", False),
                                "trade": trade,
                            }
                        )
                    return out

                self._send(
                    200,
                    {
                        "rising": enrich(rising, "LONG"),
                        "falling": enrich(falling, "SHORT"),
                        "scanned_date": scanned,
                        "sl_pct": sl_pct,
                        "target_pct": tgt_pct,
                        "date": today,
                        "market_open": is_market_open(),
                        "capital": capital,
                        "capital_deployed": round(deployed, 2),
                        "capital_free": round(capital - deployed, 2),
                    },
                )

            elif p == "/trending_params":
                sl = float(qs.get("sl_pct", ["-1"])[0])
                tgt = float(qs.get("target_pct", ["-1"])[0])
                if sl > 0 and tgt > 0:
                    with _tr_lock:
                        _tr_state["sl_pct"] = sl
                        _tr_state["target_pct"] = tgt
                    db_update_setting("tr_sl", sl)
                    db_update_setting("tr_tgt", tgt)
                with _tr_lock:
                    self._send(
                        200,
                        {
                            "sl_pct": _tr_state["sl_pct"],
                            "target_pct": _tr_state["target_pct"],
                        },
                    )

            elif p == "/trending_set_capital":
                amount = float(qs.get("amount", ["0"])[0])
                if amount < 100_000:
                    self._send(400, {"error": "Minimum capital is ₹1 lac"})
                else:
                    db_update_setting("tr_capital_next", amount)
                    with _tr_lock:
                        _tr_state["capital_next"] = amount
                        current = _tr_state["capital"]
                    self._send(
                        200,
                        {
                            "ok": True,
                            "msg": f"Capital ₹{amount:,.0f} will apply from next trading day",
                            "current": current,
                            "next": amount,
                        },
                    )

            elif p == "/trending_scan_now":

                def _bg():
                    try:
                        r, f = _scan_trending_stocks()
                        with _tr_lock:
                            _tr_state["rising"] = r
                            _tr_state["falling"] = f
                            _tr_state["scanned_date"] = ist_date_str()
                            _tr_state["intraday"] = {}
                            _tr_state["capital_deployed"] = 0.0
                    except Exception as e:
                        print(f"  [TR] manual scan error: {e}")

                threading.Thread(target=_bg, daemon=True).start()
                self._send(
                    200, {"ok": True, "msg": "Scan started in background (~2-3 min)"}
                )

            elif p == "/trending_history":
                self._send(
                    200,
                    {
                        "history": trending_get_history(
                            qs.get("symbol", [None])[0],
                            int(qs.get("days", ["30"])[0]),
                        )
                    },
                )

            elif p == "/trending_daily_summary":
                self._send(200, {"summary": trending_daily_summary()})

            elif p == "/trending_ltp":
                with _tr_lock:
                    all_stocks = list(_tr_state["rising"]) + list(_tr_state["falling"])
                if not all_stocks:
                    self._send(200, {"ltps": {}})
                    return
                fut_keys = []
                sym_to_key = {}
                for s in all_stocks:
                    fk = s.get("fut_key")
                    if fk:
                        fut_keys.append(fk)
                        sym_to_key[s["symbol"]] = fk
                if not fut_keys:
                    self._send(200, {"ltps": {}})
                    return
                ltps = {}
                for i in range(0, len(fut_keys), 50):
                    chunk = fut_keys[i : i + 50]
                    try:
                        data = upstox_get(
                            "/market-quote/ltp", {"instrument_key": ",".join(chunk)}
                        )
                        key_to_sym = {v: k for k, v in sym_to_key.items()}
                        for raw_key, v in (data.get("data") or {}).items():
                            for fk, sym in key_to_sym.items():
                                if fk.split("|")[-1] in raw_key or raw_key in fk:
                                    ltps[sym] = v.get("last_price")
                                    break
                    except Exception as e:
                        print(f"  trending_ltp batch error: {e}")
                self._send(200, {"ltps": ltps})

            # ── FnO endpoints ──────────────────────────────────────────────
            elif p == "/fno_state":
                today = ist_date_str()
                with _fno_lock:
                    balance = _fno_state["balance"]
                    sl_pct = _fno_state["sl_pct"]
                    target_pct = _fno_state["target_pct"]
                    pending = list(_fno_state["pending_funds"])
                fut_keys = [
                    _futkey_cache.get(s["symbol"], s["eq_key"]) for s in FNO_STOCKS
                ]
                ltps_raw = get_ltp_multi(fut_keys)
                stocks_status = []
                for s in FNO_STOCKS:
                    sym = s["symbol"]
                    trade = fno_trade_today(sym, today)
                    lot = get_lot_size(sym)
                    fut_k = _futkey_cache.get(sym, s["eq_key"])
                    ltp = None
                    for k, v in ltps_raw.items():
                        if sym.upper() in k.upper() or k.upper() in sym.upper():
                            ltp = v
                            break
                    with _fno_lock:
                        or_d = dict(_fno_state["or_data"].get(sym, {}))
                    stocks_status.append(
                        {
                            "symbol": sym,
                            "lot_size": lot,
                            "fut_key": fut_k,
                            "ltp": ltp,
                            "trade": trade,
                            "or_high": or_d.get("or_high"),
                            "or_low": or_d.get("or_low"),
                            "or_done": or_d.get("or_done", False),
                            "is_pending": any(pp["symbol"] == sym for pp in pending),
                        }
                    )
                self._send(
                    200,
                    {
                        "balance": round(balance, 2),
                        "sl_pct": sl_pct,
                        "target_pct": target_pct,
                        "stocks": stocks_status,
                        "pending": pending,
                        "date": today,
                        "market_open": is_market_open(),
                    },
                )

            elif p == "/fno_params":
                sl = float(qs.get("sl_pct", ["-1"])[0])
                tgt = float(qs.get("target_pct", ["-1"])[0])
                if sl > 0 and tgt > 0:
                    with _fno_lock:
                        _fno_state["sl_pct"] = sl
                        _fno_state["target_pct"] = tgt
                    db_update_setting("fno_sl", sl)
                    db_update_setting("fno_tgt", tgt)
                with _fno_lock:
                    self._send(
                        200,
                        {
                            "sl_pct": _fno_state["sl_pct"],
                            "target_pct": _fno_state["target_pct"],
                        },
                    )

            elif p == "/fno_add_funds":
                amount = float(qs.get("amount", ["0"])[0])
                if amount <= 0:
                    self._send(400, {"error": "amount must be positive"})
                else:
                    with _fno_lock:
                        _fno_state["balance"] += amount
                        new_bal = _fno_state["balance"]
                    fno_balance_log_insert(
                        "FUND_ADD", amount, new_bal, f"Manual top-up Rs{amount:,.0f}"
                    )
                    _retry_pending_on_funds(ist_date_str())
                    with _fno_lock:
                        pending = list(_fno_state["pending_funds"])
                    self._send(
                        200,
                        {"balance": round(new_bal, 2), "pending_count": len(pending)},
                    )

            elif p == "/fno_history":
                sym = qs.get("symbol", [None])[0]
                days = int(qs.get("days", ["30"])[0])
                self._send(
                    200,
                    {
                        "trades": fno_trades_history(days, sym),
                        "balance_log": fno_balance_history(days),
                    },
                )

            # ── Equity Cash endpoints ──────────────────────────────────────
            elif p == "/eq_state":
                today = ist_date_str()
                with _tr_lock:
                    rising = list(_tr_state["rising"])[:EQ_TOP_N]
                    falling = list(_tr_state["falling"])[:EQ_TOP_N]
                with _eq_lock:
                    capital = _eq_state["capital"]
                    sl_pct = _eq_state["sl_pct"]
                    target_pct = _eq_state["target_pct"]
                    intraday = dict(_eq_state["intraday"])

                all_stocks = [(s, "LONG") for s in rising] + [
                    (s, "SHORT") for s in falling
                ]
                stocks_out = []
                for s, side in all_stocks:
                    sym = s["symbol"]
                    trade = eq_trade_today(sym, today)
                    iday = intraday.get(sym, {})
                    try:
                        ltp = get_ltp(s.get("instrument_key"))
                    except Exception:
                        ltp = None
                    stocks_out.append(
                        {
                            "symbol": sym,
                            "eq_key": s.get("instrument_key"),
                            "side": side,
                            "gain3d": s.get("gain3d_pct"),
                            "ltp": ltp,
                            "day_high": iday.get("day_high"),
                            "day_low": iday.get("day_low"),
                            "entered": iday.get("entered", False),
                            "qty": iday.get("qty"),
                            "trade": trade,
                        }
                    )
                self._send(
                    200,
                    {
                        "stocks": stocks_out,
                        "capital": capital,
                        "sl_pct": sl_pct,
                        "target_pct": target_pct,
                        "date": today,
                        "market_open": is_market_open(),
                    },
                )

            elif p == "/eq_params":
                capital = float(qs.get("capital", ["-1"])[0])
                sl = float(qs.get("sl_pct", ["-1"])[0])
                tgt = float(qs.get("target_pct", ["-1"])[0])
                with _eq_lock:
                    if capital > 0:
                        _eq_state["capital"] = capital
                        db_update_setting("eq_cap", capital)
                    if sl > 0:
                        _eq_state["sl_pct"] = sl
                        db_update_setting("eq_sl", sl)
                    if tgt > 0:
                        _eq_state["target_pct"] = tgt
                        db_update_setting("eq_tgt", tgt)
                    self._send(
                        200,
                        {
                            "capital": _eq_state["capital"],
                            "sl_pct": _eq_state["sl_pct"],
                            "target_pct": _eq_state["target_pct"],
                        },
                    )

            elif p == "/eq_history":
                from_d = qs.get("from_date", [None])[0]
                to_d = qs.get("to_date", [None])[0]
                sym = qs.get("symbol", [None])[0]
                if not from_d or not to_d:
                    self._send(400, {"error": "from_date and to_date required"})
                else:
                    self._send(200, {"trades": eq_trades_history(from_d, to_d, sym)})

            elif p == "/fno_lot_sizes":
                self._send(
                    200, {s["symbol"]: get_lot_size(s["symbol"]) for s in FNO_STOCKS}
                )

            else:
                self._send(404, {"error": "unknown path"})

        except requests.HTTPError as e:
            self._send(
                502,
                {
                    "error": "upstox",
                    "detail": str(e),
                    "body": getattr(e.response, "text", ""),
                },
            )
        except Exception as e:
            import traceback

            self._send(500, {"error": str(e), "trace": traceback.format_exc()[-500:]})


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    db_init()
    db_load_settings()
    _load_deployed_capital()
    candle_cache_init()
    threading.Thread(target=midnight_wiper, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=new_trending_scheduler, daemon=True).start()
    threading.Thread(target=eq_cash_scheduler, daemon=True).start()
    threading.Thread(target=fno_scheduler, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"JAGOAR OI server -> http://0.0.0.0:{PORT}")
    print(
        f"Market open: {is_market_open()} | TR Capital: ₹{_tr_state['capital']:,.0f} | FnO Balance: ₹{INITIAL_BALANCE:,.0f}"
    )
    server.serve_forever()
