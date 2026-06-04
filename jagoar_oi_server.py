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

PORT      = 6180
HTML_FILE = "jagoar_oi_scanner.html"
DB_FILE   = "jagoar_oi.db"
BASE      = "https://api.upstox.com/v2"
POLL_INTERVAL = 3 * 60   # 3 minutes in seconds
TEN_LAC   = 1_000_000

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
    """System local time — VM clock is IST."""
    return datetime.now()

def ist_date_str():
    return now_ist().strftime("%Y-%m-%d")

# ── Market status via Upstox API ───────────────────────────────────────────────
def is_market_open():
    """Ask Upstox directly — handles weekends, holidays, early closes."""
    try:
        r = requests.get(
            f"{BASE}/market/status/NSE",
            headers=HEADERS, timeout=(5, 10)
        )
        r.raise_for_status()
        status = r.json().get("data", {}).get("status", "")
        return status == "NORMAL_OPEN"
    except Exception:
        # Fallback: time-based check if API call fails
        t = now_ist()
        if t.weekday() >= 5:
            return False
        cur_min = t.hour * 60 + t.minute
        return (9 * 60 + 15) <= cur_min <= (15 * 60 + 15)

# ── SQLite setup ───────────────────────────────────────────────────────────────
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
    """Get the diff value of the most recent row for a scrip+expiry today."""
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
    """Fetch expiry list and return the nearest upcoming one."""
    data = upstox_get("/option/contract", {"instrument_key": instrument_key})
    expiries = sorted({d.get("expiry") for d in data.get("data", []) if d.get("expiry")})
    today = ist_date_str()
    # Pick first expiry that is today or later
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
    """Compute ATM±1 aggregated OI row and save to DB."""
    start = max(0, atm - 1)
    end   = min(len(rows), atm + 2)
    call_oi = sum(rows[i]["call_oi_change"] for i in range(start, end))
    put_oi  = sum(rows[i]["put_oi_change"]  for i in range(start, end))
    diff    = call_oi - put_oi

    last_diff  = db_get_last_diff(scrip, expiry)
    change_diff = 0 if last_diff is None else diff - last_diff

    action = ""
    if call_oi - put_oi >  TEN_LAC: action = "BUY PUT"
    if put_oi  - call_oi > TEN_LAC: action = "BUY CALL"

    ts = now_ist().strftime("%H:%M:%S")
    db_insert_log(scrip, expiry, ts, call_oi, put_oi, diff, change_diff, action)
    print(f"[{ts} IST] Logged {scrip} | expiry={expiry} | callOI={call_oi} putOI={put_oi} diff={diff} action={action or '-'}")

# ── Server-side scheduler ──────────────────────────────────────────────────────
def scheduler():
    """
    Every 3 minutes during market hours:
    - Check market status via Upstox API
    - For each index, fetch nearest expiry + option chain
    - Compute and save Section D row to DB
    Browser reads from DB on load — no dependency on browser being open.
    """
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
                        print(f"  {scrip}: no expiry found, skipping.")
                        continue
                    chain  = get_option_chain(ik, expiry)
                    rows   = chain["rows"]
                    spot   = chain["spot"]
                    if not rows or spot is None:
                        print(f"  {scrip}: empty chain, skipping.")
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
                self._send(200, upstox_get("/market-quote/ltp",
                                           {"instrument_key": keys}))

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

            # Dedicated log endpoint — browser can still send manual log if needed
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
                self._send(200, {
                    "indices":     INDEX_KEYS,
                    "strike_step": STRIKE_STEP,
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

    threading.Thread(target=midnight_wiper, daemon=True).start()
    print(f"[{now_ist().strftime('%H:%M:%S')} IST] Midnight wiper started.")

    threading.Thread(target=scheduler, daemon=True).start()

    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"JAGOAR OI server → http://0.0.0.0:{PORT}")
    print(f"Market open now  : {is_market_open()}")
    server.serve_forever()