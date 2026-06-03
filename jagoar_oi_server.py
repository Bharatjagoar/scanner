#!/usr/bin/env python3
"""
MANJIT JAGOAR OI DATA SCANNER - backend server.

Why this exists: Upstox API cannot be called directly from a browser (CORS).
All Upstox calls happen here, server-side. The HTML frontend talks ONLY to
this local server on http://localhost:6180.

Change in OI definition used everywhere = oi - prev_oi from the option chain
endpoint. prev_oi is the previous trading day's OI, so this matches Upstox's
own day-over-day "Chng in OI" figure. VERIFY on first live run that this
matches the Upstox option-chain UI; if not, the only fix is the dedicated
Change-in-OI API (launched 2026-05-11).

Usage:
    1. pip install requests
    2. Put your Upstox access token in the env var or paste below.
    3. python jagoar_oi_server.py
    4. Open jagoar_oi_scanner.html in a browser.
"""

import http.server
import socketserver
import json
import os
import urllib.parse
from datetime import datetime

try:
    import requests
except ImportError:
    raise SystemExit("Run: pip install requests")

PORT = 6180
HTML_FILE = "jagoar_oi_scanner.html"  # served at / — must sit beside this script
BASE = "https://api.upstox.com/v2"
# Paste your token here OR set UPSTOX_ACCESS_TOKEN in the environment.
ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1OTUzNjIiLCJqdGkiOiI2OWZmMzBhMjU0NzlhOTZjOWM4MmJkYzQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaXNFeHRlbmRlZCI6dHJ1ZSwiaWF0IjoxNzc4MzMxODEwLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE4MDk5MDAwMDB9.luATCCj9PL3Rz3xx0_-wXGuJjH2i1MfvAN0JL534WHQ")

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
}

# Underlying instrument keys for the four indices.
# NOTE: Sensex/BankNifty live on BSE/NSE — verify these keys against your
# instrument master on first run; index keys occasionally change.
INDEX_KEYS = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "SENSEX": "BSE_INDEX|SENSEX",
}

# Strike step per underlying (used to pick ATM +- N). Stocks vary; this is a
# starting map. Unknown scrips fall back to nearest-strike detection from the
# chain itself, so the step here is only a hint.
STRIKE_STEP = {
    "NIFTY 50": 50,
    "BANK NIFTY": 100,
    "FINNIFTY": 50,
    "SENSEX": 100,
}


def upstox_get(path, params):
    url = f"{BASE}{path}"
    # (connect timeout, read timeout). The read timeout catches a connection
    # that opens but never finishes sending — the failure mode that can slip
    # past a single combined timeout and wedge a request.
    r = requests.get(url, headers=HEADERS, params=params, timeout=(5, 10))
    r.raise_for_status()
    return r.json()


def get_option_chain(instrument_key, expiry_date):
    """Return list of strike rows with computed change-in-OI (oi - prev_oi)."""
    data = upstox_get("/option/chain", {
        "instrument_key": instrument_key,
        "expiry_date": expiry_date,
    })
    rows = []
    pcr_vals = []
    spot = None
    for item in data.get("data", []):
        spot = item.get("underlying_spot_price")
        if item.get("pcr") is not None:
            pcr_vals.append(item.get("pcr"))
        call = item.get("call_options", {}).get("market_data", {}) or {}
        put = item.get("put_options", {}).get("market_data", {}) or {}

        call_oi = call.get("oi", 0) or 0
        call_prev = call.get("prev_oi", 0) or 0
        put_oi = put.get("oi", 0) or 0
        put_prev = put.get("prev_oi", 0) or 0

        rows.append({
            "strike": item.get("strike_price"),
            "call_oi_change": call_oi - call_prev,   # day-over-day
            "put_oi_change": put_oi - put_prev,       # day-over-day
            "call_oi": call_oi,
            "put_oi": put_oi,
            "call_volume": call.get("volume", 0) or 0,
            "put_volume": put.get("volume", 0) or 0,
            "call_ltp": call.get("ltp", 0) or 0,
            "put_ltp": put.get("ltp", 0) or 0,
        })
    rows.sort(key=lambda x: (x["strike"] is None, x["strike"]))
    return {
        "rows": rows,
        "spot": spot,
        "pcr": pcr_vals[0] if pcr_vals else None,
        "ts": datetime.now().strftime("%H:%M:%S"),
    }


def get_expiries(instrument_key):
    data = upstox_get("/option/contract", {"instrument_key": instrument_key})
    expiries = sorted({d.get("expiry") for d in data.get("data", []) if d.get("expiry")})
    return expiries


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # quiet

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path == "/indices":
                # Spot + change for the header strip.
                # market-quote/ltp gives ltp + close for % change.
                keys = ",".join(INDEX_KEYS.values())
                data = upstox_get("/market-quote/ltp", {"instrument_key": keys})
                self._send(200, data)

            elif parsed.path == "/expiries":
                ik = qs.get("instrument_key", [""])[0]
                self._send(200, {"expiries": get_expiries(ik)})

            elif parsed.path == "/chain":
                ik = qs.get("instrument_key", [""])[0]
                exp = qs.get("expiry_date", [""])[0]
                self._send(200, get_option_chain(ik, exp))

            elif parsed.path == "/" or parsed.path == "/index.html":
                # Serve the scanner HTML from the same folder, single-origin.
                try:
                    with open(HTML_FILE, "rb") as f:
                        body = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except FileNotFoundError:
                    self._send(404, {"error": f"{HTML_FILE} not found"})

            elif parsed.path == "/instruments":
                # Frontend dropdown list. Index keys are static; FnO stock list
                # should be loaded from the Upstox instrument master CSV which
                # you download separately. Returning indices here as a baseline.
                self._send(200, {
                    "indices": INDEX_KEYS,
                    "strike_step": STRIKE_STEP,
                })
            else:
                self._send(404, {"error": "unknown path"})
        except requests.HTTPError as e:
            self._send(502, {"error": "upstox", "detail": str(e),
                             "body": getattr(e.response, "text", "")})
        except Exception as e:
            self._send(500, {"error": str(e)})


if __name__ == "__main__":
    if ACCESS_TOKEN == "PASTE_YOUR_ACCESS_TOKEN_HERE":
        print("WARNING: set your Upstox access token first.")
    # 0.0.0.0 = listen on all interfaces so phone/other devices can reach it.
    # SECURITY: this exposes the server (and your Upstox token) to anyone who
    # can reach this IP:PORT. Restrict the OCI Security List + ufw to your own
    # IP, or put auth in front. Do NOT leave 6180 open to 0.0.0.0/0 long-term.
    # ThreadingHTTPServer: each request runs in its own thread, so one slow
    # or stuck Upstox call can no longer freeze the entire server (the cause
    # of the earlier hang where even localhost stopped responding).
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"JAGOAR OI server running on http://0.0.0.0:{PORT}  (open from <VM_IP>:{PORT})")
    server.serve_forever()