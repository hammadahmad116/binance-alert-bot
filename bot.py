"""
Binance Futures 10% Candle Alert Bot
=====================================
Monitors ALL Binance USD-M Perpetual Futures pairs for 15-minute candles
that move 10%+ (open → current price, tick by tick).

Notifications:
  - Telegram message to your phone (primary — works 24/7 on mobile)
  - Console + log file

Runs on any server/cloud (Railway, Oracle Free, VPS, etc.)
No Windows dependencies.

Usage:
  pip install -r requirements.txt
  python bot.py
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone

import requests
import websocket
from dotenv import load_dotenv

load_dotenv()

# ─── Logging setup ────────────────────────────────────────────────────────────

LOG_FILE = os.getenv("LOG_FILE", "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("binance-alert")

# ─── Config ───────────────────────────────────────────────────────────────────

THRESHOLD_PCT   = float(os.getenv("THRESHOLD_PCT", "10.0"))   # % move to alert
KLINE_INTERVAL  = os.getenv("KLINE_INTERVAL", "15m")          # candle timeframe
BINANCE_REST    = "https://fapi.binance.com"                   # USD-M Futures REST
BINANCE_WS_BASE = "wss://fstream.binance.com"                  # USD-M Futures WS

# Alternative REST hosts — tried in order if the primary is geo-blocked
BINANCE_REST_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]

# Telegram — required for mobile notifications
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Suppress repeat alerts for the same symbol (seconds)
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", str(60 * 15)))  # 15 min default

# Max symbols per WebSocket connection (Binance allows up to 1024)
CHUNK_SIZE = 200

# ─── State ────────────────────────────────────────────────────────────────────

last_alert: dict[str, float] = {}
alert_lock = threading.Lock()

# ─── Binance helpers ──────────────────────────────────────────────────────────

def get_futures_symbols() -> list[str]:
    """Fetch all active USDT perpetual futures pairs, trying multiple hosts."""
    for host in BINANCE_REST_HOSTS:
        try:
            url  = f"{host}/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 451:
                log.warning("Host %s returned 451 (geo-blocked), trying next…", host)
                continue
            resp.raise_for_status()
            data = resp.json()
            symbols = [
                s["symbol"].lower()
                for s in data["symbols"]
                if s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING"
                and s["contractType"] == "PERPETUAL"
            ]
            log.info("Connected via %s — tracking %d USDT perpetual futures pairs", host, len(symbols))
            global BINANCE_REST
            BINANCE_REST = host
            return symbols
        except Exception as e:
            log.warning("Host %s failed: %s", host, e)

    raise RuntimeError("All Binance REST hosts are blocked or unreachable from this server.")


def candle_change_pct(open_price: float, close_price: float) -> float:
    if open_price == 0:
        return 0.0
    return ((close_price - open_price) / open_price) * 100


# ─── Notification channels ────────────────────────────────────────────────────

def send_telegram(symbol: str, pct: float, open_p: float, close_p: float):
    """Send alert to Telegram. This is the primary mobile notification."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured — skipping mobile notification")
        return

    direction = "🚀 PUMP" if pct > 0 else "💥 DUMP"
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    text = (
        f"{direction}\n"
        f"*{symbol.upper()}* moved *{abs(pct):.2f}%* in 15m\n"
        f"Open: `{open_p:.6g}` → Now: `{close_p:.6g}`\n"
        f"⏰ {now_utc}"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
            },
            timeout=8,
        )
        if not r.ok:
            log.warning("Telegram API error: %s", r.text)
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


# ─── Alert dispatcher ─────────────────────────────────────────────────────────

def fire_alert(symbol: str, pct: float, open_p: float, close_p: float):
    """Deduplicate and dispatch alert to all channels."""
    now = time.time()
    with alert_lock:
        if now - last_alert.get(symbol, 0) < ALERT_COOLDOWN:
            return  # Still in cooldown for this symbol
        last_alert[symbol] = now

    direction = "PUMP 🚀" if pct > 0 else "DUMP 💥"
    log.info(
        "ALERT  %-12s  %s  %.2f%%  open=%.6g  now=%.6g",
        symbol.upper(), direction, abs(pct), open_p, close_p,
    )

    # Send Telegram in a background thread so it never blocks the WS loop
    threading.Thread(
        target=send_telegram,
        args=(symbol, pct, open_p, close_p),
        daemon=True,
    ).start()


# ─── WebSocket callbacks ──────────────────────────────────────────────────────

def on_message(ws, raw_message):
    try:
        msg  = json.loads(raw_message)
        data = msg.get("data", msg)   # combined stream wraps under "data"

        if data.get("e") != "kline":
            return

        kline   = data["k"]
        symbol  = kline["s"].lower()
        open_p  = float(kline["o"])
        close_p = float(kline["c"])   # live close, updates every tick

        pct = candle_change_pct(open_p, close_p)
        if abs(pct) >= THRESHOLD_PCT:
            fire_alert(symbol, pct, open_p, close_p)

    except Exception as e:
        log.error("on_message error: %s", e)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, code, msg):
    log.warning("WebSocket closed (%s). Will reconnect…", code)


def on_open(ws):
    log.info("WebSocket connected.")


# ─── Stream runner ────────────────────────────────────────────────────────────

def build_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s}@kline_{KLINE_INTERVAL}" for s in symbols)
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


def run_ws_chunk(symbols: list[str], chunk_id: int):
    """Run one WebSocket connection for a chunk of symbols, auto-reconnecting."""
    url = build_stream_url(symbols)
    log.info("WS chunk #%d: %d symbols", chunk_id, len(symbols))

    while True:
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        log.info("WS chunk #%d reconnecting in 5s…", chunk_id)
        time.sleep(5)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("Binance Futures 10%% Candle Alert Bot")
    log.info("Market    : USD-M Perpetual Futures")
    log.info("Interval  : %s", KLINE_INTERVAL)
    log.info("Threshold : %.1f%%", THRESHOLD_PCT)
    log.info("Cooldown  : %ds", ALERT_COOLDOWN)
    log.info("Telegram  : %s", "enabled" if TELEGRAM_TOKEN else "NOT configured")
    log.info("=" * 50)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning(
            "TELEGRAM_TOKEN / TELEGRAM_CHAT_ID not set in .env — "
            "you will NOT receive mobile alerts!"
        )

    symbols = get_futures_symbols()
    chunks  = [symbols[i : i + CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]

    for idx, chunk in enumerate(chunks):
        t = threading.Thread(
            target=run_ws_chunk,
            args=(chunk, idx + 1),
            daemon=True,
        )
        t.start()
        time.sleep(0.5)   # stagger connections slightly

    log.info("%d WebSocket connection(s) running. Ctrl+C to stop.", len(chunks))

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Stopped by user.")


if __name__ == "__main__":
    main()
