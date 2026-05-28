"""
Binance Futures 10% Candle Alert Bot
=====================================
Monitors ALL Binance USD-M Perpetual Futures pairs for 15-minute candles
that move 10%+ (open → current price, tick by tick).

Key logic:
  - Alert fires as soon as price crosses 10% from candle open (not just at close)
  - One alert per symbol per candle (deduped by candle open timestamp)
  - Covers every coin — no hardcoded symbols

Notifications:
  - Telegram message to your phone
  - Console + log file

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

# ─── Logging ──────────────────────────────────────────────────────────────────

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

THRESHOLD_PCT   = float(os.getenv("THRESHOLD_PCT", "10.0"))
KLINE_INTERVAL  = os.getenv("KLINE_INTERVAL", "15m")
BINANCE_WS_BASE = "wss://fstream.binance.com"

BINANCE_REST_HOSTS = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
]

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

CHUNK_SIZE = 200

# ─── State ────────────────────────────────────────────────────────────────────

# Key: symbol, Value: candle open timestamp (ms) of the last alert fired
# This ensures exactly ONE alert per symbol per candle, no matter how many
# ticks cross the threshold during that candle.
alerted_candles: dict[str, int] = {}
state_lock = threading.Lock()

# ─── Binance symbol fetch ─────────────────────────────────────────────────────

def get_futures_symbols() -> list[str]:
    """Fetch all active USDT perpetual futures pairs, trying multiple hosts."""
    for host in BINANCE_REST_HOSTS:
        try:
            url  = f"{host}/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 451:
                log.warning("Host %s geo-blocked (451), trying next…", host)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict) or "symbols" not in data:
                log.warning("Host %s bad response, trying next…", host)
                continue
            symbols = [
                s["symbol"].lower()
                for s in data["symbols"]
                if s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING"
                and s["contractType"] == "PERPETUAL"
            ]
            log.info("Fetched %d USDT perpetual pairs via %s", len(symbols), host)
            return symbols
        except Exception as e:
            log.warning("Host %s error: %s", host, e)

    # Proxy fallback
    log.warning("All direct hosts blocked — trying proxy…")
    try:
        url  = "https://api.allorigins.win/raw?url=https://fapi.binance.com/fapi/v1/exchangeInfo"
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
        data = resp.json()
        symbols = [
            s["symbol"].lower()
            for s in data["symbols"]
            if s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["contractType"] == "PERPETUAL"
        ]
        log.info("Fetched %d USDT perpetual pairs via proxy", len(symbols))
        return symbols
    except Exception as e:
        log.warning("Proxy failed: %s", e)

    raise RuntimeError("Cannot reach Binance REST API from this server.")


# ─── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(text: str, chat_id: str = None):
    if not TELEGRAM_TOKEN:
        return
    cid = chat_id or TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": cid, "text": text, "parse_mode": "Markdown"},
            timeout=8,
        )
        if not r.ok:
            log.warning("Telegram error: %s", r.text)
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


def send_alert_telegram(symbol: str, pct: float, open_p: float, close_p: float):
    direction = "🚀 PUMP" if pct > 0 else "💥 DUMP"
    now_utc   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    text = (
        f"{direction}\n"
        f"*{symbol.upper()}* moved *{abs(pct):.2f}%* in {KLINE_INTERVAL}\n"
        f"Open: `{open_p:.6g}` → Now: `{close_p:.6g}`\n"
        f"⏰ {now_utc}"
    )
    send_telegram(text)


# ─── Alert dispatcher ─────────────────────────────────────────────────────────

def fire_alert(symbol: str, candle_open_ts: int, pct: float, open_p: float, close_p: float):
    """
    Fire alert for this symbol only if we haven't already alerted
    for this exact candle (identified by its open timestamp).
    """
    with state_lock:
        if alerted_candles.get(symbol) == candle_open_ts:
            return  # Already alerted for this candle
        alerted_candles[symbol] = candle_open_ts

    direction = "PUMP 🚀" if pct > 0 else "DUMP 💥"
    log.info(
        "ALERT  %-12s  %s  %.2f%%  open=%.6g  now=%.6g",
        symbol.upper(), direction, abs(pct), open_p, close_p,
    )

    threading.Thread(
        target=send_alert_telegram,
        args=(symbol, pct, open_p, close_p),
        daemon=True,
    ).start()


# ─── WebSocket ────────────────────────────────────────────────────────────────

def on_message(ws, raw_message):
    try:
        msg  = json.loads(raw_message)
        data = msg.get("data", msg)

        if data.get("e") != "kline":
            return

        kline          = data["k"]
        symbol         = kline["s"].lower()
        candle_open_ts = int(kline["t"])       # candle open time in ms — unique per candle
        open_p         = float(kline["o"])
        close_p        = float(kline["c"])
        is_closed      = kline["x"]            # True on final tick of candle

        if open_p == 0:
            return

        pct = ((close_p - open_p) / open_p) * 100

        # Log any candle that closes above 7% for visibility
        if is_closed and abs(pct) >= 7.0:
            log.info(
                "CANDLE CLOSED  %-12s  %.2f%%  open=%.6g  close=%.6g",
                kline["s"], pct, open_p, close_p,
            )

        if abs(pct) >= THRESHOLD_PCT:
            fire_alert(symbol, candle_open_ts, pct, open_p, close_p)

    except Exception as e:
        log.error("on_message error: %s", e)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, code, msg):
    log.warning("WebSocket closed (%s). Reconnecting…", code)


def on_open(ws):
    log.info("WebSocket connected.")


# ─── Stream runner ────────────────────────────────────────────────────────────

def build_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s}@kline_{KLINE_INTERVAL}" for s in symbols)
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


def run_ws_chunk(symbols: list[str], chunk_id: int):
    url = build_stream_url(symbols)
    log.info("WS chunk #%d started: %d symbols", chunk_id, len(symbols))
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


# ─── Telegram command polling ─────────────────────────────────────────────────

def poll_telegram_commands(total_symbols: int):
    if not TELEGRAM_TOKEN:
        return

    offset = 0
    log.info("Telegram polling started. Send /test or /status to @memristor_bot")

    while True:
        try:
            url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            resp = requests.get(url, params={"timeout": 30, "offset": offset}, timeout=35)
            if not resp.ok:
                time.sleep(5)
                continue

            for update in resp.json().get("result", []):
                offset  = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if text == "/test":
                    log.info("/test from %s", chat_id)
                    send_telegram(
                        "✅ *Bot is working\\!*\n\n"
                        "Example alert:\n\n"
                        "🚀 PUMP\n"
                        "*BTCUSDT* moved *12\\.45%* in 15m\n"
                        "Open: `95000` → Now: `106832`\n"
                        "⏰ 10:25:00 UTC\n\n"
                        "_Real alerts fire automatically when any coin moves 10%\\+ in a 15m candle\\._",
                        chat_id=chat_id,
                    )
                elif text == "/status":
                    send_telegram(
                        f"📡 *Bot Status*\n"
                        f"• Pairs monitored: {total_symbols}\n"
                        f"• Threshold: {THRESHOLD_PCT}%\n"
                        f"• Interval: {KLINE_INTERVAL}\n"
                        f"• Server: EU West \\(Amsterdam\\)\n"
                        f"• Status: ✅ Online",
                        chat_id=chat_id,
                    )

        except Exception as e:
            log.warning("Telegram polling error: %s", e)
            time.sleep(5)


# ─── Entry point ──────────────────────────────────────────────────────────────

def refresh_symbols_periodically(interval_hours: int = 6):
    """Restart the process every N hours so new Binance pairs are picked up."""
    time.sleep(interval_hours * 3600)
    log.info("Scheduled restart to refresh symbol list…")
    os.execv(sys.executable, [sys.executable] + sys.argv)


def main():
    log.info("=" * 50)
    log.info("Binance Futures 10%% Candle Alert Bot")
    log.info("Market    : USD-M Perpetual Futures")
    log.info("Interval  : %s", KLINE_INTERVAL)
    log.info("Threshold : %.1f%%", THRESHOLD_PCT)
    log.info("Telegram  : %s", "enabled" if TELEGRAM_TOKEN else "NOT configured")
    log.info("=" * 50)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials missing — no mobile alerts will be sent!")

    symbols = get_futures_symbols()
    chunks  = [symbols[i : i + CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]
    log.info("Monitoring %d symbols across %d WebSocket connections", len(symbols), len(chunks))

    # Auto-restart every 6 hours to pick up any new Binance pairs
    threading.Thread(target=refresh_symbols_periodically, daemon=True).start()

    # Start Telegram command listener
    threading.Thread(
        target=poll_telegram_commands,
        args=(len(symbols),),
        daemon=True,
    ).start()

    # Start one WebSocket thread per chunk
    for idx, chunk in enumerate(chunks):
        threading.Thread(
            target=run_ws_chunk,
            args=(chunk, idx + 1),
            daemon=True,
        ).start()
        time.sleep(0.5)

    log.info("All streams running. Waiting for alerts…")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
