# Binance Futures 10% Candle Alert Bot

Monitors **all Binance USD-M Perpetual Futures** pairs in real-time.  
Fires a **Telegram message to your phone** whenever any coin's 15-minute candle moves 10%+.

Runs 24/7 on a free cloud server — no PC needed.

---

## Step 1 — Set up Telegram Bot (5 minutes)

1. Open Telegram → search **@BotFather** → send `/newbot`
2. Give it a name (e.g. `My Binance Alerts`) and a username (e.g. `mybinancealerts_bot`)
3. BotFather gives you a **token** like `7123456789:AAF...` → copy it
4. Start a chat with your new bot (search its username, press Start)
5. Get your **chat ID** — open this URL in your browser (replace YOUR_TOKEN):
   ```
   https://api.telegram.org/botYOUR_TOKEN/getUpdates
   ```
   Look for `"chat":{"id": 123456789}` — that number is your chat ID
6. Fill both values in `.env`:
   ```
   TELEGRAM_TOKEN=7123456789:AAF...
   TELEGRAM_CHAT_ID=123456789
   ```

---

## Step 2 — Deploy to Railway (free, 24/7)

Railway gives you a free server that runs your bot forever.

1. Push this folder to a GitHub repo (free account is fine)
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
3. Select your repo
4. Go to your service → **Variables** tab → add:
   - `TELEGRAM_TOKEN` = your token
   - `TELEGRAM_CHAT_ID` = your chat ID
5. Railway auto-builds the Dockerfile and starts the bot
6. Done — your phone gets alerts 24/7

---

## Run locally (optional)

```bash
pip install -r requirements.txt
# fill in .env first
python bot.py
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM_TOKEN` | — | Your Telegram bot token (required) |
| `TELEGRAM_CHAT_ID` | — | Your Telegram chat ID (required) |
| `THRESHOLD_PCT` | `10.0` | % candle move to trigger alert |
| `KLINE_INTERVAL` | `15m` | Candle timeframe |
| `ALERT_COOLDOWN` | `900` | Seconds before re-alerting same coin |

---

## What the alert looks like on your phone

```
💥 DUMP
XRPUSDT moved 12.34% in 15m
Open: 0.5821 → Now: 0.5103
⏰ 14:32:07 UTC
```
