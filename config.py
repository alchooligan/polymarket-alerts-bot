"""
Configuration and constants for the Polymarket Telegram Bot.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Polymarket API URLs
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# API Endpoints
EVENTS_ENDPOINT = f"{GAMMA_API_BASE}/events"
TAGS_ENDPOINT = f"{GAMMA_API_BASE}/tags"
PRICES_HISTORY_ENDPOINT = f"{CLOB_API_BASE}/prices-history"

# Alert thresholds
BIG_MOVE_THRESHOLD = 10  # Percentage points (e.g., 45% -> 55% = 10% move)
CHECK_INTERVAL_MINUTES = 5

# Volume milestone thresholds (in dollars)
# Alert when a market crosses these for the first time
VOLUME_THRESHOLDS = [10_000, 50_000]  # $10K, $50K

# Database
DATABASE_PATH = "bot_data.db"

# Spam filter patterns - crypto price prediction markets to exclude
SPAM_CRYPTO_TICKERS = [
    "BTC", "Bitcoin", "ETH", "Ethereum", "SOL", "Solana",
    "XRP", "DOGE", "Dogecoin", "ADA", "Cardano", "AVAX",
    "DOT", "Polkadot", "MATIC", "Polygon", "LINK", "Chainlink"
]

SPAM_PRICE_KEYWORDS = [
    "above", "below", "hit", "reach", "price", "at or above",
    "at or below", "higher than", "lower than"
]

SPAM_TIMEFRAME_KEYWORDS = [
    "daily", "hourly", "weekly", "midnight", "noon",
    "end of day", "by EOD", "tonight", "tomorrow",
    "5m", "15m", "30m", "1h", "4h", "1m", "10m"
]

# Direct spam phrases that indicate price prediction markets
SPAM_PHRASES = [
    "up or down",
    "higher or lower",
    "above or below",
]
