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
# Higher thresholds = less noise, more signal
VOLUME_THRESHOLDS = [
    100_000,    # $100K - serious money
    250_000,    # $250K - major market
    500_000,    # $500K - whale territory
    1_000_000,  # $1M - massive
]

# Discovery threshold - minimum volume for first-seen markets to alert
DISCOVERY_MIN_VOLUME = 25_000  # $25K - market launched with traction

# Velocity alert thresholds (dollars gained per hour)
# Catches breaking news - money flowing in fast
VELOCITY_THRESHOLDS = [
    5_000,      # $5K/hr - early action
    10_000,     # $10K/hr - something's happening
    25_000,     # $25K/hr - news breaking
    50_000,     # $50K/hr - major event
    100_000,    # $100K/hr - huge story
]

# Closing soon alert settings
CLOSING_SOON_HOURS = 12  # Alert for markets closing within 12 hours
CLOSING_SOON_MIN_VELOCITY = 5_000  # Minimum $5K/hr velocity

# Alert limits per cycle (safety net to prevent spam)
ALERT_CAP_PER_CYCLE = 10  # Max individual alerts per type per cycle
MARKETS_TO_SCAN = 2000  # Fetch all markets (Polymarket has ~1000-2000 active)

# Daily digest settings
DAILY_DIGEST_HOUR = 9  # 9 AM UTC
DAILY_DIGEST_MINUTE = 0

# Database - uses Railway persistent volume at /data, local fallback for dev
import os
DATABASE_PATH = "/data/bot_data.db" if os.path.exists("/data") else "bot_data.db"

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

# Sports/esports slug patterns to exclude (no edge on sports betting)
SPORTS_SLUG_PATTERNS = [
    "nfl-", "nba-", "nhl-", "mlb-", "mls-",
    "afc-", "nfc-",  # NFL conferences
    "epl-", "uefa-", "fifa-", "cfb-", "ncaa-",
    "boxing-", "ufc-", "mma-", "wwe-",
    "dota-", "csgo-", "lol-", "valorant-", "esport",
    "f1-", "nascar-", "tennis-", "golf-", "pga-",
    "olympics-", "world-cup-", "super-bowl-",
]

# Sports-related title patterns
SPORTS_TITLE_KEYWORDS = [
    "NFL", "NBA", "NHL", "MLB", "MLS",
    "AFC North", "AFC South", "AFC East", "AFC West",  # NFL divisions
    "NFC North", "NFC South", "NFC East", "NFC West",  # NFL divisions
    "Premier League", "Champions League", "La Liga", "Serie A",
    "Super Bowl", "World Series", "Stanley Cup", "Finals",
    "vs.", " vs ", "match", "game ",
    "UFC", "boxing", "fight",
    "Dota", "CS:GO", "League of Legends", "Valorant",
    "Playoff", "playoffs", "Division Winner", "Conference Winner",
]

# Category filters - maps user-friendly names to tag/slug patterns
CATEGORY_FILTERS = {
    "crypto": {
        "tags": ["crypto", "cryptocurrency", "bitcoin", "ethereum", "defi", "web3"],
        "title_keywords": ["Bitcoin", "BTC", "Ethereum", "ETH", "crypto", "token", "blockchain"],
    },
    "politics": {
        "tags": ["politics", "elections", "government", "congress", "senate", "president"],
        "title_keywords": ["Trump", "Biden", "Congress", "Senate", "election", "vote", "president", "governor"],
    },
    "tech": {
        "tags": ["technology", "tech", "ai", "artificial-intelligence", "software"],
        "title_keywords": ["AI", "OpenAI", "Google", "Apple", "Microsoft", "Tesla", "Meta", "Amazon", "tech"],
    },
    "econ": {
        "tags": ["economics", "economy", "finance", "fed", "inflation", "rates"],
        "title_keywords": ["Fed", "inflation", "GDP", "recession", "rates", "stock", "market", "economy"],
    },
    "entertainment": {
        "tags": ["entertainment", "movies", "music", "celebrity", "awards"],
        "title_keywords": ["Oscar", "Grammy", "Emmy", "movie", "film", "celebrity", "album"],
    },
    "world": {
        "tags": ["world", "international", "geopolitics", "war", "conflict"],
        "title_keywords": ["Russia", "Ukraine", "China", "war", "NATO", "UN", "Israel", "Gaza"],
    },
}
