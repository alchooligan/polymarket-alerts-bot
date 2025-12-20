"""
Database setup and queries for Polymarket Telegram Bot.
Uses SQLite for simplicity.
"""

import sqlite3
from datetime import datetime
from typing import Optional
from config import DATABASE_PATH


def get_connection() -> sqlite3.Connection:
    """Get a database connection."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn


def init_database() -> None:
    """Initialize database tables. Safe to call multiple times."""
    conn = get_connection()
    cursor = conn.cursor()

    # Users table - stores user preferences
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            new_markets_enabled INTEGER DEFAULT 1,
            big_moves_enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # User category preferences (many-to-many)
    # If user has no rows here, they get ALL categories
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_categories (
            telegram_id INTEGER,
            category_slug TEXT,
            PRIMARY KEY (telegram_id, category_slug),
            FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
        )
    """)

    # Seen markets - to avoid duplicate "new market" alerts
    # telegram_id is NULL for global (system-wide), or set for per-user tracking
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seen_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_slug TEXT NOT NULL,
            event_id TEXT,
            title TEXT,
            telegram_id INTEGER DEFAULT NULL,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_slug, telegram_id)
        )
    """)

    # Index for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_seen_markets_slug
        ON seen_markets(event_slug)
    """)

    # Price snapshots - for detecting big price movements
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_slug TEXT NOT NULL,
            yes_price REAL NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Index for time-based queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_snapshots_slug_time
        ON price_snapshots(event_slug, recorded_at)
    """)

    # Alerts log - for future rate limiting and history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            alert_type TEXT NOT NULL,
            event_slug TEXT,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Per-user alert memory - tracks which markets each user has been alerted about
    # Prevents duplicate alerts for the same market to the same user
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_alerts (
            telegram_id INTEGER NOT NULL,
            event_slug TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (telegram_id, event_slug, alert_type)
        )
    """)

    # Watchlist - users can watch specific markets for any price move
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            telegram_id INTEGER NOT NULL,
            event_slug TEXT NOT NULL,
            title TEXT,
            last_price REAL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (telegram_id, event_slug)
        )
    """)

    # Volume milestones - track which thresholds each market has crossed
    # Once a market crosses $10K, we record it so we never alert again for $10K
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS volume_milestones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_slug TEXT NOT NULL,
            threshold INTEGER NOT NULL,
            crossed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            volume_at_crossing REAL,
            UNIQUE(event_slug, threshold)
        )
    """)

    # Index for fast lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_volume_milestones_slug
        ON volume_milestones(event_slug)
    """)

    # Volume baselines - stores last known volume for delta detection
    # This enables true "crossing" detection: was below threshold, now above
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS volume_baselines (
            event_slug TEXT PRIMARY KEY,
            last_volume REAL NOT NULL,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # System flags - for tracking one-time operations like seeding
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_flags (
            flag_name TEXT PRIMARY KEY,
            flag_value TEXT,
            set_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Volume snapshots - for velocity/acceleration detection
    # Store volume at each check to calculate delta over time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS volume_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_slug TEXT NOT NULL,
            volume REAL NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Index for time-range queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_volume_snapshots_slug_time
        ON volume_snapshots(event_slug, recorded_at)
    """)

    conn.commit()
    conn.close()
    print("Database initialized.")


# ============================================
# User functions
# ============================================

def get_user(telegram_id: int) -> Optional[dict]:
    """Get user by telegram_id."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def create_user(telegram_id: int, username: str = None) -> dict:
    """Create a new user with default settings."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO users (telegram_id, username) VALUES (?, ?)",
        (telegram_id, username)
    )
    conn.commit()
    conn.close()
    return get_user(telegram_id)


def get_or_create_user(telegram_id: int, username: str = None) -> dict:
    """Get user or create if doesn't exist."""
    user = get_user(telegram_id)
    if not user:
        user = create_user(telegram_id, username)
    return user


def update_user_setting(telegram_id: int, setting: str, value: int) -> None:
    """Update a user setting (new_markets_enabled or big_moves_enabled)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE users SET {setting} = ? WHERE telegram_id = ?",
        (value, telegram_id)
    )
    conn.commit()
    conn.close()


def toggle_user_setting(telegram_id: int, setting: str) -> int:
    """Toggle a boolean setting. Returns the new value."""
    user = get_user(telegram_id)
    if not user:
        return 0

    current_value = user.get(setting, 0)
    new_value = 0 if current_value else 1
    update_user_setting(telegram_id, setting, new_value)
    return new_value


def get_all_users_with_alerts_enabled() -> list[dict]:
    """Get all users who have at least one alert type enabled."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM users
        WHERE new_markets_enabled = 1 OR big_moves_enabled = 1
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# ============================================
# Seen markets functions (for new market alerts)
# ============================================

def is_market_seen(event_slug: str, telegram_id: int = None) -> bool:
    """
    Check if we've already seen this market.
    telegram_id=None checks global (system-wide) seen status.
    """
    conn = get_connection()
    cursor = conn.cursor()
    if telegram_id is None:
        cursor.execute(
            "SELECT 1 FROM seen_markets WHERE event_slug = ? AND telegram_id IS NULL",
            (event_slug,)
        )
    else:
        cursor.execute(
            "SELECT 1 FROM seen_markets WHERE event_slug = ? AND telegram_id = ?",
            (event_slug, telegram_id)
        )
    result = cursor.fetchone()
    conn.close()
    return result is not None


def mark_market_seen(event_slug: str, title: str, event_id: str = None, telegram_id: int = None) -> None:
    """
    Mark a market as seen.
    telegram_id=None marks it globally (system-wide).
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO seen_markets (event_slug, event_id, title, telegram_id) VALUES (?, ?, ?, ?)",
        (event_slug, event_id, title, telegram_id)
    )
    conn.commit()
    conn.close()


def mark_markets_seen_bulk(markets: list[dict], telegram_id: int = None) -> None:
    """Mark multiple markets as seen in one transaction."""
    if not markets:
        return

    conn = get_connection()
    cursor = conn.cursor()
    for market in markets:
        cursor.execute(
            "INSERT OR IGNORE INTO seen_markets (event_slug, event_id, title, telegram_id) VALUES (?, ?, ?, ?)",
            (market.get("slug"), market.get("event_id"), market.get("title"), telegram_id)
        )
    conn.commit()
    conn.close()


def get_unseen_slugs(event_slugs: list[str], telegram_id: int = None) -> list[str]:
    """Given a list of slugs, return only the ones we haven't seen."""
    if not event_slugs:
        return []

    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(event_slugs))

    if telegram_id is None:
        cursor.execute(
            f"SELECT event_slug FROM seen_markets WHERE event_slug IN ({placeholders}) AND telegram_id IS NULL",
            event_slugs
        )
    else:
        cursor.execute(
            f"SELECT event_slug FROM seen_markets WHERE event_slug IN ({placeholders}) AND telegram_id = ?",
            (*event_slugs, telegram_id)
        )

    seen = {row["event_slug"] for row in cursor.fetchall()}
    conn.close()

    return [slug for slug in event_slugs if slug not in seen]


def get_recently_seen_slugs(hours: int = 24) -> list[dict]:
    """
    Get markets that were first seen within the last N hours.
    Returns list of dicts with slug, title, first_seen_at.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT event_slug, title, first_seen_at
        FROM seen_markets
        WHERE telegram_id IS NULL
        AND first_seen_at >= datetime('now', ?)
        ORDER BY first_seen_at DESC
        """,
        (f"-{hours} hours",)
    )

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


# ============================================
# Price snapshot functions (for big move alerts)
# ============================================

def save_price_snapshot(event_slug: str, yes_price: float) -> None:
    """Save a price snapshot for an event."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO price_snapshots (event_slug, yes_price) VALUES (?, ?)",
        (event_slug, yes_price)
    )
    conn.commit()
    conn.close()


def get_price_from_hours_ago(event_slug: str, hours: int = 1) -> Optional[float]:
    """Get the price snapshot from approximately X hours ago."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT yes_price FROM price_snapshots
        WHERE event_slug = ?
        AND recorded_at <= datetime('now', ?)
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (event_slug, f"-{hours} hours"))
    row = cursor.fetchone()
    conn.close()
    return row["yes_price"] if row else None


def cleanup_old_snapshots(days: int = 7) -> int:
    """Delete price snapshots older than X days. Returns count deleted."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM price_snapshots WHERE recorded_at < datetime('now', ?)",
        (f"-{days} days",)
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def save_price_snapshots_bulk(events: list[dict]) -> None:
    """Save price snapshots for multiple events in one transaction."""
    if not events:
        return

    conn = get_connection()
    cursor = conn.cursor()
    for event in events:
        cursor.execute(
            "INSERT INTO price_snapshots (event_slug, yes_price) VALUES (?, ?)",
            (event.get("slug"), event.get("yes_price", 0))
        )
    conn.commit()
    conn.close()


# ============================================
# Alerts log functions (for rate limiting)
# ============================================

def log_alert(telegram_id: int, alert_type: str, event_slug: str = None) -> None:
    """Log that an alert was sent to a user."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO alerts_log (telegram_id, alert_type, event_slug) VALUES (?, ?, ?)",
        (telegram_id, alert_type, event_slug)
    )
    conn.commit()
    conn.close()


def was_user_alerted(telegram_id: int, event_slug: str, alert_type: str) -> bool:
    """Check if a user has already been alerted about a specific market."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM user_alerts WHERE telegram_id = ? AND event_slug = ? AND alert_type = ?",
        (telegram_id, event_slug, alert_type)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None


def mark_user_alerted(telegram_id: int, event_slug: str, alert_type: str) -> None:
    """Record that a user has been alerted about a market."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO user_alerts (telegram_id, event_slug, alert_type) VALUES (?, ?, ?)",
        (telegram_id, event_slug, alert_type)
    )
    conn.commit()
    conn.close()


def mark_user_alerted_bulk(telegram_id: int, event_slugs: list[str], alert_type: str) -> None:
    """Record that a user has been alerted about multiple markets."""
    if not event_slugs:
        return
    conn = get_connection()
    cursor = conn.cursor()
    for slug in event_slugs:
        cursor.execute(
            "INSERT OR IGNORE INTO user_alerts (telegram_id, event_slug, alert_type) VALUES (?, ?, ?)",
            (telegram_id, slug, alert_type)
        )
    conn.commit()
    conn.close()


def filter_unseen_markets(telegram_id: int, markets: list[dict], alert_type: str) -> list[dict]:
    """Filter markets to only include ones the user hasn't been alerted about."""
    if not markets:
        return []

    conn = get_connection()
    cursor = conn.cursor()

    # Get all slugs this user has been alerted about for this type
    cursor.execute(
        "SELECT event_slug FROM user_alerts WHERE telegram_id = ? AND alert_type = ?",
        (telegram_id, alert_type)
    )
    seen_slugs = {row["event_slug"] for row in cursor.fetchall()}
    conn.close()

    return [m for m in markets if m.get("slug") not in seen_slugs]


# ============================================
# Watchlist functions
# ============================================

def add_to_watchlist(telegram_id: int, event_slug: str, title: str, current_price: float) -> bool:
    """Add a market to user's watchlist. Returns True if added, False if already exists."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO watchlist (telegram_id, event_slug, title, last_price) VALUES (?, ?, ?, ?)",
        (telegram_id, event_slug, title, current_price)
    )
    added = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return added


def remove_from_watchlist(telegram_id: int, event_slug: str) -> bool:
    """Remove a market from user's watchlist. Returns True if removed."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM watchlist WHERE telegram_id = ? AND event_slug = ?",
        (telegram_id, event_slug)
    )
    removed = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return removed


def get_watchlist(telegram_id: int) -> list[dict]:
    """Get all markets in user's watchlist."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT event_slug, title, last_price, added_at FROM watchlist WHERE telegram_id = ?",
        (telegram_id,)
    )
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_all_watched_markets() -> list[dict]:
    """Get all watched markets across all users (for scheduler)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT telegram_id, event_slug, title, last_price FROM watchlist")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_watchlist_price(telegram_id: int, event_slug: str, new_price: float) -> None:
    """Update the last known price for a watched market."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE watchlist SET last_price = ? WHERE telegram_id = ? AND event_slug = ?",
        (new_price, telegram_id, event_slug)
    )
    conn.commit()
    conn.close()


def get_alerts_sent_in_last_hour(telegram_id: int, alert_type: str = None) -> int:
    """Count alerts sent to user in the last hour. For future rate limiting."""
    conn = get_connection()
    cursor = conn.cursor()
    if alert_type:
        cursor.execute("""
            SELECT COUNT(*) as count FROM alerts_log
            WHERE telegram_id = ? AND alert_type = ?
            AND sent_at >= datetime('now', '-1 hour')
        """, (telegram_id, alert_type))
    else:
        cursor.execute("""
            SELECT COUNT(*) as count FROM alerts_log
            WHERE telegram_id = ?
            AND sent_at >= datetime('now', '-1 hour')
        """, (telegram_id,))
    result = cursor.fetchone()
    conn.close()
    return result["count"] if result else 0


# ============================================
# Volume milestone functions
# ============================================

def has_crossed_threshold(event_slug: str, threshold: int) -> bool:
    """Check if a market has already crossed a specific volume threshold."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM volume_milestones WHERE event_slug = ? AND threshold = ?",
        (event_slug, threshold)
    )
    result = cursor.fetchone()
    conn.close()
    return result is not None


def get_crossed_thresholds(event_slug: str) -> list[int]:
    """Get all thresholds a market has already crossed."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT threshold FROM volume_milestones WHERE event_slug = ?",
        (event_slug,)
    )
    rows = cursor.fetchall()
    conn.close()
    return [row["threshold"] for row in rows]


def record_milestone(event_slug: str, threshold: int, volume: float) -> None:
    """Record that a market has crossed a volume threshold."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO volume_milestones (event_slug, threshold, volume_at_crossing) VALUES (?, ?, ?)",
        (event_slug, threshold, volume)
    )
    conn.commit()
    conn.close()


def get_uncrossed_thresholds(event_slug: str, all_thresholds: list[int]) -> list[int]:
    """Given a list of thresholds, return which ones this market hasn't crossed yet."""
    crossed = set(get_crossed_thresholds(event_slug))
    return [t for t in all_thresholds if t not in crossed]


def record_milestones_bulk(milestones: list[tuple]) -> None:
    """
    Record multiple milestones at once. Used for seeding.
    milestones: list of (event_slug, threshold, volume) tuples
    """
    if not milestones:
        return

    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR IGNORE INTO volume_milestones (event_slug, threshold, volume_at_crossing) VALUES (?, ?, ?)",
        milestones
    )
    conn.commit()
    conn.close()


# ============================================
# Volume baseline functions (for delta detection)
# ============================================

def get_volume_baseline(event_slug: str) -> Optional[float]:
    """Get the last known volume for a market. Returns None if never seen."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT last_volume FROM volume_baselines WHERE event_slug = ?",
        (event_slug,)
    )
    row = cursor.fetchone()
    conn.close()
    return row["last_volume"] if row else None


def get_volume_baselines_bulk(event_slugs: list[str]) -> dict[str, float]:
    """Get last known volumes for multiple markets. Returns {slug: volume} dict."""
    if not event_slugs:
        return {}

    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ",".join("?" * len(event_slugs))
    cursor.execute(
        f"SELECT event_slug, last_volume FROM volume_baselines WHERE event_slug IN ({placeholders})",
        event_slugs
    )
    rows = cursor.fetchall()
    conn.close()
    return {row["event_slug"]: row["last_volume"] for row in rows}


def update_volume_baseline(event_slug: str, volume: float) -> None:
    """Update or insert volume baseline for a market."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO volume_baselines (event_slug, last_volume, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(event_slug) DO UPDATE SET
           last_volume = excluded.last_volume,
           updated_at = CURRENT_TIMESTAMP""",
        (event_slug, volume)
    )
    conn.commit()
    conn.close()


def update_volume_baselines_bulk(volumes: list[tuple]) -> None:
    """
    Update or insert volume baselines for multiple markets.
    volumes: list of (event_slug, volume) tuples
    """
    if not volumes:
        return

    conn = get_connection()
    cursor = conn.cursor()
    for event_slug, volume in volumes:
        cursor.execute(
            """INSERT INTO volume_baselines (event_slug, last_volume, updated_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(event_slug) DO UPDATE SET
               last_volume = excluded.last_volume,
               updated_at = CURRENT_TIMESTAMP""",
            (event_slug, volume)
        )
    conn.commit()
    conn.close()


# ============================================
# System flags (for one-time operations)
# ============================================

def get_system_flag(flag_name: str) -> Optional[str]:
    """Get a system flag value. Returns None if not set."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT flag_value FROM system_flags WHERE flag_name = ?",
        (flag_name,)
    )
    row = cursor.fetchone()
    conn.close()
    return row["flag_value"] if row else None


def set_system_flag(flag_name: str, flag_value: str = "1") -> None:
    """Set a system flag."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO system_flags (flag_name, flag_value, set_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(flag_name) DO UPDATE SET
           flag_value = excluded.flag_value,
           set_at = CURRENT_TIMESTAMP""",
        (flag_name, flag_value)
    )
    conn.commit()
    conn.close()


def is_volume_seeded() -> bool:
    """Check if we've done the initial volume baseline seeding."""
    return get_system_flag("volume_baselines_seeded") is not None


def mark_volume_seeded() -> None:
    """Mark that we've completed the initial volume baseline seeding."""
    set_system_flag("volume_baselines_seeded", "1")


# ============================================
# Volume snapshot functions (for velocity detection)
# ============================================

def save_volume_snapshot(event_slug: str, volume: float) -> None:
    """Save a volume snapshot for an event."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO volume_snapshots (event_slug, volume) VALUES (?, ?)",
        (event_slug, volume)
    )
    conn.commit()
    conn.close()


def save_volume_snapshots_bulk(events: list[dict]) -> None:
    """Save volume snapshots for multiple events in one transaction."""
    if not events:
        return

    conn = get_connection()
    cursor = conn.cursor()
    for event in events:
        cursor.execute(
            "INSERT INTO volume_snapshots (event_slug, volume) VALUES (?, ?)",
            (event.get("slug"), event.get("total_volume", 0))
        )
    conn.commit()
    conn.close()


def get_volume_from_hours_ago(event_slug: str, hours: int = 1) -> Optional[float]:
    """Get the volume snapshot from approximately X hours ago."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT volume FROM volume_snapshots
        WHERE event_slug = ?
        AND recorded_at <= datetime('now', ?)
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (event_slug, f"-{hours} hours"))
    row = cursor.fetchone()
    conn.close()
    return row["volume"] if row else None


def get_volume_delta(event_slug: str, hours: int = 1) -> Optional[float]:
    """
    Calculate volume change over the last X hours.
    Returns delta (current - old), or None if no old snapshot.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get most recent snapshot
    cursor.execute("""
        SELECT volume FROM volume_snapshots
        WHERE event_slug = ?
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (event_slug,))
    current_row = cursor.fetchone()

    if not current_row:
        conn.close()
        return None

    current_volume = current_row["volume"]

    # Get snapshot from X hours ago
    cursor.execute("""
        SELECT volume FROM volume_snapshots
        WHERE event_slug = ?
        AND recorded_at <= datetime('now', ?)
        ORDER BY recorded_at DESC
        LIMIT 1
    """, (event_slug, f"-{hours} hours"))
    old_row = cursor.fetchone()
    conn.close()

    if not old_row:
        return None

    return current_volume - old_row["volume"]


def get_volume_deltas_bulk(event_slugs: list[str], hours: int = 1) -> dict[str, float]:
    """
    Get volume deltas for multiple markets.
    Returns {slug: delta} dict. Missing = no old snapshot.

    Optimized: Single query with CTEs instead of N*2 queries.
    """
    if not event_slugs:
        return {}

    conn = get_connection()
    cursor = conn.cursor()

    # Build placeholders for IN clause
    placeholders = ",".join("?" * len(event_slugs))
    time_threshold = f"-{hours} hours"

    # Single query using CTEs for current and old volumes
    query = f"""
    WITH current_volumes AS (
        SELECT event_slug, volume,
               ROW_NUMBER() OVER (PARTITION BY event_slug ORDER BY recorded_at DESC) as rn
        FROM volume_snapshots
        WHERE event_slug IN ({placeholders})
    ),
    old_volumes AS (
        SELECT event_slug, volume,
               ROW_NUMBER() OVER (PARTITION BY event_slug ORDER BY recorded_at DESC) as rn
        FROM volume_snapshots
        WHERE event_slug IN ({placeholders})
        AND recorded_at <= datetime('now', ?)
    )
    SELECT
        c.event_slug,
        c.volume as current_volume,
        o.volume as old_volume,
        (c.volume - o.volume) as delta
    FROM current_volumes c
    JOIN old_volumes o ON c.event_slug = o.event_slug
    WHERE c.rn = 1 AND o.rn = 1
    """

    # Parameters: slugs for current, slugs for old, time threshold
    params = list(event_slugs) + list(event_slugs) + [time_threshold]
    cursor.execute(query, params)

    results = {row["event_slug"]: row["delta"] for row in cursor.fetchall()}

    conn.close()
    return results


def cleanup_old_volume_snapshots(days: int = 7) -> int:
    """Delete volume snapshots older than X days. Returns count deleted."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM volume_snapshots WHERE recorded_at < datetime('now', ?)",
        (f"-{days} days",)
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def get_volume_snapshot_count() -> int:
    """Get total number of volume snapshots (for diagnostics)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM volume_snapshots")
    row = cursor.fetchone()
    conn.close()
    return row["count"] if row else 0


# ============================================
# Test / Debug
# ============================================

if __name__ == "__main__":
    print("Initializing database...")
    init_database()

    print("\nTesting user functions...")
    user = get_or_create_user(12345, "testuser")
    print(f"Created/got user: {user}")

    new_val = toggle_user_setting(12345, "new_markets_enabled")
    print(f"Toggled new_markets_enabled to: {new_val}")

    print("\nTesting seen markets...")
    mark_market_seen("evt_123", "test-market-slug", "Test Market Title")
    print(f"Is market seen? {is_market_seen('test-market-slug')}")
    print(f"Is other market seen? {is_market_seen('other-slug')}")

    print("\nDatabase test complete!")
