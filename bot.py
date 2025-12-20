"""
Polymarket Telegram Alert Bot - Main bot file.
"""

import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, CHECK_INTERVAL_MINUTES
from polymarket import get_unique_events, get_popular_markets, get_all_markets_paginated
from database import (
    init_database,
    get_or_create_user,
    toggle_user_setting,
    get_volume_deltas_bulk,
    get_volume_snapshot_count,
    add_to_watchlist,
    remove_from_watchlist,
    get_watchlist,
    get_recently_seen_slugs,
)
from scheduler import start_scheduler, stop_scheduler, run_manual_cycle, run_daily_digest
from alerts import check_underdog_alerts, format_bundled_underdogs, filter_sports, _format_volume

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    # Register user in database
    user = update.effective_user
    get_or_create_user(user.id, user.username)

    welcome_message = """Welcome to Polymarket Alerts Bot!

I'll help you track prediction markets on Polymarket.
Sports markets are filtered out (no edge there).

On-demand commands:
/top - Biggest markets by volume
/hot - Fastest moving (velocity)
/hot 24h - Velocity over 24 hours
/new - Markets added in last 24h
/underdogs - YES <20% with action
/discover - Rising markets

Watchlist:
/watch <slug> - Track a market
/watchlist - Your tracked markets

Settings:
/settings - Toggle push alerts
/checknow - Manual scan
/how - How everything works (detailed)

Push alerts (every 5 min):
• Volume milestones ($100K+)
• Discoveries (new + $25K+)
• Closing soon (<12h)
• Watchlist moves (5%+)"""

    await update.message.reply_text(welcome_message)


async def how_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /how command - detailed explanation of how everything works.
    Split into multiple messages for readability.
    """
    # Message 1: Overview
    msg1 = """HOW THIS BOT WORKS (1/5)
━━━━━━━━━━━━━━━━━━━━━━━━

ARCHITECTURE:
Every 5 minutes, the bot:
1. Fetches ~500 markets from Polymarket API
2. Saves volume snapshots to database
3. Compares current state to previous state
4. Detects signals (milestones, velocity, etc.)
5. Filters per-user (no duplicate alerts)
6. Sends bundled Telegram messages

DATA STORAGE:
• SQLite database on Railway persistent volume
• Survives redeploys (your data is safe)
• Tracks: snapshots, baselines, milestones, user alerts

SPORTS FILTER:
All alerts/commands exclude sports/esports markets.
Pattern matching on slugs (nfl-, nba-, ufc-) and
titles (vs, match, Super Bowl, etc.).
Reason: No information edge on sports betting."""

    # Message 2: Push Alerts
    msg2 = """HOW PUSH ALERTS WORK (2/5)
━━━━━━━━━━━━━━━━━━━━━━━━

1. VOLUME MILESTONES ($100K, $250K, $500K, $1M)
━━━
How it works:
• We track each market's "baseline" volume
• Every cycle, compare: previous < threshold <= current
• If crossed, alert once per threshold per market
• You'll NEVER get the same milestone alert twice

Example:
• Market at $80K → $120K = crosses $100K = ALERT
• Market at $120K → $130K = no new threshold = silent
• Market at $450K → $520K = crosses $500K = ALERT

Why these thresholds:
• $100K = serious money, filters out noise
• $1M = massive, rare but important

2. DISCOVERIES (first-seen + $25K+)
━━━
How it works:
• When we first see a market in our scan
• AND it already has $25K+ volume
• Alert: "this launched big, don't miss it"

Why:
• Markets can launch on weekends
• By Monday, they have $50K but we never saw them
• Discovery catches these so you don't miss them"""

    # Message 3: More Push Alerts
    msg3 = """HOW PUSH ALERTS WORK (3/5)
━━━━━━━━━━━━━━━━━━━━━━━━

3. CLOSING SOON (<12h + $5K/hr velocity)
━━━
How it works:
• Market ends in less than 12 hours
• AND has $5K+/hour flowing in right now
• Alert: "last-minute action before resolution"

Why:
• People bet near resolution when they have info
• $5K/hr on a closing market = someone knows something
• Tighter window (12h not 24h) = more signal

4. WATCHLIST (5% price move)
━━━
How it works:
• You add markets with /watch <slug>
• We track last_price for each watched market
• If price moves 5%+ from last alert, notify you
• After alerting, we update last_price

Example:
• You watch "trump-pardon" at YES: 60%
• It moves to 66% (+6%) = ALERT
• New baseline is 66%
• It moves to 69% (+3%) = silent (under 5%)
• It moves to 72% (+6% from 66%) = ALERT"""

    # Message 4: On-Demand Commands
    msg4 = """HOW COMMANDS WORK (4/5)
━━━━━━━━━━━━━━━━━━━━━━━━

/hot [1h|6h|24h] - VELOCITY LEADERS
━━━
How it works:
• Compares current volume to volume N hours ago
• Difference = velocity ($/hr)
• Ranks top 20 by velocity
• Sports excluded

Example output:
1. Trump pardon Biden?
   +$45K/hr | Total: $234K | YES: 89%

/underdogs - CONTRARIAN PLAYS
━━━
How it works (NEW SMART LOGIC):
• YES price < 20% (underdog)
• Total volume >= $10K (real market)
• Velocity > 10% of total volume
  (not absolute - RELATIVE to size)

Why relative velocity matters:
• Old logic: $1K/hr minimum = too low
• New logic: $50K market with $5K/hr = 10% = interesting
• $5M market with $5K/hr = 0.1% = noise

/new [24h|48h] - RECENTLY ADDED
━━━
How it works:
• Queries database for first_seen_at timestamp
• Returns markets added in last N hours
• Sorted by volume (highest first)
• Sports excluded"""

    # Message 5: Technical Details
    msg5 = """TECHNICAL DETAILS (5/5)
━━━━━━━━━━━━━━━━━━━━━━━━

DEDUPLICATION:
• user_alerts table tracks (user, market, alert_type)
• You never get the same alert twice
• New users don't get flooded with old milestones

VOLUME SNAPSHOTS:
• Stored every 5 minutes
• Used to calculate velocity (delta over time)
• Kept for historical comparison

VOLUME BASELINES:
• One row per market (latest known volume)
• Used for milestone delta detection
• Updated after each cycle

API CALLS:
• Polymarket Gamma API (public, no auth)
• Paginated: 100 markets per request
• We fetch 5 pages = 500 markets

THRESHOLDS SUMMARY:
━━━
• Milestones: $100K, $250K, $500K, $1M
• Discovery: $25K on first sight
• Closing soon: <12h + $5K/hr
• Watchlist: 5% price change
• Underdog: YES<20%, vol>$10K, velocity>10% of vol
• Hot: any positive velocity, ranked

SCHEDULER:
• APScheduler (async)
• Runs every 5 minutes
• Daily digest at 9am UTC

━━━━━━━━━━━━━━━━━━━━━━━━
Questions? Check the code:
github.com/alchooligan/polymarket-alerts-bot"""

    # Send all messages
    await update.message.reply_text(msg1)
    await update.message.reply_text(msg2)
    await update.message.reply_text(msg3)
    await update.message.reply_text(msg4)
    await update.message.reply_text(msg5)


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /top command - show 5 top markets by volume."""
    await update.message.reply_text("Fetching top markets...")

    try:
        # Fetch popular markets (sorted by volume from API)
        events = await get_popular_markets(limit=100, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Show top 5 events by volume
        response_lines = ["Top Markets by Volume:\n"]

        for event in events[:5]:
            title = event["title"]
            yes_price = event["yes_price"]
            volume = event["total_volume"]
            slug = event["slug"]

            # Format volume nicely
            if volume >= 1_000_000:
                volume_str = f"${volume / 1_000_000:.1f}M"
            elif volume >= 1_000:
                volume_str = f"${volume / 1_000:.1f}K"
            else:
                volume_str = f"${volume:.0f}"

            market_text = f"""- {title}
  YES: {yes_price:.0f}% | Volume: {volume_str}
  polymarket.com/event/{slug}
"""
            response_lines.append(market_text)

        response = "\n".join(response_lines)
        await update.message.reply_text(response, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error fetching markets: {e}")
        await update.message.reply_text("Error fetching markets. Please try again later.")


async def discover_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /discover command - show markets that are waking up (velocity-based)."""
    await update.message.reply_text("Finding markets waking up...")

    try:
        # Check if we have enough snapshots
        snapshot_count = get_volume_snapshot_count()
        if snapshot_count < 100:
            await update.message.reply_text(
                f"Not enough data yet ({snapshot_count} snapshots).\n"
                "Run /checknow a few times or wait ~30 min for snapshots to accumulate."
            )
            return

        # Fetch markets
        events = await get_all_markets_paginated(target_count=500, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Get volume deltas for last hour
        slugs = [e["slug"] for e in events]
        deltas = get_volume_deltas_bulk(slugs, hours=1)

        if not deltas:
            await update.message.reply_text(
                "No velocity data yet. Need ~1 hour of snapshots.\n"
                "Run /checknow periodically to build history."
            )
            return

        # Build list with deltas
        markets_with_delta = []
        for event in events:
            slug = event["slug"]
            if slug in deltas:
                delta = deltas[slug]
                total_volume = event["total_volume"]

                # Filter out giants (>$500K total volume) to force discovery
                if total_volume > 500_000:
                    continue

                # Only include positive velocity (growing markets)
                if delta > 0:
                    markets_with_delta.append({
                        **event,
                        "delta_1h": delta,
                    })

        # Sort by delta (highest velocity first)
        markets_with_delta.sort(key=lambda x: x["delta_1h"], reverse=True)

        if not markets_with_delta:
            await update.message.reply_text(
                "No waking-up markets found right now.\n"
                "Try again later when markets start moving."
            )
            return

        # Format response
        response_lines = ["Waking Up (by 1h velocity):\n"]

        for event in markets_with_delta[:10]:
            title = event["title"][:40]
            yes_price = event["yes_price"]
            delta = event["delta_1h"]
            total = event["total_volume"]
            slug = event["slug"]

            # Format delta
            if delta >= 1_000:
                delta_str = f"+${delta / 1_000:.1f}K"
            else:
                delta_str = f"+${delta:.0f}"

            # Format total
            if total >= 1_000:
                total_str = f"${total / 1_000:.0f}K"
            else:
                total_str = f"${total:.0f}"

            market_text = f"""- {title}
  {delta_str}/hr | Total: {total_str} | YES: {yes_price:.0f}%
  polymarket.com/event/{slug}
"""
            response_lines.append(market_text)

        response = "\n".join(response_lines)
        await update.message.reply_text(response, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in discover: {e}")
        await update.message.reply_text(f"Error: {e}")


async def underdogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /underdogs command - show contrarian plays.
    Markets with YES <20% but significant velocity relative to their size.
    """
    await update.message.reply_text("Finding underdogs...")

    try:
        # Get underdogs with new smart logic
        underdogs = await check_underdog_alerts(target_count=500)

        if not underdogs:
            await update.message.reply_text(
                "No underdogs found right now.\n\n"
                "Underdogs require: YES < 20%, volume >= $10K, and\n"
                "velocity >= 10% of total volume (significant action)."
            )
            return

        # Format and send
        message = format_bundled_underdogs(underdogs)
        await update.message.reply_text(message, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in underdogs: {e}")
        await update.message.reply_text(f"Error: {e}")


async def hot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /hot command - show markets by velocity (money moving fast).
    Usage: /hot [1h|6h|24h] - defaults to 1h
    """
    # Parse time window from args
    hours = 1
    time_label = "1h"

    if context.args:
        arg = context.args[0].lower()
        if arg in ["6h", "6"]:
            hours = 6
            time_label = "6h"
        elif arg in ["24h", "24"]:
            hours = 24
            time_label = "24h"

    await update.message.reply_text(f"Finding hottest markets ({time_label})...")

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=500, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Filter out sports
        events = filter_sports(events)

        # Get volume deltas for specified time window
        slugs = [e.get("slug") for e in events if e.get("slug")]
        deltas = get_volume_deltas_bulk(slugs, hours=hours)

        if not deltas:
            await update.message.reply_text(
                f"No velocity data for {time_label} window yet.\n"
                "Need more snapshots to accumulate. Try /checknow."
            )
            return

        # Build list with deltas
        hot_markets = []
        for event in events:
            slug = event.get("slug")
            if slug in deltas and deltas[slug] > 0:
                velocity = deltas[slug]
                # For multi-hour windows, normalize to per-hour rate
                velocity_per_hour = velocity / hours if hours > 1 else velocity

                hot_markets.append({
                    **event,
                    "velocity": velocity,
                    "velocity_per_hour": velocity_per_hour,
                })

        # Sort by velocity (highest first)
        hot_markets.sort(key=lambda x: x["velocity"], reverse=True)

        if not hot_markets:
            await update.message.reply_text(
                f"No markets with positive velocity in last {time_label}.\n"
                "Markets may be quiet right now."
            )
            return

        # Format top 20
        lines = [f"Hottest Markets ({time_label})", ""]

        for i, m in enumerate(hot_markets[:20], 1):
            title = m.get("title", "Unknown")[:40]
            velocity = m["velocity"]
            velocity_per_hour = m["velocity_per_hour"]
            total_volume = m.get("total_volume", 0)
            yes_price = m.get("yes_price", 0)
            slug = m.get("slug", "")

            # Format velocity
            if hours > 1:
                # Show total delta and per-hour rate
                if velocity >= 1000:
                    vel_str = f"+${velocity/1000:.0f}K"
                else:
                    vel_str = f"+${velocity:.0f}"
                if velocity_per_hour >= 1000:
                    rate_str = f"(${velocity_per_hour/1000:.1f}K/hr)"
                else:
                    rate_str = f"(${velocity_per_hour:.0f}/hr)"
                vel_display = f"{vel_str} {rate_str}"
            else:
                if velocity >= 1000:
                    vel_display = f"+${velocity/1000:.0f}K/hr"
                else:
                    vel_display = f"+${velocity:.0f}/hr"

            vol_str = _format_volume(total_volume)

            lines.append(f"{i}. {title}")
            lines.append(f"   {vel_display} | Total: {vol_str} | YES: {yes_price:.0f}%")
            lines.append(f"   polymarket.com/event/{slug}")
            lines.append("")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in hot: {e}")
        await update.message.reply_text(f"Error: {e}")


async def new_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /new command - show markets first seen recently.
    Usage: /new [24h|48h] - defaults to 24h
    """
    # Parse time window from args
    hours = 24
    time_label = "24h"

    if context.args:
        arg = context.args[0].lower()
        if arg in ["48h", "48"]:
            hours = 48
            time_label = "48h"

    await update.message.reply_text(f"Finding new markets (last {time_label})...")

    try:
        # Get recently seen slugs from database
        recent = get_recently_seen_slugs(hours=hours)

        if not recent:
            await update.message.reply_text(
                f"No new markets in the last {time_label}.\n"
                "Run /checknow to scan for new ones."
            )
            return

        # Get current market data for these slugs
        events = await get_all_markets_paginated(target_count=500, include_spam=False)
        events = filter_sports(events)

        # Build lookup
        event_map = {e.get("slug"): e for e in events}

        # Enrich recent markets with current data
        new_markets = []
        for r in recent:
            slug = r.get("event_slug")
            if slug in event_map:
                event = event_map[slug]
                new_markets.append({
                    "slug": slug,
                    "title": event.get("title", r.get("title", "Unknown")),
                    "total_volume": event.get("total_volume", 0),
                    "yes_price": event.get("yes_price", 0),
                    "first_seen_at": r.get("first_seen_at"),
                })

        if not new_markets:
            await update.message.reply_text(
                f"No new markets (non-sports) in the last {time_label}."
            )
            return

        # Sort by volume (highest first)
        new_markets.sort(key=lambda x: x["total_volume"], reverse=True)

        # Format top 20
        lines = [f"New Markets (last {time_label})", ""]

        for i, m in enumerate(new_markets[:20], 1):
            title = m.get("title", "Unknown")[:40]
            total_volume = m.get("total_volume", 0)
            yes_price = m.get("yes_price", 0)
            slug = m.get("slug", "")

            vol_str = _format_volume(total_volume)

            lines.append(f"{i}. {title}")
            lines.append(f"   Volume: {vol_str} | YES: {yes_price:.0f}%")
            lines.append(f"   polymarket.com/event/{slug}")
            lines.append("")

        if len(new_markets) > 20:
            lines.append(f"+{len(new_markets) - 20} more new markets")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in new: {e}")
        await update.message.reply_text(f"Error: {e}")


def build_settings_keyboard(user: dict) -> InlineKeyboardMarkup:
    """Build the settings inline keyboard based on user preferences."""
    alerts_enabled = user.get("new_markets_enabled", False)
    alerts_status = "ON" if alerts_enabled else "OFF"

    keyboard = [
        [InlineKeyboardButton(
            f"Push Alerts: {alerts_status}",
            callback_data="toggle_new_markets"
        )],
    ]
    return InlineKeyboardMarkup(keyboard)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /settings command - show settings with inline buttons."""
    telegram_user = update.effective_user
    user = get_or_create_user(telegram_user.id, telegram_user.username)

    text = """Alert Settings

Push alerts include:
• Volume milestones ($100K+)
• Discoveries (new markets with $25K+)
• Closing soon (<12h with action)
• Watchlist price moves (5%+)

On-demand commands (no toggle needed):
• /hot - velocity leaders
• /underdogs - contrarian plays
• /top - biggest markets"""

    keyboard = build_settings_keyboard(user)
    await update.message.reply_text(text, reply_markup=keyboard)


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline button presses for settings."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press

    telegram_user = update.effective_user
    callback_data = query.data

    # Toggle alerts
    if callback_data == "toggle_new_markets":
        new_value = toggle_user_setting(telegram_user.id, "new_markets_enabled")
        status = "ON" if new_value else "OFF"
        logger.info(f"User {telegram_user.id} toggled alerts to {status}")

    # Refresh the keyboard with updated settings
    user = get_or_create_user(telegram_user.id, telegram_user.username)
    keyboard = build_settings_keyboard(user)

    text = """Alert Settings

Push alerts include:
• Volume milestones ($100K+)
• Discoveries (new markets with $25K+)
• Closing soon (<12h with action)
• Watchlist price moves (5%+)

On-demand commands (no toggle needed):
• /hot - velocity leaders
• /underdogs - contrarian plays
• /top - biggest markets"""

    await query.edit_message_text(text, reply_markup=keyboard)


async def watch_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /watch command - add a market to watchlist."""
    if not context.args:
        await update.message.reply_text(
            "Usage: /watch <market-slug>\n\n"
            "Example: /watch will-trump-win-2024\n\n"
            "Find the slug in the Polymarket URL:\n"
            "polymarket.com/event/<slug>"
        )
        return

    slug = context.args[0].lower().strip()
    user_id = update.effective_user.id

    # Just add it - we'll find price data on next scan
    added = add_to_watchlist(user_id, slug, slug, 0)

    if added:
        await update.message.reply_text(
            f"Added to watchlist: {slug}\n\n"
            f"You'll get alerts when this market moves.\n"
            f"Use /watchlist to see all watched markets."
        )
    else:
        await update.message.reply_text(f"'{slug}' is already in your watchlist.")


async def unwatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /unwatch command - remove a market from watchlist."""
    if not context.args:
        await update.message.reply_text("Usage: /unwatch <market-slug>")
        return

    slug = context.args[0].lower().strip()
    user_id = update.effective_user.id

    removed = remove_from_watchlist(user_id, slug)

    if removed:
        await update.message.reply_text(f"Removed '{slug}' from your watchlist.")
    else:
        await update.message.reply_text(f"'{slug}' was not in your watchlist.")


async def watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /watchlist command - show user's watchlist."""
    user_id = update.effective_user.id
    watchlist = get_watchlist(user_id)

    if not watchlist:
        await update.message.reply_text(
            "Your watchlist is empty.\n\n"
            "Use /watch <market-slug> to add markets."
        )
        return

    lines = ["Your Watchlist\n"]
    for item in watchlist:
        title = item.get("title", "Unknown")[:40]
        last_price = item.get("last_price", 0)
        slug = item.get("event_slug", "")

        lines.append(f"- {title}")
        lines.append(f"  Last: {last_price:.0f}% | /unwatch {slug}")
        lines.append("")

    await update.message.reply_text("\n".join(lines))


async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /debug command - show database stats for diagnostics."""
    from database import get_connection, is_volume_seeded

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Count snapshots
        cursor.execute("SELECT COUNT(*) as count FROM volume_snapshots")
        snapshot_count = cursor.fetchone()["count"]

        # Count baselines
        cursor.execute("SELECT COUNT(*) as count FROM volume_baselines")
        baseline_count = cursor.fetchone()["count"]

        # Count milestones
        cursor.execute("SELECT COUNT(*) as count FROM volume_milestones")
        milestone_count = cursor.fetchone()["count"]

        # Get snapshot time range
        cursor.execute("SELECT MIN(recorded_at) as oldest, MAX(recorded_at) as latest FROM volume_snapshots")
        row = cursor.fetchone()
        oldest = row["oldest"]
        latest = row["latest"]

        # Check seeded flag
        seeded = is_volume_seeded()

        # Check user settings
        user_id = update.effective_user.id
        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (user_id,))
        user_row = cursor.fetchone()
        if user_row:
            user_info = f"new_markets={user_row['new_markets_enabled']}, big_moves={user_row['big_moves_enabled']}"
        else:
            user_info = "NOT IN DB"

        conn.close()

        response = f"""Database Stats

Snapshots: {snapshot_count}
Oldest: {oldest or 'None'}
Latest: {latest or 'None'}
Baselines: {baseline_count}
Milestones: {milestone_count}
Seeded: {seeded}

Your settings: {user_info}"""

        await update.message.reply_text(response)
    except Exception as e:
        await update.message.reply_text(f"Debug error: {e}")


async def digest_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /digest command - send digest to THIS user (for testing)."""
    await update.message.reply_text("Building digest...")

    try:
        # Build digest for the requesting user
        events = await get_all_markets_paginated(target_count=100, include_spam=False)

        if not events:
            await update.message.reply_text("No market data available.")
            return

        # Top by volume
        top_by_volume = sorted(events, key=lambda x: x.get("total_volume", 0), reverse=True)[:5]

        # Get velocity data
        slugs = [e.get("slug") for e in events if e.get("slug")]
        deltas = get_volume_deltas_bulk(slugs, hours=24)

        velocity_leaders = []
        for e in events:
            slug = e.get("slug")
            if slug in deltas and deltas[slug] > 0:
                velocity_leaders.append({**e, "delta_24h": deltas[slug]})
        velocity_leaders.sort(key=lambda x: x["delta_24h"], reverse=True)
        velocity_leaders = velocity_leaders[:5]

        # Build message
        lines = ["Daily Digest", ""]
        lines.append("Top Markets:")
        for e in top_by_volume:
            title = e.get("title", "Unknown")[:35]
            vol = e.get("total_volume", 0)
            vol_str = f"${vol/1_000_000:.1f}M" if vol >= 1_000_000 else f"${vol/1_000:.0f}K"
            lines.append(f"• {title} ({vol_str})")
        lines.append("")

        if velocity_leaders:
            lines.append("Fastest Growing (24h):")
            for e in velocity_leaders:
                title = e.get("title", "Unknown")[:35]
                delta = e.get("delta_24h", 0)
                delta_str = f"+${delta/1_000:.0f}K" if delta >= 1_000 else f"+${delta:.0f}"
                lines.append(f"• {title} ({delta_str})")
        else:
            lines.append("(Need 24h of data for velocity)")

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"Error in digest: {e}")
        await update.message.reply_text(f"Error: {e}")


async def seed_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /seed command - force re-seed volume baselines."""
    from alerts import seed_volume_baselines
    from database import set_system_flag

    await update.message.reply_text("Force seeding volume baselines...")

    try:
        # Clear the seeded flag first so we can reseed
        set_system_flag("volume_baselines_seeded", None)

        # Run seeding
        stats = await seed_volume_baselines(target_count=500)

        response = f"""Seeding complete!

Markets scanned: {stats.get('markets_scanned', 0)}
Baselines recorded: {stats.get('baselines_recorded', 0)}
Milestones recorded: {stats.get('milestones_recorded', 0)}

Now try /checknow to detect new milestones."""

        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Seeding error: {e}")
        await update.message.reply_text(f"Seeding error: {e}")


async def checknow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /checknow command - manually trigger an alert cycle."""
    await update.message.reply_text("Running alert check...")

    try:
        stats = await run_manual_cycle(context.application)

        # Build informative response
        response = f"""Scan complete

Scanned ~{stats['markets_scanned']} markets (sports excluded)
• {stats['milestones']} volume milestones ($100K+)
• {stats['discoveries']} discoveries (new + $25K+)
• {stats['closing_soon']} closing soon (<12h)
• {stats['watchlist']} watchlist moves (5%+)

Alerts sent: {stats['alerts_sent']}

Use /hot for velocity, /underdogs for contrarian plays"""

        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Error in manual alert check: {e}")
        await update.message.reply_text(f"Error running alert check: {e}")


async def main() -> None:
    """Start the bot."""
    # Check for token
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN not found!")
        print("Make sure you have a .env file with your token.")
        return

    # Initialize database
    init_database()

    print("Starting Polymarket Alert Bot...")
    print("Press Ctrl+C to stop.")

    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("how", how_command))
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("markets", top_command))  # Alias for backwards compatibility
    application.add_handler(CommandHandler("discover", discover_command))
    application.add_handler(CommandHandler("underdogs", underdogs_command))
    application.add_handler(CommandHandler("hot", hot_command))
    application.add_handler(CommandHandler("new", new_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("checknow", checknow_command))
    application.add_handler(CommandHandler("debug", debug_command))
    application.add_handler(CommandHandler("seed", seed_command))
    application.add_handler(CommandHandler("watch", watch_command))
    application.add_handler(CommandHandler("unwatch", unwatch_command))
    application.add_handler(CommandHandler("watchlist", watchlist_command))
    application.add_handler(CommandHandler("digest", digest_command))

    # Add callback handler for inline buttons
    application.add_handler(CallbackQueryHandler(settings_callback))

    # Start the bot using async context
    async with application:
        await application.start()
        await application.updater.start_polling()

        # Start the scheduler for periodic alert checks
        start_scheduler(application)
        print(f"Scheduler running. Checking every {CHECK_INTERVAL_MINUTES} minutes.")

        # Keep running until interrupted
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            # Stop scheduler and bot
            stop_scheduler()
            await application.updater.stop()
            await application.stop()


if __name__ == "__main__":
    asyncio.run(main())
