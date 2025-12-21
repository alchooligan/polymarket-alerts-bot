"""
Polymarket Telegram Alert Bot - Main bot file.
"""

import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, CHECK_INTERVAL_MINUTES
from polymarket import get_unique_events, get_all_markets_paginated
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
    get_price_deltas_bulk,
)
from scheduler import start_scheduler, stop_scheduler, run_manual_cycle, run_daily_digest
from alerts import check_underdog_alerts, format_bundled_underdogs, filter_sports, filter_resolved, filter_by_category, get_available_categories, _format_volume

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
/hot - Fastest moving (velocity)
/discover - Rising markets
/movers - Biggest price swings
/quiet - Sleeping giants
/underdogs - Contrarian plays
/new - Recently added markets

Category filters (add to any command):
crypto, politics, tech, econ, world
Example: /hot crypto or /movers politics

Watchlist:
/watch <slug> - Track a market
/watchlist - Your tracked markets

Settings:
/settings - Toggle push alerts
/checknow - Manual scan
/how - Detailed explanation

Push alerts (every 5 min):
• Volume milestones ($100K+)
• Discoveries (new + $25K+)
• Closing soon (<12h)
• Watchlist moves (5%+)"""

    await update.message.reply_text(welcome_message)


async def how_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /how command - simple explanation of commands and alerts.
    Consumer-friendly, not technical.
    """
    msg = """HOW IT WORKS
━━━━━━━━━━━━━━━━━━━━━━━━

AUTOMATIC ALERTS (every 5 min):

Volume Milestones
→ Alert when market crosses $100K, $250K, $500K, or $1M
→ You only get each milestone once per market

Discoveries
→ Alert when we find a NEW market that already has $25K+
→ Catches markets that "launched big"

Closing Soon
→ Alert when market ends in <12 hours AND has $5K+/hr action
→ Last-minute betting often = someone knows something

Watchlist
→ Alert when YOUR tracked markets move 5%+
→ Add with: /watch <slug>

━━━━━━━━━━━━━━━━━━━━━━━━

ON-DEMAND COMMANDS:

/hot [time] [category]
→ Markets by velocity (money flowing in)
→ Try: /hot 6h, /hot 24h, /hot crypto

/discover [category]
→ Rising markets (<$500K) with momentum
→ Shows velocity, volume changes, price history

/movers [category]
→ Biggest price swings in 24h (±5%+)
→ Split into GAINERS and LOSERS

/quiet [category]
→ Sleeping giants: big ($100K+) but quiet
→ Could wake up anytime

/underdogs
→ YES <20% but price rising (+2% in 24h)
→ Contrarian money moving the needle

/new [time]
→ Recently added markets
→ Try: /new 48h

━━━━━━━━━━━━━━━━━━━━━━━━

CATEGORY FILTERS:
crypto, politics, tech, econ, world, entertainment

Example: /hot politics or /movers crypto

━━━━━━━━━━━━━━━━━━━━━━━━

NOTE: Sports/esports markets excluded.
No edge on sports betting."""

    await update.message.reply_text(msg)


async def discover_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /discover command - show markets that are waking up with rich data.
    Usage: /discover [category]
    Categories: crypto, politics, tech, econ, entertainment, world
    """
    # Parse category from args
    category = None
    available_cats = get_available_categories()

    for arg in context.args:
        if arg.lower() in available_cats:
            category = arg.lower()
            break

    status_msg = "Finding markets waking up"
    if category:
        status_msg += f" [{category}]"
    await update.message.reply_text(status_msg + "...")

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
        events = await get_all_markets_paginated(target_count=2000, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Filter out sports and resolved markets
        events = filter_sports(events)
        events = filter_resolved(events)

        # Apply category filter if specified
        if category:
            events = filter_by_category(events, category)

        slugs = [e["slug"] for e in events]

        # Get volume deltas for multiple windows
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)
        deltas_6h = get_volume_deltas_bulk(slugs, hours=6)
        deltas_24h = get_volume_deltas_bulk(slugs, hours=24)

        # Get price deltas
        price_deltas_6h = get_price_deltas_bulk(slugs, hours=6)
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

        if not deltas_1h:
            await update.message.reply_text(
                "No velocity data yet. Need ~1 hour of snapshots.\n"
                "Run /checknow periodically to build history."
            )
            return

        # Build list with momentum scores
        markets_with_delta = []
        for event in events:
            slug = event["slug"]
            if slug in deltas_1h:
                delta_1h = deltas_1h[slug]
                delta_6h = deltas_6h.get(slug, 0)
                total_volume = event["total_volume"]

                # Filter out giants (>$500K total volume) to force discovery
                if total_volume > 500_000:
                    continue

                # Only include positive velocity (growing markets)
                if delta_1h > 0:
                    # Calculate velocity as % of market per hour
                    velocity_pct = (delta_1h / total_volume * 100) if total_volume > 0 else 0

                    # Calculate 6h volume growth %
                    volume_6h_ago = total_volume - delta_6h
                    volume_growth_pct = (delta_6h / volume_6h_ago * 100) if volume_6h_ago > 0 else 0

                    # Get price change
                    price_data_6h = price_deltas_6h.get(slug, {})
                    price_change = abs(price_data_6h.get("delta", 0))

                    # MOMENTUM SCORE: combines velocity %, volume growth %, and price change
                    # Weight: velocity (40%) + volume growth (30%) + price change (30%)
                    momentum_score = (velocity_pct * 0.4) + (volume_growth_pct * 0.3) + (price_change * 0.3)

                    markets_with_delta.append({
                        **event,
                        "delta_1h": delta_1h,
                        "delta_6h": delta_6h,
                        "delta_24h": deltas_24h.get(slug, 0),
                        "velocity_pct": velocity_pct,
                        "volume_growth_pct": volume_growth_pct,
                        "price_change_6h": price_data_6h.get("delta", 0),
                        "momentum_score": momentum_score,
                        "price_6h": price_deltas_6h.get(slug, {}),
                        "price_24h": price_deltas_24h.get(slug, {}),
                    })

        # Sort by MOMENTUM SCORE (not raw velocity)
        markets_with_delta.sort(key=lambda x: x["momentum_score"], reverse=True)

        if not markets_with_delta:
            cat_msg = f" in {category}" if category else ""
            await update.message.reply_text(
                f"No waking-up markets{cat_msg} found right now.\n"
                "Try again later when markets start moving."
            )
            return

        # Format response with rich data
        header = "Waking Up (velocity + data)"
        if category:
            header += f" [{category}]"
        response_lines = [header, ""]

        for event in markets_with_delta[:10]:
            title = event["title"][:45]
            yes_price = event["yes_price"]
            delta_1h = event["delta_1h"]
            delta_6h = event["delta_6h"]
            total = event["total_volume"]
            velocity_pct = event["velocity_pct"]
            slug = event["slug"]
            price_6h = event.get("price_6h", {})
            price_24h = event.get("price_24h", {})

            # Format velocity
            vel_str = f"+${delta_1h/1000:.1f}K/hr" if delta_1h >= 1000 else f"+${delta_1h:.0f}/hr"

            # Format total volume
            vol_str = _format_volume(total)

            # Format 6h volume change
            if delta_6h > 0:
                delta_6h_pct = (delta_6h / (total - delta_6h) * 100) if (total - delta_6h) > 0 else 0
                vol_6h_str = f"+${delta_6h/1000:.0f}K" if delta_6h >= 1000 else f"+${delta_6h:.0f}"
                vol_change_str = f"{vol_6h_str} in 6h (+{delta_6h_pct:.0f}%)"
            else:
                vol_change_str = "no 6h data"

            # Format price changes
            price_parts = []
            if price_6h:
                p6_delta = price_6h.get("delta", 0)
                p6_old = price_6h.get("old", 0)
                if p6_delta != 0:
                    p6_sign = "+" if p6_delta > 0 else ""
                    price_parts.append(f"was {p6_old:.0f}% 6h ago ({p6_sign}{p6_delta:.0f}%)")
            if price_24h:
                p24_delta = price_24h.get("delta", 0)
                p24_old = price_24h.get("old", 0)
                if p24_delta != 0:
                    p24_sign = "+" if p24_delta > 0 else ""
                    price_parts.append(f"was {p24_old:.0f}% 24h ago ({p24_sign}{p24_delta:.0f}%)")

            price_str = " | ".join(price_parts) if price_parts else "no price history"

            response_lines.append(f"- {title}")
            response_lines.append(f"  Velocity: {vel_str} ({velocity_pct:.1f}%/hr of market)")
            response_lines.append(f"  Volume: {vol_str} total | {vol_change_str}")
            response_lines.append(f"  Price: {yes_price:.0f}% now | {price_str}")
            response_lines.append(f"  polymarket.com/event/{slug}")
            response_lines.append("")

        response = "\n".join(response_lines).strip()
        await update.message.reply_text(response, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in discover: {e}")
        await update.message.reply_text(f"Error: {e}")


async def underdogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /underdogs command - show contrarian plays.
    Markets with YES <20% where price is actually rising (someone betting against consensus).
    """
    await update.message.reply_text("Finding underdogs with rising prices...")

    try:
        # Get underdogs with price movement logic
        underdogs = await check_underdog_alerts(target_count=500)

        if not underdogs:
            await update.message.reply_text(
                "No underdogs found right now.\n\n"
                "Underdogs require:\n"
                "- YES price < 20%\n"
                "- Volume >= $50K\n"
                "- Price went UP +2% in last 24h\n\n"
                "This catches contrarian money moving the needle."
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
    Handle the /hot command - show markets by velocity with rich data.
    Usage: /hot [1h|6h|24h] [category] - defaults to 1h, no filter
    Categories: crypto, politics, tech, econ, entertainment, world
    """
    # Parse time window and category from args
    hours = 1
    time_label = "1h"
    category = None

    available_cats = get_available_categories()

    for arg in context.args:
        arg_lower = arg.lower()
        if arg_lower in ["6h", "6"]:
            hours = 6
            time_label = "6h"
        elif arg_lower in ["24h", "24"]:
            hours = 24
            time_label = "24h"
        elif arg_lower in available_cats:
            category = arg_lower

    status_msg = f"Finding hottest markets ({time_label})"
    if category:
        status_msg += f" [{category}]"
    await update.message.reply_text(status_msg + "...")

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=2000, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Filter out sports and resolved markets
        events = filter_sports(events)
        events = filter_resolved(events)

        # Apply category filter if specified
        if category:
            events = filter_by_category(events, category)

        slugs = [e.get("slug") for e in events if e.get("slug")]

        # Get volume deltas for specified time window
        deltas = get_volume_deltas_bulk(slugs, hours=hours)

        # Get price deltas for the same window
        price_deltas = get_price_deltas_bulk(slugs, hours=hours)

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
                total_volume = event.get("total_volume", 0)

                # Calculate velocity as % of market per hour
                velocity_pct_per_hour = (velocity / total_volume * 100 / hours) if total_volume > 0 else 0

                # For multi-hour windows, normalize to per-hour rate
                velocity_per_hour = velocity / hours if hours > 1 else velocity

                hot_markets.append({
                    **event,
                    "velocity": velocity,
                    "velocity_per_hour": velocity_per_hour,
                    "velocity_pct": velocity_pct_per_hour,
                    "price_data": price_deltas.get(slug, {}),
                })

        # Sort by velocity %/hr (relative to market size) - THIS IS THE KEY SIGNAL
        hot_markets.sort(key=lambda x: x["velocity_pct"], reverse=True)

        if not hot_markets:
            cat_msg = f" in {category}" if category else ""
            await update.message.reply_text(
                f"No markets{cat_msg} with positive velocity in last {time_label}.\n"
                "Markets may be quiet right now."
            )
            return

        # Format top 15 with rich data
        header = f"Hottest Markets ({time_label})"
        if category:
            header += f" [{category}]"
        lines = [header, ""]

        for i, m in enumerate(hot_markets[:15], 1):
            title = m.get("title", "Unknown")[:40]
            velocity = m["velocity"]
            velocity_per_hour = m["velocity_per_hour"]
            velocity_pct = m["velocity_pct"]
            total_volume = m.get("total_volume", 0)
            yes_price = m.get("yes_price", 0)
            slug = m.get("slug", "")
            price_data = m.get("price_data", {})

            # Format velocity
            vel_str = f"+${velocity/1000:.0f}K" if velocity >= 1000 else f"+${velocity:.0f}"
            rate_str = f"${velocity_per_hour/1000:.1f}K/hr" if velocity_per_hour >= 1000 else f"${velocity_per_hour:.0f}/hr"

            vol_str = _format_volume(total_volume)

            # Format price change
            price_change = price_data.get("delta", 0)
            if price_change != 0:
                price_sign = "+" if price_change > 0 else ""
                price_str = f"{price_sign}{price_change:.0f}%"
            else:
                price_str = "flat"

            lines.append(f"{i}. {title}")
            lines.append(f"   Velocity: {vel_str} in {time_label} ({rate_str}, {velocity_pct:.1f}%/hr)")
            lines.append(f"   Volume: {vol_str} | Price: {yes_price:.0f}% ({price_str})")
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
        events = filter_resolved(events)

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


async def quiet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /quiet command - find sleeping giants.
    Big markets ($100K+) with low activity that could wake up.
    Usage: /quiet [category]
    """
    # Parse category from args
    category = None
    available_cats = get_available_categories()

    for arg in context.args:
        if arg.lower() in available_cats:
            category = arg.lower()
            break

    status_msg = "Finding sleeping giants"
    if category:
        status_msg += f" [{category}]"
    await update.message.reply_text(status_msg + "...")

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=2000, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Filter out sports and resolved markets
        events = filter_sports(events)
        events = filter_resolved(events)

        # Apply category filter if specified
        if category:
            events = filter_by_category(events, category)

        slugs = [e["slug"] for e in events]

        # Get volume deltas for 1h
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

        # Get price deltas for 24h
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

        sleeping_giants = []

        for event in events:
            slug = event["slug"]
            total_volume = event["total_volume"]
            yes_price = event["yes_price"]

            # Must be a big market ($100K+)
            if total_volume < 100_000:
                continue

            velocity = deltas_1h.get(slug, 0)

            # Low velocity (<1% of market per hour)
            velocity_pct = (velocity / total_volume * 100) if total_volume > 0 else 0
            if velocity_pct > 1:
                continue

            # Stable price (moved less than ±2% in 24h)
            price_data = price_deltas_24h.get(slug, {})
            price_change = abs(price_data.get("delta", 0))
            if price_change > 2:
                continue

            sleeping_giants.append({
                **event,
                "velocity": velocity,
                "velocity_pct": velocity_pct,
                "price_change_24h": price_data.get("delta", 0),
            })

        # Sort by volume (biggest first)
        sleeping_giants.sort(key=lambda x: x["total_volume"], reverse=True)

        if not sleeping_giants:
            cat_msg = f" in {category}" if category else ""
            await update.message.reply_text(
                f"No sleeping giants{cat_msg} found right now.\n\n"
                "Sleeping giants require:\n"
                "- Volume >= $100K\n"
                "- Velocity < 1%/hr of market\n"
                "- Price stable (±2% in 24h)"
            )
            return

        # Format response
        header = "Sleeping Giants (big + quiet)"
        if category:
            header += f" [{category}]"
        lines = [header, ""]

        for i, m in enumerate(sleeping_giants[:15], 1):
            title = m["title"][:40]
            yes_price = m["yes_price"]
            total_volume = m["total_volume"]
            velocity_pct = m["velocity_pct"]
            price_change = m["price_change_24h"]
            slug = m["slug"]

            vol_str = _format_volume(total_volume)
            price_sign = "+" if price_change > 0 else ""

            lines.append(f"{i}. {title}")
            lines.append(f"   Volume: {vol_str} | YES: {yes_price:.0f}%")
            lines.append(f"   Activity: {velocity_pct:.1f}%/hr | Price: {price_sign}{price_change:.0f}% (24h)")
            lines.append(f"   polymarket.com/event/{slug}")
            lines.append("")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in quiet: {e}")
        await update.message.reply_text(f"Error: {e}")


async def movers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /movers command - find biggest price swings.
    Markets with the largest price changes in last 24h.
    Usage: /movers [category]
    """
    # Parse category from args
    category = None
    available_cats = get_available_categories()

    for arg in context.args:
        if arg.lower() in available_cats:
            category = arg.lower()
            break

    status_msg = "Finding biggest movers"
    if category:
        status_msg += f" [{category}]"
    await update.message.reply_text(status_msg + "...")

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=2000, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # Filter out sports and resolved markets
        events = filter_sports(events)
        events = filter_resolved(events)

        # Apply category filter if specified
        if category:
            events = filter_by_category(events, category)

        slugs = [e["slug"] for e in events]

        # Get price deltas for 24h
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

        # Get velocity for context
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

        movers = []

        for event in events:
            slug = event["slug"]
            total_volume = event["total_volume"]

            # Get price change (no volume filter - let the price movement speak)
            price_data = price_deltas_24h.get(slug, {})
            price_change = price_data.get("delta", 0)
            old_price = price_data.get("old", 0)

            # Must have meaningful move (>=3%) - lowered from 5%
            if abs(price_change) < 3:
                continue

            velocity = deltas_1h.get(slug, 0)
            velocity_pct = (velocity / total_volume * 100) if total_volume > 0 else 0

            movers.append({
                **event,
                "old_price": old_price,
                "price_change": price_change,
                "velocity": velocity,
                "velocity_pct": velocity_pct,
            })

        # Sort by absolute price change (biggest swings first)
        movers.sort(key=lambda x: abs(x["price_change"]), reverse=True)

        if not movers:
            cat_msg = f" in {category}" if category else ""
            await update.message.reply_text(
                f"No movers{cat_msg} found right now.\n\n"
                "Movers require:\n"
                "- Price moved >=3% in 24h\n"
                "- May need 24h of price data to accumulate"
            )
            return

        # Split into gainers and losers
        gainers = [m for m in movers if m["price_change"] > 0][:10]
        losers = [m for m in movers if m["price_change"] < 0][:10]

        header = "Biggest Movers (24h)"
        if category:
            header += f" [{category}]"
        lines = [header, ""]

        if gainers:
            lines.append("GAINERS:")
            for m in gainers:
                title = m["title"][:35]
                yes_price = m["yes_price"]
                old_price = m["old_price"]
                price_change = m["price_change"]
                total_volume = m["total_volume"]
                velocity = m.get("velocity", 0)
                velocity_pct = m.get("velocity_pct", 0)
                slug = m["slug"]

                vol_str = _format_volume(total_volume)
                vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

                lines.append(f"- {title}")
                lines.append(f"  {old_price:.0f}% -> {yes_price:.0f}% (+{price_change:.0f}%)")
                lines.append(f"  {vol_str} | {vel_str} ({velocity_pct:.1f}%/hr)")
                lines.append(f"  polymarket.com/event/{slug}")
                lines.append("")

        if losers:
            lines.append("LOSERS:")
            for m in losers:
                title = m["title"][:35]
                yes_price = m["yes_price"]
                old_price = m["old_price"]
                price_change = m["price_change"]
                total_volume = m["total_volume"]
                velocity = m.get("velocity", 0)
                velocity_pct = m.get("velocity_pct", 0)
                slug = m["slug"]

                vol_str = _format_volume(total_volume)
                vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

                lines.append(f"- {title}")
                lines.append(f"  {old_price:.0f}% -> {yes_price:.0f}% ({price_change:.0f}%)")
                lines.append(f"  {vol_str} | {vel_str} ({velocity_pct:.1f}%/hr)")
                lines.append(f"  polymarket.com/event/{slug}")
                lines.append("")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in movers: {e}")
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
• /underdogs - contrarian plays"""

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
• /underdogs - contrarian plays"""

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
    """Handle the /digest command - consolidated 6h summary of what's happening."""
    await update.message.reply_text("Building 6h digest...")

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=500, include_spam=False)

        if not events:
            await update.message.reply_text("No market data available.")
            return

        # Filter out sports and resolved
        events = filter_sports(events)
        events = filter_resolved(events)

        slugs = [e.get("slug") for e in events if e.get("slug")]

        # Get velocity and price data for 6h window
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)
        deltas_6h = get_volume_deltas_bulk(slugs, hours=6)
        price_deltas_6h = get_price_deltas_bulk(slugs, hours=6)

        lines = ["6-Hour Digest", ""]

        # 1. HOTTEST (by velocity %) - top 5
        hot_markets = []
        for e in events:
            slug = e.get("slug")
            total_volume = e.get("total_volume", 0)
            velocity = deltas_1h.get(slug, 0)
            if velocity > 0 and total_volume > 0:
                velocity_pct = velocity / total_volume * 100
                hot_markets.append({**e, "velocity": velocity, "velocity_pct": velocity_pct})
        hot_markets.sort(key=lambda x: x["velocity_pct"], reverse=True)

        if hot_markets[:5]:
            lines.append("🔥 HOTTEST (velocity %/hr):")
            for m in hot_markets[:5]:
                title = m.get("title", "Unknown")[:35]
                vel_pct = m["velocity_pct"]
                velocity = m["velocity"]
                vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
                emoji = "🔥🔥" if vel_pct >= 20 else ("🔥" if vel_pct >= 10 else "")
                lines.append(f"• {title}")
                lines.append(f"  {vel_str} ({vel_pct:.1f}%/hr) {emoji}")
            lines.append("")

        # 2. BIGGEST MOVERS (price change 6h) - top 5 each direction
        movers = []
        for e in events:
            slug = e.get("slug")
            price_data = price_deltas_6h.get(slug, {})
            price_change = price_data.get("delta", 0)
            if abs(price_change) >= 3:
                movers.append({**e, "price_change": price_change, "old_price": price_data.get("old", 0)})
        movers.sort(key=lambda x: abs(x["price_change"]), reverse=True)

        gainers = [m for m in movers if m["price_change"] > 0][:3]
        losers = [m for m in movers if m["price_change"] < 0][:3]

        if gainers or losers:
            lines.append("📊 MOVERS (6h):")
            if gainers:
                for m in gainers:
                    title = m.get("title", "Unknown")[:30]
                    emoji = "🚀" if m["price_change"] >= 15 else "⬆️"
                    lines.append(f"• {emoji} {title} (+{m['price_change']:.0f}%)")
            if losers:
                for m in losers:
                    title = m.get("title", "Unknown")[:30]
                    emoji = "💀" if m["price_change"] <= -15 else "⬇️"
                    lines.append(f"• {emoji} {title} ({m['price_change']:.0f}%)")
            lines.append("")

        # 3. VOLUME SURGE (biggest 6h volume growth %)
        volume_surge = []
        for e in events:
            slug = e.get("slug")
            total_volume = e.get("total_volume", 0)
            delta_6h = deltas_6h.get(slug, 0)
            if delta_6h > 0 and total_volume > 10000:  # Min $10K
                volume_6h_ago = total_volume - delta_6h
                if volume_6h_ago > 0:
                    growth_pct = delta_6h / volume_6h_ago * 100
                    if growth_pct >= 10:  # At least 10% growth
                        volume_surge.append({**e, "delta_6h": delta_6h, "growth_pct": growth_pct})
        volume_surge.sort(key=lambda x: x["growth_pct"], reverse=True)

        if volume_surge[:5]:
            lines.append("💰 VOLUME SURGE (6h growth %):")
            for m in volume_surge[:5]:
                title = m.get("title", "Unknown")[:30]
                delta = m["delta_6h"]
                growth = m["growth_pct"]
                delta_str = f"+${delta/1000:.0f}K" if delta >= 1000 else f"+${delta:.0f}"
                lines.append(f"• {title}")
                lines.append(f"  {delta_str} (+{growth:.0f}%)")
            lines.append("")

        lines.append("Use /hot, /movers, /discover for full lists")

        await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)
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
    application.add_handler(CommandHandler("discover", discover_command))
    application.add_handler(CommandHandler("underdogs", underdogs_command))
    application.add_handler(CommandHandler("hot", hot_command))
    application.add_handler(CommandHandler("new", new_command))
    application.add_handler(CommandHandler("quiet", quiet_command))
    application.add_handler(CommandHandler("movers", movers_command))
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
