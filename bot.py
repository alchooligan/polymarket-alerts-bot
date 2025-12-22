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
from alerts import (
    check_underdog_alerts,
    format_bundled_underdogs,
    filter_sports,
    filter_resolved,
    filter_by_category,
    get_available_categories,
    _format_volume,
    _format_odds,
    _escape_markdown,
    format_market_card,
)

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Pagination cache for /new command: {user_id: {markets: [], page: 0, time_label: str, ...}}
from datetime import datetime, timezone
from collections import defaultdict

pagination_cache = defaultdict(dict)
ITEMS_PER_PAGE = 10


def format_new_page(markets: list, page: int, time_label: str, total_eligible: int,
                    sports_filtered: int, resolved_filtered: int) -> tuple[str, InlineKeyboardMarkup]:
    """Format a single page of /new results with navigation buttons."""
    start_idx = page * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_markets = markets[start_idx:end_idx]
    total_pages = (len(markets) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

    header = f"""🆕 New Markets (last {time_label}) — Page {page + 1}/{total_pages}

Sorted by volume — money flowing to fresh markets.
"""
    lines = [header]

    for i, m in enumerate(page_markets, start_idx + 1):
        title = _escape_markdown(m.get("title", "Unknown")[:50])
        total_volume = m.get("total_volume", 0)
        slug = m.get("slug", "")
        hours_ago = m.get("hours_ago", 0)
        velocity = m.get("velocity", 0)
        velocity_pct = m.get("velocity_pct", 0)

        vol_str = _format_volume(total_volume)
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

        # Velocity emoji
        vel_emoji = ""
        if velocity_pct >= 20:
            vel_emoji = " 🔥🔥"
        elif velocity_pct >= 10:
            vel_emoji = " 🔥"

        # Time ago string
        if hours_ago < 1:
            time_ago = f"{hours_ago*60:.0f}m ago"
        else:
            time_ago = f"{hours_ago:.0f}h ago"

        # Format odds
        odds_str = _format_odds(m)

        lines.append(f"━━━ {i} ━━━")
        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Launched: {time_ago}")
        lines.append(f"Volume: {vol_str} | Vel: {vel_str}{vel_emoji}")
        lines.append(f"Odds: {odds_str}")
        lines.append("")

    # Footer with stats
    lines.append(f"{len(markets)} new in {time_label} (of {total_eligible} eligible)")
    lines.append(f"_Filtered: {sports_filtered} sports, {resolved_filtered} resolved_")

    # Navigation buttons
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("◀ Prev", callback_data=f"new_page_{page - 1}"))
    if end_idx < len(markets):
        buttons.append(InlineKeyboardButton("Next ▶", callback_data=f"new_page_{page + 1}"))

    keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

    return "\n".join(lines).strip(), keyboard


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    # Register user in database
    user = update.effective_user
    get_or_create_user(user.id, user.username)

    welcome_message = """👋 Welcome to PolySniffer

I find Polymarket alpha before it's obvious.

━━━ PRIMARY COMMANDS ━━━

/discover — Markets waking up (momentum)
/hot — Where money flows (velocity %/hr)
/movers — Biggest price moves
/new — Recently launched markets

Time + count: /hot 6h 20, /movers 1h 15
Category: /discover crypto, /hot politics

━━━ AUTOMATIC ALERTS ━━━

⚡ *Wakeup* — Market was quiet, now hot
📈 *Fast Mover* — 10%+ price move with volume
🌱 *Early Heat* — New market gaining traction
🆕 *New Launch* — Brand new markets
📊 *Watchlist* — Your tracked markets move

/settings — Toggle alerts
/watch [market] — Track a market

━━━━━━━━━━━━━━━━━━━━━━━━

Type /how for the full guide

Sports + near-resolved markets filtered."""

    await update.message.reply_text(welcome_message)


async def how_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /how command - comprehensive guide."""
    msg = """📖 HOW POLYSNIFFER WORKS

━━━━━━━━━━━━━━━━━━━━━━━━

🎯 PRIMARY COMMANDS

/discover [time] [category] [count]
Markets gaining momentum. Sorted by velocity + volume growth + price change.
→ /discover crypto, /discover 6h 20

/hot [time] [category] [count]
Where money flows NOW. Sorted by velocity %/hr.
→ /hot 1h, /hot 6h crypto 30

/movers [time] [category] [count]
Biggest price changes. Opinion shifted = something happened.
→ /movers 1h, /movers 6h 20

/new [time] [count]
Newly launched markets sorted by volume.
→ /new 6h, /new 24h 25

/digest
Consolidated summary — less spam, same signal.

━━━━━━━━━━━━━━━━━━━━━━━━

🔔 AUTOMATIC ALERTS (V2)

⚡ Wakeup
→ Market was quiet (<2%/hr), now hot (>10%/hr)
→ Catches breaking news

📈 Fast Mover
→ Price moved 10%+ in 2h with $10K+ volume behind
→ Informed money moving prices

🌱 Early Heat
→ New market (<24h) with >15%/hr velocity
→ Catches markets launching with traction

🆕 New Launch
→ Brand new markets (<1h old)
→ Be first to see new opportunities

📊 Watchlist
→ Your tracked markets move 5%+
→ /watch <slug> to add

━━━━━━━━━━━━━━━━━━━━━━━━

🏷️ FILTERS

Time: 1h, 6h, 24h
Category: crypto, politics, tech, econ, world
Count: 10-50 (default 10)

Examples: /hot 1h crypto 20, /movers 6h 30

━━━━━━━━━━━━━━━━━━━━━━━━

📊 READING THE DATA

Velocity %/hr = money flow / market size
→ 10%/hr = market could double in 10h
→ 1%/hr = slow, steady accumulation

🔥 = >10%/hr (heating up)
🔥🔥 = >20%/hr (on fire)
🚀 = price +15%
💀 = price -15%

━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ SETTINGS

/settings — Toggle alert types
/watch <slug> — Add to watchlist
/watchlist — See tracked markets
/top — Volume leaders ($ added)

━━━━━━━━━━━━━━━━━━━━━━━━

📦 LEGACY COMMANDS

/quiet — Sleeping giants (low activity)
/underdogs — Long shots rising

━━━━━━━━━━━━━━━━━━━━━━━━

💡 TIPS

• /discover for daily alpha hunting
• /hot 1h when news breaks
• /movers 6h to see what moved
• /new 24h for fresh opportunities

Sports + resolved (95%+) markets filtered."""

    await update.message.reply_text(msg)


async def discover_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /discover command - show markets that are waking up with rich data.
    Usage: /discover [category] [count]
    Categories: crypto, politics, tech, econ, entertainment, world
    """
    # Parse category and count from args
    category = None
    count = 8  # Default results
    available_cats = get_available_categories()

    for arg in context.args:
        if arg.lower() in available_cats:
            category = arg.lower()
        elif arg.isdigit():
            count = min(int(arg), 50)  # Cap at 50

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

        # Enrich markets with velocity data for format_market_card
        for m in markets_with_delta:
            m["velocity"] = m.get("delta_1h", 0)
            price_24h = m.get("price_24h", {})
            m["price_change_24h"] = price_24h.get("delta", 0)

        # Format header with explanation
        cat_label = f" [{category}]" if category else ""
        header = f"""🔍 Discover{cat_label}

Small markets waking up, sorted by momentum score.
Momentum = velocity % + volume growth % + price change.
"""

        # Format using market cards
        lines = [header]
        for i, m in enumerate(markets_with_delta[:count], 1):
            lines.append(f"━━━ {i} ━━━")
            lines.append(format_market_card(m, style="full"))
            lines.append("")

        if len(markets_with_delta) > count:
            lines.append(f"+{len(markets_with_delta) - count} more. Use /hot for velocity focus.")

        response = "\n".join(lines).strip()
        await update.message.reply_text(response, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in discover: {e}")
        await update.message.reply_text(f"Error: {e}")


async def underdogs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /underdogs command - show contrarian plays.
    Markets with YES <20% where price is actually rising (someone betting against consensus).
    Usage: /underdogs [count]
    """
    # Parse count from args
    count = 8  # Default results
    for arg in context.args:
        if arg.isdigit():
            count = min(int(arg), 50)
            break

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

        # Enrich with data for format_market_card
        for m in underdogs:
            m["velocity"] = m.get("velocity", 0)
            m["price_change_24h"] = m.get("price_change", 0)
            m["velocity_pct"] = (m["velocity"] / m.get("total_volume", 1) * 100) if m.get("total_volume", 0) > 0 else 0

        header = """🎯 Underdogs

Long shots (YES <20%) with rising prices.
Contrarian money moving the needle.
"""
        lines = [header]

        for i, m in enumerate(underdogs[:count], 1):
            price_change = m.get("price_change", 0)
            old_price = m.get("old_price", 0)
            ctx = f"⬆️ Price: {old_price:.0f}% → {m['yes_price']:.0f}% (+{price_change:.0f}% in 24h)"
            lines.append(f"━━━ {i} ━━━")
            lines.append(format_market_card(m, style="full", context=ctx))
            lines.append("")

        if len(underdogs) > count:
            lines.append(f"+{len(underdogs) - count} more underdogs rising.")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in underdogs: {e}")
        await update.message.reply_text(f"Error: {e}")


async def hot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /hot command - show markets by velocity with rich data.
    Usage: /hot [1h|6h|24h] [category] [count] - defaults to 1h, no filter, 10 results
    Categories: crypto, politics, tech, econ, entertainment, world
    """
    # Parse time window, category, and count from args
    hours = 1
    time_label = "1h"
    category = None
    count = 10  # Default results

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
        elif arg.isdigit():
            count = min(int(arg), 50)  # Cap at 50

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

        # Always get 6h volume deltas for card display
        deltas_6h = get_volume_deltas_bulk(slugs, hours=6) if hours != 6 else deltas

        # Get price deltas for the same window
        price_deltas = get_price_deltas_bulk(slugs, hours=hours)

        # Always get 6h/24h price deltas for card display
        price_deltas_6h = get_price_deltas_bulk(slugs, hours=6) if hours != 6 else price_deltas
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24) if hours != 24 else price_deltas

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

        # Enrich with velocity data for format_market_card
        for m in hot_markets:
            slug = m.get("slug")
            total_volume = m.get("total_volume", 0)
            m["velocity"] = m.get("velocity_per_hour", 0)

            # 6h volume data
            delta_6h = deltas_6h.get(slug, 0)
            m["delta_6h"] = delta_6h
            vol_6h_ago = total_volume - delta_6h
            m["volume_growth_pct"] = (delta_6h / vol_6h_ago * 100) if vol_6h_ago > 0 else 0

            # Price data for 6h and 24h
            price_data_6h = price_deltas_6h.get(slug, {})
            price_data_24h = price_deltas_24h.get(slug, {})
            m["price_change_6h"] = price_data_6h.get("delta", 0)
            m["price_change_24h"] = price_data_24h.get("delta", 0)

        # Format header with explanation
        cat_label = f" [{category}]" if category else ""
        header = f"""🔥 Hot Markets ({time_label}){cat_label}

Where money is flowing NOW.
Sorted by velocity %/hr — fast movers, not just big markets.
"""

        # Format using market cards
        lines = [header]
        for i, m in enumerate(hot_markets[:count], 1):
            lines.append(f"━━━ {i} ━━━")
            lines.append(format_market_card(m, style="full"))
            lines.append("")

        if len(hot_markets) > count:
            lines.append(f"+{len(hot_markets) - count} more with positive velocity.")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in hot: {e}")
        await update.message.reply_text(f"Error: {e}")


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /top command - show markets by absolute volume added.
    Usage: /top [1h|6h|24h] [category] [count] - defaults to 1h
    Unlike /hot (velocity %), this shows raw dollars flowing in.
    """
    hours = 1
    time_label = "1h"
    category = None
    count = 10  # Default results

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
        elif arg.isdigit():
            count = min(int(arg), 50)  # Cap at 50

    status_msg = f"Finding top volume gainers ({time_label})"
    if category:
        status_msg += f" [{category}]"
    await update.message.reply_text(status_msg + "...")

    try:
        events = await get_all_markets_paginated(target_count=2000, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        events = filter_sports(events)
        events = filter_resolved(events)

        if category:
            events = filter_by_category(events, category)

        slugs = [e.get("slug") for e in events if e.get("slug")]
        deltas = get_volume_deltas_bulk(slugs, hours=hours)
        deltas_6h = get_volume_deltas_bulk(slugs, hours=6) if hours != 6 else deltas
        price_deltas = get_price_deltas_bulk(slugs, hours=hours)
        price_deltas_6h = get_price_deltas_bulk(slugs, hours=6) if hours != 6 else price_deltas
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24) if hours != 24 else price_deltas

        if not deltas:
            await update.message.reply_text(
                f"No volume data for {time_label} window yet.\n"
                "Need more snapshots to accumulate. Try /checknow."
            )
            return

        top_markets = []
        for event in events:
            slug = event.get("slug")
            if slug in deltas and deltas[slug] > 0:
                volume_added = deltas[slug]
                total_volume = event.get("total_volume", 0)
                velocity_pct = (volume_added / total_volume * 100 / hours) if total_volume > 0 else 0

                top_markets.append({
                    **event,
                    "volume_added": volume_added,
                    "velocity": volume_added / hours if hours > 1 else volume_added,
                    "velocity_pct": velocity_pct,
                    "price_data": price_deltas.get(slug, {}),
                })

        # Sort by ABSOLUTE volume added (not %)
        top_markets.sort(key=lambda x: x["volume_added"], reverse=True)

        if not top_markets:
            cat_msg = f" in {category}" if category else ""
            await update.message.reply_text(
                f"No markets{cat_msg} with volume growth in last {time_label}."
            )
            return

        # Enrich for format_market_card
        for m in top_markets:
            slug = m.get("slug")
            total_volume = m.get("total_volume", 0)

            # 6h volume data
            delta_6h = deltas_6h.get(slug, 0)
            m["delta_6h"] = delta_6h
            vol_6h_ago = total_volume - delta_6h
            m["volume_growth_pct"] = (delta_6h / vol_6h_ago * 100) if vol_6h_ago > 0 else 0

            # Price data
            price_data_6h = price_deltas_6h.get(slug, {})
            price_data_24h = price_deltas_24h.get(slug, {})
            m["price_change_6h"] = price_data_6h.get("delta", 0)
            m["price_change_24h"] = price_data_24h.get("delta", 0)

        cat_label = f" [{category}]" if category else ""
        header = f"""💰 Top Volume ({time_label}){cat_label}

Most dollars flowing in — big money markets.
Sorted by absolute $ added, not velocity %.
"""

        lines = [header]
        for i, m in enumerate(top_markets[:count], 1):
            vol_added = m["volume_added"]
            vol_str = f"+${vol_added/1000:.0f}K" if vol_added >= 1000 else f"+${vol_added:.0f}"
            lines.append(f"━━━ {i}. {vol_str} ━━━")
            lines.append(format_market_card(m, style="full"))
            lines.append("")

        if len(top_markets) > count:
            lines.append(f"+{len(top_markets) - count} more with volume growth.")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in top: {e}")
        await update.message.reply_text(f"Error: {e}")


async def new_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /new command - show newly launched markets.
    Usage: /new [1h|6h|12h|24h] [count] - defaults to 24h, 15 results
    Sorted by volume - money flowing to fresh markets.
    """
    from datetime import datetime, timezone

    # Parse time window and count from args
    hours = 24
    time_label = "24h"
    count = 15  # Default results

    for arg in context.args:
        arg_lower = arg.lower()
        if arg_lower in ["1h", "1"]:
            hours = 1
            time_label = "1h"
        elif arg_lower in ["6h", "6"]:
            hours = 6
            time_label = "6h"
        elif arg_lower in ["12h", "12"]:
            hours = 12
            time_label = "12h"
        elif arg_lower in ["24h", "24"]:
            hours = 24
            time_label = "24h"
        elif arg_lower in ["48h", "48"]:
            hours = 48
            time_label = "48h"
        elif arg_lower in ["7d", "168h", "168"]:
            hours = 168
            time_label = "7d"
        elif arg.isdigit():
            count = min(int(arg), 50)

    await update.message.reply_text(f"Finding new markets (last {time_label})...")

    try:
        # Get ALL markets from API (Polymarket has ~2000-3000 active)
        all_events = await get_all_markets_paginated(target_count=10000, include_spam=False)
        total_fetched = len(all_events)

        events_no_sports = filter_sports(all_events)
        sports_filtered = total_fetched - len(events_no_sports)

        events = filter_resolved(events_no_sports)
        resolved_filtered = len(events_no_sports) - len(events)

        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - (hours * 3600)

        # Get velocity data
        slugs = [e.get("slug") for e in events if e.get("slug")]
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

        # Filter to recently created markets using API created_at field
        new_markets = []

        for event in events:
            created_at = event.get("created_at", "")
            if not created_at:
                continue

            # Parse creation time
            try:
                created_str = created_at
                if isinstance(created_str, str):
                    # Handle various date formats
                    created_str = created_str.replace("Z", "+00:00")

                    # Try ISO format first
                    if "T" in created_str:
                        created_dt = datetime.fromisoformat(created_str)
                    else:
                        # Try common formats
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f"]:
                            try:
                                created_dt = datetime.strptime(created_str.split("+")[0].split(".")[0], fmt)
                                created_dt = created_dt.replace(tzinfo=timezone.utc)
                                break
                            except ValueError:
                                continue
                        else:
                            continue  # No format matched
                else:
                    created_dt = created_str.replace(tzinfo=timezone.utc) if created_str.tzinfo is None else created_str

                # Check if within time window
                if created_dt.timestamp() < cutoff:
                    continue

                hours_ago = (now - created_dt).total_seconds() / 3600
            except Exception as e:
                logger.debug(f"Date parse error for {event.get('slug')}: {e}")
                continue

            total_volume = event.get("total_volume", 0)
            slug = event.get("slug", "")

            # Get velocity
            velocity = deltas_1h.get(slug, 0)
            velocity_pct = (velocity / total_volume * 100) if total_volume > 0 else 0

            new_markets.append({
                "slug": slug,
                "title": event.get("title", "Unknown"),
                "total_volume": total_volume,
                "yes_price": event.get("yes_price", 0),
                "outcomes": event.get("outcomes", []),
                "hours_ago": hours_ago,
                "velocity": velocity,
                "velocity_pct": velocity_pct,
            })

        if not new_markets:
            # Debug: count events with/without created_at
            with_date = sum(1 for e in events if e.get("created_at"))
            without_date = len(events) - with_date

            logger.warning(f"/new debug: {len(events)} events total, {with_date} with created_at, {without_date} without")

            if with_date == 0:
                await update.message.reply_text(
                    f"No creation dates available from API.\n"
                    f"Scanned {len(events)} markets.\n\n"
                    f"Try /discover or /hot instead."
                )
            else:
                await update.message.reply_text(
                    f"No markets created in the last {time_label}.\n"
                    f"({with_date} markets have dates, all older than {time_label})\n\n"
                    f"Try /new 48h or /new 168h (7 days)"
                )
            return

        # Sort by volume (highest first)
        new_markets.sort(key=lambda x: x["total_volume"], reverse=True)

        # Store in pagination cache for this user
        user_id = update.effective_user.id
        pagination_cache[user_id] = {
            "markets": new_markets,
            "time_label": time_label,
            "total_eligible": len(events),
            "sports_filtered": sports_filtered,
            "resolved_filtered": resolved_filtered,
        }

        # Format first page with navigation buttons
        message, keyboard = format_new_page(
            new_markets, 0, time_label, len(events), sports_filtered, resolved_filtered
        )
        await update.message.reply_text(
            message, parse_mode="Markdown", disable_web_page_preview=True, reply_markup=keyboard
        )

    except Exception as e:
        logger.error(f"Error in new: {e}")
        await update.message.reply_text(f"Error: {e}")


async def quiet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /quiet command - find sleeping giants.
    Big markets ($100K+) with low activity that could wake up.
    Usage: /quiet [category] [count]
    """
    # Parse category and count from args
    category = None
    count = 8  # Default results
    available_cats = get_available_categories()

    for arg in context.args:
        if arg.lower() in available_cats:
            category = arg.lower()
        elif arg.isdigit():
            count = min(int(arg), 50)  # Cap at 50

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

        # Enrich with data for format_market_card
        for m in sleeping_giants:
            m["velocity"] = m.get("velocity", 0)
            m["price_change_24h"] = m.get("price_change_24h", 0)

        cat_label = f" [{category}]" if category else ""
        header = f"""💤 Sleeping Giants{cat_label}

Big markets ($100K+) with low activity.
Could wake up anytime with the right catalyst.
"""
        lines = [header]

        for i, m in enumerate(sleeping_giants[:count], 1):
            ctx = f"💤 Activity: {m['velocity_pct']:.2f}%/hr — sleeping"
            lines.append(f"━━━ {i} ━━━")
            lines.append(format_market_card(m, style="full", context=ctx))
            lines.append("")

        if len(sleeping_giants) > count:
            lines.append(f"+{len(sleeping_giants) - count} more sleeping giants.")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in quiet: {e}")
        await update.message.reply_text(f"Error: {e}")


async def movers_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /movers command - find biggest price swings.
    Usage: /movers [1h|6h|24h] [category] [count]
    Categories: crypto, politics, tech, econ, entertainment, world
    """
    # Parse time window, category, and count from args
    hours = 24
    time_label = "24h"
    category = None
    count = 10  # Default results

    available_cats = get_available_categories()

    for arg in context.args:
        arg_lower = arg.lower()
        if arg_lower in ["1h", "1"]:
            hours = 1
            time_label = "1h"
        elif arg_lower in ["6h", "6"]:
            hours = 6
            time_label = "6h"
        elif arg_lower in ["24h", "24"]:
            hours = 24
            time_label = "24h"
        elif arg_lower in available_cats:
            category = arg_lower
        elif arg.isdigit():
            count = min(int(arg), 50)  # Cap at 50

    # Adjust threshold based on time window (shorter = smaller moves matter)
    threshold_map = {1: 1, 6: 2, 24: 3}
    threshold = threshold_map.get(hours, 3)

    status_msg = f"Finding biggest movers ({time_label})"
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

        # Get price deltas for specified time window
        price_deltas = get_price_deltas_bulk(slugs, hours=hours)

        # Get velocity for context
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

        movers = []

        for event in events:
            slug = event["slug"]
            total_volume = event["total_volume"]

            # Get price change for the specified window
            price_data = price_deltas.get(slug, {})
            price_change = price_data.get("delta", 0)
            old_price = price_data.get("old", 0)

            # Must have meaningful move (threshold scales with time window)
            if abs(price_change) < threshold:
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

            # Check if we have price data at all
            has_price_data = len(price_deltas) > 0

            if not has_price_data:
                await update.message.reply_text(
                    f"No price history data for {time_label} window yet.\n\n"
                    f"The bot needs to run for {hours}h to compare prices.\n"
                    f"Run /checknow to trigger a scan and build history.\n"
                    f"Try /hot instead - it works immediately."
                )
            else:
                await update.message.reply_text(
                    f"No significant movers{cat_msg} in last {time_label}.\n\n"
                    f"Threshold: >={threshold}% price change.\n"
                    f"Try: /movers 6h or /movers 24h"
                )
            return

        # Enrich with data for format_market_card
        for m in movers:
            m["velocity"] = m.get("velocity", 0)
            m["price_change_24h"] = m.get("price_change", 0)

        # Split into gainers and losers (half of count each)
        half_count = max(count // 2, 3)
        gainers = [m for m in movers if m["price_change"] > 0][:half_count]
        losers = [m for m in movers if m["price_change"] < 0][:half_count]

        cat_label = f" [{category}]" if category else ""
        header = f"""📊 Biggest Movers ({time_label}){cat_label}

Prices changed = opinions shifted.
Something happened in these markets.
"""
        lines = [header]

        if gainers:
            lines.append("⬆️ GAINERS:")
            lines.append("")
            for i, m in enumerate(gainers, 1):
                emoji = "🚀" if m["price_change"] >= 15 else "⬆️"
                context = f"{emoji} Price: {m['old_price']:.0f}% → {m['yes_price']:.0f}% (+{m['price_change']:.0f}%)"
                lines.append(format_market_card(m, style="full", context=context))
                lines.append("")

        if losers:
            lines.append("⬇️ LOSERS:")
            lines.append("")
            for i, m in enumerate(losers, 1):
                emoji = "💀" if m["price_change"] <= -15 else "⬇️"
                context = f"{emoji} Price: {m['old_price']:.0f}% → {m['yes_price']:.0f}% ({m['price_change']:.0f}%)"
                lines.append(format_market_card(m, style="full", context=context))
                lines.append("")

        message = "\n".join(lines).strip()
        await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error in movers: {e}")
        await update.message.reply_text(f"Error: {e}")


def build_settings_keyboard(user: dict) -> InlineKeyboardMarkup:
    """Build the settings inline keyboard based on user preferences."""
    alerts_enabled = user.get("new_markets_enabled", False)
    alerts_status = "ON" if alerts_enabled else "OFF"

    whale_enabled = user.get("whale_alerts_enabled", True)
    whale_status = "ON" if whale_enabled else "OFF"

    keyboard = [
        [InlineKeyboardButton(
            f"Push Alerts: {alerts_status}",
            callback_data="toggle_new_markets"
        )],
        [InlineKeyboardButton(
            f"Whale Alerts ($50K+): {whale_status}",
            callback_data="toggle_whale_alerts"
        )],
    ]
    return InlineKeyboardMarkup(keyboard)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /settings command - show settings with inline buttons."""
    telegram_user = update.effective_user
    user = get_or_create_user(telegram_user.id, telegram_user.username)

    text = """*Alert Settings*

*Push Alerts* include:
• Wakeup (market was quiet, now hot)
• Fast Mover (price moved with volume)
• Early Heat (new market gaining traction)
• Watchlist price moves (5%+)

*Whale Alerts* ($50K+ trades):
• Large trade alerts (follow the money)
• Mega whale alerts ($100K+)

On-demand commands (no toggle needed):
• /hot, /movers, /new, /top"""

    keyboard = build_settings_keyboard(user)
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline button presses (settings, pagination, etc.)."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press

    telegram_user = update.effective_user
    callback_data = query.data

    # Handle pagination for /new command
    if callback_data.startswith("new_page_"):
        page = int(callback_data.replace("new_page_", ""))
        user_id = telegram_user.id

        # Get cached data
        cache = pagination_cache.get(user_id)
        if not cache:
            await query.edit_message_text("Session expired. Please run /new again.")
            return

        message, keyboard = format_new_page(
            cache["markets"], page, cache["time_label"],
            cache["total_eligible"], cache["sports_filtered"], cache["resolved_filtered"]
        )
        await query.edit_message_text(
            message, parse_mode="Markdown", disable_web_page_preview=True, reply_markup=keyboard
        )
        return

    # Toggle alerts (settings)
    if callback_data == "toggle_new_markets":
        new_value = toggle_user_setting(telegram_user.id, "new_markets_enabled")
        status = "ON" if new_value else "OFF"
        logger.info(f"User {telegram_user.id} toggled alerts to {status}")
    elif callback_data == "toggle_whale_alerts":
        new_value = toggle_user_setting(telegram_user.id, "whale_alerts_enabled")
        status = "ON" if new_value else "OFF"
        logger.info(f"User {telegram_user.id} toggled whale alerts to {status}")

    # Refresh the keyboard with updated settings
    user = get_or_create_user(telegram_user.id, telegram_user.username)
    keyboard = build_settings_keyboard(user)

    text = """*Alert Settings*

*Push Alerts* include:
• Wakeup (market was quiet, now hot)
• Fast Mover (price moved with volume)
• Early Heat (new market gaining traction)
• Watchlist price moves (5%+)

*Whale Alerts* ($50K+ trades):
• Large trade alerts (follow the money)
• Mega whale alerts ($100K+)

On-demand commands (no toggle needed):
• /hot, /movers, /new, /top"""

    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")


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
    """
    Handle the /seed command - create historical snapshots for immediate testing.
    This creates fake historical data so /hot, /movers, etc. work right after deploy.
    ONLY works on empty/new databases to prevent corrupting real data.
    """
    from alerts import seed_volume_baselines
    from database import set_system_flag, get_connection
    from datetime import datetime, timezone, timedelta

    # Safety check - don't seed if we already have real data
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM volume_snapshots')
    vol_count = cursor.fetchone()[0]

    if vol_count > 1000:
        await update.message.reply_text(
            f"Database already has {vol_count:,} real snapshots.\n"
            "Seeding would corrupt real data - aborting.\n\n"
            "Use /dbstatus to check database state."
        )
        return

    await update.message.reply_text("Seeding database with historical snapshots...")

    try:
        # 1. Seed volume baselines (existing behavior)
        set_system_flag("volume_baselines_seeded", None)
        stats = await seed_volume_baselines(target_count=500)

        # 2. Fetch current markets
        events = await get_all_markets_paginated(target_count=500, include_spam=False)

        if not events:
            await update.message.reply_text("No markets found. Try again later.")
            return

        # 3. Create historical snapshots at multiple time points
        conn = get_connection()
        cursor = conn.cursor()
        now = datetime.now(timezone.utc)

        # Time points: 1h, 6h, 12h, 24h ago
        time_points = [
            (1, 0.98),   # 1h ago, 98% of current volume (simulates small growth)
            (6, 0.95),   # 6h ago, 95% of current
            (12, 0.92),  # 12h ago, 92% of current
            (24, 0.88),  # 24h ago, 88% of current
        ]

        volume_count = 0
        price_count = 0

        for event in events:
            slug = event.get("slug")
            if not slug:
                continue

            current_volume = event.get("total_volume", 0)
            current_price = event.get("yes_price", 50)

            for hours_ago, volume_factor in time_points:
                ts = (now - timedelta(hours=hours_ago)).strftime('%Y-%m-%d %H:%M:%S')

                # Historical volume (slightly lower than current)
                old_volume = current_volume * volume_factor
                cursor.execute(
                    'INSERT OR REPLACE INTO volume_snapshots (event_slug, volume, recorded_at) VALUES (?, ?, ?)',
                    (slug, old_volume, ts)
                )
                volume_count += 1

                # Historical price (add small random variance)
                import random
                price_variance = random.uniform(-5, 5)
                old_price = max(1, min(99, current_price + price_variance))
                cursor.execute(
                    'INSERT OR REPLACE INTO price_snapshots (event_slug, yes_price, recorded_at) VALUES (?, ?, ?)',
                    (slug, old_price, ts)
                )
                price_count += 1

        # Also save current snapshots
        for event in events:
            slug = event.get("slug")
            if not slug:
                continue
            ts_now = now.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                'INSERT OR REPLACE INTO volume_snapshots (event_slug, volume, recorded_at) VALUES (?, ?, ?)',
                (slug, event.get("total_volume", 0), ts_now)
            )
            cursor.execute(
                'INSERT OR REPLACE INTO price_snapshots (event_slug, yes_price, recorded_at) VALUES (?, ?, ?)',
                (slug, event.get("yes_price", 50), ts_now)
            )

        conn.commit()

        response = f"""Seeding complete!

Markets: {len(events)}
Volume snapshots: {volume_count + len(events)}
Price snapshots: {price_count + len(events)}
Baselines: {stats.get('baselines_recorded', 0)}

All commands now work immediately:
• /hot - velocity data ready
• /movers - price history ready
• /discover - momentum data ready

Run /checknow to trigger alerts."""

        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Seeding error: {e}")
        await update.message.reply_text(f"Seeding error: {e}")


async def unseed_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /unseed command - remove all snapshot data and start fresh.
    Use this to clean out fake/seed data and rebuild with real data only.
    """
    from database import get_connection

    # Check for confirmation argument
    args = context.args
    if not args or args[0].lower() != "confirm":
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM volume_snapshots')
        vol_count = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM price_snapshots')
        price_count = cursor.fetchone()[0]

        await update.message.reply_text(
            f"This will DELETE all snapshot data:\n"
            f"• {vol_count:,} volume snapshots\n"
            f"• {price_count:,} price snapshots\n\n"
            f"Commands like /hot and /movers will show no data until\n"
            f"the scheduler rebuilds history (1-6 hours).\n\n"
            f"To confirm, run: /unseed confirm"
        )
        return

    await update.message.reply_text("Clearing all snapshot data...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Delete all snapshots
        cursor.execute('DELETE FROM volume_snapshots')
        vol_deleted = cursor.rowcount
        cursor.execute('DELETE FROM price_snapshots')
        price_deleted = cursor.rowcount
        conn.commit()

        # Run one checknow to start fresh data collection
        await update.message.reply_text(
            f"Cleared {vol_deleted:,} volume + {price_deleted:,} price snapshots.\n\n"
            f"Running /checknow to start fresh data collection..."
        )

        # Trigger a fresh data collection
        from scheduler import run_manual_cycle
        stats = await run_manual_cycle(context.application)

        await update.message.reply_text(
            f"Fresh start complete!\n\n"
            f"Collected {stats['markets_scanned']} markets.\n"
            f"Scheduler will build history every 5 min.\n\n"
            f"• /hot works after ~1 hour\n"
            f"• /movers works after ~6 hours\n\n"
            f"Use /dbstatus to check progress."
        )
    except Exception as e:
        logger.error(f"Unseed error: {e}")
        await update.message.reply_text(f"Error: {e}")


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


async def dbstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /dbstatus command - show database statistics."""
    from database import get_connection, get_volume_snapshot_count
    from config import DATABASE_PATH

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Count snapshots
        cursor.execute('SELECT COUNT(*) FROM volume_snapshots')
        vol_count = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM price_snapshots')
        price_count = cursor.fetchone()[0]

        # Get oldest/newest snapshots
        cursor.execute('SELECT MIN(recorded_at), MAX(recorded_at) FROM volume_snapshots')
        vol_range = cursor.fetchone()

        cursor.execute('SELECT MIN(recorded_at), MAX(recorded_at) FROM price_snapshots')
        price_range = cursor.fetchone()

        # Count other tables
        cursor.execute('SELECT COUNT(*) FROM users')
        user_count = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM volume_milestones')
        milestone_count = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM watchlist')
        watch_count = cursor.fetchone()[0]

        response = f"""Database Status

Path: {DATABASE_PATH}

Snapshots:
• Volume: {vol_count:,} rows
  {vol_range[0] or 'N/A'} to {vol_range[1] or 'N/A'}
• Price: {price_count:,} rows
  {price_range[0] or 'N/A'} to {price_range[1] or 'N/A'}

Other:
• Users: {user_count}
• Milestones: {milestone_count}
• Watchlist: {watch_count}

Commands ready:
• /hot: {'YES' if vol_count >= 100 else 'NO (need more snapshots)'}
• /movers: {'YES' if price_count >= 100 else 'NO (need more snapshots)'}

Run /seed to populate test data."""

        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Error in dbstatus: {e}")
        await update.message.reply_text(f"Error: {e}")


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
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("new", new_command))
    application.add_handler(CommandHandler("quiet", quiet_command))
    application.add_handler(CommandHandler("movers", movers_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("checknow", checknow_command))
    application.add_handler(CommandHandler("debug", debug_command))
    application.add_handler(CommandHandler("seed", seed_command))
    application.add_handler(CommandHandler("unseed", unseed_command))
    application.add_handler(CommandHandler("watch", watch_command))
    application.add_handler(CommandHandler("unwatch", unwatch_command))
    application.add_handler(CommandHandler("watchlist", watchlist_command))
    application.add_handler(CommandHandler("digest", digest_command))
    application.add_handler(CommandHandler("dbstatus", dbstatus_command))

    # Add callback handler for inline buttons
    application.add_handler(CallbackQueryHandler(callback_handler))

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
