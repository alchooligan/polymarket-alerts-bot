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
)
from scheduler import start_scheduler, stop_scheduler, run_manual_cycle, run_daily_digest

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

Commands:
/top - Top markets by volume
/discover - Markets waking up (velocity)
/watch <slug> - Watch a specific market
/watchlist - See your watched markets
/settings - Configure alerts
/checknow - Manual scan

Alerts every 5 min:
• Volume milestones ($10K→$1M)
• Velocity (money flowing fast)
• Price moves (10%+)
• Watchlist updates

Try /top for giants, /discover for rising."""

    await update.message.reply_text(welcome_message)


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


def build_settings_keyboard(user: dict) -> InlineKeyboardMarkup:
    """Build the settings inline keyboard based on user preferences."""
    new_markets_status = "ON" if user.get("new_markets_enabled") else "OFF"
    big_moves_status = "ON" if user.get("big_moves_enabled") else "OFF"

    keyboard = [
        [InlineKeyboardButton(
            f"New Markets: {new_markets_status}",
            callback_data="toggle_new_markets"
        )],
        [InlineKeyboardButton(
            f"Big Moves (10%+): {big_moves_status}",
            callback_data="toggle_big_moves"
        )],
    ]
    return InlineKeyboardMarkup(keyboard)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /settings command - show settings with inline buttons."""
    telegram_user = update.effective_user
    user = get_or_create_user(telegram_user.id, telegram_user.username)

    text = """Alert Settings

Toggle which alerts you want to receive:"""

    keyboard = build_settings_keyboard(user)
    await update.message.reply_text(text, reply_markup=keyboard)


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline button presses for settings."""
    query = update.callback_query
    await query.answer()  # Acknowledge the button press

    telegram_user = update.effective_user
    callback_data = query.data

    # Toggle the appropriate setting
    if callback_data == "toggle_new_markets":
        new_value = toggle_user_setting(telegram_user.id, "new_markets_enabled")
        status = "ON" if new_value else "OFF"
        logger.info(f"User {telegram_user.id} toggled new_markets to {status}")

    elif callback_data == "toggle_big_moves":
        new_value = toggle_user_setting(telegram_user.id, "big_moves_enabled")
        status = "ON" if new_value else "OFF"
        logger.info(f"User {telegram_user.id} toggled big_moves to {status}")

    # Refresh the keyboard with updated settings
    user = get_or_create_user(telegram_user.id, telegram_user.username)
    keyboard = build_settings_keyboard(user)

    text = """Alert Settings

Toggle which alerts you want to receive:"""

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

    # Try to find the market to get its title and current price
    try:
        events = await get_unique_events(limit=200, include_spam=False)
        market = next((e for e in events if e.get("slug", "").lower() == slug), None)

        if not market:
            await update.message.reply_text(
                f"Market '{slug}' not found.\n\n"
                "Make sure you're using the exact slug from the URL."
            )
            return

        title = market.get("title", "Unknown")
        current_price = market.get("yes_price", 0)

        added = add_to_watchlist(user_id, slug, title, current_price)

        if added:
            await update.message.reply_text(
                f"Added to watchlist:\n\n"
                f"- {title}\n"
                f"  YES: {current_price:.0f}%\n\n"
                f"You'll get alerts for any price movement.\n"
                f"Use /watchlist to see all watched markets."
            )
        else:
            await update.message.reply_text(f"'{slug}' is already in your watchlist.")

    except Exception as e:
        logger.error(f"Error in watch command: {e}")
        await update.message.reply_text(f"Error: {e}")


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

        # Get most recent snapshot time
        cursor.execute("SELECT MAX(recorded_at) as latest FROM volume_snapshots")
        latest = cursor.fetchone()["latest"]

        # Check seeded flag
        seeded = is_volume_seeded()

        conn.close()

        response = f"""Database Stats

Volume snapshots: {snapshot_count}
Volume baselines: {baseline_count}
Volume milestones: {milestone_count}
Latest snapshot: {latest or 'None'}
Seeded flag: {seeded}"""

        await update.message.reply_text(response)
    except Exception as e:
        await update.message.reply_text(f"Debug error: {e}")


async def digest_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /digest command - manually trigger daily digest."""
    await update.message.reply_text("Sending daily digest...")

    try:
        stats = await run_daily_digest(context.application)
        await update.message.reply_text(f"Digest sent to {stats['users_sent']} users.")
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

Scanned ~{stats['markets_scanned']} markets
• {stats['milestones']} volume milestones
• {stats['velocity']} velocity alerts
• {stats['big_moves']} big price moves
• {stats['new_markets']} new markets
• {stats['watchlist']} watchlist updates

Alerts sent: {stats['alerts_sent']}"""

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
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("markets", top_command))  # Alias for backwards compatibility
    application.add_handler(CommandHandler("discover", discover_command))
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
