"""
Polymarket Telegram Alert Bot - Main bot file.
"""

import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import TELEGRAM_BOT_TOKEN, CHECK_INTERVAL_MINUTES
from polymarket import get_unique_events, get_popular_markets
from database import init_database, get_or_create_user, toggle_user_setting
from scheduler import start_scheduler, stop_scheduler, run_manual_cycle

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
/start - Show this welcome message
/markets - Show top markets by volume
/settings - Configure your alert preferences
/checknow - Manually trigger alert check

Alerts run automatically every 5 minutes.
Use /settings to enable:
- New market alerts
- Big price movement alerts (10%+ in 1 hour)

Let's get started! Try /markets to see what's trending."""

    await update.message.reply_text(welcome_message)


async def markets_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /markets command - show 5 top markets by volume."""
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


async def checknow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /checknow command - manually trigger an alert cycle."""
    await update.message.reply_text("Running alert check...")

    try:
        await run_manual_cycle(context.application)
        await update.message.reply_text("Alert check complete. Check above for any alerts.")
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
    application.add_handler(CommandHandler("markets", markets_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("checknow", checknow_command))

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
