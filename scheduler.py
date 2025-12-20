"""
Scheduler for periodic alert checks.
Uses APScheduler to run checks every X minutes.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telegram.ext import Application

from config import CHECK_INTERVAL_MINUTES, ALERT_CAP_PER_CYCLE, MARKETS_TO_SCAN
from database import (
    get_all_users_with_alerts_enabled,
    log_alert,
    save_volume_snapshots_bulk,
    filter_unseen_markets,
    mark_user_alerted_bulk,
)
from polymarket import get_all_markets_paginated
from alerts import (
    check_new_markets,
    check_price_movements,
    check_volume_milestones,
    format_bundled_milestones,
    format_bundled_big_moves,
    format_bundled_new_markets,
)

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: AsyncIOScheduler = None


async def send_alert_to_user(app: Application, telegram_id: int, message: str, alert_type: str, event_slug: str = None) -> bool:
    """
    Send an alert message to a user.
    Returns True if sent successfully, False otherwise.
    """
    try:
        await app.bot.send_message(
            chat_id=telegram_id,
            text=message,
            disable_web_page_preview=True
        )
        # Log the alert
        log_alert(telegram_id, alert_type, event_slug)
        return True
    except Exception as e:
        logger.error(f"Failed to send alert to {telegram_id}: {e}")
        return False


async def run_alert_cycle(app: Application) -> dict:
    """
    Run one cycle of alert checks.
    Called by the scheduler every X minutes.

    Returns:
        Dict with stats about what was checked
    """
    logger.info("Running alert cycle...")

    stats = {
        "markets_scanned": 0,
        "new_markets": 0,
        "big_moves": 0,
        "milestones": 0,
        "alerts_sent": 0,
    }

    # Save volume snapshots FIRST - we need this data regardless of users
    try:
        logger.info("Fetching markets for snapshots...")
        events = await get_all_markets_paginated(target_count=MARKETS_TO_SCAN, include_spam=False)
        if events:
            save_volume_snapshots_bulk(events)
            stats["markets_scanned"] = len(events)
            logger.info(f"Saved {len(events)} volume snapshots")
        else:
            logger.warning("No events returned from API")
    except Exception as e:
        logger.error(f"Failed to save volume snapshots: {e}")

    # Get users with alerts enabled
    users = get_all_users_with_alerts_enabled()
    if not users:
        logger.info("No users with alerts enabled, skipping alert checks")
        return stats

    # Separate users by alert type
    new_market_users = [u for u in users if u.get("new_markets_enabled")]
    big_move_users = [u for u in users if u.get("big_moves_enabled")]

    # Check for new markets
    if new_market_users:
        logger.info(f"Checking new markets for {len(new_market_users)} users...")
        new_markets = await check_new_markets(
            limit=100,
            min_volume=100,  # Filter out $0 markets
            mark_seen=True
        )
        stats["new_markets"] = len(new_markets)

        if new_markets:
            # Per-user filtering - each user only sees markets they haven't been alerted about
            for user in new_market_users:
                user_id = user["telegram_id"]
                user_markets = filter_unseen_markets(user_id, new_markets, "new_market")

                if not user_markets:
                    continue

                to_send = user_markets[:ALERT_CAP_PER_CYCLE]
                overflow = len(user_markets) - len(to_send)

                message = format_bundled_new_markets(to_send)
                if overflow > 0:
                    message += f"\n\n+{overflow} more new markets"

                sent = await send_alert_to_user(app, user_id, message, "new_market_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    # Mark these markets as alerted for this user
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "new_market")

    # Check for price movements
    if big_move_users:
        logger.info(f"Checking price movements for {len(big_move_users)} users...")
        big_moves = await check_price_movements(
            limit=100,
            threshold=10,  # 10% move
            hours=1,
            save_snapshots=True
        )
        stats["big_moves"] = len(big_moves)

        if big_moves:
            # Per-user filtering
            for user in big_move_users:
                user_id = user["telegram_id"]
                user_moves = filter_unseen_markets(user_id, big_moves, "big_move")

                if not user_moves:
                    continue

                to_send = user_moves[:ALERT_CAP_PER_CYCLE]
                overflow = len(user_moves) - len(to_send)

                message = format_bundled_big_moves(to_send)
                if overflow > 0:
                    message += f"\n\n+{overflow} more big moves"

                sent = await send_alert_to_user(app, user_id, message, "big_move_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "big_move")

    # Check for volume milestones (the KEY signal)
    # Send to users with new_markets_enabled for now
    if new_market_users:
        logger.info(f"Checking volume milestones for {len(new_market_users)} users...")
        milestones = await check_volume_milestones(
            target_count=MARKETS_TO_SCAN,
            record=True
        )
        stats["milestones"] = len(milestones)
        stats["markets_scanned"] = MARKETS_TO_SCAN  # Approximate

        if milestones:
            # Per-user filtering
            for user in new_market_users:
                user_id = user["telegram_id"]
                user_milestones = filter_unseen_markets(user_id, milestones, "milestone")

                if not user_milestones:
                    continue

                to_send = user_milestones[:ALERT_CAP_PER_CYCLE]
                overflow = len(user_milestones) - len(to_send)

                message = format_bundled_milestones(to_send)
                if overflow > 0:
                    message += f"\n\n+{overflow} more volume milestones. Use /top to explore."

                sent = await send_alert_to_user(app, user_id, message, "milestone_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "milestone")

    logger.info("Alert cycle complete")
    return stats


def start_scheduler(app: Application) -> AsyncIOScheduler:
    """
    Start the scheduler with the alert cycle job.
    Must be called after the bot application is built.
    """
    global scheduler

    scheduler = AsyncIOScheduler()

    # Add the alert cycle job
    scheduler.add_job(
        run_alert_cycle,
        trigger=IntervalTrigger(minutes=CHECK_INTERVAL_MINUTES),
        args=[app],
        id="alert_cycle",
        name="Polymarket Alert Cycle",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(f"Scheduler started. Running every {CHECK_INTERVAL_MINUTES} minutes.")

    return scheduler


def stop_scheduler() -> None:
    """Stop the scheduler if running."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


async def run_manual_cycle(app: Application) -> dict:
    """
    Run a single alert cycle manually (for testing).
    Call this from bot.py or a command handler.

    Returns:
        Dict with stats about what was checked
    """
    return await run_alert_cycle(app)
