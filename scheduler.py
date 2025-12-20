"""
Scheduler for periodic alert checks.
Uses APScheduler to run checks every X minutes.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telegram.ext import Application

from config import CHECK_INTERVAL_MINUTES, ALERT_CAP_PER_CYCLE, MARKETS_TO_SCAN
from database import get_all_users_with_alerts_enabled, log_alert, save_volume_snapshots_bulk
from polymarket import get_all_markets_paginated
from alerts import (
    check_new_markets,
    check_price_movements,
    check_volume_milestones,
    format_new_market_alert,
    format_price_move_alert,
    format_volume_milestone_alert,
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

    # Get users with alerts enabled
    users = get_all_users_with_alerts_enabled()
    if not users:
        logger.info("No users with alerts enabled, skipping cycle")
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
            logger.info(f"Found {len(new_markets)} new markets, sending alerts...")
            for market in new_markets[:5]:  # Limit to 5 alerts per cycle
                message = format_new_market_alert(market)
                for user in new_market_users:
                    sent = await send_alert_to_user(
                        app,
                        user["telegram_id"],
                        message,
                        "new_market",
                        market.get("slug")
                    )
                    if sent:
                        stats["alerts_sent"] += 1

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
            logger.info(f"Found {len(big_moves)} big moves, sending alerts...")
            for move in big_moves[:5]:  # Limit to 5 alerts per cycle
                message = format_price_move_alert(move)
                for user in big_move_users:
                    sent = await send_alert_to_user(
                        app,
                        user["telegram_id"],
                        message,
                        "big_move",
                        move.get("slug")
                    )
                    if sent:
                        stats["alerts_sent"] += 1

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
            total_found = len(milestones)
            logger.info(f"Found {total_found} volume milestones")

            # Apply cap
            alerts_to_send = milestones[:ALERT_CAP_PER_CYCLE]
            overflow_count = max(0, total_found - ALERT_CAP_PER_CYCLE)

            # Send individual alerts
            for milestone in alerts_to_send:
                message = format_volume_milestone_alert(milestone)
                for user in new_market_users:
                    sent = await send_alert_to_user(
                        app,
                        user["telegram_id"],
                        message,
                        "volume_milestone",
                        milestone.get("slug")
                    )
                    if sent:
                        stats["alerts_sent"] += 1

            # Send digest if we hit the cap
            if overflow_count > 0:
                digest_msg = f"📊 +{overflow_count} more volume milestones this cycle.\nUse /top to see top markets."
                for user in new_market_users:
                    await send_alert_to_user(
                        app,
                        user["telegram_id"],
                        digest_msg,
                        "volume_digest",
                        None
                    )

    # Save volume snapshots for velocity detection (Phase 1)
    # Do this regardless of user settings - we need historical data
    try:
        events = await get_all_markets_paginated(target_count=MARKETS_TO_SCAN, include_spam=False)
        save_volume_snapshots_bulk(events)
        stats["markets_scanned"] = len(events)
        logger.info(f"Saved {len(events)} volume snapshots")
    except Exception as e:
        logger.error(f"Failed to save volume snapshots: {e}")

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
