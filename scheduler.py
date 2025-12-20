"""
Scheduler for periodic alert checks.
Uses APScheduler to run checks every X minutes.
"""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from telegram.ext import Application

from config import (
    CHECK_INTERVAL_MINUTES,
    ALERT_CAP_PER_CYCLE,
    MARKETS_TO_SCAN,
    DAILY_DIGEST_HOUR,
    DAILY_DIGEST_MINUTE,
)
from database import (
    get_all_users_with_alerts_enabled,
    log_alert,
    save_volume_snapshots_bulk,
    filter_unseen_markets,
    mark_user_alerted_bulk,
    get_all_watched_markets,
    update_watchlist_price,
)
from polymarket import get_all_markets_paginated
from alerts import (
    check_new_markets,
    check_price_movements,
    check_volume_milestones,
    check_velocity_alerts,
    check_underdog_alerts,
    check_closing_soon_alerts,
    format_bundled_milestones,
    format_bundled_big_moves,
    format_bundled_new_markets,
    format_bundled_velocity,
    format_bundled_underdogs,
    format_bundled_closing_soon,
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
        "velocity": 0,
        "underdogs": 0,
        "closing_soon": 0,
        "watchlist": 0,
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

    # Check for velocity alerts (money moving fast - breaking news)
    if new_market_users:
        logger.info(f"Checking velocity alerts for {len(new_market_users)} users...")
        velocity_alerts = await check_velocity_alerts(target_count=MARKETS_TO_SCAN)
        stats["velocity"] = len(velocity_alerts)

        if velocity_alerts:
            # Per-user filtering
            for user in new_market_users:
                user_id = user["telegram_id"]
                user_velocity = filter_unseen_markets(user_id, velocity_alerts, "velocity")

                if not user_velocity:
                    continue

                to_send = user_velocity[:ALERT_CAP_PER_CYCLE]
                overflow = len(user_velocity) - len(to_send)

                message = format_bundled_velocity(to_send)
                if overflow > 0:
                    message += f"\n\n+{overflow} more fast-moving markets"

                sent = await send_alert_to_user(app, user_id, message, "velocity_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "velocity")

    # Check for underdog alerts (YES <20% but money flowing)
    if new_market_users:
        logger.info("Checking underdog alerts...")
        underdogs = await check_underdog_alerts(target_count=MARKETS_TO_SCAN)
        stats["underdogs"] = len(underdogs)

        if underdogs:
            for user in new_market_users:
                user_id = user["telegram_id"]
                user_underdogs = filter_unseen_markets(user_id, underdogs, "underdog")

                if not user_underdogs:
                    continue

                to_send = user_underdogs[:ALERT_CAP_PER_CYCLE]
                message = format_bundled_underdogs(to_send)

                sent = await send_alert_to_user(app, user_id, message, "underdog_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "underdog")

    # Check for closing soon alerts (markets ending in 24h with action)
    if new_market_users:
        logger.info("Checking closing soon alerts...")
        closing = await check_closing_soon_alerts(target_count=MARKETS_TO_SCAN)
        stats["closing_soon"] = len(closing)

        if closing:
            for user in new_market_users:
                user_id = user["telegram_id"]
                user_closing = filter_unseen_markets(user_id, closing, "closing_soon")

                if not user_closing:
                    continue

                to_send = user_closing[:ALERT_CAP_PER_CYCLE]
                message = format_bundled_closing_soon(to_send)

                sent = await send_alert_to_user(app, user_id, message, "closing_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "closing_soon")

    # Check watchlist price changes (5% moves)
    watched = get_all_watched_markets()
    if watched:
        logger.info(f"Checking {len(watched)} watched markets...")

        # Build a map of current prices from already-fetched events
        price_map = {e.get("slug"): e.get("yes_price", 0) for e in events if e.get("slug")}

        for item in watched:
            user_id = item["telegram_id"]
            slug = item["event_slug"]
            title = item.get("title", "Unknown")
            last_price = item.get("last_price", 0)

            current_price = price_map.get(slug)
            if current_price is None:
                continue

            # Check for significant price change (5% to avoid noise)
            change = current_price - last_price
            if abs(change) >= 5:
                stats["watchlist"] += 1

                change_str = f"+{change:.0f}%" if change > 0 else f"{change:.0f}%"
                message = f"""Watchlist Alert

- {title}
  YES: {last_price:.0f}% → {current_price:.0f}% ({change_str})
  polymarket.com/event/{slug}"""

                sent = await send_alert_to_user(app, user_id, message, "watchlist", slug)
                if sent:
                    stats["alerts_sent"] += 1

                # Update the stored price
                update_watchlist_price(user_id, slug, current_price)

    logger.info("Alert cycle complete")
    return stats


def start_scheduler(app: Application) -> AsyncIOScheduler:
    """
    Start the scheduler with the alert cycle job.
    Must be called after the bot application is built.
    """
    global scheduler

    scheduler = AsyncIOScheduler()

    # Add the alert cycle job (every 5 min)
    scheduler.add_job(
        run_alert_cycle,
        trigger=IntervalTrigger(minutes=CHECK_INTERVAL_MINUTES),
        args=[app],
        id="alert_cycle",
        name="Polymarket Alert Cycle",
        replace_existing=True,
    )

    # Add daily digest job (9am UTC)
    scheduler.add_job(
        run_daily_digest,
        trigger=CronTrigger(hour=DAILY_DIGEST_HOUR, minute=DAILY_DIGEST_MINUTE),
        args=[app],
        id="daily_digest",
        name="Daily Digest",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(f"Scheduler started. Alerts every {CHECK_INTERVAL_MINUTES} min, digest at {DAILY_DIGEST_HOUR}:00 UTC.")

    return scheduler


def stop_scheduler() -> None:
    """Stop the scheduler if running."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


async def run_daily_digest(app: Application) -> dict:
    """
    Send a daily digest to all users at 9am.
    Summarizes: top markets, biggest movers, velocity leaders.
    """
    logger.info("Running daily digest...")

    stats = {"users_sent": 0}

    # Get users with alerts enabled
    users = get_all_users_with_alerts_enabled()
    if not users:
        logger.info("No users for daily digest")
        return stats

    try:
        # Fetch top markets
        events = await get_all_markets_paginated(target_count=100, include_spam=False)

        if not events:
            logger.warning("No events for daily digest")
            return stats

        # Sort by volume for top markets
        top_by_volume = sorted(events, key=lambda x: x.get("total_volume", 0), reverse=True)[:5]

        # Get velocity data
        slugs = [e.get("slug") for e in events if e.get("slug")]
        from database import get_volume_deltas_bulk
        deltas = get_volume_deltas_bulk(slugs, hours=24)

        # Find top velocity (24h)
        velocity_leaders = []
        for e in events:
            slug = e.get("slug")
            if slug in deltas and deltas[slug] > 0:
                velocity_leaders.append({**e, "delta_24h": deltas[slug]})
        velocity_leaders.sort(key=lambda x: x["delta_24h"], reverse=True)
        velocity_leaders = velocity_leaders[:5]

        # Build digest message
        lines = ["Daily Digest", ""]

        # Top by volume
        lines.append("Top Markets:")
        for e in top_by_volume:
            title = e.get("title", "Unknown")[:35]
            vol = e.get("total_volume", 0)
            if vol >= 1_000_000:
                vol_str = f"${vol/1_000_000:.1f}M"
            else:
                vol_str = f"${vol/1_000:.0f}K"
            lines.append(f"• {title} ({vol_str})")
        lines.append("")

        # Velocity leaders
        if velocity_leaders:
            lines.append("Fastest Growing (24h):")
            for e in velocity_leaders:
                title = e.get("title", "Unknown")[:35]
                delta = e.get("delta_24h", 0)
                if delta >= 1_000:
                    delta_str = f"+${delta/1_000:.0f}K"
                else:
                    delta_str = f"+${delta:.0f}"
                lines.append(f"• {title} ({delta_str})")
            lines.append("")

        lines.append("Use /discover for more rising markets.")

        message = "\n".join(lines)

        # Send to all users
        for user in users:
            try:
                await app.bot.send_message(
                    chat_id=user["telegram_id"],
                    text=message,
                    disable_web_page_preview=True
                )
                stats["users_sent"] += 1
            except Exception as e:
                logger.error(f"Failed to send digest to {user['telegram_id']}: {e}")

        logger.info(f"Daily digest sent to {stats['users_sent']} users")

    except Exception as e:
        logger.error(f"Error in daily digest: {e}")

    return stats


async def run_manual_cycle(app: Application) -> dict:
    """
    Run a single alert cycle manually (for testing).
    Call this from bot.py or a command handler.

    Returns:
        Dict with stats about what was checked
    """
    return await run_alert_cycle(app)
