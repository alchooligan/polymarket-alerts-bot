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
    save_price_snapshots_bulk,
    filter_unseen_markets,
    mark_user_alerted_bulk,
    get_all_watched_markets,
    update_watchlist_price,
)
from polymarket import get_all_markets_paginated
from alerts import (
    check_new_markets,
    check_volume_milestones,
    check_closing_soon_alerts,
    format_bundled_milestones,
    format_bundled_discoveries,
    format_bundled_new_markets,
    format_bundled_closing_soon,
    filter_sports,
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
        "milestones": 0,
        "discoveries": 0,
        "closing_soon": 0,
        "watchlist": 0,
        "alerts_sent": 0,
    }

    # Save volume snapshots FIRST - we need this data regardless of users
    events = []  # Initialize to prevent undefined variable
    try:
        logger.info("Fetching markets for snapshots...")
        events = await get_all_markets_paginated(target_count=MARKETS_TO_SCAN, include_spam=False)
        if events:
            save_volume_snapshots_bulk(events)
            save_price_snapshots_bulk(events)  # Critical for /movers, /underdogs, price history
            stats["markets_scanned"] = len(events)
            logger.info(f"Saved {len(events)} volume + price snapshots")
        else:
            logger.warning("API returned 0 events - velocity/watchlist checks will be skipped")
            return stats  # Can't do anything useful without market data
    except Exception as e:
        logger.error(f"Failed to fetch markets: {e} - aborting cycle")
        return stats  # Don't continue with stale/no data

    # Get users with alerts enabled
    users = get_all_users_with_alerts_enabled()
    if not users:
        logger.info("No users with alerts enabled, skipping alert checks")
        return stats

    # Users who want milestone/discovery alerts
    milestone_users = [u for u in users if u.get("new_markets_enabled")]

    # Check for volume milestones (the KEY signal)
    if milestone_users:
        logger.info(f"Checking volume milestones for {len(milestone_users)} users...")
        milestones, discoveries = await check_volume_milestones(
            target_count=MARKETS_TO_SCAN,
            record=True
        )
        stats["milestones"] = len(milestones)
        stats["discoveries"] = len(discoveries)
        stats["markets_scanned"] = MARKETS_TO_SCAN  # Approximate

        if milestones:
            # Per-user filtering
            for user in milestone_users:
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

        # Send discovery alerts (first-seen markets that launched big)
        if discoveries:
            for user in milestone_users:
                user_id = user["telegram_id"]
                user_discoveries = filter_unseen_markets(user_id, discoveries, "discovery")

                if not user_discoveries:
                    continue

                to_send = user_discoveries[:ALERT_CAP_PER_CYCLE]
                overflow = len(user_discoveries) - len(to_send)

                message = format_bundled_discoveries(to_send)
                if overflow > 0:
                    message += f"\n\n+{overflow} more new discoveries"

                sent = await send_alert_to_user(app, user_id, message, "discovery_bundle", None)
                if sent:
                    stats["alerts_sent"] += 1
                    mark_user_alerted_bulk(user_id, [d["slug"] for d in to_send], "discovery")

    # Check for closing soon alerts (markets ending soon with action)
    closing_users = [u for u in users if u.get("new_markets_enabled")]
    if closing_users:
        logger.info("Checking closing soon alerts...")
        closing = await check_closing_soon_alerts(target_count=MARKETS_TO_SCAN)
        stats["closing_soon"] = len(closing)

        if closing:
            for user in closing_users:
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
    Shows: hottest by velocity %, biggest movers, volume surge leaders.
    """
    from database import get_volume_deltas_bulk, get_price_deltas_bulk

    logger.info("Running daily digest...")

    stats = {"users_sent": 0}

    # Get users with alerts enabled
    users = get_all_users_with_alerts_enabled()
    if not users:
        logger.info("No users for daily digest")
        return stats

    try:
        # Fetch markets
        events = await get_all_markets_paginated(target_count=500, include_spam=False)

        if not events:
            logger.warning("No events for daily digest")
            return stats

        # Filter out sports and resolved markets
        events = filter_sports(events)
        # Filter resolved (95%+ or 5%-)
        events = [e for e in events if 5 < e.get("yes_price", 50) < 95]

        slugs = [e.get("slug") for e in events if e.get("slug")]

        # Get velocity and price data
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)
        deltas_24h = get_volume_deltas_bulk(slugs, hours=24)
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

        lines = ["Daily Digest", ""]

        # 1. HOTTEST (by velocity %/hr) - the KEY signal
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

        # 2. BIGGEST MOVERS (price change 24h)
        movers = []
        for e in events:
            slug = e.get("slug")
            price_data = price_deltas_24h.get(slug, {})
            price_change = price_data.get("delta", 0)
            if abs(price_change) >= 5:
                movers.append({**e, "price_change": price_change})
        movers.sort(key=lambda x: abs(x["price_change"]), reverse=True)

        gainers = [m for m in movers if m["price_change"] > 0][:3]
        losers = [m for m in movers if m["price_change"] < 0][:3]

        if gainers or losers:
            lines.append("📊 MOVERS (24h):")
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

        # 3. VOLUME SURGE (biggest 24h growth %)
        volume_surge = []
        for e in events:
            slug = e.get("slug")
            total_volume = e.get("total_volume", 0)
            delta_24h = deltas_24h.get(slug, 0)
            if delta_24h > 0 and total_volume > 10000:
                volume_24h_ago = total_volume - delta_24h
                if volume_24h_ago > 0:
                    growth_pct = delta_24h / volume_24h_ago * 100
                    if growth_pct >= 25:  # At least 25% growth
                        volume_surge.append({**e, "delta_24h": delta_24h, "growth_pct": growth_pct})
        volume_surge.sort(key=lambda x: x["growth_pct"], reverse=True)

        if volume_surge[:5]:
            lines.append("💰 VOLUME SURGE (24h):")
            for m in volume_surge[:5]:
                title = m.get("title", "Unknown")[:30]
                delta = m["delta_24h"]
                growth = m["growth_pct"]
                delta_str = f"+${delta/1000:.0f}K" if delta >= 1000 else f"+${delta:.0f}"
                lines.append(f"• {title} ({delta_str}, +{growth:.0f}%)")
            lines.append("")

        lines.append("Use /hot, /movers, /discover for full lists")

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
