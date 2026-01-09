"""
Scheduler for periodic alert checks.
Uses APScheduler to run checks every X minutes.

ALERTS V2:
- Wakeup: Market was quiet, now hot (breaking news)
- Fast Mover: Price moved 10%+ with volume behind it
- Early Heat: New market (<24h) gaining traction
- New Launch: Brand new market alerts
- Watchlist: Price moves on watched markets
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
    ALERT_CHANNEL_ID,
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
    get_recently_alerted_slugs,
    mark_channel_alerted_bulk,
    cleanup_old_channel_alerts,
)
from polymarket import get_all_markets_paginated
from alerts import (
    filter_noise,
    # V2 Alert functions
    check_wakeup_alerts,
    check_fast_mover_alerts,
    check_big_swing_alerts,
    check_early_heat_alerts,
    check_new_launch_alerts,
    # V2 Formatters
    format_bundled_wakeups,
    format_bundled_fast_movers,
    format_bundled_big_swings,
    format_bundled_early_heat,
    format_bundled_new_launches,
    _escape_markdown,
)

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: AsyncIOScheduler = None

# Track last run for status monitoring
last_cycle_time: str = None
last_cycle_stats: dict = None


async def send_alert_to_user(app: Application, telegram_id: int, message: str, alert_type: str, event_slug: str = None) -> bool:
    """
    Send an alert message to a user.
    Returns True if sent successfully, False otherwise.
    """
    try:
        await app.bot.send_message(
            chat_id=telegram_id,
            text=message,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        # Log the alert
        log_alert(telegram_id, alert_type, event_slug)
        return True
    except Exception as e:
        logger.error(f"Failed to send alert to {telegram_id}: {e}")
        return False


async def send_alert_to_channel(app: Application, message: str, alert_type: str) -> bool:
    """
    Send an alert message to the configured channel.
    Returns True if sent successfully, False otherwise.
    """
    if not ALERT_CHANNEL_ID:
        return False

    try:
        await app.bot.send_message(
            chat_id=ALERT_CHANNEL_ID,
            text=message,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        logger.info(f"Sent {alert_type} to channel {ALERT_CHANNEL_ID}")
        return True
    except Exception as e:
        logger.error(f"Failed to send alert to channel {ALERT_CHANNEL_ID}: {e}")
        return False


async def run_alert_cycle(app: Application) -> dict:
    """
    Run one cycle of alert checks (V2).
    Called by the scheduler every X minutes.

    V2 Alerts:
    - Wakeup: Was quiet, now hot
    - Fast Mover: Price + volume spike
    - Early Heat: New market with traction
    - New Launch: Brand new markets
    - Watchlist: Price moves

    Returns:
        Dict with stats about what was checked
    """
    logger.info("Running alert cycle (V2)...")

    stats = {
        "markets_scanned": 0,
        "wakeups": 0,
        "fast_movers": 0,
        "big_swings": 0,
        "early_heat": 0,
        "new_launches": 0,
        "watchlist": 0,
        "alerts_sent": 0,
    }

    # Save volume snapshots FIRST - we need this data regardless of users
    events = []
    try:
        logger.info("Fetching markets for snapshots...")
        events = await get_all_markets_paginated(target_count=MARKETS_TO_SCAN, include_spam=False)
        if events:
            save_volume_snapshots_bulk(events)
            save_price_snapshots_bulk(events)
            stats["markets_scanned"] = len(events)
            logger.info(f"Saved {len(events)} volume + price snapshots")
        else:
            logger.warning("API returned 0 events - skipping alerts")
            return stats
    except Exception as e:
        logger.error(f"Failed to fetch markets: {e} - aborting cycle")
        return stats

    # Check if we're in channel mode or individual user mode
    use_channel = bool(ALERT_CHANNEL_ID)

    if use_channel:
        logger.info(f"Channel mode enabled - sending alerts to {ALERT_CHANNEL_ID}")
        alert_users = [{"telegram_id": ALERT_CHANNEL_ID}]  # Dummy user for channel
    else:
        # Get users with alerts enabled
        users = get_all_users_with_alerts_enabled()
        if not users:
            logger.info("No users with alerts enabled, skipping alert checks")
            return stats
        alert_users = [u for u in users if u.get("new_markets_enabled")]

    # Track slugs alerted THIS CYCLE to avoid duplicate alerts across alert types
    # e.g., same market triggering both Wakeup and Early Heat
    cycle_alerted_slugs = set()

    # For channel mode: also check slugs alerted in PREVIOUS cycles (last 4 hours)
    # This prevents the same market from triggering different alert types across cycles
    # Extended from 1h to 4h to reduce duplicate alerts for the same story
    if use_channel:
        recently_alerted = get_recently_alerted_slugs(hours=4)
        cycle_alerted_slugs.update(recently_alerted)
        if recently_alerted:
            logger.info(f"Excluding {len(recently_alerted)} recently alerted markets from this cycle")

    # ==========================================
    # ALERT 1: Wakeup (was quiet, now hot)
    # ==========================================
    if alert_users:
        try:
            logger.info("Checking wakeup alerts...")
            wakeups = await check_wakeup_alerts(target_count=MARKETS_TO_SCAN)
            stats["wakeups"] = len(wakeups)

            if wakeups:
                if use_channel:
                    # Send to channel (no per-user filtering)
                    to_send = wakeups[:ALERT_CAP_PER_CYCLE]
                    message = format_bundled_wakeups(to_send)
                    sent = await send_alert_to_channel(app, message, "wakeup_bundle")
                    if sent:
                        stats["alerts_sent"] += 1
                        # Track alerted slugs to avoid duplicates in other alert types
                        slugs_sent = [m["slug"] for m in to_send]
                        cycle_alerted_slugs.update(slugs_sent)
                        # Persist to DB for cross-cycle deduplication
                        mark_channel_alerted_bulk(slugs_sent, "wakeup")
                else:
                    # Send to individual users
                    for user in alert_users:
                        user_id = user["telegram_id"]
                        user_wakeups = filter_unseen_markets(user_id, wakeups, "wakeup")

                        if not user_wakeups:
                            continue

                        to_send = user_wakeups[:ALERT_CAP_PER_CYCLE]
                        message = format_bundled_wakeups(to_send)

                        sent = await send_alert_to_user(app, user_id, message, "wakeup_bundle", None)
                        if sent:
                            stats["alerts_sent"] += 1
                            mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "wakeup")
                            cycle_alerted_slugs.update(m["slug"] for m in to_send)
        except Exception as e:
            logger.error(f"Error in wakeup alerts: {e}")

    # ==========================================
    # ALERT 2: Fast Mover (price + volume spike)
    # ==========================================
    if alert_users:
        try:
            logger.info("Checking fast mover alerts...")
            movers = await check_fast_mover_alerts(target_count=MARKETS_TO_SCAN)
            # Filter out markets already alerted this cycle
            movers = [m for m in movers if m["slug"] not in cycle_alerted_slugs]
            stats["fast_movers"] = len(movers)

            if movers:
                if use_channel:
                    to_send = movers[:ALERT_CAP_PER_CYCLE]
                    message = format_bundled_fast_movers(to_send)
                    sent = await send_alert_to_channel(app, message, "fast_mover_bundle")
                    if sent:
                        stats["alerts_sent"] += 1
                        slugs_sent = [m["slug"] for m in to_send]
                        cycle_alerted_slugs.update(slugs_sent)
                        mark_channel_alerted_bulk(slugs_sent, "fast_mover")
                else:
                    for user in alert_users:
                        user_id = user["telegram_id"]
                        user_movers = filter_unseen_markets(user_id, movers, "fast_mover")

                        if not user_movers:
                            continue

                        to_send = user_movers[:ALERT_CAP_PER_CYCLE]
                        message = format_bundled_fast_movers(to_send)

                        sent = await send_alert_to_user(app, user_id, message, "fast_mover_bundle", None)
                        if sent:
                            stats["alerts_sent"] += 1
                            mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "fast_mover")
                            cycle_alerted_slugs.update(m["slug"] for m in to_send)
        except Exception as e:
            logger.error(f"Error in fast mover alerts: {e}")

    # ==========================================
    # ALERT 3: Big Swings (15%+ in 1 hour)
    # ==========================================
    if alert_users:
        try:
            logger.info("Checking big swing alerts...")
            swings = await check_big_swing_alerts(target_count=MARKETS_TO_SCAN)
            # Filter out markets already alerted this cycle
            swings = [m for m in swings if m["slug"] not in cycle_alerted_slugs]
            stats["big_swings"] = len(swings)

            if swings:
                if use_channel:
                    to_send = swings[:ALERT_CAP_PER_CYCLE]
                    message = format_bundled_big_swings(to_send)
                    sent = await send_alert_to_channel(app, message, "big_swing_bundle")
                    if sent:
                        stats["alerts_sent"] += 1
                        slugs_sent = [m["slug"] for m in to_send]
                        cycle_alerted_slugs.update(slugs_sent)
                        mark_channel_alerted_bulk(slugs_sent, "big_swing")
                else:
                    for user in alert_users:
                        user_id = user["telegram_id"]
                        user_swings = filter_unseen_markets(user_id, swings, "big_swing")

                        if not user_swings:
                            continue

                        to_send = user_swings[:ALERT_CAP_PER_CYCLE]
                        message = format_bundled_big_swings(to_send)

                        sent = await send_alert_to_user(app, user_id, message, "big_swing_bundle", None)
                        if sent:
                            stats["alerts_sent"] += 1
                            mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "big_swing")
                            cycle_alerted_slugs.update(m["slug"] for m in to_send)
        except Exception as e:
            logger.error(f"Error in big swing alerts: {e}")

    # ==========================================
    # ALERT 4: Early Heat (new market + traction)
    # ==========================================
    if alert_users:
        try:
            logger.info("Checking early heat alerts...")
            early = await check_early_heat_alerts(target_count=MARKETS_TO_SCAN)
            # Filter out markets already alerted this cycle
            early = [m for m in early if m["slug"] not in cycle_alerted_slugs]
            stats["early_heat"] = len(early)

            if early:
                if use_channel:
                    to_send = early[:ALERT_CAP_PER_CYCLE]
                    message = format_bundled_early_heat(to_send)
                    sent = await send_alert_to_channel(app, message, "early_heat_bundle")
                    if sent:
                        stats["alerts_sent"] += 1
                        slugs_sent = [m["slug"] for m in to_send]
                        cycle_alerted_slugs.update(slugs_sent)
                        mark_channel_alerted_bulk(slugs_sent, "early_heat")
                else:
                    for user in alert_users:
                        user_id = user["telegram_id"]
                        user_early = filter_unseen_markets(user_id, early, "early_heat")

                        if not user_early:
                            continue

                        to_send = user_early[:ALERT_CAP_PER_CYCLE]
                        message = format_bundled_early_heat(to_send)

                        sent = await send_alert_to_user(app, user_id, message, "early_heat_bundle", None)
                        if sent:
                            stats["alerts_sent"] += 1
                            mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "early_heat")
                            cycle_alerted_slugs.update(m["slug"] for m in to_send)
        except Exception as e:
            logger.error(f"Error in early heat alerts: {e}")

    # ==========================================
    # ALERT 5: New Launch (brand new markets)
    # ==========================================
    if alert_users:
        try:
            logger.info("Checking new launch alerts...")
            launches = await check_new_launch_alerts(target_count=MARKETS_TO_SCAN)
            # Filter out markets already alerted this cycle
            launches = [m for m in launches if m["slug"] not in cycle_alerted_slugs]
            stats["new_launches"] = len(launches)

            if launches:
                if use_channel:
                    to_send = launches[:ALERT_CAP_PER_CYCLE]
                    message = format_bundled_new_launches(to_send)
                    sent = await send_alert_to_channel(app, message, "new_launch_bundle")
                    if sent:
                        stats["alerts_sent"] += 1
                        slugs_sent = [m["slug"] for m in to_send]
                        cycle_alerted_slugs.update(slugs_sent)
                        mark_channel_alerted_bulk(slugs_sent, "new_launch")
                else:
                    for user in alert_users:
                        user_id = user["telegram_id"]
                        user_launches = filter_unseen_markets(user_id, launches, "new_launch")

                        if not user_launches:
                            continue

                        to_send = user_launches[:ALERT_CAP_PER_CYCLE]
                        message = format_bundled_new_launches(to_send)

                        sent = await send_alert_to_user(app, user_id, message, "new_launch_bundle", None)
                        if sent:
                            stats["alerts_sent"] += 1
                            mark_user_alerted_bulk(user_id, [m["slug"] for m in to_send], "new_launch")
                            cycle_alerted_slugs.update(m["slug"] for m in to_send)
        except Exception as e:
            logger.error(f"Error in new launch alerts: {e}")

    # ==========================================
    # ALERT 6: Watchlist (price moves)
    # Note: Watchlist is per-user, so skip in channel mode
    # ==========================================
    if not use_channel:
        watched = get_all_watched_markets()
    else:
        watched = []

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
                emoji = "📈" if change > 0 else "📉"
                title_escaped = _escape_markdown(title[:40])

                message = f"""{emoji} *Watchlist Alert*

[{title_escaped}](https://polymarket.com/event/{slug})

YES: {last_price:.0f}% → {current_price:.0f}% ({change_str})"""

                sent = await send_alert_to_user(app, user_id, message, "watchlist", slug)
                if sent:
                    stats["alerts_sent"] += 1

                # Update the stored price
                update_watchlist_price(user_id, slug, current_price)

    # Track last run for status monitoring
    global last_cycle_time, last_cycle_stats
    from datetime import datetime, timezone
    last_cycle_time = datetime.now(timezone.utc).isoformat()
    last_cycle_stats = stats

    logger.info(f"Alert cycle complete: {stats}")
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
        name="Polymarket Alert Cycle V2",
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
    logger.info(f"Scheduler started (V2). Alerts every {CHECK_INTERVAL_MINUTES} min, digest at {DAILY_DIGEST_HOUR}:00 UTC.")

    return scheduler


def stop_scheduler() -> None:
    """Stop the scheduler if running."""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


def get_scheduler_status() -> dict:
    """Get scheduler status for monitoring."""
    global scheduler, last_cycle_time, last_cycle_stats
    from datetime import datetime, timezone

    status = {
        "running": scheduler is not None and scheduler.running,
        "last_cycle_time": last_cycle_time,
        "last_cycle_stats": last_cycle_stats,
        "jobs": [],
    }

    if scheduler and scheduler.running:
        for job in scheduler.get_jobs():
            next_run = job.next_run_time
            status["jobs"].append({
                "id": job.id,
                "name": job.name,
                "next_run": next_run.isoformat() if next_run else None,
            })

    return status


async def run_daily_digest(app: Application) -> dict:
    """
    Send a daily digest to all users at 9am.
    Shows: hottest by velocity %, biggest movers, volume surge leaders.
    """
    from database import get_volume_deltas_bulk, get_price_deltas_bulk
    from alerts import _escape_markdown

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
        events = filter_noise(events)  # Sports + Up/Down + Weather
        events = [e for e in events if 5 < e.get("yes_price", 50) < 95]

        slugs = [e.get("slug") for e in events if e.get("slug")]

        # Get velocity and price data
        deltas_1h = get_volume_deltas_bulk(slugs, hours=1)
        deltas_24h = get_volume_deltas_bulk(slugs, hours=24)
        price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

        lines = ["*Daily Digest*", ""]

        # 1. HOTTEST (by velocity %/hr)
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
            lines.append("🔥 *HOTTEST* (velocity %/hr):")
            for m in hot_markets[:5]:
                title = _escape_markdown(m.get("title", "Unknown")[:35])
                slug = m.get("slug", "")
                vel_pct = m["velocity_pct"]
                velocity = m["velocity"]
                vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
                emoji = "🔥🔥" if vel_pct >= 20 else ("🔥" if vel_pct >= 10 else "")
                lines.append(f"• [{title}](https://polymarket.com/event/{slug})")
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
            lines.append("📊 *MOVERS* (24h):")
            for m in gainers:
                title = _escape_markdown(m.get("title", "Unknown")[:30])
                slug = m.get("slug", "")
                emoji = "🚀" if m["price_change"] >= 15 else "⬆️"
                lines.append(f"• {emoji} [{title}](https://polymarket.com/event/{slug}) (+{m['price_change']:.0f}%)")
            for m in losers:
                title = _escape_markdown(m.get("title", "Unknown")[:30])
                slug = m.get("slug", "")
                emoji = "💀" if m["price_change"] <= -15 else "⬇️"
                lines.append(f"• {emoji} [{title}](https://polymarket.com/event/{slug}) ({m['price_change']:.0f}%)")
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
                    if growth_pct >= 25:
                        volume_surge.append({**e, "delta_24h": delta_24h, "growth_pct": growth_pct})
        volume_surge.sort(key=lambda x: x["growth_pct"], reverse=True)

        if volume_surge[:5]:
            lines.append("💰 *VOLUME SURGE* (24h):")
            for m in volume_surge[:5]:
                title = _escape_markdown(m.get("title", "Unknown")[:30])
                slug = m.get("slug", "")
                delta = m["delta_24h"]
                growth = m["growth_pct"]
                delta_str = f"+${delta/1000:.0f}K" if delta >= 1000 else f"+${delta:.0f}"
                lines.append(f"• [{title}](https://polymarket.com/event/{slug})")
                lines.append(f"  {delta_str} (+{growth:.0f}%)")
            lines.append("")

        lines.append("Use /hot, /movers, /discover, /new for full lists")

        message = "\n".join(lines)

        # Send to all users
        for user in users:
            try:
                await app.bot.send_message(
                    chat_id=user["telegram_id"],
                    text=message,
                    parse_mode="Markdown",
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
