"""
Alert checking logic for Polymarket Telegram Bot.
Detects volume milestones, price movements, and new markets.
"""

import logging
from config import BIG_MOVE_THRESHOLD, VOLUME_THRESHOLDS, VELOCITY_THRESHOLDS
from polymarket import get_unique_events, get_popular_markets, get_all_markets_paginated
from database import (
    is_market_seen,
    mark_markets_seen_bulk,
    get_price_from_hours_ago,
    save_price_snapshots_bulk,
    get_uncrossed_thresholds,
    record_milestone,
    record_milestones_bulk,
    get_volume_baselines_bulk,
    update_volume_baselines_bulk,
    is_volume_seeded,
    mark_volume_seeded,
    get_volume_deltas_bulk,
)

logger = logging.getLogger(__name__)


async def check_new_markets(
    limit: int = 100,
    min_volume: float = 0,
    mark_seen: bool = True
) -> list[dict]:
    """
    Check for new markets that haven't been seen before.

    Args:
        limit: Max events to fetch from API
        min_volume: Minimum volume to consider (filters out $0 markets)
        mark_seen: If True, mark returned markets as seen

    Returns:
        List of new market dicts (not seen before)
    """
    # Fetch current markets
    events = await get_unique_events(limit=limit, include_spam=False)

    # Filter by minimum volume
    if min_volume > 0:
        events = [e for e in events if e.get("total_volume", 0) >= min_volume]

    # Find markets we haven't seen
    new_markets = []
    for event in events:
        slug = event.get("slug")
        if slug and not is_market_seen(slug):
            new_markets.append(event)

    # Mark them as seen so we don't alert again
    if mark_seen and new_markets:
        mark_markets_seen_bulk(new_markets)

    return new_markets


async def check_price_movements(
    limit: int = 100,
    threshold: float = None,
    hours: int = 1,
    save_snapshots: bool = True,
    min_volume: float = 5000,
    min_volume_delta: float = 2000,
) -> list[dict]:
    """
    Check for markets with significant price movements WITH volume confirmation.

    Phase 2: Only alert if price move has real volume behind it.

    Args:
        limit: Max events to fetch from API
        threshold: Minimum % change to alert (default: BIG_MOVE_THRESHOLD from config)
        hours: How far back to compare prices
        save_snapshots: If True, save current prices as new snapshots
        min_volume: Minimum total volume required ($5K default)
        min_volume_delta: Minimum volume change in last hour ($2K default)

    Returns:
        List of dicts with market info and price change details
    """
    if threshold is None:
        threshold = BIG_MOVE_THRESHOLD

    # Fetch current markets
    events = await get_unique_events(limit=limit, include_spam=False)

    # Get volume deltas for confirmation
    slugs = [e.get("slug") for e in events if e.get("slug")]
    volume_deltas = get_volume_deltas_bulk(slugs, hours=1)

    big_moves = []
    events_to_snapshot = []
    candidates = 0  # Track how many passed price threshold but failed volume

    for event in events:
        slug = event.get("slug")
        current_price = event.get("yes_price", 0)
        total_volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Get old price from snapshots
        old_price = get_price_from_hours_ago(slug, hours=hours)

        if old_price is not None:
            # Calculate change
            change = current_price - old_price

            # Check if it exceeds threshold (absolute value)
            if abs(change) >= threshold:
                candidates += 1

                # VOLUME CONFIRMATION (Phase 2)
                # Large markets ($500K+): price move alone is signal
                # Small/medium markets: need volume + delta confirmation
                volume_delta = volume_deltas.get(slug, 0)
                large_market_threshold = 500_000

                passes_confirmation = (
                    total_volume >= large_market_threshold or  # Large market exception
                    (total_volume >= min_volume and volume_delta >= min_volume_delta)  # Standard confirmation
                )

                if passes_confirmation:
                    big_moves.append({
                        "title": event.get("title"),
                        "slug": slug,
                        "old_price": old_price,
                        "new_price": current_price,
                        "change": change,
                        "total_volume": total_volume,
                        "volume_delta": volume_delta,
                        "tags": event.get("tags", []),
                    })

        # Track for snapshot saving
        events_to_snapshot.append(event)

    # Log filtering stats
    if candidates > 0:
        logger.info(f"Big moves: {candidates} candidates, {len(big_moves)} passed volume confirmation")

    # Save current prices as new snapshots
    if save_snapshots and events_to_snapshot:
        save_price_snapshots_bulk(events_to_snapshot)

    # Sort by absolute change (biggest moves first)
    big_moves.sort(key=lambda x: abs(x["change"]), reverse=True)

    return big_moves


def format_new_market_alert(market: dict) -> str:
    """Format a new market for Telegram message."""
    title = market.get("title", "Unknown")
    volume = market.get("total_volume", 0)
    slug = market.get("slug", "")
    yes_price = market.get("yes_price", 0)

    # Format volume
    if volume >= 1_000_000:
        volume_str = f"${volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        volume_str = f"${volume / 1_000:.1f}K"
    else:
        volume_str = f"${volume:.0f}"

    return f"""New Market

- {title}
  YES: {yes_price:.0f}% | Volume: {volume_str}
  polymarket.com/event/{slug}"""


def format_price_move_alert(move: dict) -> str:
    """Format a price movement for Telegram message."""
    title = move.get("title", "Unknown")
    old_price = move.get("old_price", 0)
    new_price = move.get("new_price", 0)
    change = move.get("change", 0)
    volume = move.get("total_volume", 0)
    volume_delta = move.get("volume_delta", 0)
    slug = move.get("slug", "")

    # Format volume
    if volume >= 1_000_000:
        volume_str = f"${volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        volume_str = f"${volume / 1_000:.1f}K"
    else:
        volume_str = f"${volume:.0f}"

    # Format volume delta
    if volume_delta >= 1_000:
        delta_str = f"+${volume_delta / 1_000:.1f}K/hr"
    else:
        delta_str = f"+${volume_delta:.0f}/hr"

    # Format change with sign
    change_str = f"+{change:.0f}%" if change > 0 else f"{change:.0f}%"

    return f"""Big Move Alert

- {title}
  YES: {old_price:.0f}% -> {new_price:.0f}% ({change_str})
  Volume: {volume_str} ({delta_str})
  polymarket.com/event/{slug}"""


# ============================================
# Volume milestone functions (Option C: baseline + delta)
# ============================================

async def seed_volume_baselines(
    target_count: int = 500,
    thresholds: list[int] = None
) -> dict:
    """
    One-time seed: scan existing markets, record baselines and crossed thresholds.
    Does NOT send any alerts. Call this on fresh deploy.

    Args:
        target_count: How many markets to fetch (uses pagination)
        thresholds: Volume thresholds to track

    Returns:
        Dict with stats: {"markets_scanned": N, "milestones_recorded": M}
    """
    if thresholds is None:
        thresholds = VOLUME_THRESHOLDS

    logger.info(f"Seeding volume baselines (target: {target_count} markets)...")

    # Fetch many markets with pagination
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    logger.info(f"Fetched {len(events)} unique markets for seeding")

    # Prepare bulk inserts
    baselines_to_insert = []  # (slug, volume)
    milestones_to_insert = []  # (slug, threshold, volume)

    for event in events:
        slug = event.get("slug")
        volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Record current volume as baseline
        baselines_to_insert.append((slug, volume))

        # Record all thresholds already crossed (without alerting)
        for threshold in thresholds:
            if volume >= threshold:
                milestones_to_insert.append((slug, threshold, volume))

    # Bulk insert baselines
    update_volume_baselines_bulk(baselines_to_insert)

    # Bulk insert milestones
    record_milestones_bulk(milestones_to_insert)

    # Mark as seeded
    mark_volume_seeded()

    stats = {
        "markets_scanned": len(events),
        "baselines_recorded": len(baselines_to_insert),
        "milestones_recorded": len(milestones_to_insert),
    }

    logger.info(f"Seeding complete: {stats}")
    return stats


async def check_volume_milestones(
    target_count: int = 500,
    thresholds: list[int] = None,
    record: bool = True
) -> list[dict]:
    """
    Check for markets that have ACTUALLY crossed volume thresholds.
    Uses delta logic: previous volume < threshold <= current volume.

    This is the KEY signal - a market hitting $10K or $50K means real interest.

    Args:
        target_count: How many markets to fetch (uses pagination)
        thresholds: Volume thresholds to check
        record: If True, record milestones and update baselines

    Returns:
        List of dicts with market info and milestone details
    """
    if thresholds is None:
        thresholds = VOLUME_THRESHOLDS

    # Check if we need to seed first
    if not is_volume_seeded():
        logger.info("Volume baselines not seeded yet, seeding now...")
        await seed_volume_baselines(target_count=target_count, thresholds=thresholds)
        # After seeding, no crossings to report (all existing ones were recorded)
        return []

    # Fetch markets with pagination
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    # Get all slugs for bulk baseline lookup
    slugs = [e.get("slug") for e in events if e.get("slug")]
    baselines = get_volume_baselines_bulk(slugs)

    milestones_crossed = []
    baselines_to_update = []

    for event in events:
        slug = event.get("slug")
        current_volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Get previous volume from baseline
        previous_volume = baselines.get(slug)

        if previous_volume is None:
            # New market we haven't seen - record baseline, no alert
            baselines_to_update.append((slug, current_volume))
            # Record any thresholds already crossed (silently)
            for threshold in thresholds:
                if current_volume >= threshold:
                    if record:
                        record_milestone(slug, threshold, current_volume)
            continue

        # Find thresholds that were CROSSED (prev < threshold <= current)
        crossed_thresholds = []
        for threshold in thresholds:
            if previous_volume < threshold <= current_volume:
                crossed_thresholds.append(threshold)
                if record:
                    record_milestone(slug, threshold, current_volume)

        if crossed_thresholds:
            # Alert for highest threshold, but note if multiple crossed
            highest = max(crossed_thresholds)
            also_crossed = [t for t in crossed_thresholds if t != highest]

            milestones_crossed.append({
                "title": event.get("title"),
                "slug": slug,
                "threshold": highest,
                "also_crossed": also_crossed,  # Other thresholds crossed in same check
                "previous_volume": previous_volume,
                "current_volume": current_volume,
                "yes_price": event.get("yes_price", 0),
                "tags": event.get("tags", []),
            })

        # Update baseline with current volume
        baselines_to_update.append((slug, current_volume))

    # Bulk update baselines
    if record and baselines_to_update:
        update_volume_baselines_bulk(baselines_to_update)

    # Sort by threshold (higher = more significant)
    milestones_crossed.sort(key=lambda x: x["threshold"], reverse=True)

    return milestones_crossed


async def check_velocity_alerts(
    target_count: int = 500,
    thresholds: list[int] = None,
) -> list[dict]:
    """
    Check for markets with high velocity (money flowing in fast).
    This catches breaking news - markets gaining $10K+/hr.

    Args:
        target_count: How many markets to fetch
        thresholds: Velocity thresholds to check ($/hr)

    Returns:
        List of dicts with market info and velocity details
    """
    if thresholds is None:
        thresholds = VELOCITY_THRESHOLDS

    # Fetch markets
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    # Get volume deltas for last hour
    slugs = [e.get("slug") for e in events if e.get("slug")]
    deltas = get_volume_deltas_bulk(slugs, hours=1)

    velocity_alerts = []
    min_threshold = min(thresholds)

    for event in events:
        slug = event.get("slug")
        if not slug or slug not in deltas:
            continue

        velocity = deltas[slug]

        # Only care about positive velocity above minimum threshold
        if velocity >= min_threshold:
            # Find the highest threshold crossed
            crossed = [t for t in thresholds if velocity >= t]
            if crossed:
                velocity_alerts.append({
                    "title": event.get("title"),
                    "slug": slug,
                    "velocity": velocity,
                    "threshold": max(crossed),
                    "total_volume": event.get("total_volume", 0),
                    "yes_price": event.get("yes_price", 0),
                    "tags": event.get("tags", []),
                })

    # Sort by velocity (highest first)
    velocity_alerts.sort(key=lambda x: x["velocity"], reverse=True)

    return velocity_alerts


def format_volume_milestone_alert(milestone: dict) -> str:
    """Format a volume milestone for Telegram message."""
    title = milestone.get("title", "Unknown")
    threshold = milestone.get("threshold", 0)
    also_crossed = milestone.get("also_crossed", [])
    volume = milestone.get("current_volume", 0)
    yes_price = milestone.get("yes_price", 0)
    slug = milestone.get("slug", "")

    def format_amount(amount: float) -> str:
        if amount >= 1_000_000:
            return f"${amount / 1_000_000:.1f}M"
        elif amount >= 1_000:
            return f"${amount / 1_000:.0f}K"
        else:
            return f"${amount:.0f}"

    threshold_str = format_amount(threshold)
    volume_str = format_amount(volume)

    # Build crossed text
    crossed_text = f"Crossed {threshold_str}"
    if also_crossed:
        also_str = ", ".join(format_amount(t) for t in sorted(also_crossed))
        crossed_text += f" (also passed {also_str})"

    return f"""Volume Milestone

- {title}
  {crossed_text}
  Now at {volume_str} | YES: {yes_price:.0f}%
  polymarket.com/event/{slug}"""


# ============================================
# Bundled alert formatters (Phase 3)
# ============================================

def _format_volume(amount: float) -> str:
    """Helper to format volume amounts."""
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    elif amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    else:
        return f"${amount:.0f}"


def format_bundled_milestones(milestones: list[dict]) -> str:
    """Format multiple volume milestones into one bundled message."""
    if not milestones:
        return ""

    lines = ["Volume Milestones", ""]

    for m in milestones:
        title = m.get("title", "Unknown")[:45]
        threshold = m.get("threshold", 0)
        volume = m.get("current_volume", 0)
        yes_price = m.get("yes_price", 0)
        slug = m.get("slug", "")

        threshold_str = _format_volume(threshold)
        volume_str = _format_volume(volume)

        lines.append(f"- {title}")
        lines.append(f"  Crossed {threshold_str} | Now {volume_str} | YES: {yes_price:.0f}%")
        lines.append(f"  polymarket.com/event/{slug}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_big_moves(moves: list[dict]) -> str:
    """Format multiple big moves into one bundled message."""
    if not moves:
        return ""

    lines = ["Big Price Moves", ""]

    for m in moves:
        title = m.get("title", "Unknown")[:45]
        old_price = m.get("old_price", 0)
        new_price = m.get("new_price", 0)
        change = m.get("change", 0)
        volume = m.get("total_volume", 0)
        slug = m.get("slug", "")

        change_str = f"+{change:.0f}%" if change > 0 else f"{change:.0f}%"
        volume_str = _format_volume(volume)

        lines.append(f"- {title}")
        lines.append(f"  {old_price:.0f}% -> {new_price:.0f}% ({change_str}) | {volume_str}")
        lines.append(f"  polymarket.com/event/{slug}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_velocity(alerts: list[dict]) -> str:
    """Format multiple velocity alerts into one bundled message."""
    if not alerts:
        return ""

    lines = ["Money Moving Fast", ""]

    for a in alerts:
        title = a.get("title", "Unknown")[:45]
        velocity = a.get("velocity", 0)
        total_volume = a.get("total_volume", 0)
        yes_price = a.get("yes_price", 0)
        slug = a.get("slug", "")

        velocity_str = _format_volume(velocity) + "/hr"
        total_str = _format_volume(total_volume)

        lines.append(f"- {title}")
        lines.append(f"  {velocity_str} flowing in | Total: {total_str} | YES: {yes_price:.0f}%")
        lines.append(f"  polymarket.com/event/{slug}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_new_markets(markets: list[dict]) -> str:
    """Format multiple new markets into one bundled message."""
    if not markets:
        return ""

    lines = ["New Markets", ""]

    for m in markets:
        title = m.get("title", "Unknown")[:45]
        volume = m.get("total_volume", 0)
        yes_price = m.get("yes_price", 0)
        slug = m.get("slug", "")

        volume_str = _format_volume(volume)

        lines.append(f"- {title}")
        lines.append(f"  YES: {yes_price:.0f}% | Volume: {volume_str}")
        lines.append(f"  polymarket.com/event/{slug}")
        lines.append("")

    return "\n".join(lines).strip()


# ============================================
# Test function - run this file directly to test
# ============================================

async def test_alerts():
    """Test alert functions manually."""
    print("=" * 60)
    print("TESTING ALERT FUNCTIONS")
    print("=" * 60)

    # Test 1: Check for new markets (without marking seen)
    print("\n[TEST 1] Checking for new markets...")
    new_markets = await check_new_markets(limit=50, min_volume=0, mark_seen=False)
    print(f"   Found {len(new_markets)} new markets")

    if new_markets:
        print("\n   Sample new market alert:")
        print("-" * 40)
        print(format_new_market_alert(new_markets[0]))
        print("-" * 40)

    # Test 2: Check for price movements
    # Note: On first run, there won't be old snapshots, so no moves detected
    print("\n[TEST 2] Checking for price movements...")
    print("   (First run will save snapshots but find no moves)")
    big_moves = await check_price_movements(limit=50, threshold=10, save_snapshots=True)
    print(f"   Found {len(big_moves)} big moves")

    if big_moves:
        print("\n   Sample price move alert:")
        print("-" * 40)
        print(format_price_move_alert(big_moves[0]))
        print("-" * 40)
    else:
        print("   (Run again after some time to detect price changes)")

    # Test 3: Show what would happen if we mark markets as seen
    print("\n[TEST 3] Simulating mark_seen flow...")
    print(f"   If we marked {len(new_markets)} markets as seen,")
    print("   they won't appear in future new market checks.")

    print("\n" + "=" * 60)
    print("ALERT TEST COMPLETE")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Run this test again to see price movement detection")
    print("2. Markets will be compared to snapshots saved just now")


if __name__ == "__main__":
    import asyncio
    from database import init_database

    # Make sure database is initialized
    init_database()

    asyncio.run(test_alerts())
