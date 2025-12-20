"""
Alert checking logic for Polymarket Telegram Bot.
Detects volume milestones, price movements, and new markets.
"""

from config import BIG_MOVE_THRESHOLD, VOLUME_THRESHOLDS
from polymarket import get_unique_events, get_popular_markets
from database import (
    is_market_seen,
    mark_markets_seen_bulk,
    get_price_from_hours_ago,
    save_price_snapshots_bulk,
    get_uncrossed_thresholds,
    record_milestone,
)


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
    save_snapshots: bool = True
) -> list[dict]:
    """
    Check for markets with significant price movements.

    Args:
        limit: Max events to fetch from API
        threshold: Minimum % change to alert (default: BIG_MOVE_THRESHOLD from config)
        hours: How far back to compare prices
        save_snapshots: If True, save current prices as new snapshots

    Returns:
        List of dicts with market info and price change details
    """
    if threshold is None:
        threshold = BIG_MOVE_THRESHOLD

    # Fetch current markets
    events = await get_unique_events(limit=limit, include_spam=False)

    big_moves = []
    events_to_snapshot = []

    for event in events:
        slug = event.get("slug")
        current_price = event.get("yes_price", 0)

        if not slug:
            continue

        # Get old price from snapshots
        old_price = get_price_from_hours_ago(slug, hours=hours)

        if old_price is not None:
            # Calculate change
            change = current_price - old_price

            # Check if it exceeds threshold (absolute value)
            if abs(change) >= threshold:
                big_moves.append({
                    "title": event.get("title"),
                    "slug": slug,
                    "old_price": old_price,
                    "new_price": current_price,
                    "change": change,
                    "total_volume": event.get("total_volume", 0),
                    "tags": event.get("tags", []),
                })

        # Track for snapshot saving
        events_to_snapshot.append(event)

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
    slug = move.get("slug", "")

    # Format volume
    if volume >= 1_000_000:
        volume_str = f"${volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        volume_str = f"${volume / 1_000:.1f}K"
    else:
        volume_str = f"${volume:.0f}"

    # Format change with sign
    change_str = f"+{change:.0f}%" if change > 0 else f"{change:.0f}%"

    return f"""Big Move Alert

- {title}
  YES: {old_price:.0f}% -> {new_price:.0f}% ({change_str})
  Volume: {volume_str}
  polymarket.com/event/{slug}"""


async def check_volume_milestones(
    limit: int = 100,
    thresholds: list[int] = None,
    record: bool = True
) -> list[dict]:
    """
    Check for markets that have crossed volume thresholds for the first time.

    This is the KEY signal - a market hitting $10K or $50K means real interest.

    Args:
        limit: Max events to fetch from API
        thresholds: Volume thresholds to check (default: VOLUME_THRESHOLDS from config)
        record: If True, record milestones so we don't alert again

    Returns:
        List of dicts with market info and milestone details
    """
    if thresholds is None:
        thresholds = VOLUME_THRESHOLDS

    # Fetch popular markets (high volume ones)
    events = await get_popular_markets(limit=limit, include_spam=False)

    milestones_crossed = []

    for event in events:
        slug = event.get("slug")
        volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Get thresholds this market hasn't crossed yet
        uncrossed = get_uncrossed_thresholds(slug, thresholds)

        # Check which thresholds were just crossed
        for threshold in uncrossed:
            if volume >= threshold:
                milestones_crossed.append({
                    "title": event.get("title"),
                    "slug": slug,
                    "threshold": threshold,
                    "current_volume": volume,
                    "yes_price": event.get("yes_price", 0),
                    "tags": event.get("tags", []),
                })

                # Record the milestone so we don't alert again
                if record:
                    record_milestone(slug, threshold, volume)

    # Sort by threshold (higher thresholds = more significant)
    milestones_crossed.sort(key=lambda x: x["threshold"], reverse=True)

    return milestones_crossed


def format_volume_milestone_alert(milestone: dict) -> str:
    """Format a volume milestone for Telegram message."""
    title = milestone.get("title", "Unknown")
    threshold = milestone.get("threshold", 0)
    volume = milestone.get("current_volume", 0)
    yes_price = milestone.get("yes_price", 0)
    slug = milestone.get("slug", "")

    # Format threshold nicely
    if threshold >= 1_000_000:
        threshold_str = f"${threshold / 1_000_000:.0f}M"
    elif threshold >= 1_000:
        threshold_str = f"${threshold / 1_000:.0f}K"
    else:
        threshold_str = f"${threshold:.0f}"

    # Format current volume
    if volume >= 1_000_000:
        volume_str = f"${volume / 1_000_000:.1f}M"
    elif volume >= 1_000:
        volume_str = f"${volume / 1_000:.1f}K"
    else:
        volume_str = f"${volume:.0f}"

    return f"""Volume Milestone

- {title}
  Crossed {threshold_str} (now {volume_str})
  YES: {yes_price:.0f}%
  polymarket.com/event/{slug}"""


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
