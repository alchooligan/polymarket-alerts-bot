"""
Alert checking logic for Polymarket Telegram Bot.
Detects volume milestones, price movements, and new markets.
"""

import logging
from config import (
    BIG_MOVE_THRESHOLD,
    VOLUME_THRESHOLDS,
    VELOCITY_THRESHOLDS,
    DISCOVERY_MIN_VOLUME,
    CLOSING_SOON_HOURS,
    CLOSING_SOON_MIN_VELOCITY,
    SPORTS_SLUG_PATTERNS,
    SPORTS_TITLE_KEYWORDS,
    CATEGORY_FILTERS,
)
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


def is_resolved_price(yes_price: float) -> bool:
    """
    Check if a market is essentially resolved (YES >= 95% or YES <= 5%).
    Only filter truly resolved markets - 90%+ can still revert on news.

    Args:
        yes_price: Current YES price (0-100 scale)

    Returns:
        True if market is essentially resolved
    """
    return yes_price >= 95 or yes_price <= 5


def filter_resolved(events: list[dict]) -> list[dict]:
    """Filter out markets that are essentially resolved (95%+ or 5%-)."""
    return [e for e in events if not is_resolved_price(e.get("yes_price", 50))]


def is_sports_market(event: dict) -> bool:
    """
    Check if a market is sports/esports related.
    These are excluded because there's no information edge on sports betting.
    """
    slug = event.get("slug", "").lower()
    title = event.get("title", "")

    # Check slug patterns
    for pattern in SPORTS_SLUG_PATTERNS:
        if pattern.lower() in slug:
            return True

    # Check title keywords
    for keyword in SPORTS_TITLE_KEYWORDS:
        if keyword.lower() in title.lower():
            return True

    return False


def filter_sports(events: list[dict]) -> list[dict]:
    """Filter out sports/esports markets from a list."""
    return [e for e in events if not is_sports_market(e)]


def matches_category(event: dict, category: str) -> bool:
    """
    Check if a market matches a category filter.
    Matches against tags and title keywords.
    """
    if category not in CATEGORY_FILTERS:
        return False

    filter_config = CATEGORY_FILTERS[category]
    tags = filter_config.get("tags", [])
    title_keywords = filter_config.get("title_keywords", [])

    # Check tags
    event_tags = event.get("tags", [])
    if isinstance(event_tags, list):
        for tag in event_tags:
            tag_lower = tag.lower() if isinstance(tag, str) else ""
            for filter_tag in tags:
                if filter_tag.lower() in tag_lower:
                    return True

    # Check title keywords
    title = event.get("title", "")
    for keyword in title_keywords:
        if keyword.lower() in title.lower():
            return True

    return False


def filter_by_category(events: list[dict], category: str) -> list[dict]:
    """Filter events to only include those matching a category."""
    if not category or category not in CATEGORY_FILTERS:
        return events
    return [e for e in events if matches_category(e, category)]


def get_available_categories() -> list[str]:
    """Get list of available category filters."""
    return list(CATEGORY_FILTERS.keys())


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


def is_recently_created(created_at_str: str, max_hours: int = 48) -> bool:
    """
    Check if a market was created within the last N hours.

    Args:
        created_at_str: ISO date string from API
        max_hours: Maximum age in hours to be considered "recent"

    Returns:
        True if created within max_hours, False otherwise or if date invalid
    """
    from datetime import datetime, timezone

    if not created_at_str:
        return False

    try:
        # Handle various date formats
        if "T" in str(created_at_str):
            created_date = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        else:
            return False

        now = datetime.now(timezone.utc)
        hours_old = (now - created_date).total_seconds() / 3600

        return hours_old <= max_hours
    except:
        return False


async def check_volume_milestones(
    target_count: int = 500,
    thresholds: list[int] = None,
    record: bool = True,
    min_discovery_volume: int = None,
    discovery_max_age_hours: int = 48,
) -> tuple[list[dict], list[dict]]:
    """
    Check for markets that have ACTUALLY crossed volume thresholds.
    Uses delta logic: previous volume < threshold <= current volume.

    Also returns "discoveries" - markets CREATED RECENTLY that launched
    with significant volume (catches new markets that hit the ground running).

    Args:
        target_count: How many markets to fetch (uses pagination)
        thresholds: Volume thresholds to check
        record: If True, record milestones and update baselines
        min_discovery_volume: Minimum volume for a first-seen market to be a "discovery"
        discovery_max_age_hours: Max age to qualify as a "discovery" (default 48h)

    Returns:
        Tuple of (milestones_crossed, discoveries)
    """
    if thresholds is None:
        thresholds = VOLUME_THRESHOLDS
    if min_discovery_volume is None:
        min_discovery_volume = DISCOVERY_MIN_VOLUME

    # Check if we need to seed first
    if not is_volume_seeded():
        logger.info("Volume baselines not seeded yet, seeding now...")
        await seed_volume_baselines(target_count=target_count, thresholds=thresholds)
        # After seeding, no crossings to report (all existing ones were recorded)
        return [], []

    # Fetch markets with pagination
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    # Filter out sports markets and resolved markets (no edge)
    events = filter_sports(events)
    events = filter_resolved(events)

    # Get all slugs for bulk baseline lookup
    slugs = [e.get("slug") for e in events if e.get("slug")]
    baselines = get_volume_baselines_bulk(slugs)

    # Get velocity data for rich alerts
    velocity_1h = get_volume_deltas_bulk(slugs, hours=1)

    milestones_crossed = []
    discoveries = []
    baselines_to_update = []

    for event in events:
        slug = event.get("slug")
        current_volume = event.get("total_volume", 0)
        created_at = event.get("created_at", "")

        if not slug:
            continue

        # Get previous volume from baseline
        previous_volume = baselines.get(slug)

        if previous_volume is None:
            # New market we haven't seen before
            baselines_to_update.append((slug, current_volume))

            # Record thresholds already crossed
            for threshold in thresholds:
                if current_volume >= threshold:
                    if record:
                        record_milestone(slug, threshold, current_volume)

            # DISCOVERY: must be ACTUALLY NEW (created within 48h) + significant volume
            # This prevents alerting on old markets we just hadn't seen yet
            if current_volume >= min_discovery_volume and is_recently_created(created_at, discovery_max_age_hours):
                # Find highest threshold it's already at
                crossed = [t for t in thresholds if current_volume >= t]
                highest = max(crossed) if crossed else min_discovery_volume

                # Calculate velocity for rich alert
                velocity = velocity_1h.get(slug, 0)
                velocity_pct = (velocity / current_volume * 100) if current_volume > 0 else 0

                discoveries.append({
                    "title": event.get("title"),
                    "slug": slug,
                    "current_volume": current_volume,
                    "threshold": highest,
                    "yes_price": event.get("yes_price", 0),
                    "tags": event.get("tags", []),
                    "created_at": created_at,
                    "velocity": velocity,
                    "velocity_pct": velocity_pct,
                    "is_discovery": True,
                })
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

            # Calculate velocity for rich alert
            velocity = velocity_1h.get(slug, 0)
            velocity_pct = (velocity / current_volume * 100) if current_volume > 0 else 0

            milestones_crossed.append({
                "title": event.get("title"),
                "slug": slug,
                "threshold": highest,
                "also_crossed": also_crossed,
                "previous_volume": previous_volume,
                "current_volume": current_volume,
                "yes_price": event.get("yes_price", 0),
                "tags": event.get("tags", []),
                "velocity": velocity,
                "velocity_pct": velocity_pct,
            })

        # Update baseline with current volume
        baselines_to_update.append((slug, current_volume))

    # Bulk update baselines
    if record and baselines_to_update:
        update_volume_baselines_bulk(baselines_to_update)

    # Sort by threshold/volume (higher = more significant)
    milestones_crossed.sort(key=lambda x: x["threshold"], reverse=True)
    discoveries.sort(key=lambda x: x["current_volume"], reverse=True)

    return milestones_crossed, discoveries


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

    # Filter out resolved markets (no edge on 95%+ or 5%- markets)
    events = filter_resolved(events)

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


def _escape_markdown(text: str) -> str:
    """Escape markdown special characters in text."""
    return text.replace("[", "\\[").replace("]", "\\]").replace("(", "\\(").replace(")", "\\)").replace("_", "\\_").replace("*", "\\*")


def _format_odds(market: dict) -> str:
    """
    Format odds line based on market outcomes.

    Binary markets (2 outcomes): "YES 26% · NO 74%"
    Multi-outcome (3+): "Top: Elon Musk 51% · Tim Cook 23%"
    Fallback: "YES at X%" if no outcomes data
    """
    outcomes = market.get("outcomes", [])
    all_outcomes = market.get("all_outcomes", outcomes)  # Aggregated outcomes

    # Use all_outcomes if available (more complete for multi-outcome)
    if all_outcomes and len(all_outcomes) > len(outcomes):
        outcomes = all_outcomes

    # Sort by price descending
    if outcomes:
        outcomes = sorted(outcomes, key=lambda x: x.get("price", 0), reverse=True)

    if not outcomes or len(outcomes) < 2:
        # Fallback to yes_price
        yes_price = market.get("yes_price", 50)
        return f"YES at {yes_price:.0f}%"

    if len(outcomes) == 2:
        # Binary market - show YES X% · NO Y%
        # Find Yes/No outcomes
        yes_outcome = None
        no_outcome = None
        for o in outcomes:
            name_lower = o.get("name", "").lower()
            if name_lower in ["yes", "y"]:
                yes_outcome = o
            elif name_lower in ["no", "n"]:
                no_outcome = o

        if yes_outcome and no_outcome:
            return f"YES {yes_outcome['price']:.0f}% · NO {no_outcome['price']:.0f}%"
        else:
            # Not standard yes/no, show as top 2
            top1 = outcomes[0]
            top2 = outcomes[1]
            return f"{top1['name']} {top1['price']:.0f}% · {top2['name']} {top2['price']:.0f}%"

    else:
        # Multi-outcome (3+) - show top 2
        top1 = outcomes[0]
        top2 = outcomes[1]
        return f"Top: {top1['name']} {top1['price']:.0f}% · {top2['name']} {top2['price']:.0f}%"


def format_market_card(
    market: dict,
    style: str = "full",
    show_emoji: bool = True,
    context: str = None,
) -> str:
    """
    UNIVERSAL MARKET CARD FORMAT
    Single source of truth for displaying any market, everywhere.

    Args:
        market: Dict with market data (title, slug, yes_price, total_volume, etc.)
        style: "full" (complete card), "compact" (single line), "alert" (for push)
        show_emoji: Whether to show indicator emojis
        context: Optional context line (e.g., "Crossed $100K", "Created 5h ago")

    Returns:
        Formatted market card string
    """
    from datetime import datetime, timezone

    # Extract data with defaults
    title = market.get("title", "Unknown")
    slug = market.get("slug", "")
    yes_price = market.get("yes_price", 50)
    total_volume = market.get("total_volume", 0)

    # Velocity data
    velocity = market.get("velocity", 0)
    velocity_pct = market.get("velocity_pct", 0)

    # Volume deltas
    delta_6h = market.get("delta_6h", 0)
    volume_growth_pct = market.get("volume_growth_pct", 0)

    # Price deltas
    price_change_6h = market.get("price_change_6h", 0)
    price_change_24h = market.get("price_change_24h", 0)
    old_price = market.get("old_price", 0)

    # End date
    end_date_str = market.get("end_date") or market.get("endDate", "")

    # === EMOJI LOGIC ===
    vel_emoji = ""
    if show_emoji and velocity_pct >= 20:
        vel_emoji = " 🔥🔥"
    elif show_emoji and velocity_pct >= 10:
        vel_emoji = " 🔥"

    vol_emoji = ""
    if show_emoji and volume_growth_pct >= 50:
        vol_emoji = " 🔥"

    price_6h_emoji = ""
    if show_emoji:
        if price_change_6h >= 15:
            price_6h_emoji = " 🚀"
        elif price_change_6h <= -15:
            price_6h_emoji = " 💀"
        elif price_change_6h >= 8:
            price_6h_emoji = " ⬆️"
        elif price_change_6h <= -8:
            price_6h_emoji = " ⬇️"

    price_24h_emoji = ""
    if show_emoji:
        if price_change_24h >= 15:
            price_24h_emoji = " 🚀"
        elif price_change_24h <= -15:
            price_24h_emoji = " 💀"
        elif price_change_24h >= 8:
            price_24h_emoji = " ⬆️"
        elif price_change_24h <= -8:
            price_24h_emoji = " ⬇️"

    # === FORMAT HELPERS ===
    vol_str = _format_volume(total_volume)
    vel_abs_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

    # 6h volume change
    if delta_6h > 0:
        delta_6h_str = f"+${delta_6h/1000:.0f}K" if delta_6h >= 1000 else f"+${delta_6h:.0f}"
        vol_6h_str = f"{delta_6h_str} (+{volume_growth_pct:.0f}%){vol_emoji}"
    else:
        vol_6h_str = "—"

    # Price changes
    if price_change_6h != 0:
        p6_sign = "+" if price_change_6h > 0 else ""
        price_6h_str = f"{p6_sign}{price_change_6h:.0f}%{price_6h_emoji}"
    else:
        price_6h_str = "flat"

    if price_change_24h != 0:
        p24_sign = "+" if price_change_24h > 0 else ""
        price_24h_str = f"{p24_sign}{price_change_24h:.0f}%{price_24h_emoji}"
    else:
        price_24h_str = "—"

    # Closes time
    closes_str = ""
    if end_date_str:
        try:
            if "T" in str(end_date_str):
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                hours_left = (end_date - now).total_seconds() / 3600
                if hours_left > 0:
                    if hours_left < 1:
                        closes_str = f"{hours_left*60:.0f}m left"
                        if show_emoji:
                            closes_str += " ⏰"
                    elif hours_left < 24:
                        closes_str = f"{hours_left:.0f}h left"
                        if show_emoji:
                            closes_str += " ⏳"
                    else:
                        closes_str = f"{hours_left/24:.0f} days"
        except:
            pass

    # === COMPACT FORMAT (for lists) ===
    if style == "compact":
        # Single line: title — price | velocity | 6h change
        title_short = title[:40] + "..." if len(title) > 40 else title
        return f"{title_short} — {yes_price:.0f}% | {vel_abs_str} ({velocity_pct:.1f}%/hr){vel_emoji} | 6h: {price_6h_str}"

    # === FULL FORMAT ===
    lines = []

    # Title as clickable link (truncate to 50 chars)
    title_display = title[:50] + "..." if len(title) > 50 else title
    # Escape markdown special chars in title
    title_escaped = _escape_markdown(title_display)
    lines.append(f"[{title_escaped}](https://polymarket.com/event/{slug})")
    lines.append("")

    # Context line if provided (e.g., "Crossed $100K", "Created 5h ago")
    if context:
        lines.append(context)

    # Odds line - show actual outcomes
    odds_str = _format_odds(market)
    lines.append(f"Odds: {odds_str}")

    # Velocity line
    lines.append(f"Velocity: {vel_abs_str} ({velocity_pct:.1f}%/hr){vel_emoji}")

    # Volume line
    lines.append(f"Volume: {vol_str} | 6h: {vol_6h_str}")

    # Price change line (use yes_price as base for deltas)
    lines.append(f"Price Δ: 6h {price_6h_str} | 24h {price_24h_str}")

    # Closes line (if available)
    if closes_str:
        lines.append(f"Closes: {closes_str}")

    return "\n".join(lines)


def format_market_list(
    markets: list[dict],
    header: str,
    explanation: str = None,
    max_full: int = 5,
    max_compact: int = 10,
    show_emoji: bool = True,
) -> str:
    """
    Format a list of markets with header and optional explanation.
    Shows first N as full cards, rest as compact lines.

    Args:
        markets: List of market dicts
        header: Title for the list (e.g., "🔥 Hottest Markets (1h)")
        explanation: Brief explanation of what this shows
        max_full: How many to show in full format
        max_compact: How many additional to show in compact format
        show_emoji: Whether to show indicator emojis
    """
    if not markets:
        return ""

    lines = [header]

    if explanation:
        lines.append(explanation)

    lines.append("")

    # Full cards for top N
    for i, m in enumerate(markets[:max_full], 1):
        lines.append(f"{i}.")
        lines.append(format_market_card(m, style="full", show_emoji=show_emoji))
        lines.append("")

    # Compact lines for rest
    if len(markets) > max_full:
        remaining = markets[max_full:max_full + max_compact]
        if remaining:
            lines.append("More:")
            for i, m in enumerate(remaining, max_full + 1):
                lines.append(f"{i}. {format_market_card(m, style='compact', show_emoji=show_emoji)}")

    return "\n".join(lines).strip()


def format_bundled_milestones(milestones: list[dict]) -> str:
    """Format multiple volume milestones into one bundled message with rich data."""
    if not milestones:
        return ""

    lines = ["*Volume Milestones*", ""]

    for m in milestones:
        title = _escape_markdown(m.get("title", "Unknown")[:45])
        threshold = m.get("threshold", 0)
        volume = m.get("current_volume", 0)
        slug = m.get("slug", "")
        velocity = m.get("velocity", 0)
        velocity_pct = m.get("velocity_pct", 0)

        threshold_str = _format_volume(threshold)
        volume_str = _format_volume(volume)
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
        odds_str = _format_odds(m)

        # Emoji for velocity
        vel_emoji = ""
        if velocity_pct >= 20:
            vel_emoji = " 🔥🔥"
        elif velocity_pct >= 10:
            vel_emoji = " 🔥"

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Crossed {threshold_str} → Now {volume_str}")
        lines.append(f"{odds_str} | {vel_str} ({velocity_pct:.1f}%/hr){vel_emoji}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_discoveries(discoveries: list[dict]) -> str:
    """Format discovery alerts - NEW markets that launched with big volume."""
    if not discoveries:
        return ""

    lines = ["*New Discovery* (created <48h, launched big)", ""]

    for d in discoveries:
        title = _escape_markdown(d.get("title", "Unknown")[:45])
        volume = d.get("current_volume", 0)
        slug = d.get("slug", "")
        velocity = d.get("velocity", 0)
        velocity_pct = d.get("velocity_pct", 0)

        volume_str = _format_volume(volume)
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
        odds_str = _format_odds(d)

        # Emoji for velocity
        vel_emoji = ""
        if velocity_pct >= 20:
            vel_emoji = " 🔥🔥"
        elif velocity_pct >= 10:
            vel_emoji = " 🔥"

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Volume: {volume_str} | {odds_str}")
        lines.append(f"Velocity: {vel_str} ({velocity_pct:.1f}%/hr){vel_emoji}")
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


async def check_underdog_alerts(
    target_count: int = 500,
    max_price: float = 20,
    min_volume: float = 50_000,
    min_price_change: float = 2.0,
) -> list[dict]:
    """
    Find underdog markets where price is moving UP (contrarian money).

    New logic: underdogs where price went UP +2% in 24h = someone betting
    against the consensus, and moving the needle.

    Args:
        target_count: How many markets to fetch
        max_price: Maximum YES price to qualify as underdog (default 20%)
        min_volume: Minimum total volume ($50K to avoid noise)
        min_price_change: Minimum price increase in 24h (default 2%)

    Returns:
        List of underdog markets with price movement data
    """
    from database import get_price_deltas_bulk

    # Fetch markets
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    # Filter out sports and resolved markets
    events = filter_sports(events)
    events = filter_resolved(events)

    # Get price changes over 24h
    slugs = [e.get("slug") for e in events if e.get("slug")]
    price_deltas_24h = get_price_deltas_bulk(slugs, hours=24)

    # Also get velocity for context
    deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

    underdogs = []

    for event in events:
        slug = event.get("slug")
        yes_price = event.get("yes_price", 50)
        total_volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Skip markets that are too small or not underdog price
        if total_volume < min_volume or yes_price > max_price:
            continue

        # Check for positive price movement in 24h
        price_data = price_deltas_24h.get(slug, {})
        price_change = price_data.get("delta", 0)
        old_price = price_data.get("old", 0)

        # Key insight: price must have gone UP (contrarian money moving needle)
        if price_change >= min_price_change:
            velocity = deltas_1h.get(slug, 0)

            underdogs.append({
                "title": event.get("title"),
                "slug": slug,
                "yes_price": yes_price,
                "old_price": old_price,
                "price_change": price_change,
                "velocity": velocity,
                "total_volume": total_volume,
                "tags": event.get("tags", []),
            })

    # Sort by price change (biggest gainers first)
    underdogs.sort(key=lambda x: x["price_change"], reverse=True)

    return underdogs


async def check_closing_soon_alerts(
    target_count: int = 500,
    hours_until_close: int = None,
    min_velocity: float = None,
) -> list[dict]:
    """
    Find markets closing soon that have significant volume activity.
    Last-minute action before resolution often indicates insider knowledge.

    Args:
        target_count: How many markets to fetch
        hours_until_close: Max hours until market closes
        min_velocity: Minimum $/hr to qualify

    Returns:
        List of closing-soon markets with activity
    """
    from datetime import datetime, timezone

    # Apply config defaults
    if hours_until_close is None:
        hours_until_close = CLOSING_SOON_HOURS
    if min_velocity is None:
        min_velocity = CLOSING_SOON_MIN_VELOCITY

    # Fetch markets
    events = await get_all_markets_paginated(
        target_count=target_count,
        include_spam=False
    )

    # Filter out sports and resolved markets
    events = filter_sports(events)
    events = filter_resolved(events)

    # Get volume deltas
    slugs = [e.get("slug") for e in events if e.get("slug")]
    deltas = get_volume_deltas_bulk(slugs, hours=1)

    closing_soon = []
    now = datetime.now(timezone.utc)

    for event in events:
        slug = event.get("slug")
        end_date_str = event.get("end_date") or event.get("endDate")

        if not slug or not end_date_str:
            continue

        # Parse end date
        try:
            # Handle various date formats
            if "T" in str(end_date_str):
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            else:
                continue
        except:
            continue

        # Check if closing within threshold
        hours_left = (end_date - now).total_seconds() / 3600
        if hours_left <= 0 or hours_left > hours_until_close:
            continue

        velocity = deltas.get(slug, 0)
        if velocity < min_velocity:
            continue

        closing_soon.append({
            "title": event.get("title"),
            "slug": slug,
            "yes_price": event.get("yes_price", 0),
            "hours_left": hours_left,
            "velocity": velocity,
            "total_volume": event.get("total_volume", 0),
            "tags": event.get("tags", []),
        })

    # Sort by hours left (most urgent first)
    closing_soon.sort(key=lambda x: x["hours_left"])

    return closing_soon


def format_bundled_underdogs(alerts: list[dict]) -> str:
    """Format underdog alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["Underdogs (YES <20% + price rising)", ""]

    for i, a in enumerate(alerts[:10], 1):
        title = a.get("title", "Unknown")[:40]
        yes_price = a.get("yes_price", 0)
        old_price = a.get("old_price", 0)
        price_change = a.get("price_change", 0)
        velocity = a.get("velocity", 0)
        total_volume = a.get("total_volume", 0)
        slug = a.get("slug", "")

        vol_str = _format_volume(total_volume)
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

        lines.append(f"{i}. {title}")
        lines.append(f"   YES: {old_price:.0f}% -> {yes_price:.0f}% (+{price_change:.0f}% in 24h)")
        lines.append(f"   Volume: {vol_str} | Velocity: {vel_str}")
        lines.append(f"   polymarket.com/event/{slug}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_closing_soon(alerts: list[dict]) -> str:
    """Format closing soon alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["*Closing Soon* (action before resolution)", ""]

    for a in alerts:
        title = _escape_markdown(a.get("title", "Unknown")[:40])
        yes_price = a.get("yes_price", 0)
        hours_left = a.get("hours_left", 0)
        velocity = a.get("velocity", 0)
        slug = a.get("slug", "")

        time_str = f"{hours_left:.0f}h left" if hours_left >= 1 else f"{hours_left*60:.0f}m left"
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"⏰ {time_str} | YES: {yes_price:.0f}% | {vel_str}")
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
# ALERTS V2 - New Alert System
# ============================================

async def check_wakeup_alerts(
    target_count: int = 500,
    quiet_threshold: float = 2.0,
    hot_threshold: float = 10.0,
    quiet_hours: int = 6,
) -> list[dict]:
    """
    Find markets that were quiet but are now waking up.

    Trigger: Was <2%/hr velocity for 6h, now >10%/hr
    This catches breaking news - market was dead, now money rushing in.

    Args:
        target_count: How many markets to fetch
        quiet_threshold: Max velocity %/hr to be considered "quiet"
        hot_threshold: Min velocity %/hr to be considered "hot now"
        quiet_hours: How long must have been quiet

    Returns:
        List of markets that just woke up
    """
    from database import get_volume_deltas_bulk

    # Fetch markets
    events = await get_all_markets_paginated(target_count=target_count, include_spam=False)
    events = filter_sports(events)
    events = filter_resolved(events)

    slugs = [e.get("slug") for e in events if e.get("slug")]

    # Get current velocity (1h)
    deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

    # Get past velocity (6h average per hour)
    deltas_6h = get_volume_deltas_bulk(slugs, hours=quiet_hours)

    wakeups = []

    for event in events:
        slug = event.get("slug")
        total_volume = event.get("total_volume", 0)

        if not slug or total_volume == 0:
            continue

        # Current velocity (1h)
        velocity_1h = deltas_1h.get(slug, 0)
        velocity_pct_now = (velocity_1h / total_volume * 100) if total_volume > 0 else 0

        # Past velocity (average per hour over 6h)
        velocity_6h = deltas_6h.get(slug, 0)
        velocity_pct_past = (velocity_6h / total_volume * 100 / quiet_hours) if total_volume > 0 else 0

        # Check: was quiet, now hot
        if velocity_pct_past < quiet_threshold and velocity_pct_now >= hot_threshold:
            wakeups.append({
                "title": event.get("title"),
                "slug": slug,
                "yes_price": event.get("yes_price", 0),
                "total_volume": total_volume,
                "velocity_past": velocity_6h / quiet_hours,
                "velocity_pct_past": velocity_pct_past,
                "velocity_now": velocity_1h,
                "velocity_pct_now": velocity_pct_now,
                "tags": event.get("tags", []),
            })

    # Sort by current velocity (hottest first)
    wakeups.sort(key=lambda x: x["velocity_pct_now"], reverse=True)

    return wakeups


async def check_fast_mover_alerts(
    target_count: int = 500,
    price_threshold: float = 10.0,
    volume_threshold: float = 10000,
    hours: int = 2,
) -> list[dict]:
    """
    Find markets with significant price moves backed by volume.

    Trigger: Price moved >=10% in 2h AND volume delta >=$10K in same period
    This catches informed money moving prices.

    Args:
        target_count: How many markets to fetch
        price_threshold: Minimum price change %
        volume_threshold: Minimum volume behind the move
        hours: Time window to check

    Returns:
        List of fast-moving markets
    """
    from database import get_volume_deltas_bulk, get_price_deltas_bulk

    # Fetch markets
    events = await get_all_markets_paginated(target_count=target_count, include_spam=False)
    events = filter_sports(events)
    events = filter_resolved(events)

    slugs = [e.get("slug") for e in events if e.get("slug")]

    # Get price and volume deltas
    price_deltas = get_price_deltas_bulk(slugs, hours=hours)
    volume_deltas = get_volume_deltas_bulk(slugs, hours=hours)
    velocity_1h = get_volume_deltas_bulk(slugs, hours=1)

    movers = []

    for event in events:
        slug = event.get("slug")
        total_volume = event.get("total_volume", 0)

        if not slug:
            continue

        # Get price change
        price_data = price_deltas.get(slug, {})
        price_change = price_data.get("delta", 0)
        old_price = price_data.get("old", 0)
        current_price = event.get("yes_price", 0)

        # Get volume behind move
        volume_behind = volume_deltas.get(slug, 0)

        # Check thresholds
        if abs(price_change) >= price_threshold and volume_behind >= volume_threshold:
            velocity = velocity_1h.get(slug, 0)
            velocity_pct = (velocity / total_volume * 100) if total_volume > 0 else 0

            movers.append({
                "title": event.get("title"),
                "slug": slug,
                "old_price": old_price,
                "current_price": current_price,
                "price_change": price_change,
                "volume_behind": volume_behind,
                "velocity": velocity,
                "velocity_pct": velocity_pct,
                "total_volume": total_volume,
                "direction": "up" if price_change > 0 else "down",
                "tags": event.get("tags", []),
            })

    # Sort by absolute price change
    movers.sort(key=lambda x: abs(x["price_change"]), reverse=True)

    return movers


async def check_early_heat_alerts(
    target_count: int = 500,
    max_age_hours: int = 24,
    max_volume: float = 50000,
    min_velocity_pct: float = 15.0,
) -> list[dict]:
    """
    Find new markets that are gaining traction fast.

    Trigger: Created <24h ago AND volume <$50K AND velocity >15%/hr
    This catches markets that are small but growing fast.

    Args:
        target_count: How many markets to fetch
        max_age_hours: Maximum age of market
        max_volume: Maximum current volume
        min_velocity_pct: Minimum velocity %/hr

    Returns:
        List of early heat markets
    """
    from datetime import datetime, timezone
    from database import get_volume_deltas_bulk, get_recently_seen_slugs

    # Get recently seen markets
    recent = get_recently_seen_slugs(hours=max_age_hours)
    recent_slugs = {r.get("event_slug"): r for r in recent}

    if not recent_slugs:
        return []

    # Fetch current market data
    events = await get_all_markets_paginated(target_count=target_count, include_spam=False)
    events = filter_sports(events)
    events = filter_resolved(events)

    # Filter to only recent markets
    events = [e for e in events if e.get("slug") in recent_slugs]

    slugs = [e.get("slug") for e in events if e.get("slug")]

    # Get velocity data
    deltas_1h = get_volume_deltas_bulk(slugs, hours=1)

    early_heat = []
    now = datetime.now(timezone.utc)

    for event in events:
        slug = event.get("slug")
        total_volume = event.get("total_volume", 0)

        if not slug or total_volume > max_volume:
            continue

        # Get velocity
        velocity = deltas_1h.get(slug, 0)
        velocity_pct = (velocity / total_volume * 100) if total_volume > 0 else 0

        if velocity_pct < min_velocity_pct:
            continue

        # Calculate hours since launch
        seen_data = recent_slugs.get(slug, {})
        first_seen = seen_data.get("first_seen_at")
        hours_ago = 0

        if first_seen:
            try:
                if isinstance(first_seen, str):
                    seen_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                else:
                    seen_dt = first_seen.replace(tzinfo=timezone.utc)
                hours_ago = (now - seen_dt).total_seconds() / 3600
            except:
                pass

        early_heat.append({
            "title": event.get("title"),
            "slug": slug,
            "yes_price": event.get("yes_price", 0),
            "total_volume": total_volume,
            "velocity": velocity,
            "velocity_pct": velocity_pct,
            "hours_ago": hours_ago,
            "tags": event.get("tags", []),
        })

    # Sort by velocity
    early_heat.sort(key=lambda x: x["velocity_pct"], reverse=True)

    return early_heat


async def check_new_launch_alerts(
    target_count: int = 500,
    max_age_hours: int = 1,
) -> list[dict]:
    """
    Find brand new markets (launched within 1 hour).

    Trigger: first_seen_at <1h ago
    This catches new market launches.

    Args:
        target_count: How many markets to fetch
        max_age_hours: Maximum age (default 1h = brand new)

    Returns:
        List of new markets
    """
    from datetime import datetime, timezone
    from database import get_recently_seen_slugs

    # Get very recently seen markets
    recent = get_recently_seen_slugs(hours=max_age_hours)
    recent_slugs = {r.get("event_slug"): r for r in recent}

    if not recent_slugs:
        return []

    # Fetch current market data
    events = await get_all_markets_paginated(target_count=target_count, include_spam=False)
    events = filter_sports(events)
    events = filter_resolved(events)

    # Filter to only recent markets
    events = [e for e in events if e.get("slug") in recent_slugs]

    new_launches = []
    now = datetime.now(timezone.utc)

    for event in events:
        slug = event.get("slug")

        if not slug:
            continue

        # Calculate hours since launch
        seen_data = recent_slugs.get(slug, {})
        first_seen = seen_data.get("first_seen_at")
        hours_ago = 0

        if first_seen:
            try:
                if isinstance(first_seen, str):
                    seen_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                else:
                    seen_dt = first_seen.replace(tzinfo=timezone.utc)
                hours_ago = (now - seen_dt).total_seconds() / 3600
            except:
                pass

        new_launches.append({
            "title": event.get("title"),
            "slug": slug,
            "yes_price": event.get("yes_price", 0),
            "total_volume": event.get("total_volume", 0),
            "hours_ago": hours_ago,
            "tags": event.get("tags", []),
        })

    # Sort by most recent first
    new_launches.sort(key=lambda x: x["hours_ago"])

    return new_launches


# ============================================
# ALERTS V2 - Formatters
# ============================================

def format_wakeup_alert(market: dict) -> str:
    """Format a wakeup alert for Telegram."""
    title = _escape_markdown(market.get("title", "Unknown")[:50])
    slug = market.get("slug", "")
    total_volume = market.get("total_volume", 0)
    velocity_past = market.get("velocity_past", 0)
    velocity_now = market.get("velocity_now", 0)
    velocity_pct_now = market.get("velocity_pct_now", 0)

    vol_str = _format_volume(total_volume)
    vel_past_str = f"<${velocity_past/1000:.0f}K/hr" if velocity_past >= 1000 else f"<${velocity_past:.0f}/hr"
    vel_now_str = f"+${velocity_now/1000:.0f}K/hr" if velocity_now >= 1000 else f"+${velocity_now:.0f}/hr"
    odds_str = _format_odds(market)

    # Emoji
    vel_emoji = " 🔥🔥" if velocity_pct_now >= 20 else " 🔥"

    return f"""⚡ *Market Waking Up*

[{title}](https://polymarket.com/event/{slug})

Was quiet: {vel_past_str} for 6h
Now hot: {vel_now_str} ({velocity_pct_now:.1f}%/hr){vel_emoji}
Volume: {vol_str} | {odds_str}"""


def format_fast_mover_alert(market: dict) -> str:
    """Format a fast mover alert for Telegram."""
    title = _escape_markdown(market.get("title", "Unknown")[:50])
    slug = market.get("slug", "")
    old_price = market.get("old_price", 0)
    current_price = market.get("current_price", 0)
    price_change = market.get("price_change", 0)
    volume_behind = market.get("volume_behind", 0)
    velocity = market.get("velocity", 0)
    velocity_pct = market.get("velocity_pct", 0)
    direction = market.get("direction", "up")

    emoji = "📈" if direction == "up" else "📉"
    rocket = " 🚀" if price_change >= 15 else (" 💀" if price_change <= -15 else "")
    change_str = f"+{price_change:.0f}%" if price_change > 0 else f"{price_change:.0f}%"
    vol_behind_str = f"+${volume_behind/1000:.0f}K" if volume_behind >= 1000 else f"+${volume_behind:.0f}"
    vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"

    return f"""{emoji} *Fast Mover*

[{title}](https://polymarket.com/event/{slug})

Price: {old_price:.0f}% → {current_price:.0f}% ({change_str}) in 2h{rocket}
Volume behind move: {vol_behind_str}
Velocity: {vel_str} ({velocity_pct:.1f}%/hr)"""


def format_early_heat_alert(market: dict) -> str:
    """Format an early heat alert for Telegram."""
    title = _escape_markdown(market.get("title", "Unknown")[:50])
    slug = market.get("slug", "")
    total_volume = market.get("total_volume", 0)
    velocity = market.get("velocity", 0)
    velocity_pct = market.get("velocity_pct", 0)
    hours_ago = market.get("hours_ago", 0)

    vol_str = _format_volume(total_volume)
    vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
    time_ago = f"{hours_ago:.0f}h ago" if hours_ago >= 1 else f"{hours_ago*60:.0f}m ago"
    odds_str = _format_odds(market)

    vel_emoji = " 🔥🔥" if velocity_pct >= 20 else " 🔥"

    return f"""🌱 *Early Heat*

[{title}](https://polymarket.com/event/{slug})

Launched: {time_ago}
Volume: {vol_str} | Velocity: {vel_str} ({velocity_pct:.1f}%/hr){vel_emoji}
Odds: {odds_str}"""


def format_new_launch_alert(market: dict) -> str:
    """Format a new launch alert for Telegram."""
    title = _escape_markdown(market.get("title", "Unknown")[:50])
    slug = market.get("slug", "")
    odds_str = _format_odds(market)

    return f"""🆕 *New Market*

[{title}](https://polymarket.com/event/{slug})

Just launched
Odds: {odds_str}"""


def format_bundled_wakeups(alerts: list[dict]) -> str:
    """Format multiple wakeup alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["⚡ *Markets Waking Up*", ""]

    for a in alerts:
        title = _escape_markdown(a.get("title", "Unknown")[:40])
        slug = a.get("slug", "")
        velocity_now = a.get("velocity_now", 0)
        velocity_pct_now = a.get("velocity_pct_now", 0)
        total_volume = a.get("total_volume", 0)

        vol_str = _format_volume(total_volume)
        vel_str = f"+${velocity_now/1000:.0f}K/hr" if velocity_now >= 1000 else f"+${velocity_now:.0f}/hr"
        vel_emoji = " 🔥🔥" if velocity_pct_now >= 20 else " 🔥"
        odds_str = _format_odds(a)

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Was quiet → Now: {vel_str} ({velocity_pct_now:.1f}%/hr){vel_emoji}")
        lines.append(f"Volume: {vol_str} | {odds_str}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_fast_movers(alerts: list[dict]) -> str:
    """Format multiple fast mover alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["📈 *Fast Movers*", ""]

    for a in alerts:
        title = _escape_markdown(a.get("title", "Unknown")[:40])
        slug = a.get("slug", "")
        old_price = a.get("old_price", 0)
        current_price = a.get("current_price", 0)
        price_change = a.get("price_change", 0)
        volume_behind = a.get("volume_behind", 0)

        change_str = f"+{price_change:.0f}%" if price_change > 0 else f"{price_change:.0f}%"
        vol_str = f"+${volume_behind/1000:.0f}K" if volume_behind >= 1000 else f"+${volume_behind:.0f}"
        emoji = "🚀" if price_change >= 15 else ("💀" if price_change <= -15 else ("⬆️" if price_change > 0 else "⬇️"))

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"{emoji} {old_price:.0f}% → {current_price:.0f}% ({change_str}) | {vol_str} behind")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_early_heat(alerts: list[dict]) -> str:
    """Format multiple early heat alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["🌱 *Early Heat*", ""]

    for a in alerts:
        title = _escape_markdown(a.get("title", "Unknown")[:40])
        slug = a.get("slug", "")
        total_volume = a.get("total_volume", 0)
        velocity = a.get("velocity", 0)
        velocity_pct = a.get("velocity_pct", 0)
        hours_ago = a.get("hours_ago", 0)

        vol_str = _format_volume(total_volume)
        vel_str = f"+${velocity/1000:.0f}K/hr" if velocity >= 1000 else f"+${velocity:.0f}/hr"
        time_ago = f"{hours_ago:.0f}h ago" if hours_ago >= 1 else f"{hours_ago*60:.0f}m ago"
        vel_emoji = " 🔥🔥" if velocity_pct >= 20 else " 🔥"
        odds_str = _format_odds(a)

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Launched: {time_ago} | {vol_str}")
        lines.append(f"Velocity: {vel_str} ({velocity_pct:.1f}%/hr){vel_emoji} | {odds_str}")
        lines.append("")

    return "\n".join(lines).strip()


def format_bundled_new_launches(alerts: list[dict]) -> str:
    """Format multiple new launch alerts into bundled message."""
    if not alerts:
        return ""

    lines = ["🆕 *New Markets*", ""]

    for a in alerts:
        title = _escape_markdown(a.get("title", "Unknown")[:40])
        slug = a.get("slug", "")
        odds_str = _format_odds(a)

        lines.append(f"[{title}](https://polymarket.com/event/{slug})")
        lines.append(f"Just launched | {odds_str}")
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
