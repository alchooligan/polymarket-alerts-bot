"""
Polymarket API wrapper for fetching markets, prices, and tags.
"""

import json
import re
import httpx
from typing import Optional
from config import (
    EVENTS_ENDPOINT,
    TAGS_ENDPOINT,
    PRICES_HISTORY_ENDPOINT,
    GAMMA_API_BASE,
    DATA_API_BASE,
    SPAM_CRYPTO_TICKERS,
    SPAM_PRICE_KEYWORDS,
    SPAM_TIMEFRAME_KEYWORDS,
    SPAM_PHRASES,
)
from cache import get_cached_markets, set_cached_markets


async def fetch_recent_events(limit: int = 100, closed: bool = False) -> list[dict]:
    """
    Fetch recent events from Polymarket (sorted by recency).
    Used for new market alerts.

    Args:
        limit: Maximum number of events to fetch
        closed: Whether to include closed markets

    Returns:
        List of event dictionaries
    """
    params = {
        "order": "id",
        "ascending": "false",
        "closed": str(closed).lower(),
        "limit": limit,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(EVENTS_ENDPOINT, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching events: {e}")
        return []


async def fetch_popular_events(limit: int = 100, closed: bool = False) -> list[dict]:
    """
    Fetch popular events from Polymarket (sorted by volume).
    Used for /markets command to show interesting markets.

    Args:
        limit: Maximum number of events to fetch
        closed: Whether to include closed markets

    Returns:
        List of event dictionaries
    """
    params = {
        "order": "volume",
        "ascending": "false",
        "closed": str(closed).lower(),
        "limit": limit,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(EVENTS_ENDPOINT, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching popular events: {e}")
        return []


async def fetch_events_paginated(
    target_count: int = 500,
    order: str = "volume",
    closed: bool = False,
    page_size: int = 100
) -> list[dict]:
    """
    Fetch events with pagination until we have enough.

    Args:
        target_count: Target number of events to fetch
        order: Sort order ("volume" for popular, "id" for recent)
        closed: Whether to include closed markets
        page_size: Events per API call (max 100)

    Returns:
        List of event dictionaries
    """
    all_events = []
    offset = 0
    # Calculate max pages needed, with a hard cap of 100 pages (10000 events)
    max_pages = min((target_count // page_size) + 1, 100)

    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(max_pages):
            params = {
                "order": order,
                "ascending": "false",
                "closed": str(closed).lower(),
                "limit": page_size,
                "offset": offset,
            }

            try:
                response = await client.get(EVENTS_ENDPOINT, params=params)
                response.raise_for_status()
                events = response.json()

                if not events:
                    # No more events to fetch
                    break

                all_events.extend(events)

                if len(all_events) >= target_count:
                    break

                offset += page_size

            except httpx.HTTPError as e:
                print(f"Error fetching events at offset {offset}: {e}")
                break

    return all_events[:target_count]


async def fetch_event_by_slug(slug: str) -> Optional[dict]:
    """
    Fetch a specific event by its slug.

    Args:
        slug: The event slug (e.g., "will-trump-win-2024")

    Returns:
        Event dictionary or None if not found
    """
    url = f"{GAMMA_API_BASE}/events/slug/{slug}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching event {slug}: {e}")
        return None


async def fetch_tags() -> list[dict]:
    """
    Fetch all available tags (categories) from Polymarket.

    Returns:
        List of tag dictionaries with 'id' and 'label' fields
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(TAGS_ENDPOINT)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching tags: {e}")
        return []


async def fetch_price_history(
    token_id: str,
    interval: str = "1h",
    fidelity: int = 60
) -> list[dict]:
    """
    Fetch price history for a specific market token.

    Args:
        token_id: The market's token ID
        interval: Time interval (e.g., "1h", "1d")
        fidelity: Data points granularity in minutes

    Returns:
        List of price history data points
    """
    params = {
        "market": token_id,
        "interval": interval,
        "fidelity": fidelity,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(PRICES_HISTORY_ENDPOINT, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching price history for {token_id}: {e}")
        return []


async def fetch_recent_trades(
    limit: int = 100,
    min_size: float = None,
) -> list[dict]:
    """
    Fetch recent trades from the Polymarket Data API.
    Returns trades sorted by timestamp descending (most recent first).

    Args:
        limit: Maximum number of trades to fetch
        min_size: Minimum trade size in USDC (optional, uses filterType=CASH)

    Returns:
        List of trade dictionaries with fields like:
        - id: trade ID
        - asset: token/asset ID
        - size: trade size in dollars
        - price: execution price (0-1 scale)
        - side: BUY or SELL
        - timestamp: trade timestamp
        - title, slug, eventSlug, outcome: market info
    """
    url = f"{DATA_API_BASE}/trades"
    params = {"limit": limit}

    # Add size filter if specified (API-level filtering is more efficient)
    if min_size is not None and min_size > 0:
        params["filterType"] = "CASH"
        params["filterAmount"] = str(min_size)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching trades: {e}")
        return []


async def fetch_market_by_asset(asset_id: str) -> Optional[dict]:
    """
    Fetch market info by asset/token ID.
    Used to get event details for a trade.

    Args:
        asset_id: The token/asset ID from a trade

    Returns:
        Market dictionary or None
    """
    url = f"{GAMMA_API_BASE}/markets/{asset_id}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"Error fetching market for asset {asset_id}: {e}")
        return None


def parse_outcome_prices(outcome_prices_str: str) -> dict[str, float]:
    """
    Parse the outcomePrices field from market data.
    It's stored as a stringified JSON array like '["0.45", "0.55"]'

    Args:
        outcome_prices_str: The stringified JSON prices

    Returns:
        Dictionary with 'yes' and 'no' prices as floats (0-100 scale)
    """
    try:
        prices = json.loads(outcome_prices_str)
        if len(prices) >= 2:
            return {
                "yes": float(prices[0]) * 100,  # Convert to percentage
                "no": float(prices[1]) * 100,
            }
    except (json.JSONDecodeError, IndexError, ValueError):
        pass

    return {"yes": 0.0, "no": 0.0}


def parse_outcomes_with_names(market: dict) -> list[dict]:
    """
    Parse outcomes with their names and prices.

    Args:
        market: Market dictionary from API

    Returns:
        List of outcome dicts: [{"name": "Yes", "price": 65.0}, ...]
        Sorted by price descending (highest first)
    """
    outcomes = []

    # Get outcome names - could be in 'outcomes' field as JSON string or list
    outcome_names = market.get("outcomes", [])
    if isinstance(outcome_names, str):
        try:
            outcome_names = json.loads(outcome_names)
        except json.JSONDecodeError:
            outcome_names = ["Yes", "No"]

    # Get outcome prices
    outcome_prices_str = market.get("outcomePrices", "[]")
    try:
        outcome_prices = json.loads(outcome_prices_str)
    except json.JSONDecodeError:
        outcome_prices = []

    # Combine names and prices
    for i, name in enumerate(outcome_names):
        price = 0.0
        if i < len(outcome_prices):
            try:
                price = float(outcome_prices[i]) * 100  # Convert to percentage
            except (ValueError, TypeError):
                pass
        outcomes.append({"name": name, "price": price})

    # Sort by price descending (top outcomes first)
    outcomes.sort(key=lambda x: x["price"], reverse=True)

    return outcomes


def _extract_outcome_name(question: str, event_title: str) -> str:
    """
    Extract a short, meaningful outcome name from a market question.

    Examples:
    - "Will Tim Cook be out as Apple CEO before 2027?" → "Tim Cook"
    - "Will the highest temperature be 40-45°F?" → "40-45°F"
    - "Will Trump release the Epstein files by January 20?" → "Jan 20"
    """
    import re

    q = question.strip().rstrip("?")

    # Remove "Will " prefix
    if q.lower().startswith("will "):
        q = q[5:]

    # Pattern 1: Look for "by [DATE]" at the end - extract the date
    by_match = re.search(r'\bby\s+(.+)$', q, re.IGNORECASE)
    if by_match:
        date_part = by_match.group(1).strip()
        # Shorten month names
        date_part = date_part.replace("January", "Jan").replace("February", "Feb")
        date_part = date_part.replace("March", "Mar").replace("April", "Apr")
        date_part = date_part.replace("September", "Sep").replace("October", "Oct")
        date_part = date_part.replace("November", "Nov").replace("December", "Dec")
        # Remove year if present and date is short enough
        date_part = re.sub(r',?\s*202\d', '', date_part).strip()
        if len(date_part) <= 15:
            return date_part

    # Pattern 2: Look for "be X" patterns for temperature/number ranges
    be_match = re.search(r'\bbe\s+(\d+[-–]\d+[°]?[FC]?|\d+\+?[°]?[FC]?)', q)
    if be_match:
        return be_match.group(1)

    # Pattern 3: Person names - "X be out as" or "X out as" or just "X be"
    # Extract first part before "be out", "out as", "be fired", etc.
    out_match = re.search(r'^(.+?)\s+(?:be\s+)?(?:out|fired|leave|resign|step down)', q, re.IGNORECASE)
    if out_match:
        name = out_match.group(1).strip()
        # Remove "the" prefix
        if name.lower().startswith("the "):
            name = name[4:]
        if len(name) <= 20:
            return name

    # Pattern 4: Just take the first meaningful words (skip "the")
    words = q.split()
    if words and words[0].lower() == "the":
        words = words[1:]

    # Take first 2-3 words that seem like a name/identifier
    result = " ".join(words[:3])

    # Truncate if still too long
    if len(result) > 18:
        result = result[:15] + "..."

    return result


def is_price_spam(title: str) -> bool:
    """
    Check if a market title looks like crypto price prediction spam.

    Examples of spam:
    - "Will BTC be above $100,000 on December 31?"
    - "ETH price at midnight: above $4000?"
    - "Bitcoin daily close above $95,000?"
    - "BTC Up or Down 5m"
    - "ETH 15m higher or lower"

    Examples of NOT spam (keep these):
    - "Will Bitcoin ETF be approved in 2024?"
    - "Will Ethereum switch to proof of stake?"
    - "Will FTX repay users?"

    Args:
        title: The market title to check

    Returns:
        True if it looks like price prediction spam
    """
    title_lower = title.lower()

    # First check if title contains a crypto ticker
    has_crypto = any(ticker.lower() in title_lower for ticker in SPAM_CRYPTO_TICKERS)

    # If no crypto mentioned, it's not crypto price spam
    if not has_crypto:
        return False

    # Now check for spam patterns (all require crypto context)

    # Pattern 1: crypto + spam phrase (e.g., "BTC up or down")
    has_spam_phrase = any(phrase.lower() in title_lower for phrase in SPAM_PHRASES)
    if has_spam_phrase:
        return True

    # Pattern 2: crypto + price keyword (e.g., "BTC above $100k")
    has_price_keyword = any(keyword.lower() in title_lower for keyword in SPAM_PRICE_KEYWORDS)
    if has_price_keyword:
        return True

    # Pattern 3: crypto + short timeframe (e.g., "BTC 5m", "ETH daily")
    has_timeframe = any(keyword.lower() in title_lower for keyword in SPAM_TIMEFRAME_KEYWORDS)
    if has_timeframe:
        return True

    # Pattern 4: crypto + price target (e.g., "BTC $100,000")
    has_price_target = bool(re.search(r'\$[\d,]+', title))
    if has_price_target:
        return True

    return False


def extract_market_info(event: dict) -> list[dict]:
    """
    Extract relevant market info from an event.

    Args:
        event: Raw event dictionary from API

    Returns:
        List of simplified market dictionaries
    """
    markets = []

    event_title = event.get("title", "")
    event_slug = event.get("slug", "")
    tags = event.get("tags", [])
    tag_labels = [tag.get("label", "") for tag in tags] if tags else []

    # Get creation date - try multiple fields
    created_at = (
        event.get("createdAt") or
        event.get("created") or
        event.get("startDate") or
        ""
    )

    for market in event.get("markets", []):
        # Parse prices
        prices = parse_outcome_prices(market.get("outcomePrices", "[]"))

        # Also check market-level creation date
        market_created = (
            market.get("createdAt") or
            market.get("created") or
            created_at
        )

        # Parse full outcomes with names
        outcomes = parse_outcomes_with_names(market)

        market_info = {
            "id": market.get("id", ""),
            "event_id": event.get("id", ""),
            "title": event_title,
            "slug": event_slug,
            "question": market.get("question", event_title),
            "yes_price": prices["yes"],
            "no_price": prices["no"],
            "outcomes": outcomes,  # Full outcomes with names
            "volume": float(market.get("volume", 0) or 0),
            "liquidity": float(market.get("liquidity", 0) or 0),
            "end_date": market.get("endDate", ""),
            "created_at": market_created,  # Add creation date
            "closed": market.get("closed", False),
            "tags": tag_labels,
            "token_id": market.get("clobTokenIds", [""])[0] if market.get("clobTokenIds") else "",
            "is_spam": is_price_spam(event_title),
        }
        markets.append(market_info)

    return markets


async def get_all_markets(limit: int = 100, include_spam: bool = False) -> list[dict]:
    """
    Fetch and process all recent markets.

    Args:
        limit: Maximum number of events to fetch
        include_spam: Whether to include price prediction spam

    Returns:
        List of processed market dictionaries
    """
    events = await fetch_recent_events(limit=limit)

    all_markets = []
    for event in events:
        markets = extract_market_info(event)
        for market in markets:
            if include_spam or not market["is_spam"]:
                all_markets.append(market)

    return all_markets


async def get_unique_events(limit: int = 100, include_spam: bool = False) -> list[dict]:
    """
    Fetch markets and deduplicate by event.
    Returns one entry per event, sorted by volume.

    Args:
        limit: Maximum number of events to fetch
        include_spam: Whether to include price prediction spam

    Returns:
        List of unique event dictionaries, sorted by volume desc
    """
    events = await fetch_recent_events(limit=limit)

    # Group markets by event slug
    event_map = {}

    for event in events:
        markets = extract_market_info(event)
        raw_markets = event.get("markets", [])
        is_multi_outcome = len(raw_markets) > 1

        for market in markets:
            # Skip spam unless requested
            if not include_spam and market["is_spam"]:
                continue

            slug = market["slug"]

            if slug not in event_map:
                # First market for this event
                event_map[slug] = {
                    "title": market["title"],
                    "slug": slug,
                    "yes_price": market["yes_price"],
                    "total_volume": market["volume"],
                    "tags": market["tags"],
                    "end_date": market["end_date"],
                    "outcomes": market.get("outcomes", []),
                    "is_multi_outcome": is_multi_outcome,
                    # For multi-outcome: collect market questions as event outcomes
                    "event_outcomes": [],
                }

            # Add volume from additional markets in same event
            if slug in event_map and event_map[slug]["total_volume"] != market["volume"]:
                event_map[slug]["total_volume"] += market["volume"]

            # For multi-outcome events, use market question as outcome name
            if is_multi_outcome:
                question = market.get("question", "")
                if question and question != market["title"]:
                    outcome_name = _extract_outcome_name(question, market["title"])
                    if outcome_name:
                        event_map[slug]["event_outcomes"].append({
                            "name": outcome_name,
                            "price": market["yes_price"],
                        })

            # Keep the highest YES price
            if market["yes_price"] > event_map[slug]["yes_price"]:
                event_map[slug]["yes_price"] = market["yes_price"]
                event_map[slug]["outcomes"] = market.get("outcomes", [])

    # Sort event_outcomes by price and set as primary outcomes for multi-outcome events
    for slug, data in event_map.items():
        if data.get("is_multi_outcome") and data.get("event_outcomes"):
            data["event_outcomes"].sort(key=lambda x: x["price"], reverse=True)
            # Use event_outcomes as the display outcomes
            data["outcomes"] = data["event_outcomes"][:10]  # Top 10 max

    # Convert to list and sort by volume descending
    unique_events = list(event_map.values())
    unique_events.sort(key=lambda x: x["total_volume"], reverse=True)

    return unique_events


async def get_popular_markets(limit: int = 100, include_spam: bool = False) -> list[dict]:
    """
    Fetch POPULAR markets (by volume) and deduplicate by event.
    Used for /markets command to show high-volume, interesting markets.

    Args:
        limit: Maximum number of events to fetch
        include_spam: Whether to include price prediction spam

    Returns:
        List of unique event dictionaries, sorted by volume desc
    """
    # Use popular events (sorted by volume from API)
    events = await fetch_popular_events(limit=limit)

    # Group markets by event slug
    event_map = {}

    for event in events:
        markets = extract_market_info(event)
        raw_markets = event.get("markets", [])
        is_multi_outcome = len(raw_markets) > 1

        for market in markets:
            # Skip spam unless requested
            if not include_spam and market["is_spam"]:
                continue

            slug = market["slug"]

            if slug not in event_map:
                event_map[slug] = {
                    "title": market["title"],
                    "slug": slug,
                    "yes_price": market["yes_price"],
                    "total_volume": market["volume"],
                    "tags": market["tags"],
                    "end_date": market["end_date"],
                    "outcomes": market.get("outcomes", []),
                    "is_multi_outcome": is_multi_outcome,
                    "event_outcomes": [],
                }

            # Add volume from additional markets in same event
            if slug in event_map and event_map[slug]["total_volume"] != market["volume"]:
                event_map[slug]["total_volume"] += market["volume"]

            # For multi-outcome events, use market question as outcome name
            if is_multi_outcome:
                question = market.get("question", "")
                if question and question != market["title"]:
                    outcome_name = _extract_outcome_name(question, market["title"])
                    if outcome_name:
                        event_map[slug]["event_outcomes"].append({
                            "name": outcome_name,
                            "price": market["yes_price"],
                        })

            if market["yes_price"] > event_map[slug]["yes_price"]:
                event_map[slug]["yes_price"] = market["yes_price"]
                event_map[slug]["outcomes"] = market.get("outcomes", [])

    # Sort event_outcomes by price and set as primary outcomes for multi-outcome events
    for slug, data in event_map.items():
        if data.get("is_multi_outcome") and data.get("event_outcomes"):
            data["event_outcomes"].sort(key=lambda x: x["price"], reverse=True)
            data["outcomes"] = data["event_outcomes"][:10]

    # Convert to list (already sorted by volume from API, but re-sort to be safe)
    unique_events = list(event_map.values())
    unique_events.sort(key=lambda x: x["total_volume"], reverse=True)

    return unique_events


async def get_all_markets_paginated(
    target_count: int = 500,
    include_spam: bool = False,
    use_cache: bool = True
) -> list[dict]:
    """
    Fetch many markets with pagination and deduplicate by event.
    Used for volume milestone detection to cover more of the ecosystem.

    Args:
        target_count: Target number of raw events to fetch (will dedupe after)
        include_spam: Whether to include price prediction spam
        use_cache: Whether to use Redis cache (default True)

    Returns:
        List of unique event dictionaries, sorted by volume desc
    """
    # Try cache first (only for non-spam filtered requests)
    if use_cache and not include_spam:
        cached = await get_cached_markets(target_count)
        if cached is not None:
            return cached

    # Fetch with pagination
    events = await fetch_events_paginated(
        target_count=target_count,
        order="volume",
        closed=False
    )

    # Group markets by event slug (same as get_popular_markets)
    event_map = {}

    for event in events:
        markets = extract_market_info(event)
        raw_markets = event.get("markets", [])
        is_multi_outcome = len(raw_markets) > 1

        for market in markets:
            if not include_spam and market["is_spam"]:
                continue

            slug = market["slug"]

            if slug not in event_map:
                event_map[slug] = {
                    "title": market["title"],
                    "slug": slug,
                    "yes_price": market["yes_price"],
                    "total_volume": market["volume"],
                    "tags": market["tags"],
                    "end_date": market["end_date"],
                    "created_at": market.get("created_at", ""),
                    "outcomes": market.get("outcomes", []),
                    "is_multi_outcome": is_multi_outcome,
                    "event_outcomes": [],
                }

            # Add volume from additional markets in same event
            if slug in event_map and event_map[slug]["total_volume"] != market["volume"]:
                event_map[slug]["total_volume"] += market["volume"]

            # For multi-outcome events, use market question as outcome name
            if is_multi_outcome:
                question = market.get("question", "")
                if question and question != market["title"]:
                    outcome_name = _extract_outcome_name(question, market["title"])
                    if outcome_name:
                        event_map[slug]["event_outcomes"].append({
                            "name": outcome_name,
                            "price": market["yes_price"],
                        })

            # Keep the highest YES price
            if market["yes_price"] > event_map[slug]["yes_price"]:
                event_map[slug]["yes_price"] = market["yes_price"]
                event_map[slug]["outcomes"] = market.get("outcomes", [])

    # Sort event_outcomes by price and set as primary outcomes for multi-outcome events
    for slug, data in event_map.items():
        if data.get("is_multi_outcome") and data.get("event_outcomes"):
            data["event_outcomes"].sort(key=lambda x: x["price"], reverse=True)
            data["outcomes"] = data["event_outcomes"][:10]

    unique_events = list(event_map.values())
    unique_events.sort(key=lambda x: x["total_volume"], reverse=True)

    # Cache the result for future requests
    if use_cache and not include_spam:
        await set_cached_markets(unique_events, target_count)

    return unique_events


# ============================================
# Test functions - run this file directly to test
# ============================================

async def test_api():
    """Test the API wrapper functions with diagnostics."""
    print("=" * 60)
    print("DIAGNOSTIC TEST - Polymarket API Wrapper")
    print("=" * 60)

    # Step 1: Fetch raw events and show what we're getting
    print("\n[STEP 1] Fetching raw events from API...")
    events = await fetch_recent_events(limit=100)
    print(f"   API returned {len(events)} events")

    if not events:
        print("\n   ERROR: No events returned from API!")
        print("   Check your internet connection or API status.")
        return

    # Step 2: Show first 10 raw event titles (before any processing)
    print("\n[STEP 2] First 10 raw event titles from API:")
    print("-" * 60)
    for i, event in enumerate(events[:10]):
        title = event.get("title", "NO TITLE")
        tags = event.get("tags", [])
        tag_names = [t.get("label", "") for t in tags][:2] if tags else ["No tags"]
        print(f"   {i+1}. {title[:60]}")
        print(f"      Tags: {', '.join(tag_names)}")
    print("-" * 60)

    # Step 3: Process all markets
    print("\n[STEP 3] Processing all markets...")
    all_markets = []
    for event in events:
        markets = extract_market_info(event)
        all_markets.extend(markets)
    print(f"   Total markets extracted: {len(all_markets)}")

    # Step 4: Categorize markets
    print("\n[STEP 4] Categorizing markets...")

    crypto_spam = []      # Crypto + spam pattern
    crypto_legit = []     # Crypto but NOT spam (e.g., ETF approval)
    non_crypto = []       # No crypto mentioned at all

    for market in all_markets:
        title_lower = market["title"].lower()
        has_crypto = any(t.lower() in title_lower for t in SPAM_CRYPTO_TICKERS)

        if not has_crypto:
            non_crypto.append(market)
        elif market["is_spam"]:
            crypto_spam.append(market)
        else:
            crypto_legit.append(market)

    print(f"\n   BREAKDOWN:")
    print(f"   - Non-crypto markets:        {len(non_crypto)}")
    print(f"   - Crypto (legit, not spam):  {len(crypto_legit)}")
    print(f"   - Crypto price spam:         {len(crypto_spam)}")
    print(f"   - TOTAL:                     {len(all_markets)}")

    # Step 5: Show examples from each category
    print("\n[STEP 5] Examples from each category:")

    print("\n   NON-CRYPTO MARKETS (should pass filter):")
    if non_crypto:
        for m in non_crypto[:5]:
            print(f"   ✅ {m['title'][:55]}")
    else:
        print("   (none found)")

    print("\n   CRYPTO LEGIT (should pass filter):")
    if crypto_legit:
        for m in crypto_legit[:5]:
            print(f"   ✅ {m['title'][:55]}")
    else:
        print("   (none found)")

    print("\n   CRYPTO SPAM (should be filtered):")
    if crypto_spam:
        for m in crypto_spam[:5]:
            print(f"   ❌ {m['title'][:55]}")
    else:
        print("   (none found)")

    # Step 6: Final summary
    print("\n" + "=" * 60)
    passing = len(non_crypto) + len(crypto_legit)
    filtered = len(crypto_spam)
    print(f"SUMMARY: {passing} markets pass filter, {filtered} filtered as spam")
    print("=" * 60)


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_api())
