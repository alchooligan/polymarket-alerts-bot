"""
Redis caching layer for Polymarket Telegram Bot.
Provides fast caching for API responses and expensive database queries.

Uses redis-py with async support. Falls back gracefully if Redis is unavailable.
"""

import json
import os
import hashlib
from typing import Optional, Any
from datetime import datetime

# Try to import redis, gracefully handle if not available
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

# Redis configuration from environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Cache TTL settings (in seconds)
CACHE_TTL = {
    "markets": 300,           # 5 min - market list from API
    "volume_deltas": 60,      # 1 min - volume changes (updates frequently)
    "price_deltas": 300,      # 5 min - price changes
    "filtered_markets": 300,  # 5 min - filtered market sets
    "user_watchlist": 1800,   # 30 min - user watchlists
}

# Global Redis client (initialized lazily)
_redis_client: Optional["redis.Redis"] = None
_redis_enabled: bool = True  # Can be disabled if Redis is down


async def get_redis() -> Optional["redis.Redis"]:
    """
    Get Redis client, creating if needed.
    Returns None if Redis is unavailable or disabled.
    """
    global _redis_client, _redis_enabled

    if not REDIS_AVAILABLE:
        return None

    if not _redis_enabled:
        return None

    if _redis_client is None:
        try:
            _redis_client = redis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            # Test connection
            await _redis_client.ping()
            print(f"[Cache] Connected to Redis at {REDIS_URL}")
        except Exception as e:
            print(f"[Cache] Redis unavailable: {e}")
            _redis_client = None
            _redis_enabled = False
            return None

    return _redis_client


async def close_redis() -> None:
    """Close Redis connection."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None


def _make_key(prefix: str, *args) -> str:
    """Create a cache key from prefix and arguments."""
    if args:
        # Hash args if they're complex (like lists)
        args_str = json.dumps(args, sort_keys=True)
        args_hash = hashlib.md5(args_str.encode()).hexdigest()[:12]
        return f"polymarket:{prefix}:{args_hash}"
    return f"polymarket:{prefix}"


async def cache_get(key: str) -> Optional[Any]:
    """
    Get value from cache.
    Returns None if not found or Redis unavailable.
    """
    client = await get_redis()
    if client is None:
        return None

    try:
        value = await client.get(key)
        if value is not None:
            return json.loads(value)
    except Exception as e:
        print(f"[Cache] Error getting {key}: {e}")

    return None


async def cache_set(key: str, value: Any, ttl: int = 300) -> bool:
    """
    Set value in cache with TTL.
    Returns True if successful.
    """
    client = await get_redis()
    if client is None:
        return False

    try:
        await client.set(key, json.dumps(value), ex=ttl)
        return True
    except Exception as e:
        print(f"[Cache] Error setting {key}: {e}")
        return False


async def cache_delete(key: str) -> bool:
    """Delete a key from cache."""
    client = await get_redis()
    if client is None:
        return False

    try:
        await client.delete(key)
        return True
    except Exception as e:
        print(f"[Cache] Error deleting {key}: {e}")
        return False


async def cache_delete_pattern(pattern: str) -> int:
    """Delete all keys matching pattern. Returns count deleted."""
    client = await get_redis()
    if client is None:
        return 0

    try:
        keys = []
        async for key in client.scan_iter(match=f"polymarket:{pattern}*"):
            keys.append(key)

        if keys:
            await client.delete(*keys)
        return len(keys)
    except Exception as e:
        print(f"[Cache] Error deleting pattern {pattern}: {e}")
        return 0


# ============================================
# High-level caching functions
# ============================================

async def get_cached_markets(target_count: int = 500) -> Optional[list[dict]]:
    """Get cached market list."""
    key = _make_key("markets", target_count)
    return await cache_get(key)


async def set_cached_markets(markets: list[dict], target_count: int = 500) -> bool:
    """Cache market list."""
    key = _make_key("markets", target_count)
    return await cache_set(key, markets, ttl=CACHE_TTL["markets"])


async def get_cached_volume_deltas(slugs_hash: str, hours: int) -> Optional[dict]:
    """Get cached volume deltas."""
    key = _make_key("volume_deltas", slugs_hash, hours)
    return await cache_get(key)


async def set_cached_volume_deltas(deltas: dict, slugs_hash: str, hours: int) -> bool:
    """Cache volume deltas."""
    key = _make_key("volume_deltas", slugs_hash, hours)
    return await cache_set(key, deltas, ttl=CACHE_TTL["volume_deltas"])


async def get_cached_price_deltas(slugs_hash: str, hours: int) -> Optional[dict]:
    """Get cached price deltas."""
    key = _make_key("price_deltas", slugs_hash, hours)
    return await cache_get(key)


async def set_cached_price_deltas(deltas: dict, slugs_hash: str, hours: int) -> bool:
    """Cache price deltas."""
    key = _make_key("price_deltas", slugs_hash, hours)
    return await cache_set(key, deltas, ttl=CACHE_TTL["price_deltas"])


def hash_slugs(slugs: list[str]) -> str:
    """Create a hash of slug list for cache key."""
    slugs_str = ",".join(sorted(slugs))
    return hashlib.md5(slugs_str.encode()).hexdigest()[:16]


async def invalidate_market_cache() -> int:
    """Invalidate all market-related caches. Called after scheduler updates."""
    count = 0
    count += await cache_delete_pattern("markets")
    count += await cache_delete_pattern("volume_deltas")
    count += await cache_delete_pattern("price_deltas")
    count += await cache_delete_pattern("filtered_markets")
    return count


# ============================================
# Cached database query wrappers
# ============================================

async def get_volume_deltas_cached(
    event_slugs: list[str],
    hours: int,
    db_func
) -> dict[str, float]:
    """
    Get volume deltas with caching.
    Falls back to db_func if cache miss.
    """
    if not event_slugs:
        return {}

    slugs_hash = hash_slugs(event_slugs)

    # Try cache
    cached = await get_cached_volume_deltas(slugs_hash, hours)
    if cached is not None:
        return cached

    # Cache miss - call DB function (sync)
    result = db_func(event_slugs, hours)

    # Cache the result
    await set_cached_volume_deltas(result, slugs_hash, hours)

    return result


async def get_price_deltas_cached(
    event_slugs: list[str],
    hours: int,
    db_func
) -> dict[str, dict]:
    """
    Get price deltas with caching.
    Falls back to db_func if cache miss.
    """
    if not event_slugs:
        return {}

    slugs_hash = hash_slugs(event_slugs)

    # Try cache
    cached = await get_cached_price_deltas(slugs_hash, hours)
    if cached is not None:
        return cached

    # Cache miss - call DB function (sync)
    result = db_func(event_slugs, hours)

    # Cache the result
    await set_cached_price_deltas(result, slugs_hash, hours)

    return result


# ============================================
# Cache stats / diagnostics
# ============================================

async def get_cache_stats() -> dict:
    """Get cache statistics."""
    client = await get_redis()
    if client is None:
        return {"status": "unavailable", "redis_available": REDIS_AVAILABLE}

    try:
        info = await client.info("memory")
        keys_count = 0
        async for _ in client.scan_iter(match="polymarket:*"):
            keys_count += 1

        return {
            "status": "connected",
            "redis_available": True,
            "used_memory_human": info.get("used_memory_human", "unknown"),
            "polymarket_keys": keys_count,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ============================================
# Test function
# ============================================

async def test_cache():
    """Test Redis cache functionality."""
    print("=" * 50)
    print("Testing Redis Cache")
    print("=" * 50)

    # Test connection
    client = await get_redis()
    if client is None:
        print("Redis not available - cache will be disabled")
        print("To enable: pip install redis && redis-server")
        return

    print(f"Connected to Redis")

    # Test basic operations
    test_key = "polymarket:test"
    test_value = {"foo": "bar", "num": 123}

    print(f"\nSetting test value...")
    await cache_set(test_key, test_value, ttl=60)

    print(f"Getting test value...")
    result = await cache_get(test_key)
    print(f"Result: {result}")

    assert result == test_value, "Cache value mismatch!"
    print("Basic cache test passed!")

    # Test market caching
    print(f"\nTesting market cache...")
    fake_markets = [{"slug": "test-1", "title": "Test 1"}, {"slug": "test-2", "title": "Test 2"}]
    await set_cached_markets(fake_markets, target_count=100)
    cached = await get_cached_markets(target_count=100)
    print(f"Cached markets: {len(cached) if cached else 0}")

    # Cleanup
    await cache_delete(test_key)
    await invalidate_market_cache()

    # Stats
    stats = await get_cache_stats()
    print(f"\nCache stats: {stats}")

    await close_redis()
    print("\nCache test complete!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_cache())
