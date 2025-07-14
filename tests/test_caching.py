import pytest
from data_ingestion.py.caching import LRUCache

@pytest.mark.asyncio
async def test_cache_capacity():
    """
    Tests that the cache respects the capacity.
    """
    cache = LRUCache(capacity=2)
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)

    assert await cache.get("a") is None
    assert await cache.get("b") == 2
    assert await cache.get("c") == 3

@pytest.mark.asyncio
async def test_cache_lru():
    """
    Tests the LRU logic of the cache.
    """
    cache = LRUCache(capacity=2)
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.get("a")
    await cache.set("c", 3)

    assert await cache.get("b") is None
    assert await cache.get("a") == 1
    assert await cache.get("c") == 3
