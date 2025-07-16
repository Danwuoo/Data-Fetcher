import asyncio
import pytest
from backtest_data_module.data_ingestion.py.caching import LRUCache


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


@pytest.mark.asyncio
async def test_cache_ttl_expiration():
    """確保 TTL 到期後會自動刪除快取項目。"""
    cache = LRUCache(capacity=2, ttl=0.1)
    await cache.set("a", 1)
    await asyncio.sleep(0.15)

    assert await cache.get("a") is None
    assert "a" not in cache


@pytest.mark.asyncio
async def test_cache_ttl_not_expired():
    """確認在 TTL 未到期前能夠取得資料。"""
    cache = LRUCache(capacity=2, ttl=0.2)
    await cache.set("a", 1)
    await asyncio.sleep(0.1)

    assert await cache.get("a") == 1
