import pickle
from unittest.mock import AsyncMock
import pytest

from data_ingestion.py.persistent_cache import RedisCache, MemcachedCache


@pytest.mark.asyncio
async def test_redis_cache_get_set():
    redis = AsyncMock()
    cache = RedisCache(redis=redis, ttl=5)
    value = {"a": 1}
    await cache.set("k", value)
    redis.set.assert_called_once()
    args, kwargs = redis.set.call_args
    assert args[0] == "k"
    assert pickle.loads(args[1]) == value
    assert kwargs["ex"] == 5

    redis.get.return_value = pickle.dumps(value)
    result = await cache.get("k")
    redis.get.assert_called_once_with("k")
    assert result == value


@pytest.mark.asyncio
async def test_memcached_cache_get_set():
    client = AsyncMock()
    cache = MemcachedCache(client=client, ttl=3)
    value = [1, 2, 3]
    await cache.set("x", value)
    client.set.assert_called_once()
    args, kwargs = client.set.call_args
    assert args[0] == b"x"
    assert pickle.loads(args[1]) == value
    assert kwargs["exptime"] == 3

    client.get.return_value = pickle.dumps(value)
    result = await cache.get("x")
    client.get.assert_called_once_with(b"x")
    assert result == value
