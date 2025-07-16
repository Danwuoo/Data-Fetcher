from __future__ import annotations

import pickle
from typing import Any

from redis.asyncio import Redis
import aiomcache

from backtest_data_module.data_ingestion.py.caching import ICache


class RedisCache(ICache):
    """利用 Redis 共享快取。"""

    def __init__(self, redis: Redis, ttl: float | None = None) -> None:
        self.redis = redis
        self.ttl = ttl

    async def get(self, key: str) -> Any | None:
        data = await self.redis.get(key)
        if data is None:
            return None
        return pickle.loads(data)

    async def set(self, key: str, value: Any) -> None:
        data = pickle.dumps(value)
        if self.ttl:
            await self.redis.set(key, data, ex=self.ttl)
        else:
            await self.redis.set(key, data)

    def __contains__(self, key: str) -> bool:  # pragma: no cover - rarely used
        return False


class MemcachedCache(ICache):
    """利用 Memcached 共享快取。"""

    def __init__(self, client: aiomcache.Client, ttl: int | None = None) -> None:
        self.client = client
        self.ttl = ttl or 0

    async def get(self, key: str) -> Any | None:
        data = await self.client.get(key.encode())
        if data is None:
            return None
        return pickle.loads(data)

    async def set(self, key: str, value: Any) -> None:
        data = pickle.dumps(value)
        await self.client.set(key.encode(), data, exptime=self.ttl)

    def __contains__(self, key: str) -> bool:  # pragma: no cover - rarely used
        return False


__all__ = ["RedisCache", "MemcachedCache"]
