import asyncio
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Any


class ICache(ABC):
    """快取介面定義。"""

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        """取得快取值，如果不存在則回傳 ``None``。"""

    @abstractmethod
    async def set(self, key: str, value: Any) -> None:
        """設定快取值。"""

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """判斷 key 是否存在於快取中。"""


class LRUCache(ICache):
    """
    A simple in-memory LRU cache.
    """

    def __init__(self, capacity: int, ttl: float | None = None):
        """
        Initializes the LRUCache.

        Args:
            capacity: The maximum number of items to store in the cache.
            ttl: 選擇性的存活時間（秒），設定後超過時間即會自動失效。
        """
        self.capacity = capacity
        self.ttl = ttl
        self.cache = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        """
        Gets an item from the cache.

        Args:
            key: The key of the item to get.

        Returns:
            The value of the item, or None if the item is not in the cache.
        """
        async with self.lock:
            if key not in self.cache:
                return None

            value, inserted_at = self.cache[key]
            expired = self.ttl is not None and (
                time.monotonic() - inserted_at > self.ttl
            )
            if expired:
                # 已過期，刪除並回傳 None
                del self.cache[key]
                return None

            self.cache.move_to_end(key)
            return value

    async def set(self, key: str, value: Any) -> None:
        """
        Sets an item in the cache.

        Args:
            key: The key of the item to set.
            value: The value of the item to set.
        """
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)

            # 將值與插入時間一起存入
            self.cache[key] = (value, time.monotonic())
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __contains__(self, key: str) -> bool:
        """判斷 key 是否存在於快取中，同時檢查是否過期。"""
        if key not in self.cache:
            return False

        value, inserted_at = self.cache[key]
        expired = self.ttl is not None and (
            time.monotonic() - inserted_at > self.ttl
        )
        if expired:
            del self.cache[key]
            return False
        return True


__all__ = ["ICache", "LRUCache"]
