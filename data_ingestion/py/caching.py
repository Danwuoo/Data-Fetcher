import asyncio
from collections import OrderedDict

class LRUCache:
    """
    A simple in-memory LRU cache.
    """

    def __init__(self, capacity: int):
        """
        Initializes the LRUCache.

        Args:
            capacity: The maximum number of items to store in the cache.
        """
        self.capacity = capacity
        self.cache = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str):
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
            else:
                self.cache.move_to_end(key)
                return self.cache[key]

    async def set(self, key: str, value):
        """
        Sets an item in the cache.

        Args:
            key: The key of the item to set.
            value: The value of the item to set.
        """
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __contains__(self, key: str) -> bool:
        """判斷 key 是否存在於快取中。"""
        return key in self.cache
