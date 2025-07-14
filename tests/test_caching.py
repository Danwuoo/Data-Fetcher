import unittest
from data_ingestion.py.caching import LRUCache

class TestLRUCache(unittest.TestCase):
    """
    Tests for the LRUCache class.
    """

    def test_cache_capacity(self):
        """
        Tests that the cache respects the capacity.
        """
        cache = LRUCache(capacity=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        self.assertIsNone(cache.get("a"))
        self.assertEqual(cache.get("b"), 2)
        self.assertEqual(cache.get("c"), 3)

    def test_cache_lru(self):
        """
        Tests the LRU logic of the cache.
        """
        cache = LRUCache(capacity=2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.get("a")
        cache.set("c", 3)

        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("a"), 1)
        self.assertEqual(cache.get("c"), 3)

if __name__ == "__main__":
    unittest.main()
