import unittest
from unittest.mock import MagicMock
from data_ingestion.py.data_source import APIDataSource
from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache

class TestAPIDataSource(unittest.TestCase):
    """
    Tests for the APIDataSource class.
    """

    def setUp(self):
        self.api_client = MagicMock(spec=ApiClient)
        self.rate_limiter = MagicMock(spec=RateLimiter)
        self.cache = MagicMock(spec=LRUCache)
        self.endpoint = "test"
        self.data_source = APIDataSource(
            api_client=self.api_client,
            rate_limiter=self.rate_limiter,
            cache=self.cache,
            endpoint=self.endpoint
        )

    def test_read_from_cache(self):
        """
        Tests that data is read from the cache if it exists.
        """
        params = {"param": "value"}
        cache_key = f"{self.endpoint}:{params}"
        expected_data = {"data": "test"}
        self.cache.get.return_value = expected_data

        data = self.data_source.read(params)

        self.assertEqual(data, expected_data)
        self.cache.get.assert_called_once_with(cache_key)
        self.rate_limiter.check_rate.assert_not_called()
        self.api_client.call_api.assert_not_called()

    def test_read_from_api(self):
        """
        Tests that data is read from the API if it's not in the cache.
        """
        params = {"param": "value"}
        cache_key = f"{self.endpoint}:{params}"
        expected_data = {"data": "test"}
        self.cache.get.return_value = None
        self.api_client.call_api.return_value = expected_data

        data = self.data_source.read(params)

        self.assertEqual(data, expected_data)
        self.cache.get.assert_called_once_with(cache_key)
        self.rate_limiter.check_rate.assert_called_once()
        self.api_client.call_api.assert_called_once_with(self.endpoint, params)
        self.cache.set.assert_called_once_with(cache_key, expected_data)

if __name__ == "__main__":
    unittest.main()
