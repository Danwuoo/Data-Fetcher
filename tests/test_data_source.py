import pytest
from unittest.mock import AsyncMock, MagicMock
from data_ingestion.py.data_source import APIDataSource
from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache


@pytest.mark.asyncio
async def test_read_from_cache():
    """
    Tests that data is read from the cache if it exists.
    """
    api_client = AsyncMock(spec=ApiClient)
    rate_limiter = MagicMock(spec=RateLimiter)
    cache = MagicMock(spec=LRUCache)
    cache.get = AsyncMock()
    endpoint = "test"
    data_source = APIDataSource(
        api_client=api_client,
        rate_limiter=rate_limiter,
        cache=cache,
        endpoint=endpoint
    )
    params = {"param": "value"}
    cache_key = f"{endpoint}:{tuple(sorted(params.items()))}"
    expected_data = {"data": "test"}
    cache.get.return_value = expected_data

    data = await data_source.read(params)

    assert data == expected_data
    cache.get.assert_called_once_with(cache_key)
    rate_limiter.acquire.assert_not_called()
    api_client.call_api.assert_not_called()


@pytest.mark.asyncio
async def test_read_from_api():
    """
    Tests that data is read from the API if it's not in the cache.
    """
    api_client = AsyncMock(spec=ApiClient)
    rate_limiter = MagicMock(spec=RateLimiter)
    cache = MagicMock(spec=LRUCache)
    cache.get = AsyncMock()
    cache.set = AsyncMock()
    endpoint = "test"
    data_source = APIDataSource(
        api_client=api_client,
        rate_limiter=rate_limiter,
        cache=cache,
        endpoint=endpoint
    )
    params = {"param": "value"}
    cache_key = f"{endpoint}:{tuple(sorted(params.items()))}"
    expected_data = {"data": "test"}
    cache.get.return_value = None
    api_client.call_api.return_value = expected_data

    data = await data_source.read(params)

    assert data == expected_data
    cache.get.assert_called_once_with(cache_key)
    rate_limiter.acquire.assert_called_once()
    api_client.call_api.assert_called_once_with(endpoint, params)
    cache.set.assert_called_once_with(cache_key, expected_data)
