from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache

class APIDataSource:
    """
    A data source that uses an API to fetch data, with rate limiting and caching.
    """

    def __init__(self, api_client: ApiClient, rate_limiter: RateLimiter, cache: LRUCache, endpoint: str):
        """
        Initializes the APIDataSource.

        Args:
            api_client: The API client to use for fetching data.
            rate_limiter: The rate limiter to use for controlling the request rate.
            cache: The cache to use for storing fetched data.
            endpoint: The API endpoint to fetch data from.
        """
        self.api_client = api_client
        self.rate_limiter = rate_limiter
        self.cache = cache
        self.endpoint = endpoint

    async def read(self, params: dict = None):
        """
        Reads data from the data source asynchronously.

        Args:
            params: The query parameters for the request.

        Returns:
            The data from the API.
        """
        cache_key = f"{self.endpoint}:{params}"
        cached_data = await self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        self.rate_limiter.check_rate()
        data = await self.api_client.call_api(self.endpoint, params)
        await self.cache.set(cache_key, data)
        return data
