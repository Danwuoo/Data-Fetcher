from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache
from data_ingestion.metrics import (
    CACHE_HIT_COUNTER,
    CACHE_MISS_COUNTER,
    CACHE_HIT_RATIO,
)


class APIDataSource:
    """
    A data source that uses an API to fetch data, with rate limiting and
    caching.
    """

    def __init__(
        self,
        api_client: ApiClient,
        cache: LRUCache,
        endpoint: str,
        rate_limiter: RateLimiter | None = None,
    ):
        """
        Initializes the APIDataSource.

        Args:
            api_client: The API client to use for fetching data.
            rate_limiter: The rate limiter to use for controlling the request
                rate.
            cache: The cache to use for storing fetched data.
            endpoint: The API endpoint to fetch data from.
        """
        self.api_client = api_client
        self.cache = cache
        self.endpoint = endpoint
        self.rate_limiter = rate_limiter
        if rate_limiter is not None:
            api_client.limiters[endpoint] = rate_limiter

    async def read(self, params: dict = None):
        """
        Reads data from the data source asynchronously.

        Args:
            params: The query parameters for the request.

        Returns:
            The data from the API.
        """
        param_tuple = tuple(sorted(params.items())) if params else None
        cache_key = f"{self.endpoint}:{param_tuple}"

        hit_metric = CACHE_HIT_COUNTER.labels(endpoint=self.endpoint)
        miss_metric = CACHE_MISS_COUNTER.labels(endpoint=self.endpoint)
        ratio_metric = CACHE_HIT_RATIO.labels(endpoint=self.endpoint)

        cached_data = await self.cache.get(cache_key)
        if cached_data is not None:
            hit_metric.inc()
            total_hits = hit_metric._value.get()
            total_misses = miss_metric._value.get()
            ratio_metric.set(total_hits / (total_hits + total_misses))
            return cached_data

        miss_metric.inc()
        data = await self.api_client.call_api(self.endpoint, params)
        await self.cache.set(cache_key, data)
        total_hits = hit_metric._value.get()
        total_misses = miss_metric._value.get()
        ratio_metric.set(total_hits / (total_hits + total_misses))
        return data
