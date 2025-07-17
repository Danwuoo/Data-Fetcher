from backtest_data_module.data_ingestion.py.api_client import ApiClient
from backtest_data_module.data_ingestion.py.rate_limiter import RateLimiter
from backtest_data_module.data_ingestion.py.caching import ICache
from backtest_data_module.data_ingestion.metrics import (
    CACHE_HIT_COUNTER,
    CACHE_MISS_COUNTER,
    CACHE_HIT_RATIO,
)


class APIDataSource:
    """透過 API 擷取資料的資料來源，支援速率限制與快取。"""

    def __init__(
        self,
        api_client: ApiClient,
        cache: ICache,
        endpoint: str,
        rate_limiter: RateLimiter | None = None,
    ):
        """初始化 APIDataSource。

        Args:
            api_client: 用於擷取資料的 API 用戶端。
            rate_limiter: 控制請求速率的限制器。
            cache: 儲存取得資料的快取。
            endpoint: 目標 API 端點。
        """
        self.api_client = api_client
        self.cache = cache
        self.endpoint = endpoint
        self.rate_limiter = rate_limiter
        if rate_limiter is not None:
            api_client.limiters[endpoint] = rate_limiter

    async def read(self, params: dict = None):
        """非同步取得資料。

        Args:
            params: 查詢參數。

        Returns:
            從 API 擷取的資料。
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
