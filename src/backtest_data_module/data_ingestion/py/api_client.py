import httpx
import asyncio
import os
import time
from tenacity import retry, wait_random_exponential, stop_after_attempt
from backtest_data_module.data_ingestion.py.rate_limiter import RateLimiter
from backtest_data_module.data_ingestion.py.redis_rate_limiter import RedisRateLimiter
from backtest_data_module.data_ingestion.metrics import REQUEST_COUNTER
from backtest_data_module.data_ingestion.py.adaptive_controller import AdaptiveController
from backtest_data_module.data_ingestion.py.middleware import RateLimitMiddleware


class ApiClient:
    """
    A simple asynchronous API client with retry logic.
    """

    def __init__(
        self,
        base_url: str,
        limiters: dict[str, RateLimiter | RedisRateLimiter] | None = None,
        default_limiter: RateLimiter | RedisRateLimiter | None = None,
        proxy_base_url: str | None = None,
        max_concurrency: int | None = int(os.getenv("CONCURRENCY", 0)),
        batch_size: int = int(os.getenv("BATCH_SIZE", 1)),
    ):
        """初始化 ApiClient。

        Args:
            base_url: API 的基底網址
            proxy_base_url: 若提供則透過此 proxy 轉發請求
            max_concurrency: 同時允許的最大 API 連線數，``0`` 或 ``None`` 表示不限制
            batch_size: `call_batch` 同時送出的請求數量
        """
        self.base_url = base_url
        self.proxy_base_url = proxy_base_url
        self.session: httpx.AsyncClient | None = None
        self.limiters = limiters or {}
        self.default_limiter = default_limiter
        self.batch_size = max(batch_size, 1)
        self.max_concurrency = max_concurrency or 0
        self.semaphore = (
            asyncio.Semaphore(self.max_concurrency) if self.max_concurrency else None
        )
        self.controller = AdaptiveController(self.batch_size, self.max_concurrency)

    async def __aenter__(self):
        """建立並回傳非同步 HTTP session。"""
        middleware = RateLimitMiddleware(self.limiters, self.default_limiter)
        self.session = httpx.AsyncClient(
            event_hooks={
                "request": [middleware.on_request],
                "response": [middleware.on_response],
            }
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """關閉 HTTP session。"""
        if self.session:
            await self.session.aclose()

    @retry(
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(6),
    )
    async def call_api(self, endpoint: str, params: dict | None = None):
        """
        Calls an API endpoint asynchronously.

        Args:
            endpoint: The API endpoint to call.
            params: The query parameters for the request.

        Returns:
            The JSON response from the API.
        """
        if self.session is None:
            middleware = RateLimitMiddleware(self.limiters, self.default_limiter)
            self.session = httpx.AsyncClient(
                event_hooks={
                    "request": [middleware.on_request],
                    "response": [middleware.on_response],
                }
            )
        REQUEST_COUNTER.labels(endpoint=endpoint).inc()
        url = f"{self.base_url}/{endpoint}"
        if self.proxy_base_url:
            url = f"{self.proxy_base_url}/{endpoint}"
        try:
            start = time.monotonic()
            if self.semaphore:
                async with self.semaphore:
                    resp = await self._execute_request(url, params)
            else:
                resp = await self._execute_request(url, params)
            latency = time.monotonic() - start
            remaining = resp.headers.get("X-RateLimit-Remaining")
            self.controller.record(latency, int(remaining) if remaining else None)
            self._apply_controller()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise e
        except httpx.TimeoutException:
            raise

    async def _execute_request(
        self,
        url: str,
        params: dict | None,
    ):
        """實際執行 HTTP 請求並處理例外。"""
        response = await self.session.get(url, params=params)
        response.raise_for_status()
        return response

    async def call_batch(self, endpoints: list[tuple[str, dict]]):
        """批次呼叫多個 API，會依據 ``batch_size`` 分段執行。"""
        results = []
        for i in range(0, len(endpoints), self.batch_size):
            batch = endpoints[i : i + self.batch_size]
            tasks = [
                asyncio.create_task(self.call_api(ep, params))
                for ep, params in batch
            ]
            results.extend(await asyncio.gather(*tasks))
        return results

    def _apply_controller(self) -> None:
        new_batch, new_conc = self.controller.get_params()
        if new_batch != self.batch_size:
            self.batch_size = new_batch
        if new_conc != self.max_concurrency:
            self.max_concurrency = new_conc
            self.semaphore = (
                asyncio.Semaphore(self.max_concurrency)
                if self.max_concurrency
                else None
            )
