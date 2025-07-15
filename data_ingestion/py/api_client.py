import httpx
import asyncio
from tenacity import retry, wait_random_exponential, stop_after_attempt
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.metrics import REQUEST_COUNTER


class ApiClient:
    """
    A simple asynchronous API client with retry logic.
    """

    def __init__(
        self,
        base_url: str,
        limiters: dict[str, RateLimiter] | None = None,
        default_limiter: RateLimiter | None = None,
        proxy_base_url: str | None = None,
        max_concurrency: int | None = None,
    ):
        """初始化 ApiClient。

        Args:
            base_url: API 的基底網址
            proxy_base_url: 若提供則透過此 proxy 轉發請求
            max_concurrency: 同時允許的最大 API 連線數，``None`` 表示不限制
        """
        self.base_url = base_url
        self.proxy_base_url = proxy_base_url
        self.session: httpx.AsyncClient | None = None
        self.limiters = limiters or {}
        self.default_limiter = default_limiter
        self.semaphore = (
            asyncio.Semaphore(max_concurrency) if max_concurrency else None
        )

    async def __aenter__(self):
        """建立並回傳非同步 HTTP session。"""
        self.session = httpx.AsyncClient()
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
            self.session = httpx.AsyncClient()
        REQUEST_COUNTER.labels(endpoint=endpoint).inc()
        limiter = self.limiters.get(endpoint, self.default_limiter)
        if limiter:
            await limiter.acquire()
        url = f"{self.base_url}/{endpoint}"
        if self.proxy_base_url:
            url = f"{self.proxy_base_url}/{endpoint}"
        try:
            if self.semaphore:
                async with self.semaphore:
                    return await self._execute_request(url, params, limiter)
            return await self._execute_request(url, params, limiter)
        except httpx.HTTPStatusError as e:
            if limiter and e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 0))
                if retry_after > 0:
                    await asyncio.sleep(retry_after)
                limiter.record_failure(status_code=429)
            raise e
        except httpx.TimeoutException:
            if limiter:
                limiter.record_failure(timeout=True)
            raise

    async def _execute_request(
        self,
        url: str,
        params: dict | None,
        limiter: RateLimiter | None,
    ):
        """實際執行 HTTP 請求並處理例外。"""
        response = await self.session.get(url, params=params)
        response.raise_for_status()
        if limiter:
            limiter.record_failure()
        return response.json()

    async def call_batch(self, endpoints: list[tuple[str, dict]]):
        """批次呼叫多個 API，遵守最大併發限制。"""
        tasks = [
            asyncio.create_task(self.call_api(ep, params)) for ep, params in endpoints
        ]
        return await asyncio.gather(*tasks)
