from __future__ import annotations

import asyncio
import httpx

from backtest_data_module.data_ingestion.py.rate_limiter import RateLimiter
from backtest_data_module.data_ingestion.py.redis_rate_limiter import RedisRateLimiter


class RateLimitMiddleware:
    """HTTPX middleware 在發送請求前等待直到可以進行下一次請求."""

    def __init__(
        self,
        limiters: dict[str, RateLimiter | RedisRateLimiter] | None = None,
        default_limiter: RateLimiter | RedisRateLimiter | None = None,
    ) -> None:
        self.limiters = limiters or {}
        self.default_limiter = default_limiter

    async def on_request(self, request: httpx.Request) -> None:
        endpoint = request.url.path.lstrip("/")
        limiter = self.limiters.get(endpoint, self.default_limiter)
        request.extensions["limiter"] = limiter
        if limiter:
            await limiter.acquire()

    async def on_response(self, response: httpx.Response) -> None:
        limiter = response.request.extensions.get("limiter")
        if not limiter:
            return
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 0))
            if retry_after > 0:
                await asyncio.sleep(retry_after)
            limiter.record_failure(status_code=429)
        else:
            limiter.record_failure()
