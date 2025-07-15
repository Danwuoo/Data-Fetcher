from __future__ import annotations

import logging
from fastapi import FastAPI, Request
from fastapi.responses import Response
import httpx
from tenacity import retry, wait_random_exponential, stop_after_attempt

from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache


logger = logging.getLogger(__name__)


def create_proxy_app(
    target_base_url: str,
    *,
    rate_limiter: RateLimiter | None = None,
    cache: LRUCache | None = None,
) -> FastAPI:
    """建立反向 proxy FastAPI 應用程式。"""

    limiter = rate_limiter or RateLimiter(calls=5, period=1)
    proxy_cache = cache or LRUCache(capacity=128, ttl=60)
    app = FastAPI()
    client = httpx.AsyncClient(base_url=target_base_url)

    @retry(wait=wait_random_exponential(min=1, max=4), stop=stop_after_attempt(3))
    async def _forward(method: str, url: str, params: dict) -> httpx.Response:
        resp = await client.request(method, url, params=params)
        resp.raise_for_status()
        return resp

    @app.api_route("/{path:path}", methods=["GET"])  # 簡化僅支援 GET
    async def proxy(path: str, request: Request) -> Response:
        await limiter.acquire()
        cache_key = f"{path}?{request.query_params}"
        cached = await proxy_cache.get(cache_key)
        if cached is not None:
            logger.info("cache hit %s", cache_key)
            return Response(
                content=cached["content"],
                status_code=cached["status"],
                media_type=cached["media_type"],
            )
        try:
            resp = await _forward(
                request.method,
                f"/{path}",
                dict(request.query_params),
            )
        except httpx.HTTPError as exc:  # pragma: no cover - 轉換成 502
            logger.error("proxy error: %s", exc)
            return Response(content=str(exc), status_code=502)
        logger.info("forward %s", request.url.path)
        await proxy_cache.set(
            cache_key,
            {
                "content": resp.content,
                "status": resp.status_code,
                "media_type": resp.headers.get("content-type"),
            },
        )
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type"),
        )

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await client.aclose()

    return app
