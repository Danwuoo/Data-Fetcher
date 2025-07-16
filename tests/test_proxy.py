import time
import pytest
import httpx
from backtest_data_module.data_ingestion.proxy import create_proxy_app
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache


@pytest.mark.asyncio
async def test_proxy_forwarding(httpx_mock):
    target_url = "http://remote.com"
    app = create_proxy_app(target_url)
    httpx_mock.add_response(url=f"{target_url}/data", json={"a": 1})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://proxy"
    ) as client:
        resp = await client.get("/data")
        assert resp.status_code == 200
        assert resp.json() == {"a": 1}
        assert len(httpx_mock.get_requests()) == 1


@pytest.mark.asyncio
async def test_proxy_rate_limit(httpx_mock):
    limiter = RateLimiter(calls=1, period=0.5)
    cache = LRUCache(capacity=1, ttl=0)
    app = create_proxy_app(
        "http://remote.com",
        rate_limiter=limiter,
        cache=cache,
    )
    httpx_mock.add_response(url="http://remote.com/data", json={"a": 1})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://proxy"
    ) as client:
        await client.get("/data")
        httpx_mock.add_response(url="http://remote.com/data", json={"a": 2})
        start = time.monotonic()
        await client.get("/data")
        delta = time.monotonic() - start
        assert delta >= 0.49
