import time
import httpx
import pytest

from backtest_data_module.data_ingestion.py.middleware import RateLimitMiddleware
from backtest_data_module.data_ingestion.py.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_middleware_acquires_token():
    limiter = RateLimiter(calls=1, period=0.2)
    middleware = RateLimitMiddleware({'ep': limiter})

    async def handler(request):
        return httpx.Response(200, json={'ok': True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        transport=transport,
        event_hooks={
            'request': [middleware.on_request],
            'response': [middleware.on_response],
        },
        base_url='http://test',
    ) as client:
        start = time.monotonic()
        await client.get('/ep')
        await client.get('/ep')
        delta = time.monotonic() - start
    assert delta >= 0.19


@pytest.mark.asyncio
async def test_middleware_respects_retry_after():
    limiter = RateLimiter(calls=5, period=1)
    middleware = RateLimitMiddleware({'ep': limiter})
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={'Retry-After': '0.1'})
        return httpx.Response(200, json={'ok': True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        transport=transport,
        event_hooks={
            'request': [middleware.on_request],
            'response': [middleware.on_response],
        },
        base_url='http://test',
    ) as client:
        start = time.monotonic()
        resp1 = await client.get('/ep')
        assert resp1.status_code == 429
        resp2 = await client.get('/ep')
        delta = time.monotonic() - start
    assert resp2.json() == {'ok': True}
    assert delta >= 0.1
