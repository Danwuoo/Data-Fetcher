import asyncio
import pytest
import httpx
from httpx import Response
from backtest_data_module.data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_call_api_success(httpx_mock):
    """
    Tests a successful API call.
    """
    base_url = "http://test.com"
    endpoint = "test"
    expected_response = {"data": "success"}
    httpx_mock.add_response(
        url=f"{base_url}/{endpoint}",
        json=expected_response,
    )
    limiter = RateLimiter(calls=2, period=1)
    async with ApiClient(base_url=base_url, limiters={endpoint: limiter}) as api_client:
        response = await api_client.call_api(endpoint)
    assert response == expected_response


@pytest.mark.asyncio
async def test_call_api_retry(httpx_mock):
    """
    Tests the retry logic for a 429 error.
    """
    base_url = "http://test.com"
    endpoint = "test"
    httpx_mock.add_callback(
        lambda request, ext: Response(429, headers={"Retry-After": "0.1"}),
    )
    httpx_mock.add_response(
        url=f"{base_url}/{endpoint}",
        json={"data": "success"},
    )
    limiter = RateLimiter(calls=2, period=1, fail_threshold=1)
    async with ApiClient(base_url=base_url, limiters={endpoint: limiter}) as api_client:
        response = await api_client.call_api(endpoint)
        assert response == {"data": "success"}
        assert len(httpx_mock.get_requests()) == 2


@pytest.mark.asyncio
async def test_call_api_with_proxy(httpx_mock):
    base_url = "http://target.com"
    proxy_url = "http://proxy"
    endpoint = "foo"
    httpx_mock.add_response(url=f"{proxy_url}/{endpoint}", json={"ok": True})
    async with ApiClient(
        base_url=base_url,
        proxy_base_url=proxy_url,
    ) as client:
        resp = await client.call_api(endpoint)
        assert resp == {"ok": True}
        assert len(httpx_mock.get_requests()) == 1


@pytest.mark.asyncio
async def test_call_batch_with_semaphore():
    """確認批次呼叫會遵守併發上限。"""
    from fastapi import FastAPI

    app = FastAPI()
    active = 0
    peak = 0

    @app.get("/{name}")
    async def handle(name: str):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1
        return {"name": name}

    transport = httpx.ASGITransport(app=app)
    client = ApiClient(base_url="http://test", max_concurrency=2)
    client.session = httpx.AsyncClient(transport=transport, base_url="http://test")

    endpoints = [(f"ep{i}", {}) for i in range(5)]
    results = await client.call_batch(endpoints)

    await client.session.aclose()

    assert [r["name"] for r in results] == [f"ep{i}" for i in range(5)]
    assert peak <= 2
