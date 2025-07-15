import asyncio
import random
import httpx
import pytest
from unittest.mock import AsyncMock

from data_ingestion.py.api_client import ApiClient
from data_ingestion.py.rate_limiter import RateLimiter
from data_ingestion.py.caching import LRUCache
from data_ingestion.py.data_source import APIDataSource
from data_ingestion.metrics import (
    REQUEST_COUNTER,
    REMAINING_GAUGE,
    RATE_LIMIT_429_COUNTER,
    CACHE_HIT_RATIO,
    start_metrics_server,
)


@pytest.mark.asyncio
async def test_request_counter(httpx_mock):
    base_url = "http://example.com"
    endpoint = "ep"
    httpx_mock.add_response(url=f"{base_url}/{endpoint}", json={"ok": True})
    before = REQUEST_COUNTER.labels(endpoint=endpoint)._value.get()
    async with ApiClient(base_url=base_url) as client:
        await client.call_api(endpoint)
    after = REQUEST_COUNTER.labels(endpoint=endpoint)._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_remaining_quota_gauge():
    endpoint = "quota"
    limiter = RateLimiter(calls=2, period=1, burst=2, endpoint=endpoint)
    await limiter.acquire()
    value = REMAINING_GAUGE.labels(endpoint=endpoint)._value.get()
    assert value == 1


@pytest.mark.asyncio
async def test_rate_limit_429_counter():
    endpoint = "err"
    limiter = RateLimiter(calls=2, period=1, endpoint=endpoint)
    before = RATE_LIMIT_429_COUNTER.labels(endpoint=endpoint)._value.get()
    limiter.record_failure(status_code=429)
    after = RATE_LIMIT_429_COUNTER.labels(endpoint=endpoint)._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_metrics_server_running():
    port = random.randint(9200, 9300)
    start_metrics_server(port)
    await asyncio.sleep(0.1)
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"http://localhost:{port}/metrics")
    assert resp.status_code == 200
    assert "data_ingestion_requests_total" in resp.text


@pytest.mark.asyncio
async def test_cache_hit_ratio():
    api_client = AsyncMock(spec=ApiClient)
    api_client.limiters = {}
    api_client.call_api = AsyncMock(return_value={"ok": True})
    cache = LRUCache(capacity=2)
    endpoint = "ratio"
    data_source = APIDataSource(
        api_client=api_client,
        cache=cache,
        endpoint=endpoint,
    )

    await data_source.read({"x": 1})  # miss
    ratio_after_miss = CACHE_HIT_RATIO.labels(endpoint=endpoint)._value.get()
    assert ratio_after_miss == 0

    await data_source.read({"x": 1})  # hit
    ratio_after_hit = CACHE_HIT_RATIO.labels(endpoint=endpoint)._value.get()
    assert ratio_after_hit == pytest.approx(0.5)
