import asyncio
import httpx
import pytest
from fastapi import FastAPI

from backtest_data_module.data_ingestion.py.api_client import ApiClient


@pytest.mark.asyncio
async def test_call_api_semaphore():
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
    client = ApiClient(base_url="http://test", max_concurrency=1)
    client.session = httpx.AsyncClient(transport=transport, base_url="http://test")

    endpoints = ["a", "b", "c"]
    results = await asyncio.gather(*[client.call_api(ep) for ep in endpoints])
    await client.session.aclose()

    assert [r["name"] for r in results] == endpoints
    assert peak <= 1
