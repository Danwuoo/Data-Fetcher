import asyncio
import httpx
import pytest
from fastapi import FastAPI
from backtest_data_module.data_ingestion.py.api_client import ApiClient


@pytest.mark.asyncio
async def test_adaptive_batch_and_concurrency():
    app = FastAPI()
    delay = 0.01
    count = 0

    @app.get("/{name}")
    async def handle(name: str):
        nonlocal delay, count
        count += 1
        await asyncio.sleep(delay)
        if count > 3:
            delay = 0.2
        return {"name": name}

    transport = httpx.ASGITransport(app=app)
    client = ApiClient(base_url="http://test", max_concurrency=3, batch_size=3)
    client.session = httpx.AsyncClient(transport=transport, base_url="http://test")

    endpoints = [(str(i), {}) for i in range(6)]
    await client.call_batch(endpoints)
    before_batch = client.batch_size
    before_conc = client.max_concurrency

    endpoints = [(str(i), {}) for i in range(6, 12)]
    await client.call_batch(endpoints)

    await client.session.aclose()
    assert client.batch_size <= before_batch
    assert client.max_concurrency <= before_conc
