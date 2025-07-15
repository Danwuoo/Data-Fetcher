import time
import pytest
import subprocess
import shutil
import asyncio
import redis.asyncio as redis

from data_ingestion.py.redis_rate_limiter import RedisRateLimiter


@pytest.mark.asyncio
async def test_redis_rate_limiter_wait(tmp_path):
    if not shutil.which("redis-server"):
        pytest.skip("redis-server not available")

    proc = subprocess.Popen(
        [
            "redis-server",
            "--port",
            "6380",
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    await asyncio.sleep(0.2)
    try:
        r = redis.Redis.from_url("redis://localhost:6380")
        limiter = RedisRateLimiter(redis=r, key="test", calls=2, period=1)
        start = time.monotonic()
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        delta = time.monotonic() - start
        assert delta >= 0.49
    finally:
        proc.terminate()
        proc.wait()
