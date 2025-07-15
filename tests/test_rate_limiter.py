import os
import time
import asyncio
import pytest
import yaml
from data_ingestion.py.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter():
    """驗證超出速率時會等待。"""
    limiter = RateLimiter(calls=5, period=1)
    start_time = time.monotonic()

    for _ in range(5):
        await limiter.acquire()

    await limiter.acquire()
    end_time = time.monotonic()

    assert end_time - start_time >= 0.19


@pytest.mark.asyncio
async def test_adaptive_throttle():
    """連續 429 時應自動降低速率。"""
    limiter = RateLimiter(calls=5, period=1, fail_threshold=2)
    original = limiter.calls
    limiter.record_failure(status_code=429)
    limiter.record_failure(status_code=429)
    assert limiter.calls < original


@pytest.mark.asyncio
async def test_refill():
    """
    Tests that the token bucket is refilled after the period.
    """
    limiter = RateLimiter(calls=5, period=1)

    for _ in range(5):
        await limiter.acquire()

    await asyncio.sleep(1)

    for _ in range(5):
        await limiter.acquire()


@pytest.mark.asyncio
async def test_from_config(tmp_path):
    """確認能夠從 YAML 載入設定。"""
    config = {
        "global": {"calls": 10, "period": 1, "burst": 10},
        "api_keys": {"key": {"calls": 2, "period": 1, "burst": 2}},
        "endpoints": {"/ep": {"calls": 3, "period": 1, "burst": 3}},
    }
    path = tmp_path / "rate.yml"
    path.write_text(yaml.dump(config), encoding="utf-8")

    limiter = RateLimiter.from_config("key", "/ep", config_path=str(path))
    assert limiter.calls == 3
    assert limiter.burst == 3


@pytest.mark.asyncio
async def test_reload_config(tmp_path):
    """設定檔更新後應重新載入參數。"""
    config = {"api_keys": {"key": {"calls": 1, "period": 1, "burst": 1}}}
    path = tmp_path / "rate.yml"
    path.write_text(yaml.dump(config), encoding="utf-8")
    limiter = RateLimiter.from_config("key", "", config_path=str(path))
    await limiter.acquire()

    config["api_keys"]["key"]["calls"] = 5
    config["api_keys"]["key"]["burst"] = 5
    path.write_text(yaml.dump(config), encoding="utf-8")
    os.utime(path, None)

    start = time.monotonic()
    await limiter.acquire()
    delta = time.monotonic() - start

    assert limiter.calls == 5
    assert delta < 0.5

    await asyncio.sleep(1)

    # After 1 second, the bucket should be full again
    for _ in range(5):
        await limiter.acquire()
