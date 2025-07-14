import time
import asyncio
import pytest
from data_ingestion.py.rate_limiter import RateLimiter

@pytest.mark.asyncio
async def test_rate_limiter():
    """
    Tests that the rate limiter correctly limits the number of calls.
    """
    limiter = RateLimiter(calls=5, period=1)
    start_time = time.monotonic()

    for _ in range(5):
        await limiter.acquire()

    # The 6th call should block
    await limiter.acquire()
    end_time = time.monotonic()

    # The total time should be at least 1 second
    assert end_time - start_time >= 1

@pytest.mark.asyncio
async def test_refill():
    """
    Tests that the token bucket is refilled after the period.
    """
    limiter = RateLimiter(calls=5, period=1)

    for _ in range(5):
        await limiter.acquire()

    await asyncio.sleep(1)

    # After 1 second, the bucket should be full again
    for _ in range(5):
        await limiter.acquire()
