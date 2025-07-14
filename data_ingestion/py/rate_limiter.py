import asyncio
import time

class RateLimiter:
    """非同步 token bucket 速率限制器。"""

    def __init__(self, calls: int, period: float):
        self.calls = calls
        self.period = period
        self._lock = asyncio.Lock()
        self._calls_made = 0
        self._period_start = time.monotonic()

    async def acquire(self):
        """等待直到可執行下一次請求。"""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._period_start
            if elapsed >= self.period:
                self._calls_made = 0
                self._period_start = now
            if self._calls_made >= self.calls:
                await asyncio.sleep(self.period - elapsed)
                self._calls_made = 0
                self._period_start = time.monotonic()
            self._calls_made += 1
