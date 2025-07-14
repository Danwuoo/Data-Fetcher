import time
from typing import Union

class RateLimiter:
    """
    A rate limiter using the Token Bucket algorithm.
    """

    def __init__(self, calls: int, period: Union[int, float]):
        """
        Initializes the RateLimiter.

        Args:
            calls: The number of calls allowed per period.
            period: The time period in seconds.
        """
        self.calls = calls
        self.period = period
        self.tokens = self.calls
        self.last_refill = time.monotonic()

    def _refill(self):
        """
        Refills the token bucket based on the elapsed time.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed > self.period:
            self.tokens = self.calls
            self.last_refill = now

    def check_rate(self):
        """
        Checks if a call is allowed. If not, it blocks until a token is available.
        """
        self._refill()
        while self.tokens <= 0:
            time.sleep(0.1)
            self._refill()
        self.tokens -= 1
