from ratelimit import limits, sleep_and_retry

class RateLimiter:
    """
    A rate limiter using the ratelimit library.
    """

    def __init__(self, calls: int, period: int):
        """
        Initializes the RateLimiter.

        Args:
            calls: The number of calls allowed per period.
            period: The time period in seconds.
        """
        self.limiter = limits(calls=calls, period=period)

    def check_rate(self):
        """
        Applies the rate limit.
        """
        @sleep_and_retry
        @self.limiter
        def _check_rate():
            pass
        _check_rate()
