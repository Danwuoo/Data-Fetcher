import time
import unittest
from data_ingestion.py.rate_limiter import RateLimiter

class TestRateLimiter(unittest.TestCase):
    """
    Tests for the RateLimiter class.
    """

    def test_rate_limiter(self):
        """
        Tests that the rate limiter correctly limits the number of calls.
        """
        limiter = RateLimiter(calls=5, period=1)
        start_time = time.monotonic()

        for _ in range(5):
            limiter.check_rate()

        # The 6th call should block
        limiter.check_rate()
        end_time = time.monotonic()

        # The total time should be at least 1 second
        self.assertGreaterEqual(end_time - start_time, 1)

    def test_refill(self):
        """
        Tests that the token bucket is refilled after the period.
        """
        limiter = RateLimiter(calls=5, period=1)

        for _ in range(5):
            limiter.check_rate()

        time.sleep(1)

        # After 1 second, the bucket should be full again
        for _ in range(5):
            limiter.check_rate()

if __name__ == "__main__":
    unittest.main()
