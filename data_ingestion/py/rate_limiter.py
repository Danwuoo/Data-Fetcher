import asyncio
import os
import time
import yaml


class RateLimiter:
    """非同步 Token Bucket 速率限制器，支援動態重新載入設定。"""

    def __init__(
        self,
        calls: int,
        period: float,
        burst: int | None = None,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        config_path: str | None = None,
    ) -> None:
        self.calls = calls
        self.period = period
        self.burst = burst if burst is not None else calls
        self.api_key = api_key
        self.endpoint = endpoint
        self.config_path = config_path

        self._lock = asyncio.Lock()
        self._tokens = float(self.burst)
        self._last_checked = time.monotonic()
        self._config_mtime = None
        if config_path and os.path.exists(config_path):
            self._config_mtime = os.path.getmtime(config_path)

    async def acquire(self):
        """等待直到可執行下一次請求，同時檢查設定是否更新。"""
        while True:
            async with self._lock:
                self._reload_if_needed()
                self._refill_tokens()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait_time = (1 - self._tokens) * self.period / self.calls
            await asyncio.sleep(wait_time)

    def _refill_tokens(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_checked
        rate = self.calls / self.period
        self._tokens = min(self.burst, self._tokens + elapsed * rate)
        self._last_checked = now

    def _reload_if_needed(self) -> None:
        if not self.config_path:
            return
        try:
            mtime = os.path.getmtime(self.config_path)
        except FileNotFoundError:
            return
        if self._config_mtime is not None and mtime == self._config_mtime:
            return
        self._config_mtime = mtime
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        params: dict = {}
        if self.api_key:
            params.update(data.get("api_keys", {}).get(self.api_key, {}))
        if self.endpoint:
            params.update(data.get("endpoints", {}).get(self.endpoint, {}))

        self.calls = params.get("calls", self.calls)
        self.period = params.get("period", self.period)
        self.burst = params.get("burst", self.burst)
        self._tokens = min(self.burst, self._tokens)
        self._last_checked = time.monotonic()

    @classmethod
    def from_config(
        cls,
        api_key: str,
        endpoint: str,
        config_path: str = "rate_limits.yml",
    ) -> "RateLimiter":
        """依照設定檔產生 RateLimiter。"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        params: dict = {}
        params.update(data.get("api_keys", {}).get(api_key, {}))
        params.update(data.get("endpoints", {}).get(endpoint, {}))

        calls = params.get("calls", 1)
        period = params.get("period", 1.0)
        burst = params.get("burst", calls)
        return cls(
            calls=calls,
            period=period,
            burst=burst,
            api_key=api_key,
            endpoint=endpoint,
            config_path=config_path,
        )
