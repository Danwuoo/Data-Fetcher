import asyncio
import os
import time
import yaml
from data_ingestion.metrics import REMAINING_GAUGE, RATE_LIMIT_429_COUNTER


class _TokenBucket:
    """簡易 Token Bucket 實作，供內部使用。"""

    def __init__(self, calls: int, period: float, burst: int) -> None:
        self.calls = calls
        self.period = period
        self.burst = burst
        self.tokens = float(burst)
        self.last_checked = time.monotonic()

    def refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_checked
        rate = self.calls / self.period
        self.tokens = min(self.burst, self.tokens + elapsed * rate)
        self.last_checked = now


class RateLimiter:
    """非同步 Token Bucket 速率限制器，支援多組速限與動態調整。"""

    def __init__(
        self,
        calls: int,
        period: float,
        burst: int | None = None,
        *,
        additional_limits: list[tuple[int, float, int]] | None = None,
        api_key: str | None = None,
        endpoint: str | None = None,
        config_path: str | None = None,
        fail_threshold: int = 3,
    ) -> None:
        self.api_key = api_key
        self.endpoint = endpoint
        self.config_path = config_path
        self.fail_threshold = fail_threshold
        self.fail_count = 0

        self._lock = asyncio.Lock()
        first_burst = burst if burst is not None else calls
        self.buckets = [_TokenBucket(calls, period, first_burst)]
        if additional_limits:
            for c, p, b in additional_limits:
                self.buckets.append(_TokenBucket(c, p, b))

        self._config_mtime = None
        if config_path and os.path.exists(config_path):
            self._config_mtime = os.path.getmtime(config_path)
        self._update_attrs()

    def _update_attrs(self) -> None:
        """同步主要參數供外部存取。"""
        main = self.buckets[0]
        self.calls = main.calls
        self.period = main.period
        self.burst = main.burst

    async def acquire(self):
        """等待直到所有速限都允許執行下一次請求。"""
        while True:
            async with self._lock:
                self._reload_if_needed()
                self._refill_tokens()
                if all(b.tokens >= 1 for b in self.buckets):
                    for b in self.buckets:
                        b.tokens -= 1
                    REMAINING_GAUGE.labels(endpoint=self.endpoint or "unknown").set(
                        self.buckets[0].tokens
                    )
                    return
                wait_time = max(
                    (1 - b.tokens) * b.period / b.calls for b in self.buckets
                )
            await asyncio.sleep(wait_time)

    def _refill_tokens(self) -> None:
        for bucket in self.buckets:
            bucket.refill()

    def _reload_if_needed(self) -> None:
        if not self.config_path:
            return
        try:
            mtime = os.path.getmtime(self.config_path)
        except FileNotFoundError:
            return
        reload_needed = self._config_mtime is None or mtime != self._config_mtime
        if not reload_needed:
            # 檔案時間未變化仍嘗試讀取，以避免檔案系統解析度不足
            pass
        self._config_mtime = mtime
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        configs: list[dict] = []
        if "global" in data:
            configs.append(data["global"])
        if self.api_key:
            configs.append(data.get("api_keys", {}).get(self.api_key, {}))
        if self.endpoint:
            configs.append(data.get("endpoints", {}).get(self.endpoint, {}))

        if configs:
            self.buckets = [
                _TokenBucket(
                    c.get("calls", 1),
                    c.get("period", 1.0),
                    c.get("burst", c.get("calls", 1)),
                )
                for c in configs
            ]
            self._update_attrs()

    def record_failure(
        self, *, status_code: int | None = None, timeout: bool = False
    ) -> None:
        """紀錄失敗事件並在達到門檻時降低速率。"""

        if status_code == 429 or timeout:
            self.fail_count += 1
            if status_code == 429:
                RATE_LIMIT_429_COUNTER.labels(
                    endpoint=self.endpoint or "unknown"
                ).inc()
        else:
            self.fail_count = 0

        if self.fail_count >= self.fail_threshold:
            for b in self.buckets:
                b.calls = max(1, int(b.calls * 0.8))
                b.burst = max(1, int(b.burst * 0.8))
                b.tokens = min(b.tokens, b.burst)
            self.fail_count = 0
            self._update_attrs()

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

        limits: list[tuple[int, float, int]] = []

        global_cfg = data.get("global")
        if global_cfg:
            limits.append(
                (
                    global_cfg.get("calls", 1),
                    global_cfg.get("period", 1.0),
                    global_cfg.get("burst", global_cfg.get("calls", 1)),
                )
            )

        key_cfg = data.get("api_keys", {}).get(api_key, {})
        ep_cfg = data.get("endpoints", {}).get(endpoint, {})

        calls = ep_cfg.get("calls", key_cfg.get("calls", 1))
        period = ep_cfg.get("period", key_cfg.get("period", 1.0))
        burst = ep_cfg.get("burst", key_cfg.get("burst", calls))

        if key_cfg:
            limits.append(
                (
                    key_cfg.get("calls", 1),
                    key_cfg.get("period", 1.0),
                    key_cfg.get("burst", key_cfg.get("calls", 1)),
                )
            )
        if ep_cfg:
            limits.append(
                (
                    ep_cfg.get("calls", 1),
                    ep_cfg.get("period", 1.0),
                    ep_cfg.get("burst", ep_cfg.get("calls", 1)),
                )
            )

        return cls(
            calls=calls,
            period=period,
            burst=burst,
            additional_limits=limits,
            api_key=api_key,
            endpoint=endpoint,
            config_path=config_path,
        )
