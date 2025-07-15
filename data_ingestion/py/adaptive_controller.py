from __future__ import annotations

import os


class AdaptiveController:
    """使用 EWMA 調整批次大小與併發數。"""

    def __init__(
        self,
        batch_size: int,
        concurrency: int,
        *,
        alpha: float | None = None,
        target_latency: float | None = None,
        min_batch: int | None = None,
        max_batch: int | None = None,
        min_concurrency: int | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        env = os.getenv
        self.alpha = float(env("THROTTLE_ALPHA", str(alpha or 0.2)))
        self.target_latency = float(env("LATENCY_TARGET", str(target_latency or 0.5)))
        self.min_batch = int(env("MIN_BATCH_SIZE", str(min_batch or 1)))
        self.max_batch = int(env("MAX_BATCH_SIZE", str(max_batch or batch_size)))
        self.min_concurrency = int(env("MIN_CONCURRENCY", str(min_concurrency or 1)))
        self.max_concurrency = int(
            env("MAX_CONCURRENCY", str(max_concurrency or max(concurrency, 1)))
        )

        self.batch_size = max(batch_size, self.min_batch)
        self.concurrency = max(concurrency, self.min_concurrency)
        self.ewma_latency = self.target_latency
        self.remaining = None

    def record(self, latency: float, remaining: int | None = None) -> None:
        """紀錄回應延遲與剩餘配額，並更新內部狀態。"""

        self.ewma_latency = self.alpha * latency + (1 - self.alpha) * self.ewma_latency
        if remaining is not None:
            self.remaining = remaining
        self._adjust()

    def _adjust(self) -> None:
        if self.ewma_latency > self.target_latency * 1.2:
            if self.batch_size > self.min_batch:
                self.batch_size -= 1
            if self.concurrency > self.min_concurrency:
                self.concurrency -= 1
        elif self.ewma_latency < self.target_latency * 0.8:
            if self.remaining is None or self.remaining > 1:
                if self.batch_size < self.max_batch:
                    self.batch_size += 1
                if self.concurrency < self.max_concurrency:
                    self.concurrency += 1

    def get_params(self) -> tuple[int, int]:
        return self.batch_size, self.concurrency
