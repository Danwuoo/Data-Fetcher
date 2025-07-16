from prometheus_client import Counter, Gauge, Histogram, start_http_server
from backtest_data_module.data_ingestion.metrics import (
    REQUEST_COUNTER,
    RATE_LIMIT_429_COUNTER,
    REMAINING_GAUGE,
    CACHE_HIT_RATIO,
)


# \u8cc7\u6599\u8655\u7406\u6b65\u9a5f\u4e2d\u6b65\u7684\u7d50\u675f\u6578
PROCESSING_STEP_COUNTER = Counter(
    "data_processing_steps_total",
    "\u8655\u7406\u6b65\u6578",
    ["step"],
)

# \u7bc4\u5f0f\u9a57\u8b49\u5931\u6557\u7d50\u675f\u6578
SCHEMA_VALIDATION_FAIL_COUNTER = Counter(
    "schema_validation_fail_total",
    "\u7bc4\u5f0f\u9a57\u8b49\u5931\u6557\u6b21\u6578",
    ["step"],
)

# \u8cc7\u6599\u5b58\u50b3\u7684\u5b58\u53d6\u6b21\u6578
STORAGE_WRITE_COUNTER = Counter(
    "data_storage_write_total",
    "\u5beb\u5165\u6b21\u6578",
    ["tier"],
)

STORAGE_READ_COUNTER = Counter(
    "data_storage_read_total",
    "\u8b80\u53d6\u6b21\u6578",
    ["tier"],
)

# \u7a7a\u9593\u79fb\u7d3c\u8017\u6642\u7de8\u8b77\u5f8c\u7684\u76f4\u65b7\u7a4d\u5206
MIGRATION_LATENCY_MS = Histogram(
    "data_storage_migration_latency_ms",
    "\u8cc7\u6599\u79fb\u7d3c\u8017\u6642\uff08ms\uff09",
    ["src_tier", "dst_tier"],
)

# \u6bcf\u500b tier 讀\u53d6的命中率
TIER_HIT_RATE = Gauge(
    "data_storage_tier_hit_rate",
    "\u5404\u5c64\u7d1a\u8b80\u53d6\u547d\u4e2d\u7387",
    ["tier"],
)


def update_tier_hit_rate() -> None:
    """\u91cd\u65b0\u8a08\u7b97\u5c64\u7d1a\u547d\u4e2d\u7387."""
    total = sum(
        STORAGE_READ_COUNTER.labels(tier=t)._value.get()
        for t in ["hot", "warm", "cold"]
    )
    if total == 0:
        return
    for t in ["hot", "warm", "cold"]:
        count = STORAGE_READ_COUNTER.labels(tier=t)._value.get()
        TIER_HIT_RATE.labels(tier=t).set(count / total)


def start_exporter(port: int) -> None:
    """\u555f\u52d5 Prometheus Exporter."""
    start_http_server(port)


__all__ = [
    "REQUEST_COUNTER",
    "RATE_LIMIT_429_COUNTER",
    "REMAINING_GAUGE",
    "CACHE_HIT_RATIO",
    "PROCESSING_STEP_COUNTER",
    "SCHEMA_VALIDATION_FAIL_COUNTER",
    "STORAGE_WRITE_COUNTER",
    "STORAGE_READ_COUNTER",
    "MIGRATION_LATENCY_MS",
    "TIER_HIT_RATE",
    "update_tier_hit_rate",
    "start_exporter",
]
