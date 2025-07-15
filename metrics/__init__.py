from prometheus_client import Counter, start_http_server
from data_ingestion.metrics import (
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


def start_exporter(port: int) -> None:
    """\u555f\u52d5 Prometheus Exporter."""
    start_http_server(port)


__all__ = [
    "REQUEST_COUNTER",
    "RATE_LIMIT_429_COUNTER",
    "REMAINING_GAUGE",
    "CACHE_HIT_RATIO",
    "PROCESSING_STEP_COUNTER",
    "STORAGE_WRITE_COUNTER",
    "STORAGE_READ_COUNTER",
    "start_exporter",
]
