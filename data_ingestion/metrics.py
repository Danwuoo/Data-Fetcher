from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

# 計數每個端點的請求次數
REQUEST_COUNTER = Counter(
    "data_ingestion_requests_total",
    "API \u8acb\u6c42\u7e3d\u6578",
    ["endpoint"],
)

# 紀錄各端點剩餘 token
REMAINING_GAUGE = Gauge(
    "data_ingestion_remaining_tokens",
    "\u5269\u9918 token \u6578",
    ["endpoint"],
)

# 計數收到 429 回應的次數
RATE_LIMIT_429_COUNTER = Counter(
    "data_ingestion_429_total",
    "HTTP 429 \u7e3d\u6578",
    ["endpoint"],
)


def start_metrics_server(port: int) -> None:
    """\u555f\u52d5 Prometheus \u670d\u52d9\uff0c\u4f9b\u7d66 /metrics."""
    start_http_server(port)
