# 監控與警報設定

本專案所有指標皆由 Prometheus Exporter 暴露。啟動方式如下：

```python
from metrics import start_exporter

start_exporter(8000)  # 在 8000 埠口提供 /metrics
```

## Grafana 連線

1. 在 Grafana 新增 Prometheus Data Source，URL 指向 `http://<host>:8000`。
2. 建立 Dashboard 以視覺化 `data_ingestion_requests_total`、`data_processing_steps_total` 等指標。
3. 設定警報條件範例：
   - 表達式：`sum(rate(data_ingestion_429_total[1m])) > 100`
   - 評估頻率：1 分鐘
   - 觸發時可透過 Email 或其他通知管道告警。
