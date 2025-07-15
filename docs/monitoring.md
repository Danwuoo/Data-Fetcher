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

## Prometheus 與 Alertmanager

1. 安裝 Prometheus，並在 `prometheus.yml` 中加入下列設定：

   ```yaml
   scrape_configs:
     - job_name: data-fetcher
       static_configs:
         - targets: ['localhost:8000']
   rule_files:
     - monitoring/alert_rules.yml
   ```
2. Alertmanager 可直接匯入 `monitoring/alert_rules.yml` 取得預設警報條件。

## 匯入 Grafana 儀表板

1. 於 Grafana 的 **Import dashboard** 畫面選擇 `monitoring/grafana_data_fetcher.json`。
2. 確認資料來源為前述 Prometheus。
