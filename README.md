# Data-Fetcher

若要檢視 CI 狀態徽章，請將下列網址中的 `<YOUR_GITHUB_ACCOUNT>` 替換成你的 GitHub 帳號：

[![CI](https://github.com/<YOUR_GITHUB_ACCOUNT>/Data-Fetcher/actions/workflows/ci.yml/badge.svg)](https://github.com/<YOUR_GITHUB_ACCOUNT>/Data-Fetcher/actions/workflows/ci.yml)

此專案提供非同步 API 資料擷取與簡易處理管線，方便在 ZXQuant 或其他回測系統中使用。

## 安裝

1. 建議使用 Python 3.12+
2. 安裝相依套件：
   ```bash
   pip install -r requirements.txt
   ```

## 範例

執行 `example.py` 會示範如何以 `APIDataSource` 搭配快取與速率限制器抓取資料：

```bash
python example.py
```

## 測試

專案採用 `pytest`，並包含非同步測試範例。執行所有測試：

```bash
pytest
```

## 環境變數

`ApiClient` 預設會讀取下列環境變數調整批次大小與併發量：

| 變數名稱 | 預設值 | 說明 |
|-----------|-------|------------------------------------------------|
| `BATCH_SIZE` | `1` | `call_batch` 同時送出的請求數 |
| `CONCURRENCY` | `0` | API 連線的最大併發數，0 表示不限 |


## 啟動排程器與註冊任務

1. 安裝所有依賴：
   ```bash
   pip install -r requirements.txt
   ```
2. 啟動 Prefect Orion 伺服器：
   ```bash
   prefect orion start
   ```
3. 建立並套用部署檔案：
   ```bash
   prefect deployment build pipelines/data_pipeline.py:data_pipeline_flow -n data-pipeline -q default
   prefect deployment apply data_pipeline_flow-deployment.yaml
   ```
4. 啟動執行代理：
   ```bash
   prefect agent start -q default
   ```

## 速率限制設定檔

`RateLimiter` 可透過 `rate_limits.yml` 讀取多組速率限制，格式如下：

```yaml
global:
  calls: 100
  period: 60
  burst: 100
api_keys:
  default:
    calls: 5
    period: 1
    burst: 5
endpoints:
  example:
    calls: 10
    period: 1
    burst: 10
```

在程式中可使用 `RateLimiter.from_config("default", "example")` 建立實例，隨時重新載入檔案內容。

## 啟動 Proxy 服務

Proxy 透過 FastAPI 以及 `create_proxy_app()` 建立。使用 `uvicorn --factory` 啟動並指定目標 API：

```bash
uvicorn 'data_ingestion.proxy:create_proxy_app("https://api.example.com")' --factory --port 8000
```

建議 `fastapi>=0.110`、`uvicorn>=0.23`。

## DAG 執行與儲存架構

Prefect Flow `data_pipeline_flow` 可透過部署檔案觸發：

```bash
prefect deployment run data-pipeline
```

處理後的資料會存入 `HybridStorageManager` 管理的 Hot/Warm/Cold 層級，詳細說明請參考 [`docs/data_storage.md`](docs/data_storage.md)。

## 監控

使用 `metrics.start_exporter()` 啟動 Prometheus 指標服務，Grafana 可連至對應的 Prometheus URL 建立儀表板。警報範例：

```
sum(rate(data_ingestion_429_total[1m])) > 100
```

更多細節請見 [`docs/monitoring.md`](docs/monitoring.md)。
