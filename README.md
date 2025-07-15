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
## CLI 使用範例

安裝完成後可透過 `zxq` 指令進行 Walk-Forward 切分：

```bash
zxq walk-forward 10 3 2 2
```

會輸出每一折的訓練與測試索引。


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

在程式中可使用 `RateLimiter.from_config("default", "example")` 建立實例，隨時重新載入檔案內容。若 `rate_limits.yml` 更新，可呼叫 `data_ingestion.reload_limits()` 立即套用新限制。
## 啟動 Proxy 服務

Proxy 透過 FastAPI 以及 `create_proxy_app()` 建立。使用 `uvicorn --factory` 啟動並指定目標 API：

```bash
uvicorn 'data_ingestion.proxy:create_proxy_app("https://api.example.com")' --factory --port 8000
```

### Docker Compose 快速啟動

倉庫內已提供 `docker-compose.yml` 及 `proxy/conf` 下的 NGINX 設定檔，
可直接啟動 NGINX 與 FastAPI Proxy 組成的服務：

```bash
docker compose up --build
```

Nginx 會在本機的 80 連接埠對外服務，FastAPI 於容器內的 8000
連接埠執行，可透過 `PROXY_TARGET` 環境變數調整轉發目標。

建議 `fastapi>=0.110`、`uvicorn>=0.23`。

## DAG 執行與儲存架構

Prefect Flow `data_pipeline_flow` 可透過部署檔案觸發：

```bash
prefect deployment run data-pipeline
```

處理後的資料會存入 `HybridStorageManager` 管理的 Hot/Warm/Cold 層級，詳細說明請參考 [`docs/data_storage.md`](docs/data_storage.md)。
Cold tier 使用 S3 時，建議開啟 Versioning 並設定跨區域複製，以確保災難復原需求。


### Airflow DAG 觸發範例

啟動 Airflow 後，可透過下列指令手動觸發 `ingest_process_dag`：

```bash
airflow dags trigger ingest_process_dag
```


## 監控

使用 `metrics.start_exporter()` 啟動 Prometheus 指標服務，Grafana 可連至對應的 Prometheus URL 建立儀表板。警報範例：

```
sum(rate(data_ingestion_429_total[1m])) > 100
```

更多細節請見 [`docs/monitoring.md`](docs/monitoring.md)。


## 版本標籤與 CHANGELOG

透過 `scripts/tag_with_changelog.sh` 建立版本標籤，可自動更新 `docs/CHANGELOG.md`：

```bash
./scripts/tag_with_changelog.sh v1.0.0 "初始版本"
```

此腳本會執行 `git tag` 並在 CHANGELOG 新增一行記錄。

## CI 工作流程

本庫的 GitHub Actions (`.github/workflows/ci.yml`) 會在推送或 PR 時自動執行 Flake8 與 pytest，僅於程式碼變更時觸發，文件修改則不會執行。
=======
## Catalog 漂移檢查

`pipelines/catalog_drift.py` 內建 Prefect 排程，每日 0 點自動呼叫
`catalog_drift_flow()` 以比對 Catalog 與實際資料的 schema。若發現
不一致，可透過設定 `SLACK_WEBHOOK_URL` 環境變數接收警報。

## Pipeline 步驟範例

下列文件展示各處理步驟執行前後的資料變化：
- [DataCleanser](docs/steps/DataCleanser.md)
- [FeatureEngineer](docs/steps/FeatureEngineer.md)
- [MissingValueHandler](docs/steps/MissingValueHandler.md)
- [TimeAligner](docs/steps/TimeAligner.md)


