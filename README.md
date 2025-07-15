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
