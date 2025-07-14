# Data-Fetcher

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


## StorageBackend

本專案提供 `HybridStorageManager` 以統一熱、溫、冷三層資料存取。預設以 DuckDB 示範，可依需求擴充其他後端。
