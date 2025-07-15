# 壓力測試

此專案提供 `stress/locust_proxy.py` 進行 Proxy 服務的壓力測試，預設模擬每秒 100 次請求。

## 執行方式

```bash
# 啟動 Proxy 服務
uvicorn 'data_ingestion.proxy:create_proxy_app("https://jsonplaceholder.typicode.com")' --factory --port 8000 &

# 在另一終端執行 Locust
locust -f stress/locust_proxy.py --headless -u 100 -r 100 -t 1m --host http://localhost:8000
```

執行完畢後可在終端看到請求統計資料。
