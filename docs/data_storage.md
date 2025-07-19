# 多層級儲存策略

本專案採用 Hot、Warm、Cold 三層架構，以取得效能與成本的平衡：

- **Hot**：使用 DuckDB 於記憶體中儲存，適合 7 天內的即時查詢。
- **Warm**：採用 TimescaleDB/PostgreSQL，適合 30~180 天的歷史資料與特徵工程。
- **Cold**：將長期資料以 Parquet 形式存放至 S3 或 MinIO，可保存數年。

`DataHandler` 類別提供統一介面操作分層儲存系統，透過 `HybridStorageManager` 依需求讀寫資料，並在容量達上限時自動向下遷移。

```python
import polars as pl
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.data_storage.storage_backend import HybridStorageManager

# 初始化 DataHandler
storage_manager = HybridStorageManager()
data_handler = DataHandler(storage_manager)

# 寫入資料至 Hot tier
data_handler.storage_manager.write(pl.DataFrame({'a': [1, 2]}), 'prices', tier='hot')

# 讀取資料，會自較熱層級開始查詢
recent = data_handler.read('prices')

# 將資料遷移到 Cold tier
data_handler.migrate('prices', 'hot', 'cold')
```

## S3 設定建議

若 Cold tier 使用 S3，建議開啟版本控制避免檔案覆寫。為了跨區備份，可啟用跨區複製並指定備援 bucket，以在主要區域故障時確保資料可存取。

## 指令列操作

亦可透過 `zxq` CLI 搬移資料表：

```bash
zxq storage migrate --table prices --to warm
```

加入 `--dry-run` 參數即可僅顯示預期動作，不會真正執行。

## 命中率統計與自動遷移

`HybridStorageManager` 會記錄每個表格的存取時間，預設由 Prefect 任務計算近七日的命中次數。若 Hot tier 使用率超過 `hot_usage_threshold`，且表格的七日命中次數低於 `low_hit_threshold`，系統將自動將其遷移至較冷的層級。這些參數可在 `storage.yaml` 中調整：

```yaml
low_hit_threshold: 2
hot_usage_threshold: 0.8
hit_stats_schedule: "0 1 * * *"
```

`pipelines/hit_stats.py` 提供了範例流程，可透過 `prefect deployment build` 部署後排程執行。
