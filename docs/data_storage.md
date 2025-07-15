# 多層級儲存策略

本專案採用 Hot、Warm、Cold 三層級架構，以取得效能與成本之平衡：

- **Hot**：以記憶體或 Redis、DuckDB In-Memory 儲存，適用於 7 天內的即時查詢，並可透過 `RedisRateLimiter` 共享速率限制。
- **Warm**：採用 TimescaleDB/PostgreSQL，適合 30~180 天的歷史資料與特徵工程。
- **Cold**：將長期資料以 Parquet 存放在 S3 或 MinIO，可保留數年。

`HybridStorageManager` 會根據資料表所在層級讀寫資料並在容量達到上限時自動下移：

```python
import pandas as pd
from data_storage import HybridStorageManager

manager = HybridStorageManager()

# 寫入資料到 Hot tier
manager.write(pd.DataFrame({'a': [1, 2]}), 'prices', tier='hot')

# 讀取資料，未指定 tier 將自動從較熱的層級開始查詢
recent = manager.read('prices')

# 移動資料到 Cold tier
manager.migrate('prices', 'hot', 'cold')
```



## S3 設定建議

Cold tier 使用 S3 儲存時，建議開啟版本控管（Versioning），確保檔案更新不會覆蓋舊資料。
若需跨區域備援，可啟用 Cross-Region Replication 並指定次要 Bucket，
確保主要區域故障時仍能在其他區域存取資料。
=======
## 指令操作範例

透過 `zxq` 指令亦可移動資料表，例如：

```bash
zxq storage migrate --table prices --to warm
```

加入 `--dry-run` 參數則僅顯示預計動作而不實際執行。

## 命中率統計與自動遷移

`HybridStorageManager` 會記錄每個表格的讀取時間。預設每天由 Prefect 任務
計算近七日的命中次數，若 Hot tier 使用率超過 `hot_usage_threshold` 且
某表格的七日讀取次數低於 `low_hit_threshold`，系統會將其自動遷移至
Warm 或 Cold 層級。相關參數可於 `storage.yaml` 中調整：

```yaml
low_hit_threshold: 2
hot_usage_threshold: 0.8
hit_stats_schedule: "0 1 * * *"
```

`pipelines/hit_stats.py` 已提供示範 Flow，可透過 `prefect deployment build`
產生部署檔後排程執行。
