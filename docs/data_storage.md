# 多層級儲存策略

本專案採用 Hot、Warm、Cold 三層級架構，以取得效能與成本之平衡：

- **Hot**：以記憶體或 Redis、DuckDB In-Memory 儲存，適用於 7 天內的即時查詢。
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

