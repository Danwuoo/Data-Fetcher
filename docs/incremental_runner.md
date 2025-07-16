# 增量處理 Runner

`IncrementalRunner` 允許從 Kafka/Redpanda 主題持續讀取資料並依序套用 `PipelineStep`。每批資料處理後會寫入 `HybridStorageManager` 以供即時回測使用。

## 建立與啟動
```python
from data_processing.pipeline import Pipeline
from data_processing.incremental_runner import IncrementalRunner
from zxq.pipeline.steps.data_cleanser import DataCleanser

pipeline = Pipeline([DataCleanser()])
runner = IncrementalRunner(
    pipeline,
    topics=["prices"],
    bootstrap_servers="localhost:9092",
    group_id="backtest",
    result_table="prices_clean"
)
runner.run_forever()
```

## 讀取結果
```python
from backtest_data_module.data_storage.storage_backend import HybridStorageManager

manager = HybridStorageManager()
latest = manager.read("prices_clean")
print(latest.tail())
```
