# Multi-Tiered Storage Strategy

This project uses a Hot, Warm, and Cold three-tiered architecture to balance performance and cost:

- **Hot**: Stored in-memory using DuckDB, suitable for real-time queries within 7 days.
- **Warm**: Uses TimescaleDB/PostgreSQL, suitable for historical data and feature engineering from 30 to 180 days.
- **Cold**: Stores long-term data as Parquet files in S3 or MinIO, which can be retained for several years.

The `DataHandler` class provides a unified interface for interacting with the tiered storage system. It uses the `HybridStorageManager` to read and write data to the appropriate tier and automatically moves data down when capacity limits are reached.

```python
import polars as pl
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.data_storage.storage_backend import HybridStorageManager

# Initialize the DataHandler
storage_manager = HybridStorageManager()
data_handler = DataHandler(storage_manager)

# Write data to the hot tier
data_handler.storage_manager.write(pl.DataFrame({'a': [1, 2]}), 'prices', tier='hot')

# Read data, automatically starting from the hottest tier
recent = data_handler.read('prices')

# Migrate data to the cold tier
data_handler.migrate('prices', 'hot', 'cold')
```

## S3 Configuration Recommendations

When using S3 for the cold tier, it is recommended to enable versioning to ensure that file updates do not overwrite old data. For cross-region backup, you can enable Cross-Region Replication and specify a secondary bucket to ensure data access in case of a primary region failure.

## Command-Line Operations

You can also move data tables using the `zxq` command-line tool:

```bash
zxq storage migrate --table prices --to warm
```

Adding the `--dry-run` parameter will only show the intended actions without actually executing them.

## Hit Rate Statistics and Automatic Migration

The `HybridStorageManager` records the access time for each table. By default, a Prefect task calculates the number of hits for each table in the last seven days. If the hot tier usage exceeds `hot_usage_threshold` and a table's seven-day hit count is below `low_hit_threshold`, the system will automatically migrate it to the warm or cold tier. These parameters can be adjusted in the `storage.yaml` file:

```yaml
low_hit_threshold: 2
hot_usage_threshold: 0.8
hit_stats_schedule: "0 1 * * *"
```

The `pipelines/hit_stats.py` file provides an example flow that can be scheduled to run after being deployed with `prefect deployment build`.
