import pandas as pd
from data_storage import HybridStorageManager


def test_access_counts_and_stats():
    manager = HybridStorageManager(low_hit_threshold=1)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl", tier="hot")
    manager.read("tbl")
    manager.read("tbl")
    stats = manager.compute_7day_hits()
    assert stats["tbl"] == 2


def test_migrate_low_hit_tables():
    manager = HybridStorageManager(hot_capacity=3, warm_capacity=3,
                                   low_hit_threshold=2, hot_usage_threshold=0.0)
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"b": [2]})
    manager.write(df1, "t1", tier="hot")
    manager.write(df2, "t2", tier="hot")
    # only read t1 to keep t2 below threshold
    manager.read("t1")
    manager.migrate_low_hit_tables()
    assert "t2" not in manager.hot_store._tables
