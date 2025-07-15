import pandas as pd
import pytest

from data_storage import HybridStorageManager


def test_hot_write_and_read():
    manager = HybridStorageManager()
    df = pd.DataFrame({"a": [1, 2]})
    manager.write(df, "tbl", tier="hot")
    result = manager.read("tbl", tiers=["hot"])
    pd.testing.assert_frame_equal(result, df)


def test_fallback_to_warm():
    manager = HybridStorageManager()
    df = pd.DataFrame({"b": [5]})
    manager.write(df, "warm_tbl", tier="warm")
    result = manager.read("warm_tbl", tiers=["hot", "warm"])
    pd.testing.assert_frame_equal(result, df)


def test_cold_migration():
    manager = HybridStorageManager()
    df = pd.DataFrame({"c": [9]})
    manager.write(df, "cold_tbl", tier="hot")
    manager.migrate("cold_tbl", "hot", "cold")
    with pytest.raises(KeyError):
        manager.read("cold_tbl", tiers=["hot"])
    result = manager.read("cold_tbl", tiers=["cold"])
    pd.testing.assert_frame_equal(result, df)
