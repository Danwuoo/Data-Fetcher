import pandas as pd
import pytest

from backtest_data_module.data_storage import HybridStorageManager
from metrics import MIGRATION_LATENCY_MS, TIER_HIT_RATE


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


def test_migration_latency_histogram():
    manager = HybridStorageManager()
    df = pd.DataFrame({"d": [10]})
    manager.write(df, "tbl_migrate", tier="hot")
    before = sum(
        b.get() for b in MIGRATION_LATENCY_MS.labels("hot", "warm")._buckets
    )
    manager.migrate("tbl_migrate", "hot", "warm")
    after = sum(
        b.get() for b in MIGRATION_LATENCY_MS.labels("hot", "warm")._buckets
    )
    assert after == before + 1


def test_tier_hit_rate():
    manager = HybridStorageManager()
    df = pd.DataFrame({"e": [5]})
    manager.write(df, "rate_tbl", tier="hot")
    before_hot = TIER_HIT_RATE.labels(tier="hot")._value.get()
    before_warm = TIER_HIT_RATE.labels(tier="warm")._value.get()
    manager.read("rate_tbl")
    after_hot = TIER_HIT_RATE.labels(tier="hot")._value.get()
    assert after_hot > before_hot
    manager.migrate("rate_tbl", "hot", "warm")
    manager.read("rate_tbl", tiers=["warm"])
    final_hot = TIER_HIT_RATE.labels(tier="hot")._value.get()
    final_warm = TIER_HIT_RATE.labels(tier="warm")._value.get()
    assert final_hot < after_hot
    assert final_warm > before_warm
