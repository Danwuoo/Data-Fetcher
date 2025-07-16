import pandas as pd
from backtest_data_module.data_storage import HybridStorageManager, Catalog


def test_auto_migration_hot_to_warm_and_cold():
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog, hot_capacity=1, warm_capacity=1)

    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"b": [2]})
    df3 = pd.DataFrame({"c": [3]})

    manager.write(df1, "t1", tier="hot")
    # writing second table triggers migration of t1 to warm
    manager.write(df2, "t2", tier="hot")
    assert catalog.get("t1").tier == "warm"

    manager.write(df3, "t3", tier="hot")
    # now t2 should move to warm and t1 move to cold due to warm capacity
    assert catalog.get("t2").tier == "warm"
    assert catalog.get("t1").tier == "cold"
