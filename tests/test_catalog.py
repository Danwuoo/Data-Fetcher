import pandas as pd
from data_storage import HybridStorageManager, Catalog


def test_catalog_updates_on_write():
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl1", tier="hot")

    entry = catalog.get("tbl1")
    assert entry is not None
    assert entry.tier == "hot"


def test_catalog_updates_on_migrate():
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl2", tier="hot")
    manager.migrate("tbl2", "hot", "cold")

    entry = catalog.get("tbl2")
    assert entry is not None
    assert entry.tier == "cold"
