import pandas as pd
from backtest_data_module.data_storage import HybridStorageManager, Catalog, check_drift
from utils.notify import SlackNotifier


def test_catalog_updates_on_write():
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl1", tier="hot")

    entry = catalog.get("tbl1")
    assert entry is not None
    assert entry.tier == "hot"
    assert entry.row_count == len(df)


def test_catalog_updates_on_migrate():
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl2", tier="hot")
    manager.migrate("tbl2", "hot", "cold")

    entry = catalog.get("tbl2")
    assert entry is not None
    assert entry.tier == "cold"
    assert entry.row_count == len(df)


def test_check_drift(monkeypatch):
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl3", tier="hot")

    # 竄改 schema_hash 以模擬漂移
    catalog.conn.execute(
        "UPDATE catalog SET schema_hash='invalid' WHERE table_name='tbl3'"
    )

    messages = []

    def fake_post(url, json):
        messages.append(json["text"])

    monkeypatch.setattr("httpx.post", fake_post)
    notifier = SlackNotifier("http://example.com")
    mismatched = check_drift(manager, notifier=notifier)

    assert "tbl3" in mismatched
    assert messages
