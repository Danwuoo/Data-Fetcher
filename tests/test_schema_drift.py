import pandas as pd
from backtest_data_module.data_storage import HybridStorageManager, Catalog, check_drift
from utils.notify import SlackNotifier


def test_column_type_change_triggers_notification(monkeypatch):
    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)
    df = pd.DataFrame({"a": [1]})
    manager.write(df, "tbl", tier="hot")

    # simulate schema drift by altering column type without updating catalog
    new_df = pd.DataFrame({"a": ["1"]})
    manager.hot_store.delete("tbl")
    manager.hot_store.write(new_df, "tbl")

    messages = []

    def fake_post(url, json):
        messages.append(json["text"])

    monkeypatch.setattr("httpx.post", fake_post)
    notifier = SlackNotifier("http://hook")
    mismatched = check_drift(manager, notifier=notifier)

    assert "tbl" in mismatched
    assert messages and "tbl" in messages[0]
