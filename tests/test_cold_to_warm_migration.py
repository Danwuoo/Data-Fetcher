import pandas as pd
from data_storage import HybridStorageManager, Catalog
from utils.notify import SlackNotifier, PagerDutyNotifier


def test_cold_to_warm_migration_preserves_data(monkeypatch):
    # mock notifier network calls
    messages = []

    def fake_post(url, json):
        messages.append(json)

    monkeypatch.setattr("httpx.post", fake_post)

    catalog = Catalog()
    manager = HybridStorageManager(catalog=catalog)

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    manager.write(df, "cold_tbl", tier="cold")

    manager.migrate("cold_tbl", "cold", "warm")
    result = manager.read("cold_tbl", tiers=["warm"])

    pd.testing.assert_frame_equal(result, df)

    # send notifications (should use mocked httpx.post)
    SlackNotifier("http://hook").send("migrated")
    PagerDutyNotifier("rk").send("migrated")

    assert messages
