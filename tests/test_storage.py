import pandas as pd
import pytest

from data_storage import DuckDBBackend, HybridStorageManager


@pytest.fixture
def storage_manager():
    backends = {
        "hot": DuckDBBackend(),
        "warm": DuckDBBackend(),
    }
    return HybridStorageManager(backends)


def test_write_and_read(storage_manager):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    storage_manager.write(df, "t", tier="hot")
    out = storage_manager.read("SELECT * FROM t")
    assert len(out) == 2


def test_migrate_updates_catalog(storage_manager):
    df = pd.DataFrame({"a": [1]})
    storage_manager.write(df, "m", tier="hot")
    storage_manager.migrate("m", "hot", "warm")
    row = storage_manager.catalog.fetch("m")
    assert row["tier"] == "warm"
