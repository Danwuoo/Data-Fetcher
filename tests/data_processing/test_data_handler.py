import pandas as pd
import pytest

from data_storage.storage_backend import HybridStorageManager, DuckHot, S3Cold
from data_processing.data_handler import DataHandler


@pytest.fixture
def storage_manager():
    return HybridStorageManager(
        hot_store=DuckHot(),
        warm_store=DuckHot(),
        cold_store=S3Cold(),
    )


@pytest.fixture
def data_handler(storage_manager):
    return DataHandler(storage_manager)


def test_read_from_hot_and_migrate_to_warm(data_handler: DataHandler):
    df = pd.DataFrame({"a": [1, 2, 3]})
    data_handler.storage_manager.write(df, "test_table", tier="hot")

    result_df = data_handler.read("test_table")
    assert df.equals(result_df)

    # Verify that the data was migrated to warm
    _ = data_handler.storage_manager.read("test_table", tiers=["warm"])
    with pytest.raises(KeyError):
        data_handler.storage_manager.read("test_table", tiers=["hot"])


def test_read_from_cold_and_migrate_to_warm(data_handler: DataHandler):
    df = pd.DataFrame({"a": [1, 2, 3]})
    data_handler.storage_manager.write(df, "test_table", tier="cold")

    result_df = data_handler.read("test_table")
    assert df.equals(result_df)

    # Verify that the data was migrated to warm
    _ = data_handler.storage_manager.read("test_table", tiers=["warm"])
    with pytest.raises(KeyError):
        data_handler.storage_manager.read("test_table", tiers=["cold"])


def test_read_from_warm(data_handler: DataHandler):
    df = pd.DataFrame({"a": [1, 2, 3]})
    data_handler.storage_manager.write(df, "test_table", tier="warm")

    result_df = data_handler.read("test_table")
    assert df.equals(result_df)

    # Verify that the data is still in warm
    _ = data_handler.storage_manager.read("test_table", tiers=["warm"])


def test_read_not_found(data_handler: DataHandler):
    with pytest.raises(KeyError):
        data_handler.read("test_table")


def test_migrate(data_handler: DataHandler):
    df = pd.DataFrame({"a": [1, 2, 3]})
    data_handler.storage_manager.write(df, "test_table", tier="hot")

    data_handler.migrate("test_table", "hot", "cold")

    _ = data_handler.storage_manager.read("test_table", tiers=["cold"])
    with pytest.raises(KeyError):
        data_handler.storage_manager.read("test_table", tiers=["hot"])


def test_stage(data_handler: DataHandler):
    df = pd.DataFrame({"a": [1, 2, 3]})
    data_handler.stage(df, "staged_table")

    result_df = data_handler.storage_manager.read("staged_table", tiers=["warm"])
    assert df.equals(result_df)
