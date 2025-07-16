import unittest
import pandas as pd
from unittest.mock import MagicMock
import sys
sys.path.append('.')
from data_processing.data_handler import DataHandler
from data_storage.storage_backend import HybridStorageManager


class TestDataHandler(unittest.TestCase):
    def setUp(self):
        self.storage_manager = MagicMock(spec=HybridStorageManager)
        self.data_handler = DataHandler(self.storage_manager)

    def test_read_from_hot_and_migrate_to_warm(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.storage_manager.read.return_value = df
        result_df = self.data_handler.read("test_table", tiers=["hot", "warm", "cold"])
        self.assertTrue(df.equals(result_df))
        self.storage_manager.read.assert_called_with("test_table", tiers=["hot"])
        self.storage_manager.migrate.assert_called_with("test_table", "hot", "warm")

    def test_read_from_cold_and_migrate_to_warm(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.storage_manager.read.side_effect = [KeyError, df]
        result_df = self.data_handler.read("test_table", tiers=["warm", "cold"])
        self.assertTrue(df.equals(result_df))
        self.storage_manager.migrate.assert_called_with("test_table", "cold", "warm")

    def test_read_from_warm(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.storage_manager.read.return_value = df
        result_df = self.data_handler.read("test_table", tiers=["warm"])
        self.assertTrue(df.equals(result_df))
        self.storage_manager.read.assert_called_with("test_table", tiers=["warm"])
        self.storage_manager.migrate.assert_not_called()

    def test_read_not_found(self):
        self.storage_manager.read.side_effect = KeyError
        with self.assertRaises(KeyError):
            self.data_handler.read("test_table")

    def test_migrate(self):
        self.data_handler.migrate("test_table", "hot", "cold")
        self.storage_manager.migrate.assert_called_with("test_table", "hot", "cold")

    def test_stage(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.data_handler.stage(df, "staged_table")
        self.storage_manager.write.assert_called_with(df, "staged_table", tier="warm")


if __name__ == '__main__':
    unittest.main()
