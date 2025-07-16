from __future__ import annotations

from typing import AsyncIterator, List

import polars as pl
import pyarrow as pa

from backtest_data_module.data_storage.storage_backend import HybridStorageManager


class DataHandler:
    def __init__(self, storage_manager: HybridStorageManager):
        self.storage_manager = storage_manager

    def read(
        self, query: str, tiers: list[str] = ["hot", "warm", "cold"]
    ) -> pl.DataFrame | pa.Table:
        for tier in tiers:
            try:
                df = self.storage_manager.read(query, tiers=[tier])
                if tier == "cold":
                    self.storage_manager.migrate(query, "cold", "warm")
                return df
            except KeyError:
                continue
        raise KeyError(f"Table {query} not found in any of the specified tiers.")

    def migrate(self, table: str, src: str, dst: str, mode: str = "sync") -> bool:
        try:
            self.storage_manager.migrate(table, src, dst)
            return True
        except Exception:
            return False

    async def stream(
        self, symbols: list[str], freq: str
    ) -> AsyncIterator[pa.RecordBatch]:
        # Mock implementation of data streaming
        schema = pa.schema([
            pa.field("symbol", pa.string()),
            pa.field("price", pa.float64()),
            pa.field("timestamp", pa.timestamp("ns"))
        ])
        for symbol in symbols:
            for i in range(10):
                data = [
                    pa.array([symbol]),
                    pa.array([100.0 + i]),
                    pa.array([pa.scalar(123456789, type=pa.timestamp('ns'))]),
                ]
                batch = pa.RecordBatch.from_arrays(data, schema=schema)
                yield batch

    def register_arrow(self, table_name: str, batch: pa.RecordBatch) -> None:
        self.storage_manager.hot_store.con.register(table_name, batch)

    def validate_schema(self, df: pl.DataFrame, expectation_suite_name: str) -> bool:
        # This is a mock implementation of schema validation.
        # In a real implementation, you would use the Great Expectations library
        # to validate the DataFrame against the expectation suite.
        if expectation_suite_name == "tick_schema":
            expected_columns = ["symbol", "price", "timestamp"]
            return all(col in df.columns for col in expected_columns)
        elif expectation_suite_name == "bar_schema":
            expected_columns = ["symbol", "open", "high", "low", "close", "volume", "timestamp"]
            return all(col in df.columns for col in expected_columns)
        return False
