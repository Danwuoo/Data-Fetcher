from __future__ import annotations

from typing import AsyncIterator, List

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import cupy as cp
import io
from cupy.cuda import runtime as cuda_runtime

from backtest_data_module.data_storage.storage_backend import HybridStorageManager


class DataHandler:
    def __init__(self, storage_manager: HybridStorageManager):
        self.storage_manager = storage_manager

    def read(
        self, query: str, tiers: list[str] = ["hot", "warm", "cold"], compressed_cols: list[str] | None = None
    ) -> pl.DataFrame | pa.Table:
        for tier in tiers:
            try:
                df = self.storage_manager.read(query, tiers=[tier])
                if tier == "cold":
                    self.storage_manager.migrate(query, "cold", "warm")
                if compressed_cols:
                    return self.decompress(df, compressed_cols)
                return df
            except KeyError:
                continue
        raise KeyError(f"Table {query} not found in any of the specified tiers.")

    def compress(self, df: pl.DataFrame, cols: list[str]) -> pa.Table:
        """Compresses specified columns of a DataFrame using dictionary and bit-packing."""
        table = df.to_arrow()
        for col in cols:
            table = table.column(col).dictionary_encode()
        return table

    def decompress(self, table: pa.Table, cols: list[str]) -> cp.ndarray:
        """Decompresses specified columns of a DataFrame on the GPU."""
        with io.BytesIO() as buf:
            pq.write_table(table, buf)
            buf.seek(0)
            dataset = cp.io.ParquetDataset(buf)
            gpu_table = dataset.read_pandas().to_cupy()
        return gpu_table

    def quantize(self, df: cp.ndarray, bits: int = 8) -> cp.ndarray:
        """Quantizes a CuPy array to a lower precision."""
        if bits not in [8, 16, 32]:
            raise ValueError("Only 8, 16, and 32-bit quantization is supported.")

        # For simplicity, we'll just cast the array to the desired type.
        # In a real implementation, you would use a more sophisticated quantization algorithm.
        if bits == 8:
            return df.astype(cp.int8)
        elif bits == 16:
            return df.astype(cp.int16)
        else:
            return df.astype(cp.int32)

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
