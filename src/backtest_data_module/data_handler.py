from __future__ import annotations

from typing import AsyncIterator, TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
try:
    import cupy as cp
    from cupy.cuda import runtime as cuda_runtime
except Exception:  # noqa: BLE001
    cp = None
    cuda_runtime = None

if TYPE_CHECKING:  # pragma: no cover - only for type hints
    from cupy import ndarray as cp_ndarray
else:  # pragma: no cover - fallback when cupy is unavailable
    cp_ndarray = Any
import io

from backtest_data_module.data_storage.storage_backend import HybridStorageManager


class DataHandler:
    """統一管理不同儲存層資料的介面。

    透過此類別可讀取、搬移及處理資料，涵蓋下列方法：
    - ``read``：依優先順序從指定儲存層取得資料。
    - ``compress`` / ``decompress``：壓縮與解壓欄位。
    - ``quantize``：將 CuPy 陣列量化成較低精度。
    - ``migrate``：在儲存層之間搬移資料。
    - ``stream``：以非同步方式串流資料批次。
    - ``register_arrow``：在熱儲存層註冊 Arrow 批次。
    - ``validate_schema``：驗證 DataFrame 符合預期結構。
    """
    def __init__(self, storage_manager: HybridStorageManager):
        self.storage_manager = storage_manager

    def read(
        self,
        query: str,
        tiers: list[str] | None = None,
        compressed_cols: list[str] | None = None,
    ) -> pl.DataFrame | pa.Table:
        if tiers is None:
            tiers = ["hot", "warm", "cold"]
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
        """使用字典編碼與 bit-packing 壓縮指定欄位。"""
        table = df.to_arrow()
        for col in cols:
            table = table.column(col).dictionary_encode()
        return table

    def decompress(self, table: pa.Table, cols: list[str]):
        """在 GPU 上解壓縮 DataFrame 指定的欄位。"""
        if cp is None:
            raise RuntimeError("cupy 未安裝，無法在 GPU 上解壓縮")
        with io.BytesIO() as buf:
            pq.write_table(table, buf)
            buf.seek(0)
            dataset = cp.io.ParquetDataset(buf)
            gpu_table = dataset.read_pandas().to_cupy()
        return gpu_table

    def quantize(self, df: cp_ndarray, bits: int = 8) -> cp_ndarray:
        """將 CuPy 陣列量化成較低精度。"""
        if cp is None:
            raise RuntimeError("cupy 未安裝，無法量化資料")
        if bits not in [8, 16, 32]:
            raise ValueError("Only 8, 16, and 32-bit quantization is supported.")

        # For simplicity, we'll just cast the array to the desired type.
        # In a real implementation, use a more sophisticated quantization algorithm.
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

    def stage(self, df: Any, table: str) -> None:
        """將資料寫入 warm 儲存層。"""
        self.storage_manager.write(df, table, tier="warm")

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
        # 這裡僅示範 schema 驗證的樣板實作
        # 真正的實作應使用 Great Expectations 套件
        # 以對 DataFrame 進行驗證
        if expectation_suite_name == "tick_schema":
            expected_columns = ["symbol", "price", "timestamp"]
            return all(col in df.columns for col in expected_columns)
        elif expectation_suite_name == "bar_schema":
            expected_columns = [
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "timestamp",
            ]
            return all(col in df.columns for col in expected_columns)
        return False
