from __future__ import annotations

import pandas as pd
import duckdb

from .backend import StorageBackend


class DuckDBBackend(StorageBackend):
    """以 DuckDB 為基礎的儲存後端，可作為 hot 或 warm 層。"""

    def __init__(self, path: str = ":memory:"):
        self.con = duckdb.connect(path)

    def write(self, df: pd.DataFrame, table: str, tier: str = "hot") -> None:
        self.con.register(table, df)

    def read(self, query: str, tiers: list[str] | None = None) -> pd.DataFrame:
        return self.con.execute(query).fetch_df()

    def migrate(self, table: str, src_tier: str, dst_tier: str) -> None:
        pass
