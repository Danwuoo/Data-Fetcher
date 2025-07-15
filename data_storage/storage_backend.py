from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd


class StorageBackend(ABC):
    """抽象化的儲存後端介面。"""

    @abstractmethod
    def write(self, df: pd.DataFrame, table: str) -> None:
        """寫入資料到指定表格。"""
        raise NotImplementedError

    @abstractmethod
    def read(self, table: str) -> pd.DataFrame:
        """根據表格名稱讀取資料。"""
        raise NotImplementedError

    @abstractmethod
    def delete(self, table: str) -> None:
        """刪除指定表格的資料。"""
        raise NotImplementedError


class DuckHot(StorageBackend):
    """模擬 Hot tier 儲存，使用記憶體儲存資料。"""

    def __init__(self) -> None:
        self._tables: dict[str, pd.DataFrame] = {}

    def write(self, df: pd.DataFrame, table: str) -> None:
        self._tables[table] = (
            pd.concat([self._tables[table], df], ignore_index=True)
            if table in self._tables
            else df.copy()
        )

    def read(self, table: str) -> pd.DataFrame:
        if table not in self._tables:
            raise KeyError(table)
        return self._tables[table]

    def delete(self, table: str) -> None:
        self._tables.pop(table, None)


class TimescaleWarm(StorageBackend):
    """模擬 Warm tier 儲存。"""

    def __init__(self) -> None:
        self._tables: dict[str, pd.DataFrame] = {}

    def write(self, df: pd.DataFrame, table: str) -> None:
        self._tables[table] = (
            pd.concat([self._tables[table], df], ignore_index=True)
            if table in self._tables
            else df.copy()
        )

    def read(self, table: str) -> pd.DataFrame:
        if table not in self._tables:
            raise KeyError(table)
        return self._tables[table]

    def delete(self, table: str) -> None:
        self._tables.pop(table, None)


class S3Cold(StorageBackend):
    """模擬 Cold tier 儲存。"""

    def __init__(self) -> None:
        self._tables: dict[str, pd.DataFrame] = {}

    def write(self, df: pd.DataFrame, table: str) -> None:
        self._tables[table] = (
            pd.concat([self._tables[table], df], ignore_index=True)
            if table in self._tables
            else df.copy()
        )

    def read(self, table: str) -> pd.DataFrame:
        if table not in self._tables:
            raise KeyError(table)
        return self._tables[table]

    def delete(self, table: str) -> None:
        self._tables.pop(table, None)


class HybridStorageManager(StorageBackend):
    """管理多層級儲存的介面。"""

    def __init__(
        self,
        hot_store: StorageBackend | None = None,
        warm_store: StorageBackend | None = None,
        cold_store: StorageBackend | None = None,
    ) -> None:
        self.hot_store = hot_store or DuckHot()
        self.warm_store = warm_store or TimescaleWarm()
        self.cold_store = cold_store or S3Cold()

    def _backend_for(self, tier: str) -> StorageBackend:
        if tier == "hot":
            return self.hot_store
        if tier == "warm":
            return self.warm_store
        if tier == "cold":
            return self.cold_store
        raise ValueError(f"未知的 tier: {tier}")

    def write(self, df: pd.DataFrame, table: str, tier: str = "hot") -> None:
        backend = self._backend_for(tier)
        backend.write(df, table)

    def read(
        self, table: str, tiers: list[str] | None = None
    ) -> pd.DataFrame:
        tiers = tiers or ["hot", "warm", "cold"]
        for tier in tiers:
            backend = self._backend_for(tier)
            try:
                return backend.read(table)
            except KeyError:
                continue
        raise KeyError(table)

    def delete(self, table: str) -> None:
        for backend in (self.hot_store, self.warm_store, self.cold_store):
            backend.delete(table)

    def migrate(self, table: str, src_tier: str, dst_tier: str) -> None:
        src = self._backend_for(src_tier)
        dst = self._backend_for(dst_tier)
        df = src.read(table)
        dst.write(df, table)
        src.delete(table)
