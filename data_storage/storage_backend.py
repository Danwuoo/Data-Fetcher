from __future__ import annotations

from abc import ABC, abstractmethod
import hashlib
import os
from collections import deque
import json
from typing import Any, cast

import pandas as pd
import yaml

from .catalog import Catalog, CatalogEntry
from metrics import (
    STORAGE_WRITE_COUNTER,
    STORAGE_READ_COUNTER,
    MIGRATION_LATENCY_MS,
    update_tier_hit_rate,
)
from time import perf_counter


class StorageBackend(ABC):
    """抽象化的儲存後端介面。"""

    @abstractmethod
    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
        """寫入資料到指定表格，metadata 可附帶額外資訊。"""
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

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
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

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
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

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
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
        catalog: Catalog | None = None,
        hot_capacity: int | None = None,
        warm_capacity: int | None = None,
        config_path: str = "storage.yaml",
    ) -> None:
        config: dict[str, object] = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

        self.hot_store = hot_store or DuckHot()
        self.warm_store = warm_store or TimescaleWarm()
        self.cold_store = cold_store or S3Cold()
        self.catalog = catalog or Catalog()
        self.tier_order: list[str] = cast(
            list[str], config.get("tier_order", ["hot", "warm", "cold"])
        )
        self.hot_capacity = (
            hot_capacity
            if hot_capacity is not None
            else int(cast(Any, config.get("hot_capacity", 3)))
        )
        self.warm_capacity = (
            warm_capacity
            if warm_capacity is not None
            else int(cast(Any, config.get("warm_capacity", 5)))
        )
        self._hot_lru: deque[str] = deque()
        self._warm_lru: deque[str] = deque()

    def _backend_for(self, tier: str) -> StorageBackend:
        if tier == "hot":
            return self.hot_store
        if tier == "warm":
            return self.warm_store
        if tier == "cold":
            return self.cold_store
        raise ValueError(f"未知的 tier: {tier}")

    def _record_lru(self, lru: deque[str], table: str) -> None:
        if table in lru:
            lru.remove(table)
        lru.append(table)

    def _check_capacity(self) -> None:
        while len(cast(DuckHot, self.hot_store)._tables) > self.hot_capacity:
            oldest = self._hot_lru.popleft()
            self.migrate(oldest, "hot", "warm")
        while len(cast(TimescaleWarm, self.warm_store)._tables) > self.warm_capacity:
            oldest = self._warm_lru.popleft()
            self.migrate(oldest, "warm", "cold")

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        *,
        tier: str = "hot",
        lineage_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        backend = self._backend_for(tier)
        meta = metadata.copy() if metadata else {}
        if lineage_id:
            df.attrs["lineage_id"] = lineage_id
            meta["lineage_id"] = lineage_id
        backend.write(df, table, metadata=meta or None)
        STORAGE_WRITE_COUNTER.labels(tier=tier).inc()

        schema_hash = hashlib.sha256(str(df.dtypes.to_dict()).encode()).hexdigest()
        partition_data = {}
        for col in ("date", "asset"):
            if col in df.columns:
                partition_data[col] = str(df[col].iloc[0])
        self.catalog.upsert(
            CatalogEntry(
                table_name=table,
                version=0,
                tier=tier,
                location=tier,
                schema_hash=schema_hash,
                row_count=len(df),
                partition_keys=json.dumps(
                    partition_data, ensure_ascii=False
                ),
                lineage="write",
            )
        )

        if tier == "hot":
            self._record_lru(self._hot_lru, table)
        elif tier == "warm":
            self._record_lru(self._warm_lru, table)

        self._check_capacity()

    def read(
        self, table: str, *, tiers: list[str] | None = None
    ) -> pd.DataFrame:
        tiers = tiers or self.tier_order
        for tier in tiers:
            backend = self._backend_for(tier)
            try:
                result = backend.read(table)
                STORAGE_READ_COUNTER.labels(tier=tier).inc()
                update_tier_hit_rate()
                return result
            except KeyError:
                continue
        raise KeyError(table)

    def delete(self, table: str) -> None:
        for backend in (self.hot_store, self.warm_store, self.cold_store):
            backend.delete(table)

    def migrate(self, table: str, src_tier: str, dst_tier: str) -> None:
        start_time = perf_counter()
        src = self._backend_for(src_tier)
        dst = self._backend_for(dst_tier)
        df = src.read(table)
        STORAGE_READ_COUNTER.labels(tier=src_tier).inc()
        update_tier_hit_rate()
        dst.write(df, table)
        STORAGE_WRITE_COUNTER.labels(tier=dst_tier).inc()
        src.delete(table)

        self.catalog.update_tier(table, dst_tier, dst_tier)

        if src_tier == "hot" and table in self._hot_lru:
            self._hot_lru.remove(table)
        if src_tier == "warm" and table in self._warm_lru:
            self._warm_lru.remove(table)

        if dst_tier == "warm":
            self._record_lru(self._warm_lru, table)
        elif dst_tier == "hot":
            self._record_lru(self._hot_lru, table)

        self._check_capacity()
        duration_ms = (perf_counter() - start_time) * 1000
        MIGRATION_LATENCY_MS.labels(src_tier=src_tier, dst_tier=dst_tier).observe(
            duration_ms
        )
