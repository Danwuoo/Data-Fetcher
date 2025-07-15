from __future__ import annotations

from abc import ABC, abstractmethod
import hashlib
import os
from collections import deque, defaultdict
import json
from typing import Any, cast, DefaultDict
from datetime import datetime, timedelta

import pandas as pd
import yaml
import duckdb
import psycopg
import boto3
import io

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
    """Hot tier 以 DuckDB 儲存，可使用檔案或記憶體資料庫。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.con = duckdb.connect(path)
        self._tables: set[str] = set()

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
        self.con.register("tmp", df)
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM tmp WHERE FALSE"
        )
        self.con.execute(f"INSERT INTO {table} SELECT * FROM tmp")
        self.con.unregister("tmp")
        self._tables.add(table)

    def read(self, table: str) -> pd.DataFrame:
        try:
            return self.con.execute(f"SELECT * FROM {table}").fetchdf()
        except duckdb.CatalogException as e:
            raise KeyError(table) from e

    def delete(self, table: str) -> None:
        self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self._tables.discard(table)


class TimescaleWarm(StorageBackend):
    """Warm tier 透過 PostgreSQL/TimescaleDB 儲存。若未提供 DSN 則使用 DuckDB 模擬。"""

    def __init__(self, dsn: str | None = None) -> None:
        if dsn:
            self.conn = psycopg.connect(dsn)
            with self.conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            self.use_pg = True
        else:
            self.conn = duckdb.connect()  # fallback for測試
            self.use_pg = False
        self._tables: set[str] = set()

    def _create_table(self, df: pd.DataFrame, table: str) -> None:
        cols = []
        for name, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "DOUBLE PRECISION"
            elif pd.api.types.is_bool_dtype(dtype):
                col_type = "BOOLEAN"
            else:
                col_type = "TEXT"
            cols.append(f'"{name}" {col_type}')
        cols_sql = ", ".join(cols)
        if self.use_pg:
            with self.conn.cursor() as cur:
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
            self.conn.commit()
        else:
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ({cols_sql})"
            )

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
        if self.use_pg:
            self._create_table(df, table)
            with self.conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(df.columns))
                insert_sql = f'INSERT INTO "{table}" VALUES ({placeholders})'
                for row in df.itertuples(index=False, name=None):
                    cur.execute(insert_sql, row)
            self.conn.commit()
        else:
            self.conn.register("tmp", df)
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM tmp WHERE FALSE"
            )
            self.conn.execute(f"INSERT INTO {table} SELECT * FROM tmp")
            self.conn.unregister("tmp")
        self._tables.add(table)

    def read(self, table: str) -> pd.DataFrame:
        if self.use_pg:
            try:
                return pd.read_sql(f'SELECT * FROM "{table}"', self.conn)
            except Exception as e:  # psycopg throws errors for missing table
                raise KeyError(table) from e
        try:
            return self.conn.execute(f"SELECT * FROM {table}").fetchdf()
        except duckdb.CatalogException as e:
            raise KeyError(table) from e

    def delete(self, table: str) -> None:
        if self.use_pg:
            with self.conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table}"')
            self.conn.commit()
        else:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        self._tables.discard(table)


class S3Cold(StorageBackend):
    """Cold tier 以 S3 儲存 Parquet 檔案，預設可在記憶體中模擬。"""

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str = "",
        s3_client: Any | None = None,
    ) -> None:
        bucket = bucket or None
        self.bucket = bucket
        self.prefix = prefix
        self.s3 = s3_client or (boto3.client("s3") if bucket else None)
        self._tables: dict[str, pd.DataFrame] | None = {} if bucket is None else None

    def _key(self, table: str) -> str:
        return f"{self.prefix}{table}.parquet"

    def write(
        self, df: pd.DataFrame, table: str, *, metadata: dict[str, object] | None = None
    ) -> None:
        if self.s3:
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self._key(table),
                Body=buf.read(),
            )
        else:
            assert self._tables is not None
            self._tables[table] = df.copy()

    def read(self, table: str) -> pd.DataFrame:
        if self.s3:
            try:
                obj = self.s3.get_object(Bucket=self.bucket, Key=self._key(table))
                return pd.read_parquet(io.BytesIO(obj["Body"].read()))
            except Exception as e:
                raise KeyError(table) from e
        assert self._tables is not None
        if table not in self._tables:
            raise KeyError(table)
        return self._tables[table]

    def delete(self, table: str) -> None:
        if self.s3:
            self.s3.delete_object(Bucket=self.bucket, Key=self._key(table))
        else:
            assert self._tables is not None
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
        low_hit_threshold: int | None = None,
        hot_usage_threshold: float | None = None,
        config_path: str = "storage.yaml",
    ) -> None:
        config: dict[str, object] = {}
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

        duck_path = cast(str, config.get("duckdb_path", ":memory:"))
        pg_dsn = cast(str, config.get("postgres_dsn", ""))
        bucket = cast(str | None, config.get("s3_bucket"))
        prefix = cast(str, config.get("s3_prefix", ""))

        self.hot_store = hot_store or DuckHot(duck_path)
        self.warm_store = warm_store or TimescaleWarm(pg_dsn or None)
        self.cold_store = cold_store or S3Cold(bucket, prefix)
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
        self.low_hit_threshold = (
            low_hit_threshold
            if low_hit_threshold is not None
            else int(cast(Any, config.get("low_hit_threshold", 0)))
        )
        self.hot_usage_threshold = (
            hot_usage_threshold
            if hot_usage_threshold is not None
            else float(cast(Any, config.get("hot_usage_threshold", 0.8)))
        )
        self.hit_stats_schedule = cast(
            str, config.get("hit_stats_schedule", "0 1 * * *")
        )
        self._hot_lru: deque[str] = deque()
        self._warm_lru: deque[str] = deque()
        self.access_log: DefaultDict[str, deque[datetime]] = defaultdict(deque)

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

    def _record_access(self, table: str) -> None:
        """記錄資料表存取時間以便統計命中率。"""
        self.access_log[table].append(datetime.utcnow())

    def _check_capacity(self) -> None:
        while len(cast(DuckHot, self.hot_store)._tables) > self.hot_capacity:
            oldest = self._hot_lru.popleft()
            self.migrate(oldest, "hot", "warm")
        while len(cast(TimescaleWarm, self.warm_store)._tables) > self.warm_capacity:
            oldest = self._warm_lru.popleft()
            self.migrate(oldest, "warm", "cold")

    def compute_7day_hits(self) -> dict[str, int]:
        """計算最近七天每個表格的讀取次數。"""
        cutoff = datetime.utcnow() - timedelta(days=7)
        stats: dict[str, int] = {}
        for table, times in self.access_log.items():
            while times and times[0] < cutoff:
                times.popleft()
            stats[table] = len(times)
        return stats

    def migrate_low_hit_tables(self) -> None:
        """根據命中率與容量閾值自動下移低頻表格。"""
        usage = len(cast(DuckHot, self.hot_store)._tables) / max(self.hot_capacity, 1)
        if usage <= self.hot_usage_threshold:
            return
        stats = self.compute_7day_hits()
        for table in list(cast(DuckHot, self.hot_store)._tables):
            if stats.get(table, 0) < self.low_hit_threshold:
                target = "warm"
                if (
                    len(cast(TimescaleWarm, self.warm_store)._tables)
                    >= self.warm_capacity
                ):
                    target = "cold"
                self.migrate(table, "hot", target)

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
                self._record_access(table)
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
