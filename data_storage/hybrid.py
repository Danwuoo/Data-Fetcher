from __future__ import annotations

from typing import Dict, Optional
import pandas as pd

from .backend import StorageBackend
from .metadata import MetadataCatalog


class HybridStorageManager(StorageBackend):
    """簡易多層儲存管理器。"""

    def __init__(
        self,
        backends: Dict[str, StorageBackend],
        catalog: Optional[MetadataCatalog] = None,
    ):
        self.backends = backends
        self.catalog = catalog or MetadataCatalog()

    def write(self, df: pd.DataFrame, table: str, tier: str = "hot") -> None:
        backend = self.backends[tier]
        backend.write(df, table, tier)
        if self.catalog:
            self.catalog.upsert(
                {
                    "table_name": table,
                    "tier": tier,
                    "location": tier,
                    "format": "duckdb",
                    "schema_hash": str(hash(tuple(df.dtypes)) ),
                    "partition_keys": "",
                    "version": 0,
                    "row_count": len(df),
                }
            )

    def read(self, query: str, tiers: list[str] | None = None) -> pd.DataFrame:
        tiers = tiers or ["hot", "warm", "cold"]
        for tier in tiers:
            backend = self.backends.get(tier)
            if backend is None:
                continue
            try:
                return backend.read(query, [tier])
            except Exception:
                continue
        raise ValueError("No backend could satisfy query")

    def migrate(self, table: str, src_tier: str, dst_tier: str) -> None:
        src = self.backends[src_tier]
        dst = self.backends[dst_tier]
        df = src.read(f"SELECT * FROM {table}", [src_tier])
        dst.write(df, table, dst_tier)
        if self.catalog:
            row = self.catalog.fetch(table) or {}
            row.update({"tier": dst_tier, "location": dst_tier})
            self.catalog.upsert(row)
