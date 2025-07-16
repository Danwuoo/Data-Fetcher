from __future__ import annotations

import pandas as pd

from data_storage.storage_backend import HybridStorageManager


class DataHandler:
    def __init__(self, storage_manager: HybridStorageManager):
        self.storage_manager = storage_manager

    def read(self, table: str, tiers: list[str] = ["hot", "warm", "cold"]) -> pd.DataFrame:
        for tier in tiers:
            try:
                df = self.storage_manager.read(table, tiers=[tier])
                if tier != "warm":
                    self.storage_manager.migrate(table, tier, "warm")
                return df
            except KeyError:
                continue
        raise KeyError(f"Table {table} not found in any of the specified tiers.")

    def migrate(self, table: str, src_tier: str, dst_tier: str):
        self.storage_manager.migrate(table, src_tier, dst_tier)

    def stage(self, df: pd.DataFrame, stage_name: str):
        self.storage_manager.write(df, stage_name, tier="warm")
