from __future__ import annotations

import pandas as pd
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """儲存後端抽象介面。"""

    @abstractmethod
    def write(self, df: pd.DataFrame, table: str, tier: str) -> None:
        """寫入資料到指定層級。"""

    @abstractmethod
    def read(self, query: str, tiers: list[str]) -> pd.DataFrame:
        """依序在層級中查詢資料並回傳。"""

    @abstractmethod
    def migrate(self, table: str, src_tier: str, dst_tier: str) -> None:
        """將資料從 src_tier 移到 dst_tier。"""
