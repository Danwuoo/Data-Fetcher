"""增量處理 Runner，從 Kafka/Redpanda 讀取資料並即時應用 Pipeline。
"""
from __future__ import annotations

import pandas as pd
from kafka import KafkaConsumer

from backtest_data_module.data_processing.pipeline import Pipeline
from backtest_data_module.data_storage.storage_backend import HybridStorageManager


class IncrementalRunner:
    """持續消費 Kafka 訊息並逐批處理。"""

    def __init__(
        self,
        pipeline: Pipeline,
        topics: list[str],
        *,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "incremental_runner",
        manager: HybridStorageManager | None = None,
        result_table: str = "incremental_results",
    ) -> None:
        self.pipeline = pipeline
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: pd.read_json(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self.manager = manager or HybridStorageManager()
        self.result_table = result_table

    def run_forever(self, num_workers: int = 1) -> None:
        """持續接收資料並寫入處理結果。"""
        for msg in self.consumer:
            df: pd.DataFrame = msg.value
            processed = self.pipeline.process(df, num_workers=num_workers)
            self.manager.write(processed, self.result_table, tier="hot")

    def read_results(self) -> pd.DataFrame:
        """讀取目前累積的結果。"""
        try:
            return self.manager.read(self.result_table)
        except KeyError:
            return pd.DataFrame()
