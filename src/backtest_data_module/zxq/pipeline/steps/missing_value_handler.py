import pandas as pd
from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep


from typing import Any


class MissingValueHandler(PipelineStep):
    """
    處理 DataFrame 遺漏值的 Pipeline 步驟。
    """

    def __init__(self, strategy: str = 'fill', fill_value: Any | None = None) -> None:
        """
        初始化 MissingValueHandler。

        Args:
            strategy: 處理遺漏值使用的策略，可為 'fill'、'dropna' 或 'interpolate'。
            fill_value: 當策略為 'fill' 時使用的值。
        """
        self.strategy = strategy
        self.fill_value = fill_value
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理 DataFrame 中的遺漏值。

        Args:
            df: 要處理的 DataFrame。

        Returns:
            處理後的 DataFrame。
        """
        if self.strategy == 'fill':
            processed_df = df.fillna(self.fill_value)
        elif self.strategy == 'dropna':
            processed_df = df.dropna()
        elif self.strategy == 'interpolate':
            processed_df = df.interpolate()
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
        return processed_df
