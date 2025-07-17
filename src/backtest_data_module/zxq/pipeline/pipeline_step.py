from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
import pandera as pa


class PipelineStep(ABC):
    """
    資料處理 Pipeline 步驟的基底類別。
    """

    def __init__(
        self,
        input_schema: Optional[pa.DataFrameSchema] = None,
        output_schema: Optional[pa.DataFrameSchema] = None,
    ) -> None:
        """
        初始化 PipelineStep。

        Args:
            input_schema: 輸入 DataFrame 的 schema。
            output_schema: 輸出 DataFrame 的 schema。
        """
        self.input_schema = input_schema
        self.output_schema = output_schema

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        處理 DataFrame。

        Args:
            df: 要處理的 DataFrame。

        Returns:
            處理後的 DataFrame。
        """
        pass

    def _validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        依據輸入 schema 驗證 DataFrame。

        Args:
            df: 要驗證的 DataFrame。

        Returns:
            驗證後的 DataFrame。
        """
        if self.input_schema:
            return self.input_schema.validate(df)
        return df

    def _validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        依據輸出 schema 驗證 DataFrame。

        Args:
            df: 要驗證的 DataFrame。

        Returns:
            驗證後的 DataFrame。
        """
        if self.output_schema:
            return self.output_schema.validate(df)
        return df
