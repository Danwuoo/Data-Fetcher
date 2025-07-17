import pandas as pd
import pandera as pa
from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep


class TimeAligner(PipelineStep):
    """
    用於對齊並重取樣時間序列 DataFrame 的步驟。
    """

    def __init__(self, resample_rule: str, time_column: str = 'timestamp'):
        """
        初始化 TimeAligner。

        Args:
            resample_rule: 重取樣規則（例如 '1T' 表示 1 分鐘）。
            time_column: 時間欄位名稱。
        """
        self.resample_rule = resample_rule
        self.time_column = time_column
        input_schema = pa.DataFrameSchema({  # type: ignore[no-untyped-call]
            time_column: pa.Column(pa.DateTime)
        })
        output_schema = pa.DataFrameSchema({  # type: ignore[no-untyped-call]
            time_column: pa.Column(pa.DateTime),
        })
        super().__init__(
            input_schema=input_schema,
            output_schema=output_schema,
        )

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        對齊並重取樣時間序列 DataFrame。

        Args:
            df: 要處理的 DataFrame。

        Returns:
            處理後的 DataFrame。
        """
        df[self.time_column] = pd.to_datetime(df[self.time_column])
        df = df.set_index(self.time_column)
        processed_df = df.resample(self.resample_rule).mean()
        return processed_df.reset_index()
