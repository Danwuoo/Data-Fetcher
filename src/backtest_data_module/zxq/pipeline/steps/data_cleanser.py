import pandas as pd
from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep


class DataCleanser(PipelineStep):
    """
    清理資料的 Pipeline 步驟。
    """

    def __init__(
        self,
        remove_outliers: bool = False,
        outlier_threshold: float = 1.5,
    ):
        """
        初始化 DataCleanser。

        Args:
            remove_outliers: 是否要移除離群值。
            outlier_threshold: 判定離群值的門檻。
        """
        self.remove_outliers = remove_outliers
        self.outlier_threshold = outlier_threshold
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理 DataFrame。

        Args:
            df: 要清理的 DataFrame。

        Returns:
            清理後的 DataFrame。
        """
        processed_df = df.copy()

        if self.remove_outliers:
            for column in processed_df.select_dtypes(include=['number']).columns:
                q1 = processed_df[column].quantile(0.25)
                q3 = processed_df[column].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - self.outlier_threshold * iqr
                upper_bound = q3 + self.outlier_threshold * iqr
                processed_df = processed_df[
                    (processed_df[column] >= lower_bound)
                    & (processed_df[column] <= upper_bound)
                ]

        return processed_df
