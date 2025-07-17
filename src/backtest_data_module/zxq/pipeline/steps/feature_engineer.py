import pandas as pd
from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep


class FeatureEngineer(PipelineStep):
    """
    在 DataFrame 中產生新特徵的 Pipeline 步驟。
    """

    def __init__(self, features_to_create: list[str]):
        """
        初始化 FeatureEngineer。

        Args:
            features_to_create: 要建立的特徵名稱列表，支援 'moving_average'。
        """
        self.features_to_create = features_to_create
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        在 DataFrame 中建立新特徵。

        Args:
            df: 要處理的 DataFrame。

        Returns:
            處理後的 DataFrame。
        """
        processed_df = df.copy()
        for feature in self.features_to_create:
            if feature == 'moving_average':
                processed_df['moving_average'] = (
                    processed_df['close'].rolling(window=5).mean()
                )
            else:
                raise ValueError(f"Unknown feature: {feature}")
        return processed_df
