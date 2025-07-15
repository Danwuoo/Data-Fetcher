import pandas as pd
from zxq.pipeline.pipeline_step import PipelineStep


class FeatureEngineer(PipelineStep):
    """
    A pipeline step to create new features in a DataFrame.
    """

    def __init__(self, features_to_create: list[str]):
        """
        Initializes the FeatureEngineer.

        Args:
            features_to_create: A list of features to create.
                Supported features: 'moving_average'.
        """
        self.features_to_create = features_to_create
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates new features in a DataFrame.

        Args:
            df: The DataFrame to process.

        Returns:
            The processed DataFrame.
        """
        df = self._validate_input(df)
        processed_df = df.copy()
        for feature in self.features_to_create:
            if feature == 'moving_average':
                processed_df['moving_average'] = (
                    processed_df['close'].rolling(window=5).mean()
                )
            else:
                raise ValueError(f"Unknown feature: {feature}")
        return self._validate_output(processed_df)
