import pandas as pd
from zxq.pipeline.pipeline_step import PipelineStep


class DataCleanser(PipelineStep):
    """
    A pipeline step to clean data.
    """

    def __init__(
        self,
        remove_outliers: bool = False,
        outlier_threshold: float = 1.5,
    ):
        """
        Initializes the DataCleanser.

        Args:
            remove_outliers: Whether to remove outliers.
            outlier_threshold: The threshold for outlier detection.
        """
        self.remove_outliers = remove_outliers
        self.outlier_threshold = outlier_threshold
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the DataFrame.

        Args:
            df: The DataFrame to clean.

        Returns:
            The cleaned DataFrame.
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
