import pandas as pd
from zxq.pipeline.pipeline_step import PipelineStep


from typing import Any


class MissingValueHandler(PipelineStep):
    """
    A pipeline step to handle missing values in a DataFrame.
    """

    def __init__(self, strategy: str = 'fill', fill_value: Any | None = None) -> None:
        """
        Initializes the MissingValueHandler.

        Args:
            strategy: The strategy to use for handling missing values.
                Can be 'fill', 'dropna', or 'interpolate'.
            fill_value: The value to use when strategy is 'fill'.
        """
        self.strategy = strategy
        self.fill_value = fill_value
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing values in a DataFrame.

        Args:
            df: The DataFrame to process.

        Returns:
            The processed DataFrame.
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
