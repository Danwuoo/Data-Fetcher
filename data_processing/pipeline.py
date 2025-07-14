import pandas as pd
from data_processing.pipeline_step import PipelineStep


class Pipeline:
    """
    A data processing pipeline.
    """

    def __init__(self, steps: list[PipelineStep]):
        """
        Initializes the Pipeline.

        Args:
            steps: A list of PipelineStep objects.
        """
        self.steps = steps

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame through the pipeline.

        Args:
            df: The DataFrame to process.

        Returns:
            The processed DataFrame.
        """
        for step in self.steps:
            df = step.process(df)
        return df
