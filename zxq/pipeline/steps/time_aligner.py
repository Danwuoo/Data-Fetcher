import pandas as pd
import pandera as pa
from zxq.pipeline.pipeline_step import PipelineStep


class TimeAligner(PipelineStep):
    """
    A pipeline step to align and resample a time-series DataFrame.
    """

    def __init__(self, resample_rule: str, time_column: str = 'timestamp'):
        """
        Initializes the TimeAligner.

        Args:
            resample_rule: The resampling rule (e.g., '1T' for 1 minute).
            time_column: The name of the time column.
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
        Aligns and resamples a time-series DataFrame.

        Args:
            df: The DataFrame to process.

        Returns:
            The processed DataFrame.
        """
        df[self.time_column] = pd.to_datetime(df[self.time_column])
        df = df.set_index(self.time_column)
        processed_df = df.resample(self.resample_rule).mean()
        return processed_df.reset_index()
