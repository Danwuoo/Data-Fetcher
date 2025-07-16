from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
import pandera as pa


class PipelineStep(ABC):
    """
    A base class for a step in a data processing pipeline.
    """

    def __init__(
        self,
        input_schema: Optional[pa.DataFrameSchema] = None,
        output_schema: Optional[pa.DataFrameSchema] = None,
    ) -> None:
        """
        Initializes the PipelineStep.

        Args:
            input_schema: The schema of the input DataFrame.
            output_schema: The schema of the output DataFrame.
        """
        self.input_schema = input_schema
        self.output_schema = output_schema

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame.

        Args:
            df: The DataFrame to process.

        Returns:
            The processed DataFrame.
        """
        pass

    def _validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates the input DataFrame against the input schema.

        Args:
            df: The DataFrame to validate.

        Returns:
            The validated DataFrame.
        """
        if self.input_schema:
            return self.input_schema.validate(df)
        return df

    def _validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates the output DataFrame against the output schema.

        Args:
            df: The DataFrame to validate.

        Returns:
            The validated DataFrame.
        """
        if self.output_schema:
            return self.output_schema.validate(df)
        return df
