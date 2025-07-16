from __future__ import annotations

import pandas as pd
from pandera.errors import SchemaError

from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep
from backtest_data_module.metrics import SCHEMA_VALIDATION_FAIL_COUNTER


class SchemaValidatorStep(PipelineStep):
    """包裝其他 Step，執行前後進行 Pandera schema 驗證。"""

    def __init__(self, step: PipelineStep) -> None:
        self.step = step
        super().__init__(step.input_schema, step.output_schema)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = self._validate_input(df)
        except SchemaError:
            SCHEMA_VALIDATION_FAIL_COUNTER.labels(
                step=self.step.__class__.__name__
            ).inc()
            raise
        result = self.step.process(df)
        try:
            result = self._validate_output(result)
        except SchemaError:
            SCHEMA_VALIDATION_FAIL_COUNTER.labels(
                step=self.step.__class__.__name__
            ).inc()
            raise
        return result
