import unittest
import pandas as pd
from data_processing.pipeline import Pipeline
from data_processing.pipeline_step import PipelineStep

class MockStep(PipelineStep):
    def __init__(self):
        super().__init__()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df['step'] = 1
        return df

class TestPipeline(unittest.TestCase):
    def test_pipeline(self):
        pipeline = Pipeline(steps=[MockStep()])
        df = pd.DataFrame({'a': [1]})
        processed_df = pipeline.process(df)
        self.assertIn('step', processed_df.columns)

if __name__ == '__main__':
    unittest.main()
