import unittest
import time
import tempfile
from pathlib import Path
import pandas as pd
from backtest_data_module.data_processing.pipeline import Pipeline
from backtest_data_module.zxq.pipeline.pipeline_step import PipelineStep


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

    def test_logging(self):
        temp_dir = tempfile.mkdtemp()
        log_file = Path(temp_dir) / "log.csv"
        pipeline = Pipeline(steps=[MockStep()], log_path=log_file)
        df = pd.DataFrame({'a': [1, 2, 3]})
        pipeline.process(df)
        log_df = pd.read_csv(log_file)
        self.assertEqual(len(log_df), 1)
        self.assertEqual(log_df['input_rows'].iloc[0], 3)
        self.assertEqual(log_df['output_rows'].iloc[0], 3)

    def test_parallel_processing(self):
        class SlowStep(PipelineStep):
            def __init__(self):
                super().__init__()

            def process(self, df: pd.DataFrame) -> pd.DataFrame:
                df['a'] = df['a'].apply(lambda x: time.sleep(0.01) or x + 1)
                return df

        df = pd.DataFrame({'a': list(range(20))})
        pipeline = Pipeline(steps=[SlowStep()])
        start = time.time()
        pipeline.process(df.copy(), num_workers=1)
        seq_duration = time.time() - start

        start = time.time()
        pipeline.process(df.copy(), num_workers=4)
        par_duration = time.time() - start

        self.assertLess(par_duration, seq_duration)


if __name__ == '__main__':
    unittest.main()
