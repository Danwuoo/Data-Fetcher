import unittest
import pandas as pd
from zxq.pipeline.steps.time_aligner import TimeAligner


class TestTimeAligner(unittest.TestCase):
    def test_time_aligner(self):
        aligner = TimeAligner(resample_rule='1T', time_column='timestamp')
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(
                ['2022-01-01 00:00:00', '2022-01-01 00:00:30']
            ),
            'value': [1, 2],
        })
        processed_df = aligner.process(df)
        self.assertEqual(len(processed_df), 1)
        self.assertEqual(processed_df['value'].iloc[0], 1.5)


if __name__ == '__main__':
    unittest.main()
