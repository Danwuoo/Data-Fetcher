import unittest
import pandas as pd
import numpy as np
from zxq.pipeline.steps.missing_value_handler import MissingValueHandler


class TestMissingValueHandler(unittest.TestCase):
    def test_fill(self):
        handler = MissingValueHandler(strategy='fill', fill_value=0)
        df = pd.DataFrame({'a': [1, np.nan]})
        processed_df = handler.process(df)
        self.assertEqual(processed_df['a'].iloc[1], 0)

    def test_dropna(self):
        handler = MissingValueHandler(strategy='dropna')
        df = pd.DataFrame({'a': [1, np.nan]})
        processed_df = handler.process(df)
        self.assertEqual(len(processed_df), 1)

    def test_interpolate(self):
        handler = MissingValueHandler(strategy='interpolate')
        df = pd.DataFrame({'a': [1, np.nan, 3]})
        processed_df = handler.process(df)
        self.assertEqual(processed_df['a'].iloc[1], 2)


if __name__ == '__main__':
    unittest.main()
