import unittest
import pandas as pd
from backtest_data_module.zxq.pipeline.steps.feature_engineer import FeatureEngineer
from backtest_data_module.zxq.pipeline.steps.schema_validator import SchemaValidatorStep


class TestFeatureEngineer(unittest.TestCase):
    def test_feature_engineer(self):
        engineer = FeatureEngineer(features_to_create=['moving_average'])
        wrapped = SchemaValidatorStep(engineer)
        df = pd.DataFrame({'close': [1, 2, 3, 4, 5, 6]})
        processed_df = wrapped.process(df)
        self.assertIn('moving_average', processed_df.columns)
        self.assertTrue(pd.isna(processed_df['moving_average'].iloc[3]))
        self.assertEqual(processed_df['moving_average'].iloc[4], 3)


if __name__ == '__main__':
    unittest.main()
