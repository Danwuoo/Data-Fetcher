import unittest
import numpy as np
import pandas as pd
from backtest_data_module.data_processing.cross_validation import (
    purged_k_fold,
    combinatorial_purged_cv,
    walk_forward_split,
    run_cpcv,
    CPCVResult,
)
from backtest_data_module.backtesting.performance import PerformanceSummary


def mock_strategy(train_data, test_data):
    return pd.Series(np.random.randn(len(test_data)).cumsum() + 100)


class TestCrossValidation(unittest.TestCase):
    def setUp(self):
        self.n_samples = 100
        self.data = pd.DataFrame({"feature": np.random.randn(self.n_samples)})

    def test_purged_k_fold(self):
        n_splits = 5
        embargo = 5
        folds = list(purged_k_fold(n_splits, self.n_samples, embargo))
        self.assertEqual(len(folds), n_splits)
        for train, test in folds:
            self.assertIsInstance(train, list)
            self.assertIsInstance(test, np.ndarray)
            # Check for overlap
            self.assertEqual(len(set(train) & set(test)), 0)

    def test_combinatorial_purged_cv(self):
        n_splits = 10
        n_test_splits = 2
        embargo = 5
        folds = list(
            combinatorial_purged_cv(
                n_splits, self.n_samples, n_test_splits, embargo
            )
        )
        self.assertEqual(len(folds), 45)  # C(10, 2) = 45
        for train, test in folds:
            self.assertIsInstance(train, list)
            self.assertIsInstance(test, list)
            self.assertEqual(len(set(train) & set(test)), 0)

    def test_walk_forward_split(self):
        train_size = 50
        test_size = 10
        step_size = 10
        folds = list(
            walk_forward_split(self.n_samples, train_size, test_size, step_size)
        )
        self.assertEqual(len(folds), 5)
        for train, test in folds:
            self.assertEqual(len(train), train_size)
            self.assertEqual(len(test), test_size)

    def test_run_cpcv(self):
        # 基本測試，確認函式能順利執行
        # 更完整的測試需搭配複雜的模擬策略
        result = run_cpcv(
            self.data, mock_strategy, n_splits=5, n_test_splits=2, embargo_pct=0.05
        )
        self.assertIsInstance(result, CPCVResult)

    def test_cpcv_result(self):
        summaries = [
            PerformanceSummary({"sharpe": 1.0, "max_drawdown": -0.1}),
            PerformanceSummary({"sharpe": 0.5, "max_drawdown": -0.2}),
        ]
        result = CPCVResult(summaries)
        df = result.to_dataframe()
        self.assertEqual(df.shape, (2, 2))
        agg_stats = result.aggregate_stats()
        self.assertIn("sharpe", agg_stats)
        self.assertAlmostEqual(agg_stats["sharpe"]["mean"], 0.75)


if __name__ == "__main__":
    unittest.main()
