import unittest
import numpy as np
import pyarrow as pa
from backtest_data_module.backtesting.performance import Performance, PerformanceSummary


class TestPerformance(unittest.TestCase):
    def setUp(self):
        self.nav_series_1 = [100, 110, 120, 130, 140, 150]
        self.perf_1 = Performance(self.nav_series_1)

        self.nav_series_2 = [100, 110, 105, 115, 110, 120]
        self.perf_2 = Performance(self.nav_series_2)

        self.nav_series_3 = [100, 90, 80, 70, 60, 50]
        self.perf_3 = Performance(self.nav_series_3)

        self.nav_series_4 = []
        self.perf_4 = Performance(self.nav_series_4)

        self.nav_series_5 = [100]
        self.perf_5 = Performance(self.nav_series_5)

    def test_total_return(self):
        self.assertAlmostEqual(self.perf_1._total_return(), 0.5)
        self.assertAlmostEqual(self.perf_2._total_return(), 0.2)
        self.assertAlmostEqual(self.perf_3._total_return(), -0.5)
        self.assertEqual(self.perf_4._total_return(), 0.0)
        self.assertEqual(self.perf_5._total_return(), 0.0)

    def test_sharpe(self):
        self.assertAlmostEqual(self.perf_1._sharpe(), 132.72, places=2)
        self.assertAlmostEqual(self.perf_2._sharpe(), 9.13, places=2)
        self.assertAlmostEqual(self.perf_3._sharpe(), -86.81, places=2)
        self.assertEqual(self.perf_4._sharpe(), 0.0)
        self.assertEqual(self.perf_5._sharpe(), 0.0)

    def test_sortino(self):
        self.assertTrue(np.isinf(self.perf_1._sortino()))
        self.assertAlmostEqual(self.perf_2._sortino(), 633.65, places=2)
        self.assertAlmostEqual(self.perf_3._sortino(), -86.81, places=2)
        self.assertEqual(self.perf_4._sortino(), 0.0)
        self.assertEqual(self.perf_5._sortino(), 0.0)

    def test_max_drawdown(self):
        self.assertAlmostEqual(self.perf_1._max_drawdown()[0], 0.0)
        self.assertAlmostEqual(self.perf_2._max_drawdown()[0], -0.04545, places=5)
        self.assertAlmostEqual(self.perf_3._max_drawdown()[0], -0.5)
        self.assertEqual(self.perf_4._max_drawdown()[0], 0.0)
        self.assertEqual(self.perf_5._max_drawdown()[0], 0.0)

    def test_max_drawdown_duration(self):
        self.assertEqual(self.perf_1._max_drawdown_duration(), 0)
        self.assertEqual(self.perf_2._max_drawdown_duration(), 1)
        self.assertEqual(self.perf_3._max_drawdown_duration(), 5)
        self.assertEqual(self.perf_4._max_drawdown_duration(), 0)
        self.assertEqual(self.perf_5._max_drawdown_duration(), 0)

    def test_var_cornish_fisher(self):
        from scipy.stats import skew, kurtosis, norm
        s = skew(self.perf_1.returns)
        k = kurtosis(self.perf_1.returns, fisher=False)
        z = norm.ppf(0.95)
        t = (
            z + (z**2 - 1) * s / 6
            + (z**3 - 3 * z) * (k - 3) / 24
            - (2 * z**3 - 5 * z) * s**2 / 36
        )
        expected_var = -(
            np.mean(self.perf_1.returns) + t * np.std(self.perf_1.returns)
        )
        self.assertAlmostEqual(
            self.perf_1._var(method="cornish-fisher"),
            expected_var,
            places=3,
        )
        s = skew(self.perf_2.returns)
        k = kurtosis(self.perf_2.returns, fisher=False)
        z = norm.ppf(0.95)
        t = (
            z + (z**2 - 1) * s / 6
            + (z**3 - 3 * z) * (k - 3) / 24
            - (2 * z**3 - 5 * z) * s**2 / 36
        )
        expected_var = -(
            np.mean(self.perf_2.returns) + t * np.std(self.perf_2.returns)
        )
        self.assertAlmostEqual(
            self.perf_2._var(method="cornish-fisher"),
            expected_var,
            places=3,
        )

        s = skew(self.perf_3.returns)
        k = kurtosis(self.perf_3.returns, fisher=False)
        z = norm.ppf(0.95)
        t = (
            z + (z**2 - 1) * s / 6
            + (z**3 - 3 * z) * (k - 3) / 24
            - (2 * z**3 - 5 * z) * s**2 / 36
        )
        expected_var = -(
            np.mean(self.perf_3.returns) + t * np.std(self.perf_3.returns)
        )
        self.assertAlmostEqual(
            self.perf_3._var(method="cornish-fisher"),
            expected_var,
            places=3,
        )
        self.assertEqual(self.perf_4._var(method="cornish-fisher"), 0.0)
        self.assertEqual(self.perf_5._var(method="cornish-fisher"), 0.0)

    def test_compute_metrics(self):
        metrics = self.perf_2.compute_metrics()
        self.assertIsInstance(metrics, PerformanceSummary)
        self.assertIn("total_return", metrics.metrics)
        self.assertIn("sharpe", metrics.metrics)
        self.assertIn("sortino", metrics.metrics)
        self.assertIn("max_drawdown", metrics.metrics)
        self.assertIn("max_drawdown_duration", metrics.metrics)
        self.assertIn("var_95_cornish_fisher", metrics.metrics)

    def test_performance_summary(self):
        metrics_dict = {"sharpe": 1.0, "max_drawdown": -0.1}
        summary = PerformanceSummary(metrics_dict)
        self.assertEqual(summary.to_dataframe().iloc[0]["sharpe"], 1.0)
        self.assertIn("sharpe", summary.to_json())
        self.assertIsInstance(summary.to_arrow(), pa.Table)


if __name__ == "__main__":
    unittest.main()
