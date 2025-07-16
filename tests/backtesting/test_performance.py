import unittest
import numpy as np
from backtesting.performance import Performance


class TestPerformance(unittest.TestCase):
    def test_performance_metrics(self):
        # Test case 1: Positive returns
        nav_series_1 = [100, 110, 120, 130, 140, 150]
        perf_1 = Performance(nav_series_1)
        metrics_1 = perf_1.compute_metrics()
        self.assertAlmostEqual(metrics_1["total_return"], 0.5)
        self.assertAlmostEqual(metrics_1["sharpe"], 132.72, places=2)
        self.assertTrue(np.isinf(metrics_1["sortino"]))
        self.assertAlmostEqual(metrics_1["max_drawdown"], 0.0)
        self.assertAlmostEqual(metrics_1["var_95"], 0.1, places=1)

        # Test case 2: Mixed returns
        nav_series_2 = [100, 110, 105, 115, 110, 120]
        perf_2 = Performance(nav_series_2)
        metrics_2 = perf_2.compute_metrics()
        self.assertAlmostEqual(metrics_2["total_return"], 0.2)
        self.assertAlmostEqual(metrics_2["sharpe"], 9.13, places=2)
        self.assertAlmostEqual(metrics_2["sortino"], 633.65, places=2)
        self.assertAlmostEqual(metrics_2["max_drawdown"], -0.04545, places=5)
        self.assertAlmostEqual(metrics_2["var_95"], 0.1, places=1)

        # Test case 3: All negative returns
        nav_series_3 = [100, 90, 80, 70, 60, 50]
        perf_3 = Performance(nav_series_3)
        metrics_3 = perf_3.compute_metrics()
        self.assertAlmostEqual(metrics_3["total_return"], -0.5)
        self.assertAlmostEqual(metrics_3["sharpe"], -87.18, places=2)
        self.assertAlmostEqual(metrics_3["sortino"], -87.18, places=2)
        self.assertAlmostEqual(metrics_3["max_drawdown"], -0.5)
        self.assertAlmostEqual(metrics_3["var_95"], 0.16, places=2)

        # Test case 4: Empty nav_series
        nav_series_4 = []
        perf_4 = Performance(nav_series_4)
        metrics_4 = perf_4.compute_metrics()
        self.assertEqual(metrics_4["total_return"], 0.0)
        self.assertEqual(metrics_4["sharpe"], 0.0)
        self.assertEqual(metrics_4["max_drawdown"], 0.0)
        self.assertEqual(metrics_4["var_95"], 0.0)

        # Test case 5: Single element nav_series
        nav_series_5 = [100]
        perf_5 = Performance(nav_series_5)
        metrics_5 = perf_5.compute_metrics()
        self.assertEqual(metrics_5["total_return"], 0.0)
        self.assertEqual(metrics_5["sharpe"], 0.0)
        self.assertEqual(metrics_5["max_drawdown"], 0.0)
        self.assertEqual(metrics_5["var_95"], 0.0)


if __name__ == "__main__":
    unittest.main()
