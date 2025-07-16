import unittest
from math import comb

import pandas as pd
from backtest_data_module.backtesting.execution import Execution
from backtest_data_module.backtesting.orchestrator import Orchestrator
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.strategies.sma_crossover import SmaCrossover
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.data_storage.storage_backend import HybridStorageManager


class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        self.storage_manager = HybridStorageManager({})
        self.data_handler = DataHandler(self.storage_manager)
        self.orchestrator = Orchestrator(
            data_handler=self.data_handler,
            strategy_cls=SmaCrossover,
            portfolio_cls=Portfolio,
            execution_cls=Execution,
            performance_cls=Performance,
        )
        self.data = pd.DataFrame(
            {
                "asset": ["AAPL"] * 200,
                "close": [100 + i + (i % 5) * 5 for i in range(200)],
            }
        )
        self.data["date"] = pd.to_datetime(
            pd.date_range(start="2020-01-01", periods=200)
        )
        self.data = self.data.set_index("date")

    def test_run_walk_forward(self):
        config = {
            "run_id": "test_walk_forward",
            "walk_forward": {
                "train_period": 50,
                "test_period": 20,
                "step_size": 20,
            },
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run(config, self.data)
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 7)
        self.assertIn("metrics", results[0])

    def test_run_cpcv(self):
        config = {
            "run_id": "test_cpcv",
            "cpcv": {"N": 10, "k": 2, "embargo_pct": 0.01},
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run(config, self.data)
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), comb(10, 2))
        self.assertIn("metrics", results[0])

    def test_run_ray_walk_forward(self):
        config = {
            "run_id": "test_ray_walk_forward",
            "walk_forward": {
                "train_period": 50,
                "test_period": 20,
                "step_size": 20,
            },
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run_ray(config, self.data)
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 7)
        self.assertIn("metrics", results[0])

    def test_run_ray_cpcv(self):
        config = {
            "run_id": "test_ray_cpcv",
            "cpcv": {"N": 10, "k": 2, "embargo_pct": 0.01},
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run_ray(config, self.data)
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), comb(10, 2))
        self.assertIn("metrics", results[0])

    def test_to_json(self):
        config = {
            "run_id": "test_to_json",
            "walk_forward": {
                "train_period": 50,
                "test_period": 20,
                "step_size": 20,
            },
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        self.orchestrator.run(config, self.data)
        self.orchestrator.to_json("test_orchestrator_results.json")
        import json

        with open("test_orchestrator_results.json", "r") as f:
            results = json.load(f)
        self.assertEqual(results["run_id"], "test_to_json")


if __name__ == "__main__":
    unittest.main()
