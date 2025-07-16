import unittest
import pandas as pd
from backtesting.orchestrator import Orchestrator
from backtesting.strategies.sma_crossover import SmaCrossover
from backtesting.portfolio import Portfolio
from backtesting.execution import Execution
from backtesting.performance import Performance
from data_processing.data_handler import DataHandler
from data_storage.storage_backend import HybridStorageManager


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
        self.data = pd.DataFrame({
            "asset": ["AAPL"] * 100,
            "close": [100 + i + (i % 5) * 5 for i in range(100)],
        })

    def test_run_walk_forward(self):
        config = {
            "run_id": "test_walk_forward",
            "walk_forward": {
                "train_period": 20,
                "test_period": 10,
                "step_size": 10,
            },
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run(config, self.data)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertIn("metrics", results[0])

    def test_run_cpcv(self):
        config = {
            "run_id": "test_cpcv",
            "cpcv": {
                "n_splits": 5,
                "n_test_splits": 2,
                "embargo": 5,
            },
            "strategy_params": {"short_window": 5, "long_window": 10},
            "portfolio_params": {"initial_cash": 100000},
        }
        results = self.orchestrator.run(config, self.data)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertIn("metrics", results[0])

    def test_to_json(self):
        config = {
            "run_id": "test_to_json",
            "walk_forward": {
                "train_period": 20,
                "test_period": 10,
                "step_size": 10,
            },
        }
        self.orchestrator.run(config, self.data)
        self.orchestrator.to_json("test_orchestrator_results.json")
        import json
        with open("test_orchestrator_results.json", "r") as f:
            results = json.load(f)
        self.assertEqual(results["run_id"], "test_to_json")


if __name__ == "__main__":
    unittest.main()
