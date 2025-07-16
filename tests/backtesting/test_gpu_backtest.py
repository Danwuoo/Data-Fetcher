import unittest
import polars as pl
import numpy as np
from backtest_data_module.backtesting.strategy import StrategyBase
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.execution import Execution
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.engine import Backtest
from backtest_data_module.backtesting.events import SignalEvent

class MockStrategy(StrategyBase):
    def on_data(self, event):
        return [SignalEvent(asset="AAPL", quantity=100)]

class TestGPUBacktest(unittest.TestCase):
    def setUp(self):
        self.data = pl.DataFrame({
            "date": ["2022-01-01", "2022-01-02"],
            "asset": ["AAPL", "AAPL"],
            "close": [100.0, 110.0]
        })
        self.strategy = MockStrategy(params={}, device="cuda")
        self.portfolio = Portfolio(initial_cash=10000)
        self.execution = Execution()
        self.performance = Performance()

    def test_gpu_backtest(self):
        try:
            import cupy
            backtest = Backtest(
                strategy=self.strategy,
                portfolio=self.portfolio,
                execution=self.execution,
                performance=self.performance,
                data=self.data
            )
            backtest.run()
            self.assertIn("pnl", backtest.results)
            self.assertIn("fills", backtest.results)
            self.assertIn("performance", backtest.results)
        except (ImportError, cupy.cuda.runtime.CUDARuntimeError) as e:
            self.skipTest(str(e))

    def test_gpu_backtest_with_quantization(self):
        try:
            import cupy
            self.strategy.quantization_bits = 8
            backtest = Backtest(
                strategy=self.strategy,
                portfolio=self.portfolio,
                execution=self.execution,
                performance=self.performance,
                data=self.data
            )
            backtest.run()
            self.assertIn("pnl", backtest.results)
            self.assertIn("fills", backtest.results)
            self.assertIn("performance", backtest.results)
        except (ImportError, cupy.cuda.runtime.CUDARuntimeError) as e:
            self.skipTest(str(e))

    def test_gpu_backtest_with_mixed_precision(self):
        try:
            import cupy
            self.strategy.precision = "amp"
            backtest = Backtest(
                strategy=self.strategy,
                portfolio=self.portfolio,
                execution=self.execution,
                performance=self.performance,
                data=self.data
            )
            backtest.run()
            self.assertIn("pnl", backtest.results)
            self.assertIn("fills", backtest.results)
            self.assertIn("performance", backtest.results)
        except (ImportError, cupy.cuda.runtime.CUDARuntimeError) as e:
            self.skipTest(str(e))

if __name__ == "__main__":
    unittest.main()
