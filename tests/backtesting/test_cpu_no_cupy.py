import unittest
from unittest import mock
import builtins
import importlib
import polars as pl

from backtest_data_module.backtesting.strategy import StrategyBase
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.execution import Execution
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.events import SignalEvent


class DummyStrategy(StrategyBase):
    def on_data(self, data):
        return [SignalEvent(asset="AAPL", quantity=1)]


class TestCPUNoCupy(unittest.TestCase):
    def test_cpu_backtest_without_cupy(self):
        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("cupy"):
                raise ImportError("No module named 'cupy'")
            return real_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            import backtest_data_module.utils.profiler as profiler
            import backtest_data_module.data_handler as data_handler
            import backtest_data_module.backtesting.engine as engine
            importlib.reload(profiler)
            importlib.reload(data_handler)
            importlib.reload(engine)

            data = pl.DataFrame({
                "date": [1, 2],
                "asset": ["AAPL", "AAPL"],
                "close": [100.0, 110.0],
            })
            strategy = DummyStrategy({}, device="cpu")
            portfolio = Portfolio(initial_cash=1000)
            execution = Execution()
            performance = Performance()

            backtest = engine.Backtest(
                strategy,
                portfolio,
                execution,
                performance,
                data,
            )
            self.assertEqual(backtest.device, "cpu")


if __name__ == "__main__":
    unittest.main()
