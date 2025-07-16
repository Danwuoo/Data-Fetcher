import unittest

import polars as pl

from backtest_data_module.backtesting.strategies.sma_crossover import SmaCrossover
from backtest_data_module.backtesting.events import MarketEvent


class TestStrategy(unittest.TestCase):
    def test_sma_crossover(self):
        strategy = SmaCrossover(params={"short_window": 2, "long_window": 5})
        data = pl.DataFrame(
            {
                "asset": ["AAPL"] * 10,
                "close": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
                "timestamp": range(10),
            }
        )

        signals = []
        for row in data.iter_rows(named=True):
            event = MarketEvent(data={"AAPL": row})
            signals.extend(strategy.on_data(event))

        self.assertEqual(len(signals), 5)
        self.assertEqual(signals[0].direction, "long")


if __name__ == "__main__":
    unittest.main()
