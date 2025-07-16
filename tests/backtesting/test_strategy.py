import unittest
import pandas as pd

from backtesting.strategies.sma_crossover import SmaCrossover
from backtesting.events import MarketData


class TestStrategy(unittest.TestCase):
    def test_sma_crossover(self):
        strategy = SmaCrossover(short_window=2, long_window=4)
        prices = [100, 101, 102, 103, 104, 103, 102, 101, 100]
        orders = []
        for price in prices:
            event = MarketData(data={"AAPL": {"close": price}})
            orders.extend(strategy.on_data(event))

        self.assertEqual(len(orders), 2)
        self.assertEqual(orders[0].quantity, 1)
        self.assertEqual(orders[1].quantity, -1)
