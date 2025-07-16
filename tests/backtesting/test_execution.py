import unittest
from collections import deque

import polars as pl

from backtest_data_module.backtesting.execution import Execution, FlatCommission, GaussianSlippage
from backtest_data_module.backtesting.events import OrderEvent


class TestExecution(unittest.TestCase):
    def test_process_orders(self):
        execution = Execution(
            commission_model=FlatCommission(0.001),
            slippage_model=GaussianSlippage(0, 0.001),
        )
        orders = [
            OrderEvent(asset="AAPL", quantity=100),
            OrderEvent(asset="GOOG", quantity=-50),
        ]
        price_data = {"AAPL": 150.0, "GOOG": 2800.0}

        for order in orders:
            execution.place_order(order, 0)

        fills = execution.process_orders(1, price_data)

        self.assertEqual(len(fills), 2)
        self.assertEqual(fills[0]["asset"], "AAPL")
        self.assertEqual(fills[0]["quantity"], 100)
        self.assertAlmostEqual(fills[0]["price"], 150.0, delta=0.5)
        self.assertAlmostEqual(fills[0]["commission"], 15.0, delta=0.5)

        self.assertEqual(fills[1]["asset"], "GOOG")
        self.assertEqual(fills[1]["quantity"], -50)
        self.assertAlmostEqual(fills[1]["price"], 2800.0, delta=10.0)
        self.assertAlmostEqual(fills[1]["commission"], 140.0, delta=1.0)


if __name__ == "__main__":
    unittest.main()
