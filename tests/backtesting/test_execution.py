import unittest
from unittest.mock import patch

from backtesting.execution import Execution, Order, FlatCommission, GaussianSlippage


class TestExecution(unittest.TestCase):
    def test_process_orders(self):
        execution = Execution()
        orders = [Order("AAPL", 100)]
        with patch("random.gauss", return_value=0):
            fills = execution.process_orders(orders, {"AAPL": 150})
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]["asset"], "AAPL")
        self.assertEqual(fills[0]["quantity"], 100)
        self.assertEqual(fills[0]["price"], 150)
        self.assertEqual(fills[0]["commission"], 100 * 150 * 0.0005)

    def test_flat_commission(self):
        commission_model = FlatCommission(0.001)
        commission = commission_model.calculate(100, 150)
        self.assertEqual(commission, 15)

    def test_gaussian_slippage(self):
        slippage_model = GaussianSlippage(0, 0.01)
        with patch("random.gauss", return_value=0.01):
            slipped_price = slippage_model.apply(150)
        self.assertAlmostEqual(slipped_price, 151.5)
