import unittest

from backtesting.portfolio import Portfolio, Position


class TestPortfolio(unittest.TestCase):
    def test_position_update(self):
        position = Position("AAPL")
        position.update(100, 150)
        self.assertEqual(position.quantity, 100)
        self.assertEqual(position.cost_basis, 150)

        position.update(-50, 160)
        self.assertEqual(position.quantity, 50)
        self.assertEqual(position.cost_basis, 140)

    def test_portfolio_update(self):
        portfolio = Portfolio()
        fills = [
            {"asset": "AAPL", "quantity": 100, "price": 150, "commission": 1},
            {"asset": "GOOG", "quantity": 50, "price": 2800, "commission": 1},
        ]
        portfolio.update(fills)
        self.assertEqual(portfolio.positions["AAPL"].quantity, 100)
        self.assertEqual(portfolio.positions["GOOG"].quantity, 50)
        self.assertEqual(portfolio.cash, 100000 - 100 * 150 - 50 * 2800 - 2)

    def test_pnl_calculation(self):
        portfolio = Portfolio()
        fills = [
            {"asset": "AAPL", "quantity": 100, "price": 150, "commission": 1},
        ]
        portfolio.update(fills)
        pnl = portfolio.get_pnl({"AAPL": 160})
        self.assertAlmostEqual(pnl, 100 * (160 - 150))
