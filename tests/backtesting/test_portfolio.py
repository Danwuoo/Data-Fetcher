import unittest

from backtest_data_module.backtesting.portfolio import Portfolio, Position


class TestPortfolio(unittest.TestCase):
    def test_update_and_pnl(self):
        portfolio = Portfolio(initial_cash=100000)
        fills = [
            {"asset": "AAPL", "quantity": 100, "price": 150.0, "commission": 15.0},
            {"asset": "GOOG", "quantity": -50, "price": 2800.0, "commission": 140.0},
        ]
        portfolio.update(fills)

        self.assertEqual(portfolio.cash, 100000 - 150.0 * 100 - 15.0 - (-50 * 2800.0) - 140.0)
        self.assertEqual(portfolio.positions["AAPL"].quantity, 100)
        self.assertEqual(portfolio.positions["GOOG"].quantity, -50)

        market_data = {"AAPL": 160.0, "GOOG": 2700.0}
        pnl = portfolio.get_pnl(market_data)
        self.assertAlmostEqual(pnl, (160 - 150) * 100 + (-50 * 2700 - -50 * 2800))


if __name__ == "__main__":
    unittest.main()
