import unittest

from backtest_data_module.backtesting.events import (
    MarketEvent,
    SignalEvent,
    OrderEvent,
    FillEvent,
)


class TestEvents(unittest.TestCase):
    def test_market_event(self):
        event = MarketEvent(data={"AAPL": {"close": 150.0}})
        self.assertEqual(event.type, "MARKET")
        self.assertEqual(event.data, {"AAPL": {"close": 150.0}})

    def test_signal_event(self):
        event = SignalEvent(asset="AAPL", quantity=100, direction="long")
        self.assertEqual(event.type, "SIGNAL")
        self.assertEqual(event.asset, "AAPL")
        self.assertEqual(event.quantity, 100)
        self.assertEqual(event.direction, "long")

    def test_order_event(self):
        event = OrderEvent(asset="AAPL", quantity=100, order_type="market")
        self.assertEqual(event.type, "ORDER")
        self.assertEqual(event.asset, "AAPL")
        self.assertEqual(event.quantity, 100)
        self.assertEqual(event.order_type, "market")

    def test_fill_event(self):
        event = FillEvent(
            asset="AAPL",
            quantity=100,
            price=150.0,
            commission=1.0,
            exchange="SIMULATED",
        )
        self.assertEqual(event.type, "FILL")
        self.assertEqual(event.asset, "AAPL")
        self.assertEqual(event.quantity, 100)
        self.assertEqual(event.price, 150.0)
        self.assertEqual(event.commission, 1.0)
        self.assertEqual(event.exchange, "SIMULATED")


if __name__ == "__main__":
    unittest.main()
