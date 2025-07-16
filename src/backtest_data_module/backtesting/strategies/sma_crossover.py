from __future__ import annotations

from typing import List

import pandas as pd

from backtesting.execution import Order
from backtesting.strategy import Strategy
from backtesting.events import MarketData


class SmaCrossover(Strategy):
    def __init__(self, short_window: int = 50, long_window: int = 200):
        self.short_window = short_window
        self.long_window = long_window
        self.prices = {}
        self.invested = False

    def on_data(self, event: MarketData) -> List[Order]:
        orders = []
        for asset, data in event.data.items():
            if asset not in self.prices:
                self.prices[asset] = []
            self.prices[asset].append(data["close"])

            if len(self.prices[asset]) > self.long_window:
                short_sma = pd.Series(self.prices[asset]).rolling(self.short_window).mean().iloc[-1]
                long_sma = pd.Series(self.prices[asset]).rolling(self.long_window).mean().iloc[-1]

                if short_sma > long_sma and not self.invested:
                    orders.append(Order(asset, 1))
                    self.invested = True
                elif short_sma < long_sma and self.invested:
                    orders.append(Order(asset, -1))
                    self.invested = False
        return orders
