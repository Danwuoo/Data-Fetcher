from __future__ import annotations

from typing import List, Union

import numpy as np
import polars as pl

from backtest_data_module.backtesting.events import SignalEvent
from backtest_data_module.backtesting.strategy import StrategyBase


class SmaCrossover(StrategyBase):
    def __init__(self, params: dict):
        super().__init__(params)
        self.short_window = params.get("short_window", 10)
        self.long_window = params.get("long_window", 30)
        self.prices = {}

    def on_data(self, event: Union[np.ndarray, pl.DataFrame]) -> List[SignalEvent]:
        signals = []
        asset = list(event.data.keys())[0]
        price = event.data[asset]["close"]

        if asset not in self.prices:
            self.prices[asset] = []
        self.prices[asset].append(price)

        if len(self.prices[asset]) > self.long_window:
            short_sma = np.mean(self.prices[asset][-self.short_window :])
            long_sma = np.mean(self.prices[asset][-self.long_window :])

            if short_sma > long_sma:
                signals.append(SignalEvent(asset=asset, quantity=100, direction="long"))
            elif short_sma < long_sma:
                signals.append(
                    SignalEvent(asset=asset, quantity=-100, direction="short")
                )
        return signals
