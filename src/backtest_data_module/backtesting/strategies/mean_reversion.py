from __future__ import annotations

from typing import List, Union

import numpy as np
import polars as pl

from backtesting.events import SignalEvent
from backtesting.strategy import StrategyBase


class MeanReversion(StrategyBase):
    def __init__(self, params: dict):
        super().__init__(params)
        self.window = params.get("window", 20)
        self.threshold = params.get("threshold", 1.5)
        self.prices = {}

    def on_data(self, event: Union[np.ndarray, pl.DataFrame]) -> List[SignalEvent]:
        signals = []
        asset = event.data["asset"]
        price = event.data[asset]["close"]

        if asset not in self.prices:
            self.prices[asset] = []
        self.prices[asset].append(price)

        if len(self.prices[asset]) > self.window:
            mean = np.mean(self.prices[asset][-self.window :])
            std = np.std(self.prices[asset][-self.window :])

            if price > mean + self.threshold * std:
                signals.append(
                    SignalEvent(asset=asset, quantity=-100, direction="short")
                )
            elif price < mean - self.threshold * std:
                signals.append(SignalEvent(asset=asset, quantity=100, direction="long"))
        return signals
