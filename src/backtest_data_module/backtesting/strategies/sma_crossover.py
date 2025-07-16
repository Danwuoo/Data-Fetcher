from __future__ import annotations

from typing import List, Union

import numpy as np
import polars as pl

from backtest_data_module.backtesting.events import SignalEvent
from backtest_data_module.backtesting.strategy import StrategyBase


class SmaCrossover(StrategyBase):
    def __init__(self, short_window: int = 10, long_window: int = 30):
        super().__init__({})
        self.short_window = short_window
        self.long_window = long_window

    def on_data(self, data: pl.DataFrame) -> List[SignalEvent]:
        signals = []
        for asset in data["asset"].unique():
            asset_data = data.filter(pl.col("asset") == asset)

            # Calculate SMAs
            asset_data = asset_data.with_columns(
                pl.col("close")
                .rolling_mean(window_size=self.short_window)
                .alias("short_sma")
            )
            asset_data = asset_data.with_columns(
                pl.col("close")
                .rolling_mean(window_size=self.long_window)
                .alias("long_sma")
            )

            # Generate signals
            asset_data = asset_data.with_columns(
                pl.when(asset_data["short_sma"] > asset_data["long_sma"])
                .then(1)
                .when(asset_data["short_sma"] < asset_data["long_sma"])
                .then(-1)
                .otherwise(0)
                .alias("signal")
            )

            # Create SignalEvents
            for row in asset_data.iter_rows(named=True):
                if row["signal"] == 1:
                    signals.append(
                        SignalEvent(asset=asset, quantity=100, direction="long")
                    )
                elif row["signal"] == -1:
                    signals.append(
                        SignalEvent(asset=asset, quantity=-100, direction="short")
                    )
        return signals
