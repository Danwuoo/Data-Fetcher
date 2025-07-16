from __future__ import annotations

import json
from collections import deque
from typing import List, Dict

import numpy as np
import polars as pl

from backtest_data_module.backtesting.events import MarketEvent, SignalEvent, OrderEvent, FillEvent
from backtest_data_module.backtesting.execution import Execution
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.strategy import StrategyBase


class Backtest:
    def __init__(
        self,
        strategy: StrategyBase,
        portfolio: Portfolio,
        execution: Execution,
        performance: Performance,
        data: pl.DataFrame,
    ):
        self.strategy = strategy
        self.portfolio = portfolio
        self.execution = execution
        self.performance = performance
        self.data = data
        self.events = deque()
        self.results = {}

    def run(self):
        self.events.append(MarketEvent())

        while self.events:
            event = self.events.popleft()

            if isinstance(event, MarketEvent):
                # In a real system, this would be a stream of data
                # For simplicity, we iterate through the dataframe
                for row in self.data.iter_rows(named=True):
                    market_event = MarketEvent(data={row["asset"]: row})
                    signals = self.strategy.on_data(market_event)
                    for signal in signals:
                        self.events.append(signal)

            elif isinstance(event, SignalEvent):
                # Simple logic to convert signals to orders
                order = OrderEvent(
                    asset=event.asset,
                    quantity=event.quantity,
                )
                self.events.append(order)

            elif isinstance(event, OrderEvent):
                # Place the order with the execution handler
                # We'll need to get the current timestamp from the market data
                # For now, we'll just use a placeholder
                timestamp = self.data.filter(pl.col("asset") == event.asset).select("timestamp").row(0)[0]
                self.execution.place_order(event, timestamp)

            # Process orders at the current time
            if isinstance(event, MarketEvent):
                timestamp = self.data.select("timestamp").row(0)[0] # Placeholder for current time
                fills = self.execution.process_orders(timestamp, event.data)
                for fill in fills:
                    self.events.append(FillEvent(**fill))

            elif isinstance(event, FillEvent):
                self.portfolio.update([event.__dict__])

            # Update portfolio performance
            if isinstance(event, (MarketEvent, FillEvent)):
                # This part needs to be improved to get the current price
                # For now, we just use the last known price
                last_prices = self.data.group_by("asset").last().select(["asset", "close"]).to_dict()
                last_prices = {a: p for a, p in zip(last_prices['asset'], last_prices['close'])}
                self.performance.nav_series.append(
                    self.portfolio.cash
                    + sum(
                        p.market_value(last_prices.get(asset, 0))
                        for asset, p in self.portfolio.positions.items()
                    )
                )

        self.performance.returns = (
            np.diff(self.performance.nav_series) / self.performance.nav_series[:-1]
            if self.performance.nav_series and len(self.performance.nav_series) > 1
            else np.array([])
        )

        last_prices = self.data.group_by("asset").last().select(["asset", "close"]).to_dict()
        last_prices = {a: p for a, p in zip(last_prices['asset'], last_prices['close'])}
        self.results = {
            "pnl": self.portfolio.get_pnl(last_prices),
            "fills": self.portfolio.fills,
            "performance": self.performance.compute_metrics(),
        }

    def to_json(self, filepath: str):
        with open(filepath, "w") as f:
            json.dump(self.results, f, indent=4)
