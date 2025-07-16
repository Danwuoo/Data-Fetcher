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
        signals = self.strategy.on_data(self.data)
        for signal in signals:
            self.events.append(signal)

        while self.events:
            event = self.events.popleft()

            if isinstance(event, SignalEvent):
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
                timestamp = self.data.filter(pl.col("asset") == event.asset).select("date").row(0)[0]
                self.execution.place_order(event, timestamp)

            elif isinstance(event, FillEvent):
                self.portfolio.update([event.__dict__])

        # Process all orders at the end of the backtest
        for timestamp in self.data["date"].unique():
            market_data_at_timestamp = self.data.filter(pl.col("date") == timestamp).to_dict(as_series=False)
            market_data_dict = {}
            for i in range(len(market_data_at_timestamp["asset"])):
                asset = market_data_at_timestamp["asset"][i]
                market_data_dict[asset] = {
                    "close": market_data_at_timestamp["close"][i]
                }

            fills = self.execution.process_orders(timestamp, market_data_dict)
            for fill in fills:
                 self.portfolio.update([fill])


        # Update portfolio performance
        for timestamp in self.data["date"].unique():
            last_prices = self.data.filter(pl.col("date") <= timestamp).group_by("asset").last().select(["asset", "close"]).to_dict()
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
