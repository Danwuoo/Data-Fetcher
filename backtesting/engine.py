from __future__ import annotations

import json
from typing import List, Dict

import pandas as pd

from backtesting.events import MarketData
from backtesting.execution import Execution
from backtesting.portfolio import Portfolio
from backtesting.strategy import Strategy


class Backtest:
    def __init__(
        self,
        strategy: Strategy,
        portfolio: Portfolio,
        execution: Execution,
        data: pd.DataFrame,
    ):
        self.strategy = strategy
        self.portfolio = portfolio
        self.execution = execution
        self.data = data
        self.results = {}

    def run(self):
        for index, row in self.data.iterrows():
            market_data = MarketData(data={row["asset"]: row.to_dict()})
            orders = self.strategy.on_data(market_data)
            if orders:
                fills = self.execution.process_orders(orders, {row["asset"]: row["close"]})
                self.portfolio.update(fills)

        self.results = {
            "pnl": self.portfolio.get_pnl(self.data.groupby("asset").last()["close"].to_dict()),
            "fills": self.portfolio.fills,
        }

    def to_json(self, filepath: str):
        with open(filepath, "w") as f:
            json.dump(self.results, f, indent=4)
