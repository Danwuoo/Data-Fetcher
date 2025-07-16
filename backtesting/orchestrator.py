from __future__ import annotations

import json
from typing import List, Dict, Type

import pandas as pd

from backtesting.engine import Backtest
from backtesting.events import MarketData
from backtesting.execution import Execution
from backtesting.performance import Performance
from backtesting.portfolio import Portfolio
from backtesting.strategy import Strategy
from data_processing.cross_validation import (
    combinatorial_purged_cv,
    walk_forward_split,
)
from data_processing.data_handler import DataHandler
from utils.json_encoder import CustomJSONEncoder


class Orchestrator:
    def __init__(
        self,
        data_handler: DataHandler,
        strategy_cls: Type[Strategy],
        portfolio_cls: Type[Portfolio],
        execution_cls: Type[Execution],
        performance_cls: Type[Performance],
    ):
        self.data_handler = data_handler
        self.strategy_cls = strategy_cls
        self.portfolio_cls = portfolio_cls
        self.execution_cls = execution_cls
        self.performance_cls = performance_cls
        self.results = {}

    def run(self, config: dict, data: pd.DataFrame) -> List[dict]:
        results = []
        if "walk_forward" in config:
            wf_config = config["walk_forward"]
            slices = walk_forward_split(
                n_samples=len(data),
                train_size=wf_config["train_period"],
                test_size=wf_config["test_period"],
                step_size=wf_config["step_size"],
            )
            for i, (train_indices, test_indices) in enumerate(slices):
                train_data = data.iloc[train_indices]
                test_data = data.iloc[test_indices]

                strategy = self.strategy_cls(**config.get("strategy_params", {}))
                portfolio = self.portfolio_cls(**config.get("portfolio_params", {}))
                execution = self.execution_cls(**config.get("execution_params", {}))
                performance = self.performance_cls()

                # For now, we'll just run the backtest on the test data
                # In a real scenario, you might train the strategy on train_data first
                backtest = Backtest(
                    strategy, portfolio, execution, performance, test_data
                )
                backtest.run()

                slice_results = {
                    "slice_id": i,
                    "train_start": train_data.index.min(),
                    "train_end": train_data.index.max(),
                    "test_start": test_data.index.min(),
                    "test_end": test_data.index.max(),
                    "metrics": backtest.results["performance"],
                }
                results.append(slice_results)
        elif "cpcv" in config:
            cpcv_config = config["cpcv"]
            slices = combinatorial_purged_cv(
                n_samples=len(data),
                n_splits=cpcv_config["n_splits"],
                n_test_splits=cpcv_config["n_test_splits"],
                embargo=cpcv_config["embargo"],
            )
            for i, (train_indices, test_indices) in enumerate(slices):
                train_data = data.iloc[train_indices]
                test_data = data.iloc[test_indices]

                strategy = self.strategy_cls(**config.get("strategy_params", {}))
                portfolio = self.portfolio_cls(**config.get("portfolio_params", {}))
                execution = self.execution_cls(**config.get("execution_params", {}))
                performance = self.performance_cls()

                backtest = Backtest(
                    strategy, portfolio, execution, performance, test_data
                )
                backtest.run()

                slice_results = {
                    "slice_id": i,
                    "train_start": train_data.index.min(),
                    "train_end": train_data.index.max(),
                    "test_start": test_data.index.min(),
                    "test_end": test_data.index.max(),
                    "metrics": backtest.results["performance"],
                }
                results.append(slice_results)
        self.results = {"run_id": config.get("run_id"), "slices": results}
        return results

    def to_json(self, filepath: str):
        with open(filepath, "w") as f:
            json.dump(self.results, f, indent=4, cls=CustomJSONEncoder)
