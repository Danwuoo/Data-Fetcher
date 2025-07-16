from __future__ import annotations

import json
from typing import List, Dict, Type

import pandas as pd

from backtest_data_module.backtesting.engine import Backtest
from backtest_data_module.backtesting.events import MarketEvent
from backtest_data_module.backtesting.execution import Execution
from backtest_data_module.backtesting.performance import Performance
from backtest_data_module.backtesting.portfolio import Portfolio
from backtest_data_module.backtesting.strategy import StrategyBase
from backtest_data_module.data_processing.cross_validation import (
    combinatorial_purged_cv,
    walk_forward_split,
)
from backtest_data_module.data_handler import DataHandler
from backtest_data_module.utils.json_encoder import CustomJSONEncoder
from backtest_data_module.reporting.report import ReportModule


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
        self.run_id = None
        self.strategy_name = None
        self.hyperparams = None

    def run(self, config: dict, data: pd.DataFrame) -> List[dict]:
        self.run_id = config.get("run_id")
        self.strategy_name = self.strategy_cls.__name__
        self.hyperparams = config.get("strategy_params", {})
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
        self.results = {"run_id": self.run_id, "slices": results}
        return results

    def to_json(self, filepath: str):
        with open(filepath, "w") as f:
            json.dump(self.results, f, indent=4, cls=CustomJSONEncoder)

    def generate_reports(self, output_dir: str = "."):
        if not self.run_id:
            raise ValueError("Run ID not set. Please run a backtest first.")

        report_module = ReportModule(
            self.run_id, self.results, self.strategy_name, self.hyperparams
        )

        # Generate JSON report
        json_report = report_module.generate_json()
        json_filepath = f"{output_dir}/{self.run_id}_report.json"
        with open(json_filepath, "w") as f:
            f.write(json_report)

        # Generate PDF report
        pdf_filepath = f"{output_dir}/{self.run_id}_report.pdf"
        report_module.generate_pdf(pdf_filepath)
