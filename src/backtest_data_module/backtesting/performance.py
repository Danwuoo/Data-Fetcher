from __future__ import annotations

import json
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
from scipy.stats import kurtosis, norm, skew


class Performance:
    def __init__(self, nav_series: List[float] | None = None):
        self.nav_series = nav_series if nav_series is not None else []
        self.returns = (
            np.diff(self.nav_series) / self.nav_series[:-1]
            if self.nav_series and len(self.nav_series) > 1
            else np.array([])
        )

    def _total_return(self) -> float:
        if not self.nav_series:
            return 0.0
        return (self.nav_series[-1] / self.nav_series[0]) - 1

    def _sharpe(self, risk_free_rate: float = 0.0, periods_in_year: int = 252) -> float:
        if len(self.returns) == 0:
            return 0.0
        mean_return = np.mean(self.returns)
        std_dev = np.std(self.returns)
        if std_dev == 0:
            return 0.0
        return (mean_return * periods_in_year - risk_free_rate) / (
            std_dev * np.sqrt(periods_in_year)
        )

    def _sortino(
        self, risk_free_rate: float = 0.0, periods_in_year: int = 252
    ) -> float:
        if len(self.returns) == 0:
            return 0.0
        mean_return = np.mean(self.returns)
        downside_returns = self.returns[self.returns < 0]
        if len(downside_returns) == 0:
            return np.inf
        downside_std_dev = np.std(downside_returns)
        if downside_std_dev == 0.0:
            return np.inf
        return (mean_return * periods_in_year - risk_free_rate) / (
            downside_std_dev * np.sqrt(periods_in_year)
        )

    def _max_drawdown(self) -> Tuple[float, int]:
        if not self.nav_series:
            return 0.0, 0
        nav_series = np.array(self.nav_series)
        cumulative_max = np.maximum.accumulate(nav_series)
        drawdowns = (nav_series - cumulative_max) / cumulative_max
        max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0.0
        return max_drawdown, np.argmin(drawdowns) if len(drawdowns) > 0 else 0

    def _max_drawdown_duration(self) -> int:
        if not self.nav_series:
            return 0
        nav_series = np.array(self.nav_series)
        cumulative_max = np.maximum.accumulate(nav_series)
        drawdowns = (nav_series - cumulative_max) / cumulative_max

        in_drawdown = drawdowns < 0
        if not np.any(in_drawdown):
            return 0

        drawdown_periods = np.split(in_drawdown, np.where(np.diff(in_drawdown))[0] + 1)
        return max(len(period) for period in drawdown_periods if period[0])

    def _var(
        self, confidence_level: float = 0.95, method: str = "cornish-fisher"
    ) -> float:
        if len(self.returns) == 0:
            return 0.0
        if method == "cornish-fisher":
            s = skew(self.returns)
            k = kurtosis(self.returns, fisher=False)
            z = norm.ppf(confidence_level)
            t = (
                z
                + (z**2 - 1) * s / 6
                + (z**3 - 3 * z) * (k - 3) / 24
                - (2 * z**3 - 5 * z) * s**2 / 36
            )
            return -(np.mean(self.returns) + t * np.std(self.returns))
        else:
            return np.percentile(np.abs(self.returns), confidence_level * 100)

    def compute_metrics(self) -> PerformanceSummary:
        max_drawdown, _ = self._max_drawdown()
        return PerformanceSummary(
            {
                "total_return": self._total_return(),
                "sharpe": self._sharpe(),
                "sortino": self._sortino(),
                "max_drawdown": max_drawdown,
                "max_drawdown_duration": self._max_drawdown_duration(),
                "var_95_cornish_fisher": self._var(0.95, "cornish-fisher"),
            }
        )


class PerformanceSummary:
    def __init__(self, metrics: Dict[str, float]):
        self.metrics = metrics

    def to_json(self) -> str:
        return json.dumps(self.metrics)

    def to_arrow(self) -> pa.Table:
        return pa.Table.from_pydict(
            {key: [value] for key, value in self.metrics.items()}
        )

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([self.metrics])
