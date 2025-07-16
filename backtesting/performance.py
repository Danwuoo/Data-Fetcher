from __future__ import annotations

from typing import List

import numpy as np


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
        mean_return = np.mean(self.returns) - risk_free_rate / periods_in_year
        std_dev = np.std(self.returns)
        if std_dev == 0:
            return 0.0
        return mean_return / std_dev * np.sqrt(periods_in_year)

    def _sortino(
        self, risk_free_rate: float = 0.0, periods_in_year: int = 252
    ) -> float:
        if len(self.returns) == 0:
            return 0.0
        mean_return = np.mean(self.returns) - risk_free_rate / periods_in_year
        downside_returns = self.returns[self.returns < 0]
        if len(downside_returns) == 0:
            return np.inf
        downside_std_dev = np.std(downside_returns)
        if downside_std_dev == 0:
            return np.inf
        return mean_return / downside_std_dev * np.sqrt(periods_in_year)

    def _max_drawdown(self) -> float:
        if not self.nav_series:
            return 0.0
        nav_series = np.array(self.nav_series)
        cumulative_max = np.maximum.accumulate(nav_series)
        drawdowns = (nav_series - cumulative_max) / cumulative_max
        return np.min(drawdowns) if len(drawdowns) > 0 else 0.0

    def _var(self, confidence_level: float = 0.95) -> float:
        if len(self.returns) == 0:
            return 0.0
        return np.percentile(np.abs(self.returns), confidence_level * 100)

    def compute_metrics(self) -> dict:
        return {
            "total_return": self._total_return(),
            "sharpe": self._sharpe(),
            "sortino": self._sortino(),
            "max_drawdown": self._max_drawdown(),
            "var_95": self._var(0.95),
        }
