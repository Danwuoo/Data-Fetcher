from __future__ import annotations

import json
from itertools import combinations
from typing import Any, Callable, Dict, Iterator, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.model_selection import KFold

from backtest_data_module.backtesting.performance import PerformanceSummary


class CPCVResult:
    def __init__(self, results: List[PerformanceSummary]):
        self.results = results

    def to_parquet(self, path: str):
        df = self.to_dataframe()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

    def to_json(self) -> str:
        return json.dumps([r.metrics for r in self.results])

    def to_arrow(self) -> pa.Table:
        return pa.Table.from_pandas(self.to_dataframe())

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([r.metrics for r in self.results])

    def aggregate_stats(self) -> Dict[str, Dict[str, float]]:
        df = self.to_dataframe()
        return {
            metric: {
                "mean": df[metric].mean(),
                "std": df[metric].std(),
                "min": df[metric].min(),
                "max": df[metric].max(),
            }
            for metric in df.columns
        }


def purged_k_fold(
    n_splits: int, n_samples: int, embargo: int
) -> Iterator[tuple[list[int], list[int]]]:
    """
    Purged K-Fold Cross-Validation?

    Args:
        n_splits: The number of folds.
        n_samples: The number of samples.
        embargo: ????????????????

    Yields:
        The train and test indices for each fold.
    """
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        # ??????
        test_start = test_index[0]
        test_end = test_index[-1]

        # ??????????
        before_start = max(0, test_start - embargo)
        before_end = test_start - 1
        after_start = test_end + 1
        after_end = min(n_samples - 1, test_end + embargo)

        # ????????????
        embargo_indices: set[int] = (
            set(range(before_start, before_end + 1))
            | set(range(after_start, after_end + 1))
        )

        purged_train_index = [i for i in train_index if i not in embargo_indices]
        yield purged_train_index, test_index


def combinatorial_purged_cv(
    n_splits: int,
    n_samples: int,
    n_test_splits: int,
    embargo: int,
) -> Iterator[tuple[list[int], list[int]]]:
    """
    ??? Purged Cross-Validation?

    Args:
        n_splits: ?????????
        n_samples: ?????
        n_test_splits: ????????????
        embargo: ????????????????

    Yields:
        ??????????????????
    """
    if n_test_splits <= 0 or n_test_splits >= n_splits:
        raise ValueError("n_test_splits ????? 1 ? n_splits-1 ????")

    fold_size = n_samples // n_splits
    indices = list(range(n_samples))
    folds = [indices[i * fold_size : (i + 1) * fold_size] for i in range(n_splits)]
    if n_samples % n_splits:
        folds[-1].extend(indices[n_splits * fold_size :])

    for combo in combinations(range(n_splits), n_test_splits):
        test_indices = sorted([i for idx in combo for i in folds[idx]])
        embargo_indices: set[int] = set()
        for idx in combo:
            start = folds[idx][0]
            end = folds[idx][-1]
            embargo_indices.update(range(max(0, start - embargo), start))
            embargo_indices.update(range(end + 1, min(n_samples, end + embargo + 1)))

        train_indices = [
            i
            for j, fold in enumerate(folds)
            if j not in combo
            for i in fold
            if i not in embargo_indices
        ]
        yield train_indices, test_indices


def walk_forward_split(
    n_samples: int, train_size: int, test_size: int, step_size: int
) -> Iterator[tuple[list[int], list[int]]]:
    """
    Walk-Forward ???????

    Args:
        n_samples: ?????
        train_size: ????????
        test_size: ????????
        step_size: ?????????

    Yields:
        ??????????????????
    """
    end = n_samples - train_size - test_size
    for start in range(0, end + 1, step_size):
        train_indices = list(range(start, start + train_size))
        test_indices = list(range(start + train_size, start + train_size + test_size))
        yield train_indices, test_indices


def run_cpcv(
    data: pd.DataFrame,
    strategy_func: Callable[[pd.DataFrame], pd.Series],
    n_splits: int,
    n_test_splits: int,
    embargo_pct: float,
) -> CPCVResult:
    n_samples = len(data)
    embargo = int(n_samples * embargo_pct)
    results = []

    for train_indices, test_indices in combinatorial_purged_cv(
        n_splits, n_samples, n_test_splits, embargo
    ):
        train_data = data.iloc[train_indices]
        test_data = data.iloc[test_indices]

        nav_series = strategy_func(train_data, test_data)

        # performance = Performance(nav_series=nav_series.tolist())
        # results.append(performance.compute_metrics())

    return CPCVResult(results)
