from __future__ import annotations

import json
from itertools import combinations
from typing import Callable, Dict, Iterator, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.model_selection import KFold

from backtest_data_module.backtesting.performance import PerformanceSummary


class CPCVResult:
    """
    A container for the results of a Combinatorial Purged Cross-Validation run.
    """

    def __init__(self, results: List[PerformanceSummary]):
        self.results = results

    def to_parquet(self, path: str):
        """
        Saves the results to a Parquet file.

        Args:
            path: The path to the Parquet file.
        """
        df = self.to_dataframe()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

    def to_json(self) -> str:
        """
        Returns the results as a JSON string.
        """
        return json.dumps([r.metrics for r in self.results])

    def to_arrow(self) -> pa.Table:
        """
        Returns the results as an Arrow table.
        """
        return pa.Table.from_pandas(self.to_dataframe())

    def to_dataframe(self) -> pd.DataFrame:
        """
        Returns the results as a Pandas DataFrame.
        """
        return pd.DataFrame([r.metrics for r in self.results])

    def aggregate_stats(self) -> Dict[str, Dict[str, float]]:
        """
        Aggregates the statistics of the results.
        """
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
    Generate indices for Purged K-Fold Cross-Validation.

    This method splits the data into k folds and removes a number of samples
    (the embargo) from the training set that are close to the test set.

    Args:
        n_splits: The number of folds.
        n_samples: The total number of samples.
        embargo: The number of samples to remove from the training set
                 around the test set.

    Yields:
        A tuple of (train_indices, test_indices) for each fold.
    """
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        test_start = test_index[0]
        test_end = test_index[-1]

        # Define the embargo periods before and after the test set
        before_start = max(0, test_start - embargo)
        before_end = test_start - 1
        after_start = test_end + 1
        after_end = min(n_samples - 1, test_end + embargo)

        # Create the set of embargo indices
        embargo_indices: set[int] = (
            set(range(before_start, before_end + 1))
            | set(range(after_start, after_end + 1))
        )

        # Purge the training indices
        purged_train_index = [i for i in train_index if i not in embargo_indices]
        yield purged_train_index, test_index


def combinatorial_purged_cv(
    n_splits: int,
    n_samples: int,
    n_test_splits: int,
    embargo: int,
) -> Iterator[tuple[list[int], list[int]]]:
    """
    Generate indices for Combinatorial Purged Cross-Validation.

    This method creates all possible combinations of train/test splits from
    N folds, where k folds are used for testing. It also applies purging
    and embargoing to prevent data leakage.

    Args:
        n_splits: The total number of folds to split the data into.
        n_samples: The total number of samples in the data.
        n_test_splits: The number of folds to use for testing in each combination.
        embargo: The number of samples to remove from the training set
                 around the test set.

    Yields:
        A tuple of (train_indices, test_indices) for each combination.
    """
    if n_test_splits <= 0 or n_test_splits >= n_splits:
        raise ValueError(
            "n_test_splits must be between 1 and n_splits-1"
        )

    fold_size = n_samples // n_splits
    indices = list(range(n_samples))
    folds = [
        indices[i * fold_size : (i + 1) * fold_size] for i in range(n_splits)
    ]
    if n_samples % n_splits:
        folds[-1].extend(indices[n_splits * fold_size :])

    for combo in combinations(range(n_splits), n_test_splits):
        test_indices = sorted([i for idx in combo for i in folds[idx]])
        embargo_indices: set[int] = set()
        for idx in combo:
            start = folds[idx][0]
            end = folds[idx][-1]
            embargo_indices.update(range(max(0, start - embargo), start))
            embargo_indices.update(
                range(end + 1, min(n_samples, end + embargo + 1))
            )

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
    Generate indices for Walk-Forward Analysis.

    This method creates a series of train/test splits that roll forward
    in time.

    Args:
        n_samples: The total number of samples in the data.
        train_size: The number of samples in the training set.
        test_size: The number of samples in the test set.
        step_size: The number of samples to move forward for each new split.

    Yields:
        A tuple of (train_indices, test_indices) for each split.
    """
    end = n_samples - train_size - test_size
    for start in range(0, end + 1, step_size):
        train_indices = list(range(start, start + train_size))
        test_indices = list(range(start + train_size, start + train_size + test_size))
        yield train_indices, test_indices


def run_cpcv(
    data: pd.DataFrame,
    strategy_func: Callable[[pd.DataFrame, pd.DataFrame], pd.Series],
    n_splits: int,
    n_test_splits: int,
    embargo_pct: float,
) -> CPCVResult:
    """
    Run a Combinatorial Purged Cross-Validation backtest.

    Args:
        data: The input data for the backtest.
        strategy_func: The function that implements the trading strategy.
        n_splits: The total number of folds to split the data into.
        n_test_splits: The number of folds to use for testing in each combination.
        embargo_pct: The percentage of the data to use for the embargo.

    Returns:
        A CPCVResult object containing the results of the backtest.
    """
    n_samples = len(data)
    embargo = int(n_samples * embargo_pct)
    results = []

    for train_indices, test_indices in combinatorial_purged_cv(
        n_splits, n_samples, n_test_splits, embargo
    ):
        train_data = data.iloc[train_indices]
        test_data = data.iloc[test_indices]

        # 直接執行策略函式，目前僅示範用途
        strategy_func(train_data, test_data)

        # 如需計算績效，可將返回的 nav_series 轉為 Performance 物件
        # performance = Performance(nav_series=nav_series.tolist())
        # results.append(performance.compute_metrics())

    return CPCVResult(results)
