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
    儲存 Combinatorial Purged Cross-Validation 結果的容器。
    """

    def __init__(self, results: List[PerformanceSummary]):
        self.results = results

    def to_parquet(self, path: str):
        """
        將結果保存為 Parquet 檔案。

        Args:
            path: Parquet 檔案路徑。
        """
        df = self.to_dataframe()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

    def to_json(self) -> str:
        """以 JSON 字串回傳結果。"""
        return json.dumps([r.metrics for r in self.results])

    def to_arrow(self) -> pa.Table:
        """以 Arrow table 回傳結果。"""
        return pa.Table.from_pandas(self.to_dataframe())

    def to_dataframe(self) -> pd.DataFrame:
        """以 Pandas DataFrame 回傳結果。"""
        return pd.DataFrame([r.metrics for r in self.results])

    def aggregate_stats(self) -> Dict[str, Dict[str, float]]:
        """彙整結果統計值。"""
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
    產生 Purged K-Fold Cross-Validation 的索引。

    此方法會將資料切成 k 個區塊，並在測試區塊前後
    去除一定數量（embargo）的訓練資料，避免資訊外洩。

    Args:
        n_splits: 區塊數量。
        n_samples: 總樣本數。
        embargo: 在測試集前後需排除的樣本數。

    Yields:
        每一折的 ``(train_indices, test_indices)``。
    """
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        test_start = test_index[0]
        test_end = test_index[-1]

        # 定義測試集前後的隔離區間
        before_start = max(0, test_start - embargo)
        before_end = test_start - 1
        after_start = test_end + 1
        after_end = min(n_samples - 1, test_end + embargo)

        # 建立需排除的索引集合
        embargo_indices: set[int] = (
            set(range(before_start, before_end + 1))
            | set(range(after_start, after_end + 1))
        )

        # 移除受隔離影響的訓練索引
        purged_train_index = [i for i in train_index if i not in embargo_indices]
        yield purged_train_index, test_index


def combinatorial_purged_cv(
    n_splits: int,
    n_samples: int,
    n_test_splits: int,
    embargo: int,
) -> Iterator[tuple[list[int], list[int]]]:
    """
    產生 Combinatorial Purged Cross-Validation 的索引。

    此方法建立所有可能的訓練/測試組合，
    在 N 個折中選取 k 個折做為測試集，並套用 purging 與 embargo 以避免資料外洩。

    Args:
        n_splits: 分割的折數。
        n_samples: 資料的總樣本數。
        n_test_splits: 每次組合用於測試的折數。
        embargo: 在測試集前後需排除的樣本數。

    Yields:
        每個組合的 ``(train_indices, test_indices)``。
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
    產生 Walk-Forward Analysis 的索引。

    此方法會依時間向前滾動產生多組訓練/測試區間。

    Args:
        n_samples: 總樣本數。
        train_size: 訓練集大小。
        test_size: 測試集大小。
        step_size: 每次向前移動的步長。

    Yields:
        每個切分的 ``(train_indices, test_indices)``。
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
    執行 Combinatorial Purged Cross-Validation 回測。

    Args:
        data: 用於回測的輸入資料。
        strategy_func: 實作交易策略的函式。
        n_splits: 資料分割的折數。
        n_test_splits: 每次組合用於測試的折數。
        embargo_pct: 用於 embargo 的資料比例。

    Returns:
        含回測結果的 ``CPCVResult`` 物件。
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
