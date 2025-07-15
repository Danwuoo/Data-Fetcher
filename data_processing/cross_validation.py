from __future__ import annotations

from itertools import combinations
from typing import Iterator

from sklearn.model_selection import KFold


def purged_k_fold(
    n_splits: int, n_samples: int, embargo: int
) -> Iterator[tuple[list[int], list[int]]]:
    """
    Purged K-Fold Cross-Validation。

    Args:
        n_splits: The number of folds.
        n_samples: The number of samples.
        embargo: 在測試區段前後需排除的樣本數。

    Yields:
        The train and test indices for each fold.
    """
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        # 測試集範圍
        test_start = test_index[0]
        test_end = test_index[-1]

        # 前後禁入範圍計算
        before_start = max(0, test_start - embargo)
        before_end = test_start - 1
        after_start = test_end + 1
        after_end = min(n_samples - 1, test_end + embargo)

        # 建立需排除的索引集合
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
    組合式 Purged Cross-Validation。

    Args:
        n_splits: 將資料切成的區塊數。
        n_samples: 總樣本數。
        n_test_splits: 每次測試所使用的區塊數。
        embargo: 在測試區段前後需排除的樣本數。

    Yields:
        每次迭代返回訓練集與測試集索引。
    """
    if n_test_splits <= 0 or n_test_splits >= n_splits:
        raise ValueError("n_test_splits 必須介於 1 與 n_splits-1 之間")

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
    Walk-Forward 時間序列切分。

    Args:
        n_samples: 總樣本數。
        train_size: 每次訓練集的大小。
        test_size: 每次測試集的大小。
        step_size: 每次向前移動的步長。

    Yields:
        每次迭代返回訓練集與測試集索引。
    """
    end = n_samples - train_size - test_size
    for start in range(0, end + 1, step_size):
        train_indices = list(range(start, start + train_size))
        test_indices = list(range(start + train_size, start + train_size + test_size))
        yield train_indices, test_indices
