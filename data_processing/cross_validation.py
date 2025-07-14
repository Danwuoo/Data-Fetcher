from sklearn.model_selection import KFold


def purged_k_fold(n_splits: int, n_samples: int, embargo: int):
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
        embargo_indices = (
            set(range(before_start, before_end + 1))
            | set(range(after_start, after_end + 1))
        )

        purged_train_index = [i for i in train_index if i not in embargo_indices]
        yield purged_train_index, test_index
