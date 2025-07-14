from sklearn.model_selection import KFold

def purged_k_fold(n_splits: int, n_samples: int, embargo: int):
    """
    Purged K-Fold Cross-Validation.

    Args:
        n_splits: The number of folds.
        n_samples: The number of samples.
        embargo: The number of samples to purge after the test set.

    Yields:
        The train and test indices for each fold.
    """
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        # Purge samples after the test set
        test_end = test_index[-1]
        purge_start = test_end + 1
        purge_end = purge_start + embargo
        purged_train_index = [i for i in train_index if i < test_index[0] or i > purge_end]
        yield purged_train_index, test_index
