from itertools import combinations
from sklearn.model_selection import KFold
import numpy as np


def purged_k_fold(n_splits: int, n_samples: int, embargo: int):
    """Purged K-Fold Cross-Validation."""
    kf = KFold(n_splits=n_splits)
    for train_index, test_index in kf.split(range(n_samples)):
        test_start = test_index[0]
        test_end = test_index[-1]
        purge_start = test_end + 1
        purge_end = purge_start + embargo
        purged_train_index = [
            i for i in train_index if i < test_start or i > purge_end
        ]
        yield purged_train_index, list(test_index)


def combinatorial_purged_cv(n_groups: int, test_groups: int, n_samples: int, embargo: int):
    """Combinatorial Purged CV splitting."""
    indices = np.arange(n_samples)
    groups = np.array_split(indices, n_groups)
    for comb in combinations(range(n_groups), test_groups):
        test_idx = np.concatenate([groups[i] for i in comb])
        purge = set(test_idx)
        for group_id in comb:
            grp = groups[group_id]
            start, end = grp[0], grp[-1]
            purge.update(range(max(0, start - embargo), start))
            purge.update(range(end + 1, min(n_samples, end + 1 + embargo)))
        train_idx = [i for i in indices if i not in purge]
        yield train_idx, test_idx.tolist()


def walk_forward_split(n_samples: int, train_size: int, test_size: int, step_size: int, embargo: int = 0):
    """Walk-forward split generator."""
    end = n_samples
    start = 0
    while start + train_size + test_size <= end:
        train_idx = list(range(start, start + train_size))
        test_start = start + train_size
        test_end = test_start + test_size
        purge = set(range(max(0, test_end), min(end, test_end + embargo)))
        train_idx = [i for i in train_idx if i not in purge]
        test_idx = list(range(test_start, test_end))
        yield train_idx, test_idx
        start += step_size

