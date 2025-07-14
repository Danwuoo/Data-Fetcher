import unittest
from data_processing.cross_validation import purged_k_fold, combinatorial_purged_cv, walk_forward_split


class TestCrossValidation(unittest.TestCase):
    def test_purged_k_fold(self):
        splits = list(purged_k_fold(n_splits=3, n_samples=9, embargo=1))
        # ensure each train set excludes embargo region after test
        for train_idx, test_idx in splits:
            test_end = max(test_idx)
            self.assertTrue(all(i < test_idx[0] or i > test_end + 1 for i in train_idx))

    def test_combinatorial_purged_cv(self):
        splits = list(combinatorial_purged_cv(n_groups=4, test_groups=2, n_samples=12, embargo=1))
        self.assertEqual(len(splits), 6)
        # check train/test disjoint
        for train_idx, test_idx in splits:
            self.assertTrue(set(train_idx).isdisjoint(test_idx))

    def test_walk_forward_split(self):
        splits = list(walk_forward_split(n_samples=10, train_size=4, test_size=2, step_size=2, embargo=1))
        self.assertEqual(len(splits), 3)
        for train_idx, test_idx in splits:
            # ensure train and test disjoint
            self.assertTrue(set(train_idx).isdisjoint(test_idx))


if __name__ == '__main__':
    unittest.main()
