import unittest

from backtest_data_module.data_processing.cross_validation import (
    combinatorial_purged_cv,
    walk_forward_split,
)


class TestCombinatorialPurgedCV(unittest.TestCase):
    def test_cpcv_basic(self):
        splits = list(combinatorial_purged_cv(
            n_splits=4,
            n_samples=12,
            n_test_splits=2,
            embargo=1,
        ))
        # C(4,2) = 6 次組合
        self.assertEqual(len(splits), 6)

        # 第一組 (fold0, fold1)
        train0, test0 = splits[0]
        self.assertEqual(test0, [0, 1, 2, 3, 4, 5])
        self.assertEqual(train0, [7, 8, 9, 10, 11])

        # 第二組 (fold0, fold2)
        train1, test1 = splits[1]
        self.assertEqual(test1, [0, 1, 2, 6, 7, 8])
        self.assertEqual(train1, [4, 10, 11])


class TestWalkForwardSplit(unittest.TestCase):
    def test_walk_forward(self):
        splits = list(walk_forward_split(
            n_samples=10,
            train_size=3,
            test_size=2,
            step_size=2,
        ))
        expected = [
            ([0, 1, 2], [3, 4]),
            ([2, 3, 4], [5, 6]),
            ([4, 5, 6], [7, 8]),
        ]
        self.assertEqual(splits, expected)


if __name__ == "__main__":
    unittest.main()
