import unittest
from data_processing.cross_validation import purged_k_fold

class TestPurgedKFold(unittest.TestCase):
    def test_embargo_before_and_after(self):
        splits = list(purged_k_fold(n_splits=3, n_samples=12, embargo=2))

        # Fold 0
        train0, test0 = splits[0]
        self.assertNotIn(4, train0)
        self.assertNotIn(5, train0)
        self.assertTrue(all(idx >= 6 for idx in train0))

        # Fold 1
        train1, test1 = splits[1]
        self.assertNotIn(2, train1)
        self.assertNotIn(3, train1)
        self.assertNotIn(8, train1)
        self.assertNotIn(9, train1)
        self.assertEqual(train1, [0, 1, 10, 11])

        # Fold 2
        train2, test2 = splits[2]
        self.assertNotIn(6, train2)
        self.assertNotIn(7, train2)
        self.assertTrue(all(idx <= 5 for idx in train2))

if __name__ == '__main__':
    unittest.main()
