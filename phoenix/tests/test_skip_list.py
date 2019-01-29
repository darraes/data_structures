from phoenix.skip_list import SkipList
import unittest


class TestFunctions(unittest.TestCase):
    def test_basic_scans(self):
        skip_list = SkipList(5, 0.5)
        for i in range(8):
            skip_list.insert(str(i), i)

        self.assertEqual([("2", 2), ("3", 3), ("4", 4)], skip_list.scan("2", "5"))
        self.assertEqual(
            [("2", 2), ("3", 3), ("4", 4), ("5", 5)],
            skip_list.scan("2", "5", include_end=True),
        )
        self.assertEqual(
            [("0", 0), ("1", 1), ("2", 2), ("3", 3), ("4", 4)], skip_list.scan("0", "5")
        )
        self.assertEqual(
            [("3", 3), ("4", 4), ("5", 5), ("6", 6), ("7", 7)], skip_list.scan("3", "9")
        )
