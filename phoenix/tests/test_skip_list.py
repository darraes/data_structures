from phoenix.skip_list import SkipList
import unittest


class TestFunctions(unittest.TestCase):
    def test_basic_scans(self):
        skip_list = SkipList(5, 0.5)
        for i in range(8):
            skip_list.insert(str(i), i)

        self.assertEqual([2, 3, 4], skip_list.scan("2", "5"))
        self.assertEqual([0, 1, 2, 3, 4], skip_list.scan("0", "5"))
        self.assertEqual([3, 4, 5, 6, 7], skip_list.scan("3", "9"))
