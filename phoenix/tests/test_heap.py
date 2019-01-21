import unittest
from phoenix.heap import Heap


class TestFunctions(unittest.TestCase):
    def test_1(self):
        heap = Heap()
        heap.build_max_heap([3, 4, 5, 1, 7, 5, 3, 8, 0, 9])
        heap.max_insert(11)
        self.assertEqual(11, heap.extract_max())
        self.assertEqual(9, heap.extract_max())
        self.assertEqual(8, heap.extract_max())
        self.assertEqual(7, heap.extract_max())
        self.assertEqual(5, heap.extract_max())
        self.assertEqual(5, heap.extract_max())

        heap.max_insert(11)
        heap.max_insert(9)
        heap.max_insert(8)
        heap.max_insert(7)
        heap.max_insert(5)
        heap.max_insert(5)

        self.assertEqual(11, heap.extract_max())
        self.assertEqual(9, heap.extract_max())
        self.assertEqual(8, heap.extract_max())
        self.assertEqual(7, heap.extract_max())
        self.assertEqual(5, heap.extract_max())
        self.assertEqual(5, heap.extract_max())
        self.assertEqual(4, heap.extract_max())
        self.assertEqual(3, heap.extract_max())
        self.assertEqual(3, heap.extract_max())
        self.assertEqual(1, heap.extract_max())
        self.assertEqual(0, heap.extract_max())

        self.assertEqual((0, []), heap.state())

        heap.max_insert(11)
        heap.max_insert(9)
        heap.max_insert(8)
        heap.max_insert(7)
        heap.max_insert(5)
        heap.max_insert(5)

        self.assertEqual(11, heap.extract_max())
        self.assertEqual(9, heap.extract_max())
        self.assertEqual(8, heap.extract_max())
        self.assertEqual(7, heap.extract_max())
        self.assertEqual(5, heap.extract_max())
        self.assertEqual(5, heap.extract_max())
