# TODO: proper test cases

import unittest
from phoenix.cache import *


class TestFunctions(unittest.TestCase):
    def test_lru_new_insertions(self):
        cache = LRUCache(3)
        cache.put("k1", "v1")
        self.assertEqual("v1", cache.get("k1"))
        cache.put("k2", "v2")
        self.assertEqual("v2", cache.get("k2"))
        cache.put("k3", "v3")
        self.assertEqual("v3", cache.get("k3"))
        cache.put("k4", "v4")
        self.assertEqual("v4", cache.get("k4"))
        self.assertEqual("v3", cache.get("k3"))
        self.assertEqual("v2", cache.get("k2"))
        self.assertEqual(-1, cache.get("k1"))

    def test_lru_tail_to_head(self):
        cache = LRUCache(3)
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k1", "v11")
        cache.put("k4", "v4")
        self.assertEqual(-1, cache.get("k2"))

        self.assertEqual("v3", cache.get("k3"))
        cache.put("k5", "v5")
        self.assertEqual(-1, cache.get("k1"))

    def test_lru_middle_to_head(self):
        cache = LRUCache(3)
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k2", "v22")
        cache.put("k4", "v4")
        self.assertEqual(-1, cache.get("k1"))

        self.assertEqual("v22", cache.get("k2"))
        cache.put("k5", "v5")
        self.assertEqual(-1, cache.get("k3"))

    def test_lru_head_to_head(self):
        cache = LRUCache(3)
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k3", "v4")
        cache.put("k4", "v4")
        self.assertEqual(-1, cache.get("k1"))

        self.assertEqual("v4", cache.get("k4"))
        cache.put("k5", "v5")
        self.assertEqual(-1, cache.get("k2"))

    def test_lfu_4(self):
        cache = LFUCache(0)

        cache.put(0, 0)
        self.assertEqual(-1, cache.get(0))

    def test_lfu_3(self):
        cache = LFUCache(2)

        cache.put(1, 1)
        cache.put(2, 2)
        self.assertEqual(1, cache.get(1))

        cache.put(3, 3)
        self.assertEqual(-1, cache.get(2))
        self.assertEqual(3, cache.get(3))

        cache.put(4, 4)
        self.assertEqual(-1, cache.get(1))
        self.assertEqual(3, cache.get(3))
        self.assertEqual(4, cache.get(4))

    def test_lfu_2(self):
        cache = LFUCache(5)
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k4", "v4")
        cache.put("k5", "v5")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k2", "v2")
        cache.put("k6", "v6")
        cache.put("k3", "v3")

    def btest_lfu_1(self):
        print("==============")
        l = DoubleLinkedList(lambda k, v: CacheNode(None, None, None))

        l.append(CacheNode(1, 1, None))
        n3 = l.append(CacheNode(3, 3, None))
        l.append(CacheNode(4, 4, None))
        l.appendleft(CacheNode(0, 0, None))
        l.appendbefore(CacheNode(2, 2, None), n3)

        self.assertEqual("F:(0, 0)->(1, 1)->(2, 2)->(3, 3)->(4, 4)->", l.print_f())
        self.assertEqual("B:(4, 4)->(3, 3)->(2, 2)->(1, 1)->(0, 0)->", l.print_b())

        l.unlink(n3)
        self.assertEqual("F:(0, 0)->(1, 1)->(2, 2)->(4, 4)->", l.print_f())
        self.assertEqual("B:(4, 4)->(2, 2)->(1, 1)->(0, 0)->", l.print_b())

        l.pop()
        l.popleft()

        self.assertEqual("F:(1, 1)->(2, 2)->", l.print_f())
        self.assertEqual("B:(2, 2)->(1, 1)->", l.print_b())
