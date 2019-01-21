# TODO: proper test cases

import unittest
from phoenix.trie import CompressedTrie


class TestFunctions(unittest.TestCase):
    def test_1(self):
        trie = CompressedTrie()

        trie.insert("facebook")
        trie.insert("face")
        trie.insert("this")
        trie.insert("there")
        trie.insert("then")
        trie.insert("the")
        trie.insert("facing")
        trie.insert("factory")

        # trie.print_trie()

        self.assertTrue(trie.search("there"))
        self.assertTrue(trie.search("face"))
        self.assertTrue(trie.search("facebook"))
        self.assertTrue(trie.search("facing"))
        self.assertTrue(trie.search("factory"))
        self.assertFalse(trie.search("they"))
        self.assertFalse(trie.search("fac"))
        self.assertFalse(trie.search("faceb"))
        self.assertFalse(trie.search("therein"))
        self.assertEqual(["facing"], trie.starts_with("faci"))
        self.assertEqual(["this", "the", "there", "then"], trie.starts_with("th"))
        self.assertEqual([], trie.starts_with("fab"))
        self.assertEqual(["face", "facebook"], trie.starts_with("face"))
        self.assertEqual(
            ["face", "facebook", "facing", "factory"], trie.starts_with("fac")
        )