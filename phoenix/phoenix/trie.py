class CompressedTrieNode:
    def __init__(self, is_end):
        self.children = {}
        self.edges = {}
        self.is_end = is_end


class CompressedTrie:
    def __init__(self):
        self.root = CompressedTrieNode(is_end=False)

    def insert(self, word):
        current_node = self.root
        i = 0

        while i < len(word):
            if word[i] not in current_node.edges:
                # Case 1: No path starting with that letter (New)
                current_node.edges[word[i]] = word[i:]
                current_node.children[word[i]] = CompressedTrieNode(is_end=True)

            else:
                idx = i
                j = 0
                label = current_node.edges[word[i]]
                while j < len(label) and i < len(word) and word[i] == label[j]:
                    j += 1
                    i += 1

                if j == len(label):
                    # Case 2: Label is a prefix of the word
                    # E.g. label=face, word=facebook
                    # face      facebook
                    #     ^     ^   ^
                    #     j   idx   i

                    current_node = current_node.children[word[idx]]
                    if i == len(word):
                        # Case 3: Label and word are the same
                        # face      face
                        #     ^     ^   ^
                        #     j   idx   i
                        current_node.is_end = True
                        return
                else:
                    if i == len(word):
                        # Case 4: word is a prefix of the label
                        # E.g. label=facebook, word=face
                        # State:
                        # facebook      face
                        #     ^         ^   ^
                        #     j       idx   i

                        existing = current_node.children[word[idx]]
                        new_child = CompressedTrieNode(is_end=True)

                        # Adding "face" edge to current node
                        current_node.edges[word[idx]] = word[idx:]
                        current_node.children[word[idx]] = new_child

                        # Adding "book" to new child
                        new_child.edges[label[j]] = label[j:]
                        new_child.children[label[j]] = existing
                    else:
                        # Case 5: word is partial match of label.
                        # E.g. label=facebook, word=facing
                        # State:
                        # facebook      facing
                        #    ^          ^  ^
                        #    j        idx  i

                        existing = current_node.children[word[idx]]
                        new_child = CompressedTrieNode(is_end=False)

                        # Adding "fac" edge to current node
                        current_node.edges[word[idx]] = word[idx:i]
                        current_node.children[word[idx]] = new_child

                        # Adding "ebook" to new child
                        new_child.edges[label[j]] = label[j:]
                        new_child.children[label[j]] = existing

                        # Adding "ing" to new child
                        new_child.edges[word[i]] = word[i:]
                        new_child.children[word[i]] = CompressedTrieNode(is_end=True)
                    return

    def search(self, word):
        current_node = self.root
        i = 0

        while i < len(word):
            if word[i] not in current_node.edges:
                # Case 1: No path starting with that letter (New)
                return False

            else:
                idx = i
                j = 0
                label = current_node.edges[word[i]]
                while j < len(label) and i < len(word) and word[i] == label[j]:
                    j += 1
                    i += 1

                if j == len(label):
                    # Case 2: label is at least a prefix for the word
                    # E.g. label=face, word=facebook
                    # face      facebook
                    #     ^     ^   ^
                    #     j   idx   i
                    current_node = current_node.children[word[idx]]
                    if i == len(word):
                        # Case 3: label == word
                        # face      face
                        #     ^     ^   ^
                        #     j   idx   i
                        return current_node.is_end

                else:
                    return False

    def starts_with(self, word):
        ans = []

        def _all_under(node, path):
            nonlocal ans
            if node.is_end:
                ans.append(path)

            for key, next_node in node.children.items():
                _all_under(next_node, path + node.edges[key])

            return ans

        current_node = self.root
        i = 0

        while i < len(word):
            if word[i] not in current_node.edges:
                # Case 1: No path starting with that letter (New)
                return []

            else:
                idx = i
                j = 0
                label = current_node.edges[word[i]]
                while j < len(label) and i < len(word) and word[i] == label[j]:
                    j += 1
                    i += 1

                if j == len(label):
                    # Case 2: label is at least a prefix for the word
                    # E.g. label=face, word=facebook
                    # face      facebook
                    #     ^     ^   ^
                    #     j   idx   i
                    current_node = current_node.children[word[idx]]
                    if i == len(word):
                        # Case 3: label == word
                        # face      face
                        #     ^     ^   ^
                        #     j   idx   i
                        return _all_under(current_node, word)

                else:
                    if i == len(word):
                        # Case 4: word is a prefix of the label
                        # E.g. label=facebook, word=face
                        # State:
                        # facebook      face
                        #     ^         ^   ^
                        #     j       idx   i
                        return _all_under(
                            current_node.children[word[idx]], word + label[j:]
                        )
                    else:
                        # Case 5: word is partial match of label.
                        # E.g. label=facebook, word=facing
                        # State:
                        # facebook      facing
                        #    ^          ^  ^
                        #    j        idx  i

                        return []

    def print_trie(self):
        def _print_trie(node, level):
            for i, edge in node.edges.items():
                print(
                    "".join(2 * level * [" "]),
                    edge,
                    "({})".format(1 if node.children[i].is_end else 0),
                )
                _print_trie(node.children[i], level + 1)

        _print_trie(self.root, 0)


###############################################################


import unittest


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

        trie.print_trie()

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


if __name__ == "__main__":
    unittest.main()
