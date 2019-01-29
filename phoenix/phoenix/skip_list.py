from random import random


class SkipNode:
    def __init__(self, key, val, max_levels):
        self.key = key
        self.val = val
        self.next = [None] * max_levels


class SkipList:
    def __init__(self, max_levels, log_factor):
        self.max_levels = max_levels
        self.log_factor = log_factor
        self.level = 0
        self.head = SkipNode(0, "", self.max_levels)

    def random_level(self):
        level = 0
        while level < self.max_levels - 1 and random() < self.log_factor:
            level += 1
        return level

    def scan(self, start, end, include_end=False):
        current = self.head
        for i in range(self.level, -1, -1):
            while current.next[i] and current.next[i].key < start:
                current = current.next[i]

        current = current.next[0]

        ans = []
        while current and (current.key < end or (include_end and current.key == end)):
            ans.append(current.val)
            current = current.next[0]
        return ans

    def insert(self, key, val):
        current = self.head
        b_links = [self.head] * self.max_levels

        for i in range(self.level, -1, -1):
            while current.next[i] and current.next[i].key < key:
                current = current.next[i]
            b_links[i] = current

        current = current.next[0]

        if current and current.key == key:
            current.val = val
            return

        node = SkipNode(key, val, self.max_levels)
        nlevel = self.random_level()

        for i in range(nlevel + 1):
            node.next[i] = b_links[i].next[i]
            b_links[i].next[i] = node

        if nlevel > self.level:
            self.level = nlevel

    def display_list(self):
        head = self.head
        for lvl in range(self.level + 1):
            print("Level {}: ".format(lvl), end=" ")
            node = head.next[lvl]
            while node != None:
                print(node.key, end=" ")
                node = node.next[lvl]
            print("")
