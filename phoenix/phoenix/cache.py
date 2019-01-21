from phoenix.lists import SentinelDoublyList, is_sentinel


class Node(object):
    def __init__(self, key, val):
        self.key = key
        self.val = val
        self.next = None
        self.prev = None

    def __str__(self):
        return "({},{})".format(self.key, self.val)


class SimpleLinkedList(object):
    def __init__(self, capacity):
        self.size = 0
        self.capacity = capacity
        self.head = None
        self.tail = None

    def add_as_head(self, n):
        self.size += 1
        self._add_as_head(n)

    def evict_last_if_full(self):
        if self.size == self.capacity:
            self.size -= 1
            return self._retreat_tail()

    def move_to_head(self, n):
        if n.key == self.head.key:
            return

        my_prev = n.prev
        my_next = n.next

        if my_prev:
            my_prev.next = my_next
        if my_next:
            my_next.prev = my_prev

        if n.key == self.tail.key:
            self._retreat_tail()

        self._add_as_head(n)

    def _retreat_tail(self):
        old_tail = None
        if self.tail:
            old_tail = self.tail
            new_tail = self.tail.prev
            old_tail.prev = None
            if new_tail:
                new_tail.next = None
            self.tail = new_tail
        return old_tail

    def _add_as_head(self, n):
        n.next = self.head
        n.prev = None

        if self.head:
            self.head.prev = n

        self.head = n

        if not self.tail:
            self.tail = self.head

    def print(self):
        buffer = ""
        node = self.head
        while node:
            buffer += str(node.key)
            buffer += " -> "
            node = node.next
        print(buffer)

        buffer = ""
        node = self.tail
        while node:
            buffer += str(node.key)
            buffer += " -> "
            node = node.prev
        print(buffer)


class LRUCache(object):
    def __init__(self, capacity):
        self.store = SimpleLinkedList(capacity)
        self.lookup = {}

    def put(self, key, val):
        if key not in self.lookup:
            n_removed = self.store.evict_last_if_full()
            if n_removed:
                del self.lookup[n_removed.key]

            n = Node(key, val)
            self.store.add_as_head(n)
            self.lookup[key] = n
        else:
            n = self.lookup[key]
            n.val = val
            self.store.move_to_head(n)

    def get(self, key):
        if key in self.lookup:
            n = self.lookup[key]
            self.store.move_to_head(n)
            return n.val

        return -1

    def print(self):
        print([k for k, v in self.lookup.items()])
        self.store.print()


class LFUCacheNode:
    def __init__(self, key, val, freq_node):
        self.key = key
        self.val = val
        self.freq_node = freq_node
        self.next = None
        self.prev = None

    def __str__(self):
        return "({}, {})".format(self.key, self.val)


class LFUFrequencyNode:
    def __init__(self, f):
        self.f = f
        self.c_list = SentinelDoublyList()
        self.next = None
        self.prev = None

    def __str__(self):
        return "({})".format(self.f)


class LFUCache:
    def __init__(self, cap):
        self.cap = cap
        self.cache_map = {}
        self.f_list = SentinelDoublyList()

    def put(self, key, val):
        if self.cap == 0:
            return

        if len(self.cache_map) == self.cap and key not in self.cache_map:
            self._evict_lfu()

        self._update(key, val)

    def get(self, key):
        if key not in self.cache_map:
            return -1

        cnode = self.cache_map[key]
        self._update(key, cnode.val)
        return cnode.val

    def _evict_lfu(self):
        fnode = self.f_list.head()

        k, v = fnode.c_list.pop_left()
        del self.cache_map[k]

        if self.f_list.size == 0:
            self.f_list.unlink(fnode)

    def _update(self, key, val):
        if key in self.cache_map:
            # Update the cache value
            cnode = self.cache_map[key]
            cnode.val = val

            # We need the frequency node to unlink the cache node and move the latter
            # to the next frequence (f + 1)
            fnode = cnode.freq_node

            # Frequency gets bump by one and the "next" node might or might not be
            # the node for the new frequency
            new_f = fnode.f + 1
            fnext = fnode.next

            # Unlink the cache node from previous frequency
            fnode.c_list.unlink(cnode)
            if fnode.c_list.size == 0:
                # If there are no more nodes left, remove the frequency node
                self.f_list.unlink(fnode)

            if not is_sentinel(fnext) and new_f == fnext.f:
                # The next frequency node is responsible for the new frequency so use it
                fnode = fnext
            else:
                # We need to create a new frequency node as new_f is not represented
                fnode = self.f_list.append_before(LFUFrequencyNode(new_f), fnext)

            # Add the cache node to its frequency node
            cnode.freq_node = fnode
            fnode.c_list.append(cnode)
        else:
            # If it is brand new key, its frequency must be 1.
            head = self.f_list.head()
            if not is_sentinel(head) and head.f == 1:
                # 1 is already on the frequency list (If so, it must be the head)
                fnode = self.f_list.head()
            else:
                # Create the new frequency node
                fnode = LFUFrequencyNode(1)
                self.f_list.append_left(fnode)

            # Connect the cache and frequency nodes and add the key to cache
            cnode = LFUCacheNode(key, val, fnode)
            fnode.c_list.append(cnode)
            self.cache_map[key] = cnode

    def print_cache(self):
        print([k for k, v in self.cache_map.items()])

        buf = ""
        node = self.f_list.head()
        while node != self.f_list.tail:
            print("Freq:", node.f)
            node.c_list.print_f()
            node = node.next
