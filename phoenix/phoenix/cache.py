from phoenix.lists import SentinelDoublyList


class LRUCache(object):
    class CacheNode(object):
        def __init__(self, key, val):
            self.key = key
            self.val = val
            self.next = None
            self.prev = None

    def __init__(self, capacity):
        self._cache_list = SentinelDoublyList()
        self._lookup = {}
        self.capacity = capacity

    @property
    def capacity(self):
        return self._capacity

    @capacity.setter
    def capacity(self, c):
        self._capacity = c

    def put(self, key, val):
        if key not in self._lookup:
            if self.capacity == self._cache_list.size():
                tail = self._cache_list.tail()
                self._cache_list.unlink(tail)
                del self._lookup[tail.key]

            n = LRUCache.CacheNode(key, val)
            self._cache_list.append_left(n)
            self._lookup[key] = n
        else:
            n = self._lookup[key]
            n.val = val
            self._cache_list.unlink(n)
            self._cache_list.append_left(n)

    def get(self, key):
        if key in self._lookup:
            n = self._lookup[key]
            self._cache_list.unlink(n)
            self._cache_list.append_left(n)
            return n.val

        return -1

    def print(self):
        print([k for k, v in self._lookup.items()])
        self.store.print()


class LFUCache:
    class CacheNode:
        def __init__(self, key, val, freq_node):
            self.key = key
            self.val = val
            self.freq_node = freq_node
            self.next = None
            self.prev = None

    class FrequencyNode:
        def __init__(self, f):
            self.f = f
            self.c_list = SentinelDoublyList()
            self.next = None
            self.prev = None

    def __init__(self, capacity):
        self._capacity = capacity
        self._cache_map = {}
        self._freq_list = SentinelDoublyList()

    def put(self, key, val):
        if self._capacity == 0:
            return

        if len(self._cache_map) == self._capacity and key not in self._cache_map:
            self._evict_lfu()

        self._update(key, val)

    def get(self, key):
        if key not in self._cache_map:
            return -1

        cnode = self._cache_map[key]
        self._update(key, cnode.val)
        return cnode.val

    def _evict_lfu(self):
        fnode = self._freq_list.head()

        k, v = fnode.c_list.pop_left()
        del self._cache_map[k]

        if self._freq_list.size() == 0:
            self._freq_list.unlink(fnode)

    def _update(self, key, val):
        if key in self._cache_map:
            # Update the cache value
            cnode = self._cache_map[key]
            cnode.val = val

            # We need the frequency node to unlink the cache node and move the latter
            # to the next frequence (f + 1)
            fnode = cnode.freq_node

            # Frequency gets bump by one and the "next" node might or might not be
            # the node for the new frequency
            new_f = fnode.f + 1
            fnext = self._freq_list.next(fnode)

            # Unlink the cache node from previous frequency
            fnode.c_list.unlink(cnode)
            if fnode.c_list.size() == 0:
                # If there are no more nodes left, remove the frequency node
                self._freq_list.unlink(fnode)

            if fnext and new_f == fnext.f:
                # The next frequency node is responsible for the new frequency so use it
                fnode = fnext
            else:
                # We need to create a new frequency node as new_f is not represented
                if fnext:
                    fnode = self._freq_list.append_before(
                        LFUCache.FrequencyNode(new_f), fnext
                    )
                else:
                    fnode = self._freq_list.append(LFUCache.FrequencyNode(new_f))

            # Add the cache node to its frequency node
            cnode.freq_node = fnode
            fnode.c_list.append(cnode)
        else:
            # If it is brand new key, its frequency must be 1.
            head = self._freq_list.head()
            if head and head.f == 1:
                # 1 is already on the frequency list (If so, it must be the head)
                fnode = self._freq_list.head()
            else:
                # Create the new frequency node
                fnode = LFUCache.FrequencyNode(1)
                self._freq_list.append_left(fnode)

            # Connect the cache and frequency nodes and add the key to cache
            cnode = LFUCache.CacheNode(key, val, fnode)
            fnode.c_list.append(cnode)
            self._cache_map[key] = cnode
