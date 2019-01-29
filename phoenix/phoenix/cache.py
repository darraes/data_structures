from phoenix.lists import SentinelDoublyList


class LRUCache(object):
    """
    Straight forward O(1) get/put Last Recently Used cache.
    This class is **NOT** Thread Safe
    """

    class CacheNode(object):
        """
        Wrapper object that will hold both the user's key and value
        """
        def __init__(self, key, val):
            self.key = key
            self.val = val

    def __init__(self, capacity):
        # Uses SentinelDoublyList which is simply a doubly linked list that allows O(1)
        # add / update / remove of any node.
        # Honors the eviction of the last recently used which will always live in its
        # tail
        self._cache_list = SentinelDoublyList()

        # Hashtable for quick lookup of nodes
        self._lookup = {}

        # Size of the cache. For now, no expanding or shrinking is allowed
        self._capacity = capacity

    @property
    def capacity(self):
        """
        Getter
        """
        return self._capacity

    def put(self, key, val):
        """
        Puts the @key/@value in the cache. It will evict the oldest entry if the cache
        is at capacity.
        """
        if key not in self._lookup:
            if self._capacity == self._cache_list.size():
                tail = self._cache_list.tail()

                # Remove the tail (oldest) from the list and it's key from the lookup
                self._cache_list.unlink(tail)
                del self._lookup[tail.key]

            n = LRUCache.CacheNode(key, val)
            self._cache_list.append_left(n)
            self._lookup[key] = n
        else:
            n = self._lookup[key]
            n.val = val

            # Rotate node to the head of the list (newest)
            self._cache_list.unlink(n)
            self._cache_list.append_left(n)

    def get(self, key):
        """
        Gets the value associated with @key and bump that key to newest.
        If key is not present, returns |None|
        """
        if key in self._lookup:
            n = self._lookup[key]

            # Rotate node to the head of the list (newest)
            self._cache_list.unlink(n)
            self._cache_list.append_left(n)
            return n.val

        return None


class LFUCache:
    """
    Straight forward O(1) get/put Last Frequently Used cache.
    This class is **NOT** Thread Safe
    """

    class CacheNode:
        def __init__(self, key, val, freq_node):
            self.key = key
            self.val = val
            self.freq_node = freq_node

    class FrequencyNode:
        def __init__(self, f):
            self.f = f
            self.c_list = SentinelDoublyList()

    def __init__(self, capacity):
        self._capacity = capacity
        self._cache_map = {}
        self._freq_list = SentinelDoublyList()

    def put(self, key, val):
        if self._capacity == 0:
            return

        if len(self._cache_map) == self._capacity and key not in self._cache_map:
            self._evict()

        self._update(key, val)

    def get(self, key):
        if key not in self._cache_map:
            return None

        cnode = self._cache_map[key]
        self._update(key, cnode.val)
        return cnode.val

    def _evict(self):
        fnode = self._freq_list.head()

        cache_node = fnode.c_list.pop_left()
        del self._cache_map[cache_node.key]

        if self._freq_list.size() == 0:
            self._freq_list.pop_left()

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
                fnode = self._freq_list.append_left(LFUCache.FrequencyNode(1))

            # Connect the cache and frequency nodes and add the key to cache
            cnode = LFUCache.CacheNode(key, val, fnode)
            fnode.c_list.append(cnode)
            self._cache_map[key] = cnode
