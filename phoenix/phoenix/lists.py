class SentinelDoublyList:
    """ 
    Doubly linked list that can manage any node as long as nodes have a @next 
    and a @prev member representing the next and the previous node respectively.

    The end of the list in both tail.__next or head.__prev is represented by a Sentinel
    Node, not by None. A helper static method _is_sentinel(n) is provided
    """

    class SentinelNode(object):
        def __init__(self):
            self.__next = None
            self.__prev = None

    def __init__(self):
        self._head = SentinelDoublyList.SentinelNode()
        self._tail = SentinelDoublyList.SentinelNode()
        self._head.__next = self._tail
        self._tail.__prev = self._head
        self._size = 0

    @staticmethod
    def _is_sentinel(n):
        return isinstance(n, SentinelDoublyList.SentinelNode)

    def head(self):
        return (
            self._head.__next
            if not SentinelDoublyList._is_sentinel(self._head.__next)
            else None
        )

    def tail(self):
        return (
            self._tail.__prev
            if not SentinelDoublyList._is_sentinel(self._tail.__prev)
            else None
        )

    def next(self, n):
        return n.__next if not SentinelDoublyList._is_sentinel(n.__next) else None

    def prev(self, n):
        return n.__prev if not SentinelDoublyList._is_sentinel(n.__prev) else None

    def size(self):
        return self._size

    def is_empty(self):
        return self._size == 0

    def append(self, n):
        if SentinelDoublyList._is_sentinel(n):
            raise "TODO Create Exception"

        ptail = self._tail.__prev

        ptail.__next = n
        n.__prev = ptail

        n.__next = self._tail
        self._tail.__prev = n

        self._size += 1
        return n

    def append_left(self, n):
        if SentinelDoublyList._is_sentinel(n):
            raise "TODO Create Exception"

        phead = self._head.__next

        self._head.__next = n
        n.__prev = self._head

        n.__next = phead
        phead.__prev = n

        self._size += 1
        return n

    def append_before(self, n, that):
        if SentinelDoublyList._is_sentinel(n):
            raise "TODO Create Exception"

        prev = that.__prev

        prev.__next = n
        n.__prev = prev

        n.__next = that
        that.__prev = n

        self._size += 1
        return n

    def pop(self):
        if self.is_empty():
            raise "TODO Create Exception"

        n = self.unlink(self._tail.__prev)
        return (n.key, n.val)

    def pop_left(self):
        if self.is_empty():
            raise "TODO Create Exception"

        n = self.unlink(self._head.__next)
        return (n.key, n.val)

    def unlink(self, n):
        if SentinelDoublyList._is_sentinel(n):
            raise "TODO Create Exception"

        n.__prev.__next = n.__next
        n.__next.__prev = n.__prev
        self._size -= 1
        return n
