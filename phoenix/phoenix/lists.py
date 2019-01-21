class SentinelDoublyList:
    """ 
    Doubly linked list that can manage any node as long as nodes have a @next 
    and a @prev member representing the next and the previous node respectively.

    The end of the list in both tail.next or head.prev is represented by a Sentinel
    Node, not by None. A helper static method is_sentinel(n) is provided
    """

    class SentinelNode(object):
        def __init__(self):
            self.next = None
            self.prev = None

    def __init__(self):
        self._head = SentinelDoublyList.SentinelNode()
        self._tail = SentinelDoublyList.SentinelNode()
        self._head.next = self._tail
        self._tail.prev = self._head
        self._size = 0

    @staticmethod
    def is_sentinel(n):
        return isinstance(n, SentinelDoublyList.SentinelNode)

    def head(self):
        return self._head.next

    def tail(self):
        return self._tail.prev

    def size(self):
        return self._size

    def is_empty(self):
        return self._size == 0

    def append(self, n):
        if SentinelDoublyList.is_sentinel(n):
            raise "TODO Create Exception"

        ptail = self._tail.prev

        ptail.next = n
        n.prev = ptail

        n.next = self._tail
        self._tail.prev = n

        self._size += 1
        return n

    def append_left(self, n):
        if SentinelDoublyList.is_sentinel(n):
            raise "TODO Create Exception"

        phead = self._head.next

        self._head.next = n
        n.prev = self._head

        n.next = phead
        phead.prev = n

        self._size += 1
        return n

    def append_before(self, n, that):
        if SentinelDoublyList.is_sentinel(n):
            raise "TODO Create Exception"

        prev = that.prev

        prev.next = n
        n.prev = prev

        n.next = that
        that.prev = n

        self._size += 1
        return n

    def pop(self):
        if self.is_empty():
            raise "TODO Create Exception"

        n = self.unlink(self._tail.prev)
        return (n.key, n.val)

    def pop_left(self):
        if self.is_empty():
            raise "TODO Create Exception"

        n = self.unlink(self._head.next)
        return (n.key, n.val)

    def unlink(self, n):
        if SentinelDoublyList.is_sentinel(n):
            raise "TODO Create Exception"

        n.prev.next = n.next
        n.next.prev = n.prev
        self._size -= 1
        return n
