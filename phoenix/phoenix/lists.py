class SentinelNode(object):
    def __init__(self):
        self.next = None
        self.prev = None


def is_sentinel(n):
    return isinstance(n, SentinelNode)


def is_valid(n):
    return not is_sentinel(n)


class SentinelDoublyList:
    """ Doubly linked list that can manage any node as long as nodes have a @next 
        and a @prev member representing the next and the previous node respectively.

        The end of the list in both tail.next or head.prev is represented by a Sentinel
        Node, not by None.
    """

    def __init__(self):
        self._head = SentinelNode()
        self._tail = SentinelNode()
        self._head.next = self._tail
        self._tail.prev = self._head
        self._size = 0

    def head(self):
        return self._head.next

    def tail(self):
        return self._tail.prev

    def size(self):
        return self._size

    def append(self, n):
        ptail = self._tail.prev

        ptail.next = n
        n.prev = ptail

        n.next = self._tail
        self._tail.prev = n

        self._size += 1
        return n

    def append_left(self, n):
        phead = self._head.next

        self._head.next = n
        n.prev = self._head

        n.next = phead
        phead.prev = n

        self._size += 1
        return n

    def append_before(self, n, next):
        prev = next.prev

        prev.next = n
        n.prev = prev

        n.next = next
        next.prev = n

        self._size += 1
        return n

    def pop(self):
        n = self.unlink(self._tail.prev)
        return (n.key, n.val)

    def pop_left(self):
        n = self.unlink(self._head.next)
        return (n.key, n.val)

    def unlink(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        self._size -= 1
        return node

    def print(self):
        self.print_f()
        self.print_b()

    def print_f(self):
        res = ""

        buf = ""
        node = self.head()
        while node != self._tail:
            buf += str(node) + "->"
            node = node.next
        print("F:", buf)
        return "F:" + buf

    def print_b(self):
        buf = ""
        node = self.tail()
        while node != self._head:
            buf += str(node) + "->"
            node = node.prev
        print("B:", buf)
        return "B:" + buf
