class DoubleLinkedList:
    def __init__(self, node_builder):
        self.builder = node_builder
        self.head = self.builder(None, None)
        self.tail = self.builder(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.size = 0

    def true_head(self):
        return self.head.next

    def true_tail(self):
        return self.tail.prev

    def append(self, n):
        ptail = self.tail.prev

        ptail.next = n
        n.prev = ptail

        n.next = self.tail
        self.tail.prev = n

        self.size += 1
        return n

    def appendleft(self, n):
        phead = self.head.next

        self.head.next = n
        n.prev = self.head

        n.next = phead
        phead.prev = n

        self.size += 1
        return n

    def appendbefore(self, n, next):
        prev = next.prev

        prev.next = n
        n.prev = prev

        n.next = next
        next.prev = n

        self.size += 1
        return n

    def pop(self):
        n = self.unlink(self.tail.prev)
        return (n.key, n.val)

    def popleft(self):
        n = self.unlink(self.head.next)
        return (n.key, n.val)

    def unlink(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        self.size -= 1
        return node

    def print(self):
        self.print_f()
        self.print_b()

    def print_f(self):
        res = ""

        buf = ""
        node = self.true_head()
        while node != self.tail:
            buf += str(node) + "->"
            node = node.next
        print("F:", buf)
        return "F:" + buf

    def print_b(self):
        buf = ""
        node = self.true_tail()
        while node != self.head:
            buf += str(node) + "->"
            node = node.prev
        print("B:", buf)
        return "B:" + buf
