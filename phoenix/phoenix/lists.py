# TODO: Need to generalize

class DoubleLinkedList:
    def __init__(self, node_builder):
        self.builder = node_builder
        self.__head = self.builder(None, None)
        self.__tail = self.builder(None, None)
        self.__head.next = self.__tail
        self.__tail.prev = self.__head
        self.size = 0

    def head(self):
        return self.__head.next

    def tail(self):
        return self.__tail.prev

    def append(self, n):
        ptail = self.__tail.prev

        ptail.next = n
        n.prev = ptail

        n.next = self.__tail
        self.__tail.prev = n

        self.size += 1
        return n

    def append_left(self, n):
        phead = self.__head.next

        self.__head.next = n
        n.prev = self.__head

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
        n = self.unlink(self.__tail.prev)
        return (n.key, n.val)

    def pop_left(self):
        n = self.unlink(self.__head.next)
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
        node = self.head()
        while node != self.__tail:
            buf += str(node) + "->"
            node = node.next
        print("F:", buf)
        return "F:" + buf

    def print_b(self):
        buf = ""
        node = self.tail()
        while node != self.__head:
            buf += str(node) + "->"
            node = node.prev
        print("B:", buf)
        return "B:" + buf
