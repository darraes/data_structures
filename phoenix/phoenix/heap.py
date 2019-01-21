class Heap(object):
    def __init__(self):
        self.heap_size = 0
        self.store = [-1]

    def max_heapify(self, i):
        if i > self.heap_size:
            return

        while i <= self.heap_size // 2:
            largest = i

            left = self._left(i)
            right = self._right(i)

            if left <= self.heap_size and self.store[left] > self.store[largest]:
                largest = left
            if right <= self.heap_size and self.store[right] > self.store[largest]:
                largest = right

            if largest != i:
                tmp = self.store[largest]
                self.store[largest] = self.store[i]
                self.store[i] = tmp

                i = largest
            else:
                break

    def build_max_heap(self, array):
        array = [-1] + array

        self.store = array
        self.heap_size = len(array) - 1

        i = self.heap_size // 2

        while i >= 1:
            self.max_heapify(i)
            i -= 1

    def max(self):
        if self.heap_size == 0:
            return None
        return self.store[1]

    def extract_max(self):
        if self.heap_size == 0:
            return None

        res = self.store[1]
        self.store[1] = self.store[self.heap_size]
        del self.store[-1]
        self.heap_size -= 1

        self.max_heapify(1)

        return res

    def max_insert(self, element):
        self.heap_size += 1
        self.store.append(element)

        i = self.heap_size
        parent_i = self._parent(i)
        while parent_i >= 1:
            if self.store[i] < self.store[parent_i]:
                break

            tmp = self.store[parent_i]
            self.store[parent_i] = self.store[i]
            self.store[i] = tmp
            i = parent_i
            parent_i = self._parent(i)

    def _left(self, i):
        return int(2 * i)

    def _right(self, i):
        return int(2 * i + 1)

    def _parent(self, i):
        return int(i / 2)

    def state(self):
        return (self.heap_size, self.store[1:])
