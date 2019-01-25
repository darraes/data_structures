from random import randint
from bisect import bisect_right
from collections import namedtuple
from phoenix.utils import merge_sorted

Range = namedtuple("Range", ["start", "count"])


class Node(object):
    def __init__(self, start, data=None):
        self._start = start
        self._data = data

    @property
    def start(self):
        return self._start

    @property
    def data(self):
        return self._data

    def __str__(self):
        return "{} ({})".format(self.start, self.data)

    def __lt__(self, other):
        return self.start < other.start

    def __eq__(self, other):
        return self.start == other.start and self.data == other.data


class ReshardUnit(object):
    def __init__(self, from_node, to_node, ranges):
        self.from_node = from_node
        self.to_node = to_node
        self.ranges = ranges

    def __str__(self):
        return "{} -> {} [{}]".format(
            self.from_node.data,
            self.to_node.data,
            ", ".join([str(r) for r in self.ranges]),
        )

    def __eq__(self, other):
        return (
            self.from_node == other.from_node
            and self.to_node == other.to_node
            and self.ranges == other.ranges
        )


class HashRing(object):
    RING_SIZE = 1 * 1000 * 1000 * 1000  # 1 Billion

    def __init__(self, spreading_factor=1):
        self.spreading_factor = spreading_factor
        self._ring = []

    def add(self, data, hash_generator=lambda: randint(0, HashRing.RING_SIZE - 1)):
        """
        Adds @self.spreading_factor new nodes to the ring. All new nodes will point
        to @data.

        :data: str              The data hanging on the nodes being added.
        :hash_generator: int()  The function to generate the new nodes ring locations
                                Must return a number in interval [0 - 1B)
        """
        moves = []
        new_nodes = []
        old_ring = [n for n in self._ring]

        for _ in range(self.spreading_factor):
            node = Node(
                start=self._create_node_hash(
                    hash_generator, set([n.start for n in old_ring])
                ),
                data=data,
            )
            new_nodes.append(node)

        new_nodes.sort()
        self._ring = merge_sorted(self._ring, new_nodes)

        if len(old_ring) == 0:
            return moves

        for n in new_nodes:
            from_node = HashRing._find_partition(old_ring, n.start)
            start_node_idx = HashRing._find_partition_idx(self._ring, n.start)
            end_node_idx = start_node_idx + 1

            if end_node_idx < len(self._ring):
                moves.append(
                    ReshardUnit(
                        from_node=from_node,
                        to_node=self._ring[start_node_idx],
                        ranges=[
                            Range(
                                start=self._ring[start_node_idx].start,
                                count=self._ring[end_node_idx].start
                                - self._ring[start_node_idx].start,
                            )
                        ],
                    )
                )
            else:
                # The new node is the last of the list therefore it has the loop where
                # we need the ranges [start, Range.End] and [0, next_node.start)
                moves.append(
                    ReshardUnit(
                        from_node=from_node,
                        to_node=self._ring[start_node_idx],
                        ranges=[
                            Range(
                                start=self._ring[start_node_idx].start,
                                count=HashRing.RING_SIZE
                                - self._ring[start_node_idx].start,
                            ),
                            Range(start=0, count=self._ring[0].start),
                        ],
                    )
                )

        return moves

    def find(self, partition_key):
        partition_hash = hash(partition_key) % HashRing.RING_SIZE
        return HashRing._find_partition(self._ring, partition_hash).data

    @staticmethod
    def _create_node_hash(hash_generator, used_hashes):
        """
        We need to guarantee that two nodes don't have the exact same hash therefore we
        loop until we create a unique new hash point
        """
        while True:
            h = hash_generator()
            if h not in used_hashes:
                return h

    @staticmethod
    def _find_partition(ring, partition_hash):
        return ring[HashRing._find_partition_idx(ring, partition_hash)]

    @staticmethod
    def _find_partition_idx(ring, partition_hash):
        idx = bisect_right(ring, Node(partition_hash))
        # If idx is 0, that means the current hash is smaller than the hash of the
        # node at index 0 therefore we need to grab the last node of the ring
        # (cicle to the back)
        return idx - 1 if idx > 0 else len(ring) - 1

    def print_ring(self):
        print(" | ".join(["{} ({})".format(n.data, n.start) for n in self._ring]))
