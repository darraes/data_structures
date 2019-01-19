from random import randint
from bisect import bisect_left, bisect_right
from collections import namedtuple

Range = namedtuple("Range", ["start", "count"])


class Node(object):
    def __init__(self, start, data=None):
        self.start = start
        self.data = data

    def __str__(self):
        return "{} ({})".format(self.start, self.data)

    def __lt__(self, other):
        return self.start < other.start

    def __eq__(self, other):
        return self.start == other.start and self.data == other.data


class MoveRequest(object):
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
    RING_SIZE = 1* 1000 * 1000 * 1000  # 1 Billion

    def __init__(self, spreading_factor=1):
        self.spreading_factor = spreading_factor
        self.ring = []

    def add(self, data, generator=lambda: randint(0, HashRing.RING_SIZE - 1)):
        """
        Adds @self.spreading_factor new nodes to the ring. All new nodes will point
        to @data.

        :data: str         The data handing on the nodes being added. E.g. Shard name
        :generator: int()  The function to generate the new nodes ring locations
                           Must return a number in interval [0 - 1B)
        """
        moves = []
        new_nodes = []
        old_ring = [n for n in self.ring]

        for _ in range(self.spreading_factor):
            # TODO: The generator can create a node with the same hash.
            # If that happens, we should generate a new hash
            node = Node(start=generator(), data=data)
            new_nodes.append(node)

        # TODO: We can potentially just sort the new_nodes and merge the lists for a
        # better time complexity
        self.ring.extend(new_nodes)
        self.ring.sort()

        if len(old_ring) == 0:
            return moves

        # TODO: For each new node, find where that data is on @old_ring and create the
        # move
        for n in new_nodes:
            from_node = HashRing._find_partition(old_ring, n.start)
            start_node_idx = HashRing._find_partition_idx(self.ring, n.start) - 1
            end_node_idx = start_node_idx + 1

            if end_node_idx < len(self.ring):
                moves.append(
                    MoveRequest(
                        from_node=from_node,
                        to_node=self.ring[start_node_idx],
                        ranges=[
                            Range(
                                start=self.ring[start_node_idx].start,
                                count=self.ring[end_node_idx].start
                                - self.ring[start_node_idx].start,
                            )
                        ],
                    )
                )
            else:
                # We are looping on the ring therefore we need two ranges to be moved
                moves.append(
                    MoveRequest(
                        from_node=from_node,
                        to_node=self.ring[start_node_idx],
                        ranges=[
                            Range(
                                start=self.ring[start_node_idx].start,
                                count=HashRing.RING_SIZE
                                - self.ring[start_node_idx].start,
                            ),
                            Range(
                                start=0,
                                count=self.ring[0].start,
                            )
                        ],
                    )
                )

        return moves

    def find(self, partition_key):
        partition_hash = hash(partition_key) % HashRing.RING_SIZE
        return HashRing._find_partition(self.ring, partition_hash)

    @staticmethod
    def _find_partition(ring, partition_hash):
        node_idx = HashRing._find_partition_idx(ring, partition_hash)
        # If node_idx is 0, that means the current hash is smaller than the hash of the
        # node at index 0 therefore we need to grab the last node of the ring
        # (cicle to the back)
        return ring[node_idx - 1] if node_idx > 0 else ring[-1]

    @staticmethod
    def _find_partition_idx(ring, partition_hash):
        return bisect_right(ring, Node(partition_hash))

    def print_ring(self):
        print(" | ".join(["{} ({})".format(n.data, n.start) for n in self.ring]))


###############################################################
import unittest


class TestFunctions(unittest.TestCase):
    def test_insert_on_middle(self):
        vertexes = [200000000, 500000000, 800000000]
        idx = -1

        def generator():
            nonlocal vertexes, idx
            idx += 1
            return vertexes[idx]

        ring = HashRing(spreading_factor=3)
        ring.add("shard_0", generator=generator)
        ring.spreading_factor = 1

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

        moves = ring.add("shard_1", generator=lambda: 300000000)

        self.assertEqual(
            [
                MoveRequest(
                    Node(200000000, "shard_0"),
                    Node(300000000, "shard_1"),
                    [Range(300000000, 500000000 - 300000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

        moves = ring.add("shard_1", generator=lambda: 600000000)
        self.assertEqual(
            [
                MoveRequest(
                    Node(500000000, "shard_0"),
                    Node(600000000, "shard_1"),
                    [Range(600000000, 800000000 - 600000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

    def test_insert_new_last(self):
        ring = HashRing()
        ring.add("shard_0", generator=lambda: 200000000)

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

        # Test for when the ring has a single node
        moves = ring.add("shard_1", generator=lambda: 400000000)

        self.assertEqual(
            [
                MoveRequest(
                    Node(200000000, "shard_0"),
                    Node(400000000, "shard_1"),
                    [Range(400000000, 1000000000 - 400000000), Range(0, 200000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 900000000).data)

        # Test for when the ring has multiple node
        moves = ring.add("shard_2", generator=lambda: 600000000)
        self.assertEqual(
            [
                MoveRequest(
                    Node(400000000, "shard_1"),
                    Node(600000000, "shard_2"),
                    [Range(600000000, 1000000000 - 600000000), Range(0, 200000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 900000000).data)

    def test_insert_on_idx_zero(self):
        ring = HashRing()
        ring.add("shard_0", generator=lambda: 500000000)

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

        # Test for when the ring has a single node
        moves = ring.add("shard_1", generator=lambda: 200000000)
        self.assertEqual(
            [
                MoveRequest(
                    Node(500000000, "shard_0"),
                    Node(200000000, "shard_1"),
                    [Range(200000000, 500000000 - 200000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)

        # Test for when the ring has multiple nodes
        moves = ring.add("shard_2", generator=lambda: 100000000)
        self.assertEqual(
            [
                MoveRequest(
                    Node(500000000, "shard_0"),
                    Node(100000000, "shard_2"),
                    [Range(100000000, 200000000 - 100000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 0).data)
        self.assertEqual("shard_2", HashRing._find_partition(ring.ring, 100000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 200000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 300000000).data)
        self.assertEqual("shard_1", HashRing._find_partition(ring.ring, 400000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 500000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 600000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 700000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 800000000).data)
        self.assertEqual("shard_0", HashRing._find_partition(ring.ring, 900000000).data)


if __name__ == "__main__":
    unittest.main()
