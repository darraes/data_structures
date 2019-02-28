# TODO: proper test cases

from unittest import TestCase
from phoenix.hash_ring import HashRing, RingNode, ReshardUnit, PartitionRange


class TestFunctions(TestCase):
    def test_insert_on_middle(self):
        vertexes = [200000000, 500000000, 800000000]
        idx = -1

        def hash_generator():
            nonlocal vertexes, idx
            idx += 1
            return vertexes[idx]

        ring = HashRing(spreading_factor=3)
        ring.add("shard_0", hash_generator=hash_generator)
        ring.spreading_factor = 1

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

        moves = ring.add("shard_1", hash_generator=lambda: 300000000)

        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(200000000, "shard_0"),
                    RingNode(300000000, "shard_1"),
                    [PartitionRange(300000000, 500000000 - 300000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

        moves = ring.add("shard_1", hash_generator=lambda: 600000000)
        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(500000000, "shard_0"),
                    RingNode(600000000, "shard_1"),
                    [PartitionRange(600000000, 800000000 - 600000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

    def test_insert_new_last(self):
        ring = HashRing()
        ring.add("shard_0", hash_generator=lambda: 200000000)

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

        # Test for when the ring has a single RingNode
        moves = ring.add("shard_1", hash_generator=lambda: 400000000)

        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(200000000, "shard_0"),
                    RingNode(400000000, "shard_1"),
                    [
                        PartitionRange(400000000, 1000000000 - 400000000),
                        PartitionRange(0, 200000000),
                    ],
                )
            ],
            moves,
        )

        self.assertEqual("shard_1", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 900000000).data
        )

        # Test for when the ring has multiple RingNode
        moves = ring.add("shard_2", hash_generator=lambda: 600000000)
        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(400000000, "shard_1"),
                    RingNode(600000000, "shard_2"),
                    [
                        PartitionRange(600000000, 1000000000 - 600000000),
                        PartitionRange(0, 200000000),
                    ],
                )
            ],
            moves,
        )

        self.assertEqual("shard_2", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 900000000).data
        )

    def test_insert_on_idx_zero(self):
        ring = HashRing()
        ring.add("shard_0", hash_generator=lambda: 500000000)

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

        # Test for when the ring has a single RingNode
        moves = ring.add("shard_1", hash_generator=lambda: 200000000)
        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(500000000, "shard_0"),
                    RingNode(200000000, "shard_1"),
                    [PartitionRange(200000000, 500000000 - 200000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )

        # Test for when the ring has multiple RingNodes
        moves = ring.add("shard_2", hash_generator=lambda: 100000000)
        self.assertEqual(
            [
                ReshardUnit(
                    RingNode(500000000, "shard_0"),
                    RingNode(100000000, "shard_2"),
                    [PartitionRange(100000000, 200000000 - 100000000)],
                )
            ],
            moves,
        )

        self.assertEqual("shard_0", HashRing._find_partition(ring._ring, 0).data)
        self.assertEqual(
            "shard_2", HashRing._find_partition(ring._ring, 100000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 200000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 300000000).data
        )
        self.assertEqual(
            "shard_1", HashRing._find_partition(ring._ring, 400000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 500000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 600000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 700000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 800000000).data
        )
        self.assertEqual(
            "shard_0", HashRing._find_partition(ring._ring, 900000000).data
        )
