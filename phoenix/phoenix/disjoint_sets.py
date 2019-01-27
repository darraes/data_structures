class DisjointSets:
    def __init__(self, n):
        self._counts = [-1] * n
        self._parent = [-1] * n
        self._rank = [1] * n
        for i in range(len(self._parent)):
            self._parent[i] = i
            self._counts[i] = 1

    def parent(self, n):
        if self._parent[n] == n:
            return n

        # Path compression technique
        self._parent[n] = self._parent(self._parent[n])
        return self._parent[n]

    def union(self, n1, n2):
        p1 = self._parent(n1)
        p2 = self._parent(n2)

        if p1 == p2:
            return

        # Applying union by rank technique
        r1 = self._rank[p1]
        r2 = self._rank[p2]

        if r1 > r2:
            self._parent[p2] = p1
            self._counts[p1] += self._counts[p2]
        elif r2 > r1:
            self._parent[p1] = p2
            self._counts[p2] += self._counts[p1]
        else:
            self._parent[p2] = p1
            self._rank[p1] += 1
            self._counts[p1] += self._counts[p2]

    def size(self, v):
        return self._counts[self._parent(v)]