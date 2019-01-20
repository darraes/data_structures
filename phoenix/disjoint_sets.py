class DisjointSets:
    def __init__(self, n):
        self.counts = [-1] * n
        self.parent = [-1] * n
        self.rank = [1] * n
        for i in range(len(self.parent)):
            self.parent[i] = i
            self.counts[i] = 1

    def representative(self, node):
        if self.parent[node] == node:
            return node

        self.parent[node] = self.representative(self.parent[node])
        return self.parent[node]

    def union(self, v1, v2):
        p1 = self.representative(v1)
        p2 = self.representative(v2)

        if p1 == p2:
            return

        r1 = self.rank[p1]
        r2 = self.rank[p2]

        if r1 > r2:
            self.parent[p2] = p1
            self.counts[p1] += self.counts[p2]
        elif r2 > r1:
            self.parent[p1] = p2
            self.counts[p2] += self.counts[p1]
        else:
            self.parent[p2] = p1
            self.rank[p1] += 1
            self.counts[p1] += self.counts[p2]

    def size(self, v):
        return self.counts[self.representative(v)]