def merge_sorted(c1, c2):
    res = [None] * (len(c1) + len(c2))

    r = i = j = 0
    while i < len(c1) or j < len(c2):
        if i < len(c1) and j < len(c2):
            if c1[i] < c2[j]:
                res[r] = c1[i]
                i += 1
            else:
                res[r] = c2[j]
                j += 1
        elif i < len(c1):
            res[r] = c1[i]
            i += 1
        else:
            res[r] = c2[j]
            j += 1

        r += 1
    return res