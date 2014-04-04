import heapq as hq

class FirstList(list):
    def __lt__(self, other):
        if self[0] > other[0]:
            return True
        elif self[0] == other[0] and self[1] > other[1]:
            return True
        else:
            return False

lst = [ (2, 2), (2, 1), (1, 1) ]
lst = [ FirstList(x) for x in lst]
print lst

hq.heapify(lst)
print lst
hq.heappush(lst, FirstList((1,0)))
print lst
hq.heappush(lst, FirstList((0,0)))
print lst
hq.heappush(lst, FirstList((2,0)))
print lst
print hq.heappop(lst)
print lst
print hq.heappop(lst)
print lst
print hq.heappop(lst)
print lst
print hq.heappop(lst)
print lst
print hq.heappop(lst)
print lst
print hq.heappop(lst)
print lst
