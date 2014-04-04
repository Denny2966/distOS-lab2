import heapq as hq

class FirstList():
    def __init__(self, val):
        self.first = val[0]
        self.second = val[1]
    def __lt__(self, other):
        if self.first > other.second:
            return True
        elif self.first == other.second and self.first > other.second:
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
x = hq.heappop(lst)
print x.first, x.second
x = hq.heappop(lst)
print x.first, x.second
x = hq.heappop(lst)
print x.first, x.second
x = hq.heappop(lst)
print x.first, x.second
x = hq.heappop(lst)
print x.first, x.second
x = hq.heappop(lst)
print x.first, x.second
