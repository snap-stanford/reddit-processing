
# class Container:
    # def __init__(self):
    #     self.arr = list(range(10))

    # def _next_index(self, i):
    #     return (i + 1) % len(self.arr)

    # def __iter__(self):
    #     self.start = 3
    #     self.i = self.start

    #     while True:
    #         yield self.arr[self.i]
    #         self.i = self._next_index(self.i)
    #         if self.i == self.start:
    #             break

import multiprocessing as mp
import ctypes

def main():
    
    arr = mp.Array(ctypes.c_int, 1000000)
    arr[0] = 1 
    arr[1] = 4

    for i in range(2, len(arr)):
        arr[i] = 2 * arr[i - 1] + arr[i - 2]

if __name__ == "__main__":
    main()
