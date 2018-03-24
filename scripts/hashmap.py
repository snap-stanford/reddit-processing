"""
File: hashmap.py

A shared memory hash map build on top of multiprocessing.Array

This hash table allows for multiple processes to have a hash table in shared memory
without relying on multiprocessing.Manager.dict
"""

import multiprocessing as mp
import ctypes
import contextlib

class HashMap:
    def __init__(self, key_value_pairs=None, key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=100000, lock=False):
        class KeyValue(ctypes.Structure):
            _fields_ = [
                ('key', key_type),
                ('value', value_type),
                ('origin', ctypes.c_int)
            ]

        self.KV_type = KeyValue
        self.key_type = key_type
        self.value_type = value_type
        self.capacity = capacity if not key_value_pairs or capacity >= len(key_value_pairs) else 2 * capacity
        self.lock = lock
        self.arr = mp.Array(self.KV_type, self.capacity, lock=lock)
        self.size = 0
        self.insert(key_value_pairs)

    def insert(self, key_value_pairs):
        if not key_value_pairs: return
        for key, value in key_value_pairs:
            self[key] = value

    def update(self, d):
        if not d:
            return
        self.insert(d.items())

    def items(self):
        for key in self:
            yield key, self.arr[key]

    def __iter__(self):
        for i in range(self.capacity):
            if self.arr[i].key:
                yield self.arr[i].key

    def __len__(self):
        return self.size

    def __contains__(self, key):
        i = self._get_bucket(key)
        return i is not None

    def __getitem__(self, key):
        i = self._get_bucket(key)
        if i is not None:
            return self.arr[i].value
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        with self.lock if self.lock else contextlib.suppress():
            start = hash(key) % self.capacity
            i = start
            while self.arr[i].key:
                if self.arr[i].key == key:
                    self.arr[i].value = value
                    return
                else:
                    i = self._next_index(i)
                    if i == start:  # came back around to the beginning
                        raise MemoryError

            self.arr[i].key = key
            self.arr[i].value = value
            self.arr[i].origin = start
            self.size += 1

    def __delitem__(self, key):
        with self.lock if self.lock else contextlib.suppress():
            i = self._get_bucket(key)
            if i is None:
                raise KeyError(key)
            self._replace(i)

    def _next_index(self, i):
        return (i + 1) % self.capacity

    def _get_bucket(self, key):
        start = hash(key) % self.capacity
        i = start
        while self.arr[i].key:
            if self.arr[i].key == key:
                return i
            else:
                i = self._next_index(i)
                if i == start:
                    return None
        return None

    def _replace(self, i, stop=None):
        j = i
        last = i
        while self.arr[j].key:
            if self.arr[j].origin <= self.arr[i].origin:  # found a replacement
                last = j
            j = self._next_index(j)
            if j == i or j == stop:
                break
        if last != i and last != stop:
            self.arr[i] = self.arr[last]
            self._replace(last)
        else:
            self._delete(i)

    def _move(self, source, sink):
        self.arr[source] = self.arr[sink]
        self._delete(source)

    def _delete(self, i):
        self.arr[i].key = None
        self.arr[i].value = None
