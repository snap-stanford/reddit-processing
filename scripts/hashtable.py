"""
File: hashtable.py

A shared memory hash map build on top of multiprocessing.Array

This hash table allows for multiple processes to have a hash table in shared memory
without relying on multiprocessing.Manager.dict
"""

import multiprocessing as mp
import ctypes
import contextlib
import sys

__author__ = "Jon Deaton"


class HashTable(object):
    def __init__(self, key_value_pairs=None,
                 key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=1000, lock=False):
        class KeyValue(ctypes.Structure):
            _fields_ = [
                ('key', key_type),
                ('value', value_type),
                ('origin', ctypes.c_int)
            ]

            # def __init__(self):
            #     super(KeyValue, self).__init__()
            #     self.origin = -1

        initializer = KeyValue()
        initializer.key = key_type()
        initializer.value = value_type()
        initializer.origin = -1

        self.KV_type = KeyValue
        self.key_type = key_type
        self.value_type = value_type
        self.capacity = capacity if not key_value_pairs or capacity >= len(key_value_pairs) else 2 * capacity
        self.lock = lock if lock else contextlib.suppress()
        self.arr = mp.Array(self.KV_type, [initializer] * self.capacity)
        self.size = mp.Value('i', 0)

        self.clear()
        self.insert(key_value_pairs)

    def insert(self, key_value_pairs):
        if key_value_pairs is None:
            return
        for key, value in key_value_pairs:
            self[key] = value

    def update(self, d):
        self.insert(d.items())

    def items(self):
        for key in self:
            yield key, self.arr[key]

    def clear(self):
        with self.lock:
            for i in range(self.capacity):
                self.arr[i].origin = -1
            self.size.value = 0

    def __iter__(self):
        seen = 0
        og_size = self.size.value
        for i in range(self.capacity):
            if self.arr[i].origin >= 0:
                next = self.arr[i].key
                yield next
                seen += 1

                # In case this element gets deleted after yielding
                while self.arr[i].origin >= 0 and next != self.arr[i].key:
                    next = self.arr[i].key
                    yield self.arr[i].key
                    seen += 1

                if seen == og_size:
                    break

    def __len__(self):
        return self.size.value

    def __contains__(self, key):
        i = self._get_bucket(key)
        return i >= 0

    def __getitem__(self, key):
        i = self._get_bucket(key)
        if i >= 0:
            return self.arr[i].value
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        with self.lock:
            start = hash(key) % self.capacity
            i = start
            while self.arr[i].origin >= 0:
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
            self.size.value += 1

    def __delitem__(self, key):
        with self.lock:
            i = self._get_bucket(key)
            if i < 0:
                raise KeyError(key)
            self._replace(i)

    def _next_index(self, i):
        return (i + 1) % self.capacity

    def _get_bucket(self, key):
        start = hash(key) % self.capacity
        i = start
        while self.arr[i].origin >= 0:
            if self.arr[i].key == key:
                return i
            else:
                i = self._next_index(i)
                if i == start:
                    return -1
        return -1

    def _replace(self, i, stop=None):
        j = i
        last = i
        while self.arr[j].origin >= 0:
            if self.arr[j].origin <= self.arr[i].origin:  # found a replacement
                last = j
            j = self._next_index(j)
            if j == i or j == stop:
                break
        if last != i and last != stop:
            self.arr[i] = self.arr[last]
            self._replace(last, stop=i)
        else:
            self._delete(i)
            self.size.value -= 1

    def _move(self, source, sink):
        self.arr[source] = self.arr[sink]
        self._delete(source)

    def _delete(self, i):
        self.arr[i].origin = -1
