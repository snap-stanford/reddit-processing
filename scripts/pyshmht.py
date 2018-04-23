#!/usr/bin/env python
"""
File: pyshmht
"""

import shmht
import marshal

class HashTable(object):
    def __init__(self, name, capacity=0, force_init=False, serializer=marshal):
        force_init = 1 if force_init else 0
        self.fd = shmht.open(name, capacity, force_init)
        self.loads = serializer.loads
        self.dumps = serializer.dumps

    def close(self):
        shmht.close(self.fd)

    def get(self, key, default=None):
        val = shmht.getval(self.fd, key)
        if val == None:
            return default
        return val

    def set(self, key, value):
        return shmht.setval(self.fd, key, value)

    def remove(self, key):
        return shmht.remove(self.fd, key)

    def foreach(self, callback, unserialize=False):
        if not unserialize:
            cb = callback
        else:
            loads = self.loads

            def mcb(key, value):
                return callback(key, loads(value))

            cb = mcb
        return shmht.foreach(self.fd, cb)

    def getobj(self, key, default=None):
        val = self.get(key, default)
        if val == default:
            return default
        return self.loads(val)

    def setobj(self, key, val):
        val = self.dumps(val)
        return self.set(key, val)

    def __getitem__(self, key):
        val = shmht.getval(self.fd, key)
        if val == None:
            raise KeyError(key)
        return val

    def __setitem__(self, key, value):
        return shmht.setval(self.fd, key, value)

    def __delitem__(self, key):
        if False == shmht.remove(self.fd, key):
            raise KeyError(key)

    def __contains__(self, key):
        return shmht.getval(self.fd, key) != None

    def to_dict(self, unserialize=False):
        d = {}

        def insert(k, v):
            d[k] = v

        self.foreach(insert, unserialize)
        return d

    def update(self, d, serialize=False):
        dumps = self.dumps
        if serialize:
            for k in d:
                self[k] = dumps(d[k])
        else:
            for k in d:
                self[k] = d[k]
