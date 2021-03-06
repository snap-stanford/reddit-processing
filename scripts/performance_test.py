"""
File: performance_test.py

This file is for comparing different implementations of key-value stores
that might be used to solve my problem shown here:

stackoverflow.com/questions/49438954/python-shared-memory-dictionary-for-mapping-big-data

"""

import time
import sys
from itertools import permutations
import ctypes
import multiprocessing as mp

python2 = sys.version_info < (3, 0)

from hashtable import HashTable
if python2:
    import gdbm as dbm
else:
    import dbm

try:
    import snap
    has_snap = True
except:
    has_snap = False

try:
    import redis
    has_redis = True
except:
    has_redis = False

perm_set = '1234567'


def test_dict_insert(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        d[p.encode()] = p.encode()


def test_dict_lookup(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        if p.encode() in d:
            pass


def test_redis_insert(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        d[p.encode()] = p.encode()


def test_redis_lookup(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        if p.encode() in d:
            pass

def test_it(d, name, norm_insert, norm_lookup, insert=test_dict_insert, lookup=test_dict_lookup):
    t = time.time()
    insert(d)
    t_insert = time.time() - t
    print("%s insert: %s sec. (x %s)" % (name, t_insert, t_insert / norm_insert))

    t = time.time()
    lookup(d)
    t_lookup = time.time() - t
    print("%s lookup: %s sec. (x %s)" % (name, t_lookup, t_lookup / norm_lookup))


def performance_test():

    # Standard Python dictionary
    d = {}
    t = time.time()
    test_dict_insert(d)
    norm_insert = time.time() - t
    print("dict insert: %s sec." % norm_insert)

    t = time.time()
    test_dict_lookup(d)
    norm_lookup = time.time() - t
    print("dict lookup: %s sec." % norm_lookup)

    # Shared memory hash table
    if python2:
        d = HashTable("table", capacity=1000000)
    else:
        d = HashTable(key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=1000000)
    test_it(d, "Shared-Memory HashTable", norm_insert, norm_lookup)


    if has_snap:
        test_it(snap.TStrStrH(), "SNAP THash", norm_insert, norm_lookup)
        test_it(mp.Manager().dict(), "mp.Manager.dict", norm_insert, norm_lookup)

    if has_redis:
        redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
        test_it(redis_db, "Redis", test_redis_insert, norm_insert, norm_lookup,
                insert=test_redis_insert, lookup=test_redis_lookup)


    db = dbm.open('/lfs/madmax3/0/jdeaton/dbm_cache/cache', 'n')
    test_it(db, "dbm", norm_insert, norm_lookup)
    db.close()


if __name__ == "__main__":
    performance_test()