"""
File: p

"""

import time
import sys
from itertools import permutations
import ctypes
import multiprocessing as mp

from hashmap import HashTable
if sys.version_info < (3, 0):
    import gdbm
else:
    import dbm
import shmht
import snap


perm_set = '1234567'

def test_dict_insert(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        d[p.encode()] = p.encode()


def test_dict_lookup(d):
    for p in [''.join(p) for p in permutations(perm_set)]:
        if p.encode() in d:
            pass


def test_it(d, name, norm_insert, norm_lookup):
    t = time.time()
    test_dict_insert(d)
    t_insert = time.time() - t
    print("%s insert: %s sec. (x %s)" % (name, t_insert, t_insert / norm_insert))

    t = time.time()
    test_dict_lookup(d)
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
    if sys.version_info < (3, 0):
        d = HashTable("table", capacity=1000000)
    else:
        d = HashTable(key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=1000000)
    test_it(d, "Shared-Memory HashTable", norm_insert, norm_lookup)

    test_it(snap.TStrStrH(), "SNAP THash", norm_insert, norm_lookup)
    test_it(mp.Manager().dict(), "mp.Manager.dict", norm_insert, norm_lookup)

    db = gdbm.open('/lfs/madmax3/0/jdeaton/dbm_cache/cache', 'c')
    test_it(db, "dbm", norm_insert, norm_lookup)
    db.close()


if __name__ == "__main__":
    performance_test()