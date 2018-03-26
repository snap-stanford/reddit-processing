import time

from itertools import permutations
import ctypes
import multiprocessing as mp

from hashmap import HashMap
import dbm
import shmht
import snap


def test_dict_insert(d):
    for p in [''.join(p) for p in permutations('1234567')]:
        d[p.encode()] = p.encode()


def test_dict_lookup(d):
    for p in [''.join(p) for p in permutations('1234567')]:
        if p.encode() in d:
            pass


def performance_test():
    d = {}
    t = time.time()
    test_dict_insert(d)
    print("dict insert: %s" % (time.time() - t))

    t = time.time()
    test_dict_lookup(d)
    print("dict lookup: %s" % (time.time() - t))

    # d = HashMap(key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=1000000)
    # t = time.time()
    # test_dict(d)
    # print("Shared-Memory HashMap insert: %s" % (time.time() - t))
    #
    # t = time.time()
    # test_lookup(d)
    # print("Shared-Memory HashMap lookup: %s" % (time.time() - t))

    d = snap.TStrStrH()
    t = time.time()
    test_dict_insert(d)
    print("SNAP THash insert: %s" % (time.time() - t))

    t = time.time()
    test_dict_lookup(d)
    print("SNAP THash lookup: %s" % (time.time() - t))

    d = mp.Manager().dict()
    t = time.time()
    test_dict_insert(d)
    print("Manager().dict insert: %s" % (time.time() - t))

    t = time.time()
    test_dict_lookup(d)
    print("Manager().dict lookup: %s" % (time.time() - t))

    db = dbm.open('cache', 'c')
    t = time.time()
    test_dict_insert(db)
    print("dbm insert time: %s" % (time.time() - t))

    t = time.time()
    test_dict_lookup(db)
    print("dbm lookup time: %s" % (time.time() - t))

    db.close()


if __name__ == "__main__":
    performance_test()