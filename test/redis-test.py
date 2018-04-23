#!/usr/bin/env python
"""
File: redis-test
Date: 3/31/18 
Author: Jon Deaton (jdeaton@stanford.edu)
"""

import unittest
import redis
import multiprocessing as mp
import string
from reddit import *

def add_kv(kv):
    add_redis(*kv)


def add_redis(key, value):
    redis_db = redis.StrictRedis(connection_pool=redis_pool)
    redis_db.set(key, value)


class TestResis(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestResis, self).__init__(*args, **kwargs)
        global redis_pool
        redis_pool = redis.ConnectionPool(host="localhost", port=6379, db=0)

    def basic_test(self):
        redis_db = redis.StrictRedis(connection_pool=redis_pool)
        redis_db.keys()
        redis_db.set('full stack', 'python')
        redis_db.set('backend', 'scala')
        redis_db.set('frontend', "javascript")

        redis_db.mset({'you': 'wanna', 'be': 'high', 'for': 'it'})
        values = redis_db.mget(['full stack', 'backend', 'mobile', 'you'])
        self.assertEqual(values, ['python', 'scala', None, 'wanna'])

    def test_fork(self):
        ps = [mp.Process(target=add_redis, args=(l, l)) for l in string.ascii_lowercase]
        for p in ps: p.start()
        for p in ps: p.join()

        # check to make sure that they all got inserted
        redis_db = redis.StrictRedis(connection_pool=redis_pool)
        for l in string.ascii_lowercase:
            self.assertEqual(redis_db.get(l), l.encode())

    def test_pool_map(self):
        pool = mp.Pool(10)
        pool.map(add_kv, [(l, l) for l in string.ascii_lowercase])

        # check to make sure that they all got inserted
        redis_db = redis.StrictRedis(connection_pool=redis_pool)
        for l in string.ascii_lowercase:
            self.assertEqual(redis_db.get(l), l.encode())


if __name__ == "__main__":
    unittest.main()
