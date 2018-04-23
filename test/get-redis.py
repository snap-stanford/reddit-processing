#!/usr/bin/env python
"""
File: get-redis.py
Date: 4/1/18 
Author: Jon Deaton (jdeaton@stanford.edu)
"""

import redis
from reddit import *

exec(open("C:\\test.py").read())

redis_pool = redis.ConnectionPool(host="localhost", port=6379, db=0)
redis_db = get_redis_db(redis_pool)


def chunk_list(l, num_chunks):
    def chunker(c):
        for i, key, in enumerate(l):
            if int(i * num_chunks / len(l)) == c:
                yield key
    for c in range(num_chunks):
        yield chunker(c)


def dump_dict_to_redis(redis_db, d, num_chunks=7, retries=5):
    if num_chunks == 1:
        try:
            redis_db.mset(d)
            return 1
        except redis.exceptions.ConnectionError:
            return dump_dict_to_redis(redis_db, d, num_chunks=10)
    try:
        for c in range(num_chunks):
            chunk = {key: value for i, (key, value) in enumerate(d.items()) if i % num_chunks == c}
            redis_db.mset(chunk)
        return num_chunks  # return. how many chunks it took... might be useful info for caller
    except redis.exceptions.ConnectionError:
        if retries == 0:
            raise
        return dump_dict_to_redis(redis_db, d, num_chunks=2 * num_chunks, retries=retries - 1)


def get_values_from_redis(redis_db, keys, num_chunks=7, retries=5):
    if num_chunks == 1:
        try:
            return redis_db.mget(keys)
        except redis.exceptions.ConnectionError:
            return get_values_from_redis(redis_db, keys)
    else:
        try:
            values = []
            for chunk in chunk_list(keys, num_chunks):
                c = list(chunk)
                if c:
                    values.extend(redis_db.mget(c))
            return values
        except redis.exceptions.ConnectionError:
            if retries == 0:
                raise
            return get_values_from_redis(redis_db, keys, num_chunks=2 * num_chunks, retries=retries - 1)

