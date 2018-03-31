#!/usr/bin/env python
"""
File: redis-test
Date: 3/31/18 
Author: Jon Deaton (jdeaton@stanford.edu)
"""

import redis

def main():
    # create a connection to the localhost Redis server instance, by default it runs on port 6379
    redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

    redis_db.keys()
    redis_db.set('full stack', 'python')
    redis_db.set('backend', 'scala')
    redis_db.set('frontend', "javascript")

    redis_db.mset({'you': 'wanna', 'be': 'high', 'for': 'it'})

    values = redis_db.mget(['full stack', 'backend', 'mobile', 'you'])

    redis_db.keys()
    # # now we have one key so the output will be "[b'full stack']"
    # redis_db.get('full stack')
    # # output is "b'python'", the key and value still exist in Redis
    # redis_db.incr('twilio')
    # # output is "1", we just incremented even though the key did not
    # # previously exist
    # redis_db.get('twilio')
    # # output is "b'1'" again, since we just obtained the value from
    # # the existing key
    # redis_db.delete('twilio')
    # # output is "1" because the command was successful
    # redis_db.get('twilio')
    # # nothing is returned because the key and value no longer exist


if __name__ == "__main__":
    main()
