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



