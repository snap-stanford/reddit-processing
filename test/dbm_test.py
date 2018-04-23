#!/usr/bin/env python
"""
File: dbm_test.py
Date: 3/25/18 
Author: Jon Deaton (jdeaton@stanford.edu)
"""

import unittest
import string
import multiprocessing as mp
import dbm

uppercase = [l.encode() for l in string.ascii_uppercase]
lowercase = [l.encode() for l in string.ascii_lowercase]

upper_to_i = {l: i for i, l in enumerate(uppercase)}
upper_to_upper = {ul: ul for ul in uppercase}
upper_to_lower = {ul: ll for ll, ul in zip(lowercase, uppercase)}
lower_to_upper = {ll: ul for ll, ul in zip(lowercase, uppercase)}

db_cache = 'test_cache'


class DBMTest(unittest.TestCase):

    def test_multiprocessing_write(self):
        clear()

        def modify():
            d = dbm.open(db_cache, 'c')
            d[b'A'] = "WAAAAN"
            d.close()

        p = mp.Process(target=modify)
        p.start()
        p.join()
        self.assertEqual(d[b'A'], "WAAAAN")

    def test_multiprocessing_lock(self):
        clear()

        def modify1():
            d = dbm.open(db_cache, 'c')
            d[b'A'] = "WAN"
            d.close()

        def modify2():
            d = dbm.open(db_cache, 'c')
            d[b'A'] = "DOO"
            d.close()

        p1 = mp.Process(target=modify1)
        p2 = mp.Process(target=modify2)

        p1.start()
        p2.start()
        p1.join()
        p2.join()

        self.assertTrue(d[b'A'] not in ["WAN", "DOO"])
        self.assertTrue(d[b'A'] in ["WAN", "DOO"])

    def test_multiprocessing_delete(self):
        clear()

        def delete_1():
            del d[b'A']

        def delete_2():
            del d[b'B']

        self.assertTrue(b'A' in d)
        self.assertTrue(b'B' in d)

        p1 = mp.Process(target=delete_1)
        p2 = mp.Process(target=delete_2)
        p1.start()
        p2.start()
        p1.join()
        p2.join()

        self.assertTrue(b'A' not in d)
        self.assertTrue(b'B' not in d)

    def test_multiprocessing_update(self):
        clear()

        def update_1():
            for key, value in {"1": "1", "11": "11", "111": "111"}.items():
                d[key] = value

        def update_2():
            for key, value in {"2": "2", "22": "22", "222": "222"}.items():
                d[key] = value

        p1 = mp.Process(target=update_1)
        p2 = mp.Process(target=update_2)
        p1.start()
        p2.start()
        p1.join()
        p2.join()

        for n in ['1', '11', '111']:
            self.assertTrue(n in d)

        for n in ['2', '22', '222']:
            self.assertTrue(n in d)


def clear():
    """
    Clears the global data base
    :return: None
    """
    global d
    for key in d.keys():
        del d[key]


if __name__ == "__main__":
    global d
    d = dbm.open(db_cache, 'n')
    unittest.main()
    d.close()