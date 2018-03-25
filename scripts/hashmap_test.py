
import unittest
import string
import ctypes
from hashmap import HashMap
import multiprocessing as mp

uppercase = [l.encode() for l in string.ascii_uppercase]
lowercase = [l.encode() for l in string.ascii_lowercase]

upper_to_i = {l: i for i, l in enumerate(uppercase)}
upper_to_upper = {ul: ul for ul in uppercase}
upper_to_lower = {ul: ll for ll, ul in zip(lowercase, uppercase)}
lower_to_upper = {ll: ul for ll, ul in zip(lowercase, uppercase)}

class HashMapTest(unittest.TestCase):

    def test_setting(self):
        d = HashMap(capacity=4)
        d[b'a'] = b'A'
        d[b'b'] = b'B'
        self.assertEqual(d[b'a'], b'A', "Failed to insert A")
        self.assertEqual(d[b'b'], b'B', "Failed to insert B")

        d[b'a'] = b'H'
        self.assertEqual(d[b'a'], b'H', "Failed to change a")
        self.assertEqual(d[b'b'], b'B', "Changing a changed b")

    def test_contains(self):
        d = HashMap(capacity=4)
        d[b'a'] = b'hi'
        d[b'b'] = b'Bee'

        self.assertTrue(b'a' in d)
        self.assertTrue(b'b' in d)
        self.assertTrue(b'nope' not in d)

        d = HashMap(key_type=ctypes.c_char_p, value_type=ctypes.c_int)
        for i, letter in enumerate(uppercase):
            d[letter] = i

        for i, letter in enumerate(uppercase):
            self.assertTrue(letter in d, "Didn't contain letter: %s" % letter)

        for i, letter in enumerate(lowercase):
            self.assertTrue(letter not in d, "Somehow contains letter: %s" % letter)

    def test_iteration(self):
        d = HashMap(key_type=ctypes.c_int, value_type=ctypes.c_char_p, capacity=30)

        for i, letter in enumerate(uppercase):
            d[i] = letter

        for key in d:
            self.assertTrue(key in d)

        keys = {key for key in d}
        for i, letter in enumerate(uppercase):
            self.assertTrue(i in keys)

        letters = set(uppercase)
        for key in d:
            self.assertTrue(d[key] in letters)
            letters -= {d[key]}
        self.assertEqual(letters, set(), "Not all of the letters were iterated through")

        for key, value in d.items():
            self.assertTrue(d[key], value)

    def test_memory_error(self):
        d = HashMap(capacity=4)
        d[b'a'] = b"a"
        d[b'b'] = b"tester"
        d[b'c'] = b"something new"
        d[b'd'] = b"DEE"

        with self.assertRaises(MemoryError):
            d[b'oh no'] = b'too many!'

    def test_initialize(self):
        d = HashMap(lower_to_upper.items())
        self.assertEqual(len(d), len(lowercase))

        for ll, ul in zip(lowercase, uppercase):
            self.assertEqual(d[ll], ul)

    def test_update(self):
        d = HashMap()
        d.update(lower_to_upper)
        self.assertEqual(len(d), len(lowercase))

        for ll, ul in lower_to_upper.items():
            self.assertEqual(d[ll], ul)

    def test_insert(self):
        d = HashMap()
        d.insert(lower_to_upper.items())
        self.assertEqual(len(d), len(lowercase))

        for ll, ul in zip(lowercase, uppercase):
            self.assertEqual(d[ll], ul)

    def test_clear(self):
        d = HashMap(upper_to_i.items(), key_type=ctypes.c_char_p, value_type=ctypes.c_int)

        d.clear()
        self.assertEqual(len(d), 0)

        d.update(upper_to_i)
        self.assertEqual(len(d), len(upper_to_i))

    def test_len(self):
        d = HashMap(key_type=ctypes.c_char_p, value_type=ctypes.c_int)
        self.assertEqual(len(d), 0)

        d.update(upper_to_i)
        self.assertEqual(len(d), len(upper_to_i))

    def test_delete(self):
        d = HashMap(upper_to_i.items(), key_type=ctypes.c_char_p, value_type=ctypes.c_int)

        del d[b'A']
        self.assertTrue(b'A' not in d)

        for l in set(uppercase) - {b'A'}:
            self.assertTrue(l in d)

        d[b'A'] = 12345
        for l in set(uppercase):
            self.assertTrue(l in d)

        for l in upper_to_i:
            del d[l]

        self.assertEqual(len(d), 0)

    def test_multiprocessing_write(self):
        d = HashMap(upper_to_i.items(), key_type=ctypes.c_char_p, value_type=ctypes.c_int)

        def modify():
            d[b'A'] = 12345

        p = mp.Process(target=modify)
        p.start()
        p.join()
        self.assertEqual(d[b'A'], 12345)

    def test_multiprocessing_lock(self):
        d = HashMap(upper_to_i.items(), key_type=ctypes.c_char_p, value_type=ctypes.c_int, lock=mp.Lock())

        def modify1():
            d[b'A'] = 1111

        def modify2():
            d[b'A'] = 2222

        p1 = mp.Process(target=modify1)
        p2 = mp.Process(target=modify2)

        p1.start()
        p2.start()
        p1.join()
        p2.join()

        self.assertNotEqual(d[b'A'], 0)
        self.assertTrue(d[b'A'] in [1111, 2222])

    def test_multiprocessing_delete(self):
        d = HashMap(upper_to_i.items(), key_type=ctypes.c_char_p, value_type=ctypes.c_int, lock=mp.Lock())

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
        d = HashMap(key_type=ctypes.c_int, value_type=ctypes.c_int, lock=mp.Lock())

        def update_1():
            d.update({1: 1, 11: 11, 111: 111})

        def update_2():
            d.update({2: 2, 22: 22, 222: 222})

        p1 = mp.Process(target=update_1)
        p2 = mp.Process(target=update_2)
        p1.start()
        p2.start()
        p1.join()
        p2.join()

        for n in [1, 11, 111]:
            self.assertTrue(n in d)

        for n in [2, 22, 222]:
            self.assertTrue(n in d)

        self.assertEqual(len(d), 6)


def performance_test():
    import time
    import dbm
    from itertools import permutations

    def test_dict(d):
        # d[-1] = 1
        # d[-2] = 2
        # for i in range(10000):
        #     d[i] = (2 * d[i - 1] + d[i - 2]) % 123454321
        #
        # s = 0
        # for i in range(10000):
        #     if d[i] % 2071 == 0:
        #         s += d[i]
        for p in [''.join(p) for p in permutations('1234567')]:
            d[p.encode()] = p.encode()

    def test_lookup(d):
        for p in [''.join(p) for p in permutations('1234567')]:
            if p.encode() in d:
                pass

    d = {}
    t = time.time()
    test_dict(d)
    print("dict insert: %s" % (time.time() - t))

    t = time.time()
    test_lookup(d)
    print("dict lookup: %s" % (time.time() - t))

    d = HashMap(key_type=ctypes.c_char_p, value_type=ctypes.c_char_p, capacity=1000000)
    t = time.time()
    test_dict(d)
    print("Shared-Memory HashMap insert: %s" % (time.time() - t))

    t = time.time()
    test_lookup(d)
    print("Shared-Memory HashMap lookup: %s" % (time.time() - t))

    d = mp.Manager().dict()
    t = time.time()
    test_dict(d)
    print("Manager().dict insert: %s" % (time.time() - t))

    t = time.time()
    test_lookup(d)
    print("Manager().dict lookup: %s" % (time.time() - t))

    db = dbm.open('cache', 'c')
    t = time.time()
    test_dict(db)
    print("dbm insert time: %s" % (time.time() - t))

    t = time.time()
    test_lookup(db)
    print("dbm lookup time: %s" % (time.time() - t))

    db.close()



if __name__ == "__main__":
    performance_test()
    unittest.main()
