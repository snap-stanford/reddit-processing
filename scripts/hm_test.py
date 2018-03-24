from hashmap import HashMap

def main():
    d = HashMap(capacity=4)

    d[b'a'] = b'Ayeeee'
    d[b'b'] = b'Bee'

    print(d[b'a'])

    for key in d:
        print(key)

    print(b'a' in d)
    print(b'c' in d)

    d[b'c'] = b"something"
    print(d[b'c'])

    d[b'c'] = b"something else entirely!"
    print(d[b'c'])

    print(len(d))
    del d[b'c']
    print(b'c' in d)

    d[b'c'] = b"something new"
    d[b'd'] = b"DEE"
    try:
        d[b'oh_no'] = b'too many'
    except MemoryError:
        print("Caught memory error")

    del d[b'd']
    d[b'oiwef'] = b"should work fine"


if __name__ == "__main__":
    main()
