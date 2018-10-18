# -*- coding: utf-8 -*-
import  binascii

DB_LOAD_NUM = 10

class NameHash:
    def __init__(self, _str, n = DB_LOAD_NUM):
        self.n = n
        self._str = _str
        self.dict = {}
        for index in range(self.n):
            if index == 0:
                self.dict.setdefault(index, "default")
            else:
                self.dict.setdefault(index, "hashdb" + str(index))

    def crcHash(self):
        hash = binascii.crc32(self._str.encode()) >> 16 & 0xffff
        return self.dict.get(hash % self.n)


if __name__ == '__main__':
    name=NameHash('17687337831')
    dbname=name.crcHash()
    print(dbname)
