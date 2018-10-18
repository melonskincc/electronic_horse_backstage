# -*- coding: utf-8 -*-
import time
import random

class genUUID64:
    tv = int(time.time())
    lastNum = 0
    rnd =  random.randint(0, 32767) << 16
    def __init__(self):
        pass

    def get(self):
        now = int(time.time())
        if self.tv != now:
            self.tv = now
            self.lastNum = 0

        self.lastNum += 1
        return (self.tv << 32) | self.rnd | self.lastNum
