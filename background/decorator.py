# -*- coding: utf-8 -*-
import time
from logger import *

def run_time_log(func):
    """测试接口运行时间"""
    def wrapper(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        endTime = time.time()
        msecs = (endTime - startTime)*1000
        DEBUG_MSG("api run time is %d ms" % msecs)
    return wrapper
