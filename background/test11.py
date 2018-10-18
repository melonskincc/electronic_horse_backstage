import random
from threading import Thread,Lock
import time

import requests

g_a=0
resp=None

class a():
    def __init__(self):
        self.attr_a=0
        self.threadLock = Lock()
    def b(self):
        self.threadLock.acquire()
        self.attr_a+=1
        self.threadLock.release()

class myThread(Thread):
    def __init__(self,name,func):
        Thread.__init__(self)
        self.name = name
        self.func=func
    def run(self):
        global resp
        # 设置重连次数
        requests.adapters.DEFAULT_RETRIES = 5
        s = requests.session()
        # 设置连接活跃状态为False
        s.keep_alive = False
        resp=requests.get(url="http://192.168.1.155:8000/user/do_lottery",params={"accountName":"15770633066"},headers={"Authorization":"MTU3NzA2MzMwNjY6M2UxMTFmZmZiNTFjYTYyZjRiNDI4NmJlMTQ3M2YxOTNjOWNkOGQwNQ=="},timeout=10)
        resp.close()
        print(resp.text)

if __name__ == '__main__':


    aa=a()
    threads = []
    t1 =myThread(name='tr_1',func=aa.b)
    t2 =myThread(name='tr_2',func=aa.b)
    t3 =myThread(name='tr_3',func=aa.b)
    t4 =myThread(name='tr_4',func=aa.b)
    t5 =myThread(name='tr_5',func=aa.b)
    t6 =myThread(name='tr_6',func=aa.b)
    t7 =myThread(name='tr_7',func=aa.b)
    t8 =myThread(name='tr_8',func=aa.b)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    threads.append(t1)
    threads.append(t2)
    threads.append(t3)
    threads.append(t4)
    threads.append(t5)
    threads.append(t6)
    threads.append(t7)
    threads.append(t8)
    for t in threads:
        t.join()
    print(aa.attr_a)



