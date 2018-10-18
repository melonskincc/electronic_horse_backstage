# -*- coding: utf-8 -*-
import socket
from logger import *

class baseClient:
    host = "localhost"  #
    port = 30041  #
    bufsize = 1024  # 定义缓冲大
    def __init__(self):
        self.addr = (self.host, self.port)
        self.udpClient = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data):
        try:
            self.udpClient.settimeout(5)
            text = data.encode('utf-8')
            self.udpClient.sendto(text, self.addr)
            data, addr = self.udpClient.recvfrom(self.bufsize)  # 接收数据和返回地址
            return data.decode(encoding="utf-8")
        except Exception as e:
            ERROR_MSG("error=%s,data=%s" % (str(e), data))
            return'{"code":1}'