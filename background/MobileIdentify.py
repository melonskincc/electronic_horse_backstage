# -*- coding: utf-8 -*-
import datetime
import hashlib
import random
import requests
import time
from logger import *
import operator as op

class MobileIdentify:
    password = 'SSy20188'
    username = 'ssyhy'
    url = "http://api.zthysms.com/sendSms.do"
    expire = 120
    developer_list = []
    def __init__(self, cache_mysql):
        self.cache_mysql = cache_mysql
        self.identify = {}

    def buildIdentifyCode(self, accountName):
        """
        define method.
        生成永旗电子马验证码
        """
        identify_code = random.randint(111111, 999999)
        sql = "insert into identify_code(accountName,code)values('%s',%i)" % (accountName, identify_code)
        self.cache_mysql.execute("default", sql)
        content = '【永旗电子马】: 您的验证码是%i' % identify_code
        starttime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        md5_password = hashlib.md5(self.password.encode(encoding='utf-8'))
        md5_all = hashlib.md5((md5_password.hexdigest() + starttime).encode(encoding='utf-8'))
        data_s = {'username': self.username, 'tkey': starttime, 'password': md5_all.hexdigest(),'content': content.encode(encoding='utf-8'), 'mobile': accountName}
        r = requests.post(self.url, data=data_s)
        statu_code = r.text.split(",")[0]
        if not statu_code.isdigit():
            errorno = 202 # 验证码返回错误
            return errorno, r.text.split(",")[1]
        errorno = int(statu_code) + 200 # 验证码返回码分隔间值
        INFO_MSG('buildIdentifyCode r.text:%s. errorno=%i' % (statu_code, errorno))
        # 201 获取验证码成功
        if errorno == 201:
            identify_info = dict()
            identify_info["time"] = time.time()
            identify_info["errorno"] = errorno
            identify_info["accountName"] = accountName
            identify_info["identify_code"] = identify_code
            info = self.identify.get(accountName)
            if info is not None:
                del self.identify[accountName]
            self.identify[accountName] = identify_info
            #INFO_MSG('buildIdentifyCode identify=%s' % self.identify)

        return errorno, r.text.split(",")[1]

    def checkIdentifyCode(self, accountName, identify_code):
        """
        define method.
        校验手机注册的验证码
        """
        if accountName in self.developer_list:
            errorno = 0  # 开发人员的短信不需要验证
            return errorno
            
        now = time.time()
        identifyInfo = self.identify.get(accountName)
        if identifyInfo is not None:
            if now - identifyInfo["time"] > self.expire:
                errorno = 300 # 验证码超时
                del self.identify[accountName]
            elif op.ne(identify_code, identifyInfo["identify_code"]):
                errorno = 302 # 验证码校验不相等
            else:
                errorno = 0 # 成功
                del self.identify[accountName]
        else:
            errorno = 301 # 验证码无效
        INFO_MSG('checkIdentifyCode identify:%s, accountName:%s, errorno:%s. ' % (self.identify, accountName, errorno))
        return errorno

    def check_identify(self):
        now = int(time.time())
        for k, v in self.identify.items():
            if now - v["time"] >= self.expire:
                del self.identify[k]
                break

    @classmethod
    def send_message(cls, accountName, content):
        starttime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        md5_password = hashlib.md5(cls.password.encode(encoding='utf-8'))
        md5_all = hashlib.md5((md5_password.hexdigest() + starttime).encode(encoding='utf-8'))
        data_s = {'username': cls.username, 'tkey': starttime, 'password': md5_all.hexdigest(),'content': content.encode(encoding='utf-8'), 'mobile': accountName}
        r = requests.post(cls.url, data=data_s)
        l = r.text.split(",")
        return l[0], l[1]
