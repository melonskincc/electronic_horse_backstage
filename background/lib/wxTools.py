import hashlib
import json
from threading import Lock
import requests
import time
from HelpFun import get_random_string
from lib.timeHelp import getNow
from logger import *

class WxTool():
    __first_init=True
    __appid = "wx29b976b1ef3336ad"
    __secret = "65e1928a5bd2a343b6bee603486b984e"
    __baseUrl = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={}&secret={}".format(__appid, __secret)

    def __init__(self,cache_redis):
        self.cache_redis=cache_redis
        if self.__first_init:
            self.get_access_token()
            self.get_jsapi_ticket()
            self.__first_init=False

    def get_access_token(self):
        #verify = False
        resp = requests.get(self.__baseUrl,timeout=20,verify = False)
        DEBUG_MSG("resp:{}".format(resp))
        if resp.status_code!=200:
            return False
        dictResp = json.loads(resp.text)
        if dictResp.get('access_token') is None:
            ERROR_MSG("获取微信access_token失败：{}".format(dictResp))
            return False
        else:
            DEBUG_MSG("新生成微信access_token:{}".format(dictResp.get('access_token')))
            ret = self.cache_redis.set_ticket_token('access_token', dictResp.get('access_token'))
            if not ret:
                return False
            return True

    def get_jsapi_ticket(self):
        #verify=False
        token=self.cache_redis.get_ticket_token('access_token')
        if not token:
            return False
        jsapi_ticket_url = "https://api.weixin.qq.com/cgi-bin/ticket/getticket?access_token={}&type=jsapi".format(token)

        resp = requests.get(jsapi_ticket_url,timeout=20,verify = False)
        DEBUG_MSG("resp:{}".format(resp))
        if resp.status_code != 200:
            return False
        dictResp=json.loads(resp.text)
        if dictResp.get('ticket') is None:
            ERROR_MSG("获取微信jsapi_ticket失败：{}".format(dictResp))
            return False
        else:
            DEBUG_MSG("新生成微信ticket:{}".format(dictResp.get('ticket')))
            ret=self.cache_redis.set_ticket_token('jsapi_ticket',dictResp.get('ticket'))
            if not ret:
                return False
            return True

