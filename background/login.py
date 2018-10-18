# -*- coding: utf-8 -*-
import manageToken
from logger import *

class login:

    background_user = {"cxz": {"password": "b03215cf71cbbe2b5ce2c563e6568ce5", "role": 1 , "no_interface": []},
                       "wc": {"password": "534250ed30ef4001966ee185da64dc2f", "role": 1, "no_interface": []},
                       "chenguanghai": {"password": "33fb7d146c59eb0f80a9dde688163b56", "role": 1, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "miaolei": {"password": "c7686922fabd63d89a4831c19c6c3b0d", "role" : 2, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "chentong": {"password": "4f31f5328d2e8cd2b5ff164628637c02", "role":2, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "wangjinzhu": {"password": "6c4d42b0ba6645a95cd0523e0e21e503", "role": 3, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "kaikai": {"password": "87a4f378a79c9afa04b89f7cf24ee481", "role": 3, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "hejianghua": {"password": "1d63deccc2040d0f17478efad61c5a5a", "role": 3, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "ydh": {"password": "9bafcbe9f0d2a1e0904bc0fd68f3d6f5", "role": 3, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       "lzm": {"password": "3d03e0f1ae4be38d50b6c9e02ec08e9b", "role": 4, "no_interface": ["/add_vbc", "/add_give", "/add_purify", "/add_renascence", "/add_prop", "/horse_build"]},
                       }
    def __init__(self):
        pass

    def background_login(self,parameter):
        if "name" not in parameter or "password" not in parameter:
            reply_str = '{"code":1,"errmsg":"参数错误"}'
            return reply_str
        if parameter["name"] not in self.background_user:
            reply_str = '{"code":2,"errmsg":"用户名字不存在"}'
            return reply_str
        if parameter["password"] != self.background_user[parameter["name"]]["password"]:
            reply_str = '{"code":3,"errmsg":"密码不正确"}'
            return reply_str
        token = manageToken.manageToken.generate_token(parameter["name"])
        reply_str = '{"code":0,"token":"%s"}' % token
        return reply_str

    def isAuthorityOfInterface(self, user, uri):
        if user not in self.background_user:
            return False
        no_interface = self.background_user[user]["no_interface"]
        if uri in no_interface:
            WARNING_MSG('user=%s,uri=%s' % (user,uri))
            return False
        return True
