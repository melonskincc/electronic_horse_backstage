# -*- coding: utf-8 -*-
import base64
import hmac
import time
import HelpFun
from logger import *

class manageToken:
    user_key = dict()
    expire = 60 * 60 * 3
    def __init__(self):
        pass

    @classmethod
    def certify_token(cls, token):
        if not token:
            return False,""
        try:
            token_str = base64.urlsafe_b64decode(token).decode('utf-8')
        except Exception as e:
            ERROR_MSG(e)
            return False,""
        token_list = token_str.split(':')
        if len(token_list) != 2:
            return False,""
        userid = token_list[0]
        if userid not in cls.user_key:
            return False,""

        known_sha1_tsstr = token_list[1]
        DEBUG_MSG('known_sha1_tsstr=%s,cls.user_key[userid]["token"]=%s' % (known_sha1_tsstr, cls.user_key[userid]["token"]))
        if cls.user_key[userid]["token"] != known_sha1_tsstr:
            return False,userid
        return True,userid

    @classmethod
    def generate_token(cls, userid):
        key = HelpFun.get_random_string(10)
        userid_byte = userid.encode("utf-8")
        sha1_tshexstr  = hmac.new(key.encode("utf-8"), userid_byte, 'sha1').hexdigest()
        
        token = userid + ':' + sha1_tshexstr
        b64_token = base64.urlsafe_b64encode(token.encode("utf-8"))
        token = b64_token.decode("utf-8")
        cls.user_key[userid] = {"token": sha1_tshexstr, "expire": int(time.time())}
        DEBUG_MSG('token=%s' % token)
        return token

    @classmethod
    def check_token(cls):
        now = int(time.time())
        for k, v in cls.user_key.items():
            if now - v["expire"] >= cls.expire:
                del cls.user_key[k]
                break
