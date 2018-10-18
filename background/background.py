# -*- coding: utf-8 -*-
import time
import hashlib
import NameHash
import baseClient
import HelpFun
from logger import *

class background:
    def __init__(self, cache_redis, cache_mysql):
        self._cache_redis = cache_redis
        self._cache_mysql = cache_mysql
        self.index        = 1
        self.db_info      = {}
        self.baseClient   = baseClient.baseClient()
        self.register_count = 0
        self._download_mode = None

    def send_notice(self, parameter):
        if "msg" not in parameter or "interval" not in parameter or "expire" not in parameter:
            return '{"code":1,"errmsg":"parameter error"}'
        return self.__send_notice(parameter["interval"], parameter["expire"], parameter["msg"])

    def get_notice(self, parameter):
        return self._cache_redis.get_notice()

    def del_notice(self, parameter):
        if "notice_id" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__del_notice(parameter["notice_id"])

    def mod_notice(self, parameter):
        if "notice_id" not in parameter or "msg" not in parameter or "interval" not in parameter or "expire" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__mod_notice(parameter["interval"], parameter["expire"], parameter["msg"], parameter["notice_id"])

    def is_online(self, parameter):
        if "accountName" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__is_online(parameter["accountName"])

    def add_vbc(self, parameter):
        if "accountName" not in parameter or "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_vbc(parameter["accountName"], parameter["count"])

    def add_give(self, parameter):
        if "accountName" not in parameter or "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_give(parameter["accountName"], parameter["count"])

    def add_purify(self, parameter):
        if "accountName" not in parameter or "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_purify(parameter["accountName"], parameter["count"])

    def add_renascence(self, parameter):
        if "accountName" not in parameter or "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_renascence(parameter["accountName"], parameter["count"])

    def add_prop(self, parameter):
        if "accountName" not in parameter or "count" not in parameter or "propId" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_prop(parameter["accountName"], parameter["propId"], parameter["count"])

    def add_other_prop(self, parameter):
        if "accountName" not in parameter or "count" not in parameter or "propId" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__add_other_prop(parameter["accountName"], parameter["propId"], parameter["count"])

    def add_reserve_horse_count(self, parameter):
        if "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__AddReserveHorseCount(parameter["count"])

    def add_reserve_horse_random(self, parameter):
        if "count" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__AddReserveHorseRandom(parameter["count"])

    def horse_build(self, parameter):
        if "accountName" not in parameter or "count" not in parameter or "sex" not in parameter or "level" not in parameter or "generation" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__horse_build(parameter["accountName"], parameter["count"], parameter["sex"], parameter["level"], parameter["generation"])

    def send_message(self, parameter):
        if "accountName" not in parameter or "msg" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__send_message(parameter["msg"], parameter["accountName"])

    def set_chat_mode(self, parameter):
        if "accountName" not in parameter or "mode" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.__set_chat_mode(parameter["mode"], parameter["accountName"])

    def request_task(self, parameter):
        if "task" not in parameter:
            return '{"code":1,"errmsg":parameter error"}'
        return self.baseClient.send(parameter["task"])

    def set_download_mode(self, parameter):
        if "mode" not in parameter or (parameter["mode"] != "0" and parameter["mode"] != "1"):
            return '{"code":1,"errmsg":parameter error"}'
        self._cache_redis.set_download_mode(parameter["mode"])
        self._download_mode = parameter["mode"]
        return '{"code":0}'

    def get_download_mode(self, parameter):
        if self._download_mode is None:
            self._download_mode = self._cache_redis.get_download_mode()
        return '{"code":0,"mode":%s}' % self._download_mode

    def get_players(self):
        data = self.baseClient.send('{"cmd":"players"}')
        data_dict = eval(data)
        if data_dict["code"] == 0:
            return data_dict["players"]
        return 0

    def get_register(self):
        count = self.register_count
        self.register_count = 0
        return count

    def __send_notice(self, interval, expire, msg):
        if self.index > 1000:
            self.index = 1
        notice_id = str(int(time.time()) + self.index)
        cmd = '{"cmd":"AddNotice","interval":%s,"expire":%s,"msg":"%s","noticeId":"%s"}' % (interval, expire, msg, notice_id)
        data = self.baseClient.send(cmd)
        return data

    def __del_notice(self, notice_id):
        cmd = '{"cmd":"DelNotice","noticeId":"%s"}' % notice_id
        data = self.baseClient.send(cmd)
        return data

    def __mod_notice(self, interval, expire, msg, notice_id):
        cmd = '{"cmd":"ModNotice","interval":%s,"expire":%s,"msg":"%s","noticeId":"%s"}' % (interval, expire, msg, notice_id)
        data = self.baseClient.send(cmd)
        return data

    def __get_notice(self):
        """
        数据太大了udp不行
        """
        cmd = '{"cmd":"GetNotice"}'
        data = self.baseClient.send(cmd)
        return data

    def __is_online(self, accountName):
        cmd = '{"cmd":"online","accountName":"%s"}' % accountName
        data = self.baseClient.send(cmd)
        return data

    def __add_vbc(self, accountName, count):
        cmd = '{"cmd":"AddVbc","count":%s,"accountName":"%s"}' % (count, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __add_give(self, accountName, count):
        cmd = '{"cmd":"AddGive","count":%s,"accountName":"%s"}' % (count, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __add_purify(self, accountName, count):
        cmd = '{"cmd":"AddPurify","count":%s,"accountName":"%s"}' % (count, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __add_renascence(self, accountName, count):
        cmd = '{"cmd":"AddRenascence","count":%s,"accountName":"%s"}' % (count, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __add_prop(self, accountName, propId, count, reason = None):
        if reason is None:
            cmd = '{"cmd":"AddProp","count":%s,"propId":%s,"accountName":"%s"}' % (count, propId, accountName)
        else:
            cmd = '{"cmd":"AddProp","count":%s,"propId":%s,"accountName":"%s","reason":1}' % (count, propId, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __add_other_prop(self, accountName, propId, count, reason = None):
        if reason is None:
            cmd = '{"cmd":"AddOtherProp","count":%s,"propId":%s,"accountName":"%s"}' % (count, propId, accountName)
        else:
            cmd = '{"cmd":"AddOtherProp","count":%s,"propId":%s,"accountName":"%s","reason":1}' % (count, propId, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __AddReserveHorseCount(self, count):
        cmd = '{"cmd":"AddReserveHorseCount","count":%s}' % count
        data = self.baseClient.send(cmd)
        return data

    def __AddReserveHorseRandom(self, count):
        cmd = '{"cmd":"AddReserveHorseRandom","count":"%s"}' % count
        data = self.baseClient.send(cmd)
        return data

    def __horse_build(self, accountName, count, sex, level, generation, reason = None):
        if reason is None:
            cmd = '{"cmd":"HorseBuild","count":%s,"sex":%s,"level":%s,"generation":%s,"accountName":"%s"}' % (count, sex, level, generation, accountName)
        else:
            cmd = '{"cmd":"HorseBuild","count":%s,"sex":%s,"level":%s,"generation":%s,"accountName":"%s","reason":1}' % (count, sex, level, generation, accountName)
        data = self.baseClient.send(cmd)
        return data

    def __send_message(self, msgType, accountName, data_json):
        cmd = '{"cmd":"SendMessage","msgType":%s,"accountName":"%s","data":%s}' % (msgType, accountName, data_json)
        data = self.baseClient.send(cmd)
        return data

    def __set_chat_mode(self, mode, accountName):
        cmd = '{"cmd":"SetChatMode","mode":%s,"accountName":"%s"}' % (mode, accountName)
        data = self.baseClient.send(cmd)
        return data

    def increase_register(self):
        self.register_count += 1

    def modifyChannelCode(self, channelCode, accountName):
        cmd = '{"cmd":"ModifyChannelCode","channelCode":"%s","accountName":"%s"}' % (channelCode, accountName)
        data = self.baseClient.send(cmd)
        return data

    def add_user(self, accountName, passwd, invitation_code, channelCode):
        DEBUG_MSG("accountName=%s, invitecode=%s, channelCode=%s" % (accountName, invitation_code, channelCode))
        name_hash = NameHash.NameHash(accountName)
        dbname = name_hash.crcHash()
        db = self._cache_mysql.conn_mysql(dbname)
        try:
            cur = db.cursor()
            sql_select = "select id from tbl_Account where sm_accountName='%s';" % accountName
            cur.execute(sql_select)
            results = cur.fetchall()
            if len(results) != 0:
                db.close()  # 关闭连接
                return 1
            uuid, nickname, selfInvitationCode = HelpFun.get_accountname_uuid_nickname()
            sql = "insert into tbl_Account(sm_accountName,sm_uuid,sm_nickname,sm_selfInvitationCode,sm_invitationCode,sm_channelCode)values('%s','%s','%s','%s','%s','%s');" % (accountName, uuid, nickname, selfInvitationCode, invitation_code, channelCode)
            cur.execute(sql)  # 执行sql语句
            db.commit()

            cur.execute(sql_select)
            results = cur.fetchall()
            row = results[0]
            entityDBID = int(row[0])
            md5 = hashlib.md5()
            md5.update(passwd.encode('utf-8'))
            sign = md5.hexdigest()
            sql = "insert into kbe_accountinfos(accountName,password,email,entityDBID,regtime)values('%s','%s','%s',%d,%i);" % (accountName, sign, accountName+"@0.0",entityDBID,int(time.time()))
            cur.execute(sql)  # 执行sql语句
            db.commit()
            db.close()  # 关闭连接
            return 0
        except Exception as e:
            ERROR_MSG("background.add_user:error=%s" % str(e))
            return 2

    def select_user(self, accountName):
        name_hash = NameHash.NameHash(accountName)
        dbname = name_hash.crcHash()
        sql = "select sm_accountName,sm_uuid,sm_nickname,sm_selfInvitationCode,sm_invitationCode,sm_channelCode from tbl_Account where sm_accountName='%s';" % accountName
        return self._cache_mysql.get_one(dbname, sql)

    def mdify_player_channel_code(self, accountName, invitecode, channelCode):
        DEBUG_MSG("accountName=%s, invitecode=%s, channelCode=%s" % (accountName, invitecode, channelCode))
        if not accountName or not invitecode or not channelCode:
            return 

        sql = ""
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select sm_accountName as accountName,sm_selfInvitationCode as selfInvitationCode from {databaseName}.tbl_Account where sm_invitationCode='{invitecode}')".format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"], invitecode=invitecode)
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            if val["accountName"] != "":
                self.modifyChannelCode(channelCode, val["accountName"])
                self.mdify_player_channel_code(val["accountName"], val["selfInvitationCode"], channelCode)

    def share_add_prop(self, accountName, propId, count):
        return self.__add_prop(accountName, propId, count, 1)

    def share_horse_build(self, accountName, count, sex, level, generation):
        return self.__horse_build(accountName, count, sex, level, generation, 1)

    def mod_user_head(self, accountName, head_url):
        DEBUG_MSG("accountName=%s, head_url=%s" % (accountName, head_url))
        cmd = '{"cmd":"ModifyUserHead","headUrl":"%s","accountName":"%s"}' % (head_url, accountName)
        data = self.baseClient.send(cmd)
        return data

    def del_other_prop(self, accountName, propId, count):
        DEBUG_MSG("accountName=%s, propId=%d, count=%d" % (accountName, propId, count))
        cmd = '{"cmd":"DelOtherProp","count":%d,"propId":%d,"accountName":"%s"}' % (count, propId, accountName)
        data = self.baseClient.send(cmd)
        return data