# -*- coding: utf-8 -*-
import datetime
import os
import redis
import json
from lib.timeHelp import str2Date, date2Str, getNow
from official.define import reward_config_table

class cache_redis:
    redis_host = "localhost"
    redis_port = 6379
    redis_password = "74bee"
    redis_notice_map_key = "horse/notice"
    redis_download_mode_key = "horse/download_mode"
    lottery_count_key="lottery_{}"
    daily_award_num_key="daily_award_num"
    lottery_data_key="{}_lottery_data"
    lottery_register_key="lottery_register_player"
    enter_active_key="{}_enter_active"

    def __init__(self):
        self.dictScriptNameSha=dict()
        self.redis_conn = self._connect_redis()

    def _connect_redis(self):
        pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, password=self.redis_password, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        return r

    def get_notice(self):
        notice_dict = dict()
        notice = self.redis_conn.hgetall(self.redis_notice_map_key)
        if notice is not None:
            notice_dict = dict()
            for notice_id, val in notice.items():
                notice_info = eval(val)
                notice_dict[notice_id] = notice_info
        reply_str = '{"code":0,"data":%s}' % json.dumps(notice_dict, ensure_ascii=False)
        return reply_str

    def get_download_mode(self):
        download_mode = self.redis_conn.get(self.redis_download_mode_key)
        if download_mode is None:
            download_mode = "0"
        return download_mode

    def set_download_mode(self, download_mode):
        self.redis_conn.set(self.redis_download_mode_key, download_mode)

    def set_lottery_count(self,accountName,count,ex):
        return self.redis_conn.set(self.lottery_count_key.format(accountName),count,ex=ex)

    def get_lottery_count(self,accountName):
        ret=self.redis_conn.get(self.lottery_count_key.format(accountName))
        if ret is None:
            return 0
        return int(ret)

    def set_daily_award_num(self):
        """设置每日奖品数量"""
        i = datetime.datetime.now()
        # i=str2Date("2018-09-01 00:00:10")
        if i.hour == 0 and i.minute == 0:
            self.redis_conn.delete(self.daily_award_num_key)
            for k,v in reward_config_table.items():
                self.redis_conn.hset(self.daily_award_num_key,k,v.get('daily_count'))

    def modify_award_num(self,k):
        #修改奖品数
        ret=self.redis_conn.eval("""
        local awardKey=KEYS[1]
        local idKey=KEYS[2]
        local count=redis.call("hget",awardKey,idKey)
        count=tonumber(count)
        if (tonumber(count)<=0)
        then
            return 0
        else
            count=count-1
            redis.call("hset",awardKey,idKey,count)
            return 1
        end
        """,2,*[self.daily_award_num_key,k])
        return ret

    def set_daily_lottery(self,accountName,is_share):
        """记录每日活动数据:账号：是否分享"""
        return self.redis_conn.hset(self.lottery_data_key.format(date2Str(datetime.datetime.now(),"%Y-%m-%d")),accountName,is_share)

    def get_is_share(self,accountName):
        #是否分享
        ret=self.redis_conn.hget(self.lottery_data_key.format(date2Str(datetime.datetime.now(),"%Y-%m-%d")),accountName)
        if ret is None:
            return 0
        return int(ret)

    def get_daily_lottery(self,accountName):
        return self.redis_conn.hget(self.lottery_data_key.format(date2Str(datetime.datetime.now(),"%Y-%m-%d")),accountName)

    def push_lottery_register(self,accountName):
        """抽奖活动带来的注册用户数"""
        return self.redis_conn.hset(self.lottery_register_key,accountName,getNow())

    def push_enter_active(self,accountName):
        """每日进入活动人"""
        return self.redis_conn.sadd(self.enter_active_key.format(date2Str(datetime.datetime.now(),"%Y-%m-%d")),accountName)

    def get_daily_lottery_data(self,date):
        dictDailyData=dict()
        # 每日分享成功人数；
        listShareData=self.redis_conn.hvals(self.lottery_data_key.format(date))
        setEnterUsers=self.redis_conn.smembers(self.enter_active_key.format(date))
        # 每日进入活动人数；
        iEnterCount=len(setEnterUsers)
        iShareCount=listShareData.count('1')
        dictDailyData['iEnterCount']=iEnterCount
        dictDailyData['iShareCount']=iShareCount
        dictDailyData['listEnterUsers']=list(setEnterUsers)
        return dictDailyData

    def get_ticket_token(self,key):
        return self.redis_conn.get(key)

    def set_ticket_token(self,key,value):
        return self.redis_conn.set(key,value)


if __name__ == '__main__':
    from main import g_cache_redis
    print(g_cache_redis.get_daily_lottery_data('2018-09-27'))