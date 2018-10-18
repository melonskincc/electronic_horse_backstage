# -*- coding: utf-8 -*-
import math
import datetime
import time
import json
import os
import re
import NameHash
import HelpFun
import base64
from lib.dataBaseCRUD import getNewPaySqlByTime, getActiveConvCount, getLoginCountSqlByTime, getPayCountSqlByTime, \
    getMaxOnlineSqlByTime, getACUSqlByTime, getRetainDataByTime, getAvgOnlineTimeByTime
from lib.gen_excel import GenExcel
from lib.timeHelp import timestamp2Str, getNow, strToTimestamp
from HelpFun import convert_to_dict
from logger import *

"""
提供给php接口
http://192.168.1.35:30040/players   获得在线人数
http://192.168.1.35:30040/register?start=1528437019&end=1528437019   获得注册人数
http://192.168.1.35:30040/pay?start=1528437019&end=1528437019   获得充值金额
http://192.168.1.35:30040/pay_frequency?start=1528437019&end=1528437019   获得周期内充值用户游戏充值的频次
http://192.168.1.35:30040/pay_total?start=1528437019&end=1528437019   获得周期内充值用户游戏充值的总额
http://192.168.1.35:30040/gold_log?accountName=13544246806&start=1528437019&end=1528437019   获得用户金币流水   operation=Purify&start=1528437019&end=1528437019   获得类型金币流水
http://192.168.1.35:30040/player_info?page_no=1&orderby=gold   获得用户信息（accountName用户名，gold总金币，purify_prop稻草，horseCount马数量，businessHorseCount上市场马数量，buyHorseCount买入马数量，sellHorseCount卖出马数量，recharge_count充值次数，recharge_sum总充值金额，first_recharge首次充值，last_recharge最后充值，regtime注册时间）
http://192.168.1.35:30040/player_detail_info?accountName=13544246806   获得单个用户信息
http://192.168.1.35:30040/player_info_count   获得所有用户数量
http://192.168.1.35:30040/player_horse_detail_info?accountName=13544246806   获得用户马的信息
http://192.168.1.35:30040/player_procreate_horse_detail_info?accountName=135442468060   获得用户繁殖马的信息
http://192.168.1.35:30040/player_detail_info?accountName=135442468060   获得单个用户信息
http://192.168.1.35:30040/player_gold?count=100   获得用户金币，限制100条,不可以大于count
http://192.168.1.35:30040/horse_trade_log         获得交易数据
http://192.168.1.35:30040/horse_procreate_log         获得繁殖数据
http://192.168.1.35:30040/player_horse_procreate_log?accountName=13544246806         获得用户繁殖数据
http://192.168.1.35:30040/business_horse         获得交易市场马的数据
http://192.168.1.35:30040/business_horse_detail_info?         获得交易市场马的数据
http://192.168.1.35:30040/reserve_horse?count=1&accountName=13544246806         count为第几次活动 获得预订马数据
"""

class Statistics:
    def get_players(self, parameter):
        if "days" not in parameter:
            return '{"code":1,"errmsg":"parameter error"}'
        field = ""
        i = datetime.datetime.now()
        hour = i.hour
        minute_list = [15,30,45,60]
        for n in range(0, hour):
            for minute in minute_list:
                hour_minute = "%dhour%d" % (n, minute)
                field += hour_minute + ","
        if 0 < i.minute <= 15:
            hour_minute = "%dhour%d" % (hour, 15)
            field += hour_minute
        elif 15 < i.minute <= 30:
            hour_minute = "%dhour%d" % (hour, 15)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 30)
            field += hour_minute
        elif 30 < i.minute <= 45:
            hour_minute = "%dhour%d" % (hour, 15)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 30)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 45)
            field += hour_minute
        else:
            hour_minute = "%dhour%d" % (hour, 15)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 30)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 45)
            field += hour_minute + ","
            hour_minute = "%dhour%d" % (hour, 60)
            field += hour_minute
        sql = "select days,%s from statistics_players_online where days='%s'" % (field,parameter["days"])
        data = self._cache_mysql.get_list("default", sql)
        return '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)

    def get_register(self, parameter):
        """获取玩家注册统计详情"""
        if "days" not in parameter:
            return '{"code":1,"errmsg":"parameter error"}'

        sql = "select * from per_day_register_record where days='%s'" % parameter["days"]
        data = self._cache_mysql.get_one("default", sql)

        return '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)

    def get_pay(self, parameter):
        if "start" not in parameter or "end" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no start or end"}'
            return reply_str
        if not parameter["start"].isdigit() or not parameter["end"].isdigit():
            reply_str = '{"code":1,"errmsg":"parameter time fail"}'
            return reply_str
        start = int(parameter["start"])
        end = int(parameter["end"])
        if end < start:
            reply_str = '{"code":1,"errmsg":"The start time cannot be greater than the end time"}'
        elif end - start > 60 * 60 * 24 * 7:
            reply_str = '{"code":1,"errmsg":"Time can not exceed 24 hours"}'
        else:
            dealDate_start = time.strftime("%Y%m%d%H%M%S", time.localtime(start))
            dealDate_end = time.strftime("%Y%m%d%H%M%S", time.localtime(end))
            sql = "select accountName,money,gold,payway,dealDate from onlinePay where status=1 and dealDate>'%s' and dealDate<='%s';" % (dealDate_start, dealDate_end)
            #DEBUG_MSG("Statistics.get_pay:sql=%s" % sql)
            data = self._cache_mysql.get_list("default", sql)
            reply_str = '{"code":0,"data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def pay_frequency(self, parameter):
        if "start" not in parameter or "end" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no start or end"}'
            return reply_str
        if not parameter["start"].isdigit() or not parameter["end"].isdigit():
            reply_str = '{"code":1,"errmsg":"parameter time fail"}'
            return reply_str
        start = int(parameter["start"])
        end = int(parameter["end"])
        if end < start:
            reply_str = '{"code":1,"errmsg":"The start time cannot be greater than the end time"}'
        elif end - start > 60 * 60 * 24 * 7:
            reply_str = '{"code":1,"errmsg":"Time can not exceed 7 day"}'
        else:
            dealDate_start = time.strftime("%Y%m%d%H%M%S", time.localtime(start))
            dealDate_end = time.strftime("%Y%m%d%H%M%S", time.localtime(end))
            all_count, data = self.__get_pay_frequency(dealDate_start, dealDate_end)
            reply_str = '{"code":0,"all_count":%d,"data":%s}' % (all_count, json.dumps(data,ensure_ascii=False))
        return reply_str

    def pay_total(self, parameter):
        if "start" not in parameter or "end" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no start or end"}'
            return reply_str
        if not parameter["start"].isdigit() or not parameter["end"].isdigit():
            reply_str = '{"code":1,"errmsg":"parameter time fail"}'
            return reply_str
        start = int(parameter["start"])
        end = int(parameter["end"])
        if end < start:
            reply_str = '{"code":1,"errmsg":"The start time cannot be greater than the end time"}'
        elif end - start > 60 * 60 * 24 * 7:
            reply_str = '{"code":1,"errmsg":"Time can not exceed 7 day"}'
        else:
            dealDate_start = time.strftime("%Y%m%d%H%M%S", time.localtime(start))
            dealDate_end = time.strftime("%Y%m%d%H%M%S", time.localtime(end))
            all_count, data = self.__get_pay_total(dealDate_start, dealDate_end)
            reply_str = '{"code":0,"all_count":%d,"data":%s}' % (all_count, json.dumps(data,ensure_ascii=False))
        return reply_str

    def gold_log(self, parameter):
        dealDate_start = None
        dealDate_end = None
        if "start" in parameter and "end" in parameter:
            if not parameter["start"].isdigit() or not parameter["end"].isdigit():
                reply_str = '{"code":1,"errmsg":"parameter time fail"}'
                return reply_str
            start = int(parameter["start"])
            end = int(parameter["end"])
            if end < start:
                reply_str = '{"code":1,"errmsg":"The start time cannot be greater than the end time"}'
                return reply_str
            elif end - start > 60 * 60 * 24 * 7:
                reply_str = '{"code":1,"errmsg":"Time can not exceed 7 day"}'
                return reply_str
            else:
                dealDate_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
                dealDate_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        accountName = None
        operation = None
        if "accountName" not in parameter and "operation" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName or operation"}'
            return reply_str
        if "accountName" in parameter:
            accountName = parameter["accountName"]
        if "operation" in parameter:
            operation = parameter["operation"]
        data = self.__get_gold_log(accountName, operation, dealDate_start, dealDate_end)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def player_info_count(self, parameter):
        sql = "select count(*) as count from ("
        index = 1
        for dbname in self._cache_mysql.db_info.keys():
            sql_select = "(select id from {databaseName}.tbl_Account)".format(
                databaseName=self._cache_mysql.db_info[dbname]["databaseName"])
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as player_info;"
        count = self._cache_mysql.get_count("default", sql)
        data = {"count":count,"page_size":50,"page_count":math.ceil(count / 50)}
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_info(self, parameter):
        if "page_no" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no page_no"}'
            return reply_str
        page_no = int(parameter["page_no"])
        if "orderby" not in parameter:
            parameter["orderby"] = "gold"
        if page_no <= 0 :
            page_no = 1
        if "sort" not in parameter:
            parameter["sort"] = "desc"
        sql = "select * from ("
        index = 1
        for dbname in self._cache_mysql.db_info.keys():
            sql_select = "(select accountName,sm_nickname as nickname,sm_gold as gold,sm_purify_prop as purify_prop," \
                         "sm_give_prop as give_prop,sm_renascence_prop as renascence_prop,sm_integral as integral," \
                         "sm_horseCount as horseCount,sm_businessHorseCount as businessHorseCount,sm_buyHorseCount as " \
                         "buyHorseCount,sm_sellHorseCount as sellHorseCount,sm_procreateHorseCount as procreateHorseCount," \
                         "sm_recharge_count as recharge_count,sm_recharge_sum as recharge_sum,sm_first_recharge as first_recharge," \
                         "sm_last_recharge as last_recharge,regtime from {databaseName}.tbl_Account as a, {databaseName}.kbe_accountinfos as " \
                         "b where a.id=b.entityDBID)".format(databaseName=self._cache_mysql.db_info[dbname]["databaseName"])
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as player_info order by %s %s limit %d, %d;" % (parameter["orderby"], parameter["sort"], (page_no-1)*50, 50)
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_detail_info(self, parameter):
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str
        name_hash = NameHash.NameHash(parameter["accountName"])
        dbname = name_hash.crcHash()
        sql = "select accountName,sm_nickname as nickname,sm_gold as gold,sm_purify_prop as purify_prop,sm_give_prop as " \
              "give_prop,sm_renascence_prop as renascence_prop,sm_integral as integral,sm_horseCount as horseCount,sm_businessHorseCount " \
              "as businessHorseCount,sm_buyHorseCount as buyHorseCount,sm_sellHorseCount as sellHorseCount,sm_procreateHorseCount " \
              "as procreateHorseCount,sm_recharge_count as recharge_count,sm_recharge_sum as recharge_sum,sm_first_recharge " \
              "as first_recharge,sm_last_recharge as last_recharge,regtime from tbl_Account as a, kbe_accountinfos as b where " \
              "a.id=b.entityDBID and a.sm_accountName='%s'" % parameter["accountName"]
        data = self._cache_mysql.get_list(dbname, sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_horse_detail_info(self, parameter):
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str
        name_hash = NameHash.NameHash(parameter["accountName"])
        dbname = name_hash.crcHash()
        sql = "select sm_hid as hid,sm_generation as generation,sm_race as race,sm_qua as qua,sm_grade as grade,sm_gene " \
              "as gene,sm_state as state,sm_cstate as cstate,sm_rstate as rstate,sm_inistr as inistr,sm_iniacc as iniacc," \
              "sm_inista as inista,sm_inipow as inipow,sm_iniski as iniski,sm_upinistr as upinistr,sm_upiniacc as upiniacc," \
              "sm_upinista as upinista,sm_upinipow as upinipow,sm_upiniski as upiniski,sm_sex as sex,sm_createtime as createtime," \
              "sm_procreate_time as procreate_time,sm_buy_time as buy_time from tbl_Account_horseinfo_values where sm_owner_name='%s'" % parameter["accountName"]
        data = self._cache_mysql.get_list(dbname, sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_procreate_horse_detail_info(self, parameter):
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str
        name_hash = NameHash.NameHash(parameter["accountName"])
        dbname = name_hash.crcHash()
        sql = "select sm_hid as hid,sm_generation as generation,sm_race as race,sm_qua as qua,sm_grade as grade,sm_gene " \
              "as gene,sm_state as state,sm_inistr as inistr,sm_iniacc as iniacc,sm_inista as inista,sm_inipow as inipow," \
              "sm_iniski as iniski,sm_upinistr as upinistr,sm_upiniacc as upiniacc,sm_upinista as upinista,sm_upinipow as " \
              "upinipow,sm_upiniski as upiniski,sm_sex as sex,sm_createtime as createtime,sm_procreate_time as procreate_time," \
              "sm_buy_time as buy_time from tbl_Account_procreatehinfo_values where sm_owner_name='%s'" % parameter["accountName"]
        data = self._cache_mysql.get_list(dbname, sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_gold(self, parameter):
        if "count" not in parameter:
            count = 100
        else:
            count = int(parameter["count"])
            if count > 1000:
                count = 1000
        sql = "select * from ("
        index = 1
        for dbname in self._cache_mysql.db_info.keys():
            sql_select = "(select accountName,sm_gold as gold,sm_purify_prop as purify_prop from {databaseName}.tbl_Account as a," \
                         " {databaseName}.kbe_accountinfos as b where a.id=b.entityDBID)".format(databaseName=self._cache_mysql.db_info[dbname]["databaseName"])
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as gold order by gold desc limit %s;" % count
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def horse_trade_log(self, parameter):
        sql = "select qua, count(id) as count, sum(gold) as gold, avg(gold) as avg_gold, sum(fee) fee from horse_trade_log group by qua;"
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def horse_procreate_log(self, parameter):
        sql = "select qua,count(id) as count, sum(gold) as golg, avg(gold) as avg_golg, sum(fee) fee from horse_procreate_log group by qua;"
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_horse_procreate_log(self, parameter):
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str
        sql = "select sowner_name,downer_name,shorse_hid,dhorse_hid,horse_uuid,horse_hid,horse_race,horse_qua,horse_sex," \
              "createTime from horse_procreate_log where sowner_name='%s';" % parameter["accountName"]
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str

    def player_givelog(self, parameter):
        """
        玩家用券记录查询
        """
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str
        sql = "select give,reasons,createTime from give_log where accountName='%s'" % parameter["accountName"]
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def player_purify_log(self, parameter):
        """
        玩家马草记录查询
        """
        if "accountName" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName"}'
            return reply_str

        sql = "select purify,reasons,createTime from give_log where accountName='%s'" % parameter["accountName"]
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def player_renascence_log(self, parameter):
        reply_str = '{"code":0,"data":[]}'
        return reply_str

    def business_horse(self, parameter):
        dic = {"1":0,"2":0,"3":0,"4":0}
        for dbname in self._cache_mysql.db_info.keys():
            self.__business_horse(dbname, dic)
        reply_str = '{"code":0,"data":[%s]}' % json.dumps(dic,ensure_ascii=False)
        return reply_str

    def business_horse_detail_info(self, parameter):
        """
        市场马详情
        """
        if "aname" in parameter and len(parameter["aname"]) > 0:
            sql = ""
            index = 1
            for dbname in self._cache_mysql.db_info.keys():
                sql_select = "(select * from %s.horseMarket where aname=%s)" % (self._cache_mysql.db_info[dbname]["databaseName"], parameter["aname"])
                if index == 1:
                    sql += sql_select
                else:
                    sql += "union" + sql_select
                index += 1
        else:
            if "sex" not in parameter or "quality" not in parameter:
                reply_str = '{"code":1,"errmsg":"parameter no sex quality"}'
                return reply_str
            sex = int(parameter["sex"])                 # 性别 1.母 2.公
            if sex not in (1, 2):
                reply_str = '{"code":1,"errmsg":"parameter sex error"}'
                return reply_str
            quality  = int(parameter["quality"])            # 品质类型 1普通; 2:优秀; 3:卓越; 4:完美
            if quality not in (1, 2, 3, 4):
                reply_str = '{"code":1,"errmsg":"parameter quality error"}'
                return reply_str
            QUALITY_1 = {1: (-1, 490), 2: (490, 990), 3: (990, 1390), 4: (1390, 1600)}
            QUALITY_2 = {1: (-1, 888), 2: (888, 1092), 3: (1092, 1271), 4: (1271, 1365)}
            sql = ""
            index = 1
            for dbname in self._cache_mysql.db_info.keys():
                sql_select = '(select * from %s.horseMarket' % self._cache_mysql.db_info[dbname]["databaseName"]
                if sex == 1:
                    sql_select += " where quality > %d and quality <= %d and sex=%d)" % (QUALITY_1[quality][0], QUALITY_1[quality][1], sex)
                elif sex == 2:
                    sql_select += " where quality > %d and quality <= %d and sex=%d)" % (QUALITY_2[quality][0], QUALITY_2[quality][1], sex)

                if index == 1:
                    sql += sql_select
                else:
                    sql += "union" + sql_select
                index += 1

            sql += " order by quality desc;"
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def reserve_horse(self, parameter):
        if "count" not in parameter:
            parameter["count"] = "1"
        if "accountName" in parameter:
            sql = "select accountName,gold,num,status,createTime,count from reserve_horse where accountName=%s and count=%s" % (parameter["accountName"],parameter["count"])
        else:
            sql = "select accountName,gold,num,status,createTime,count from reserve_horse where count=%s" % parameter["count"]
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data,ensure_ascii=False)
        return reply_str
 
    def horse_info(self, parameter):
        """
        玩家马详情
        """
        if ("sm_hid" in parameter and len(parameter["sm_hid"]) > 0) or ("sm_owner_name" in parameter and len(parameter["sm_owner_name"]) > 0):
            sql = ""
            index = 1
            for dbname in self._cache_mysql.db_info.keys():
                sql_select = "(select sm_hid, sm_generation, sm_race, sm_qua, sm_grade, sm_gene, sm_state, sm_cstate, sm_rstate, \
                             sm_inistr, sm_iniacc, sm_inista, sm_inipow, sm_iniski, sm_upinistr, sm_upiniacc, sm_upinista, sm_upinipow, \
                             sm_upiniski, sm_sex, sm_createtime, sm_procreate_time, sm_buy_time, sm_isTrade as isTrade, sm_owner_name as owner_name\
                             from %s.tbl_Account_horseinfo_values where " % self._cache_mysql.db_info[dbname]["databaseName"]
                if "sm_hid" in parameter and len(parameter["sm_hid"]) > 0:
                    where = "sm_hid like '%{sm_hid}%')".format(sm_hid=parameter["sm_hid"])
                else:
                    where = "sm_owner_name='%s')" % parameter["sm_owner_name"]
                sql_select += where
                if index == 1:
                    sql += sql_select
                else:
                    sql += "union" + sql_select
                index += 1
        else:
            if "sm_sex" not in parameter or "sm_qua_type" not in parameter:
                reply_str = '{"code":1,"errmsg":"parameter no sm_sex sm_qua_type"}'
                return reply_str
            sm_sex = int(parameter["sm_sex"])                 # 性别 1.母 2.公
            if sm_sex not in (1, 2):
                reply_str = '{"code":1,"errmsg":"parameter sex error"}'
                return reply_str
            sm_qua_type  = int(parameter["sm_qua_type"])            # 品质类型 1普通; 2:优秀; 3:卓越; 4:完美
            if sm_qua_type not in (1, 2, 3, 4):
                reply_str = '{"code":1,"errmsg":"parameter quality error"}'
                return reply_str
            if "sm_generation" in parameter and len(parameter["sm_generation"]) > 0:
                sm_generation = int(parameter["sm_generation"])
            else:
                sm_generation = -1

            sql = ""
            index = 1
            QUA_1 = {1:(-1, 490), 2:(490, 990), 3:(990, 1390), 4:(1390, 1600)}
            QUA_2 = {1:(-1, 888), 2:(888, 1092), 3:(1092, 1271), 4:(1271, 1365)}

            for dbname in self._cache_mysql.db_info.keys():
                sql_select = "(select sm_hid, sm_generation, sm_race, sm_qua, sm_grade, sm_gene, sm_state, sm_cstate, sm_rstate, \
                             sm_inistr, sm_iniacc, sm_inista, sm_inipow, sm_iniski, sm_upinistr, sm_upiniacc, sm_upinista, sm_upinipow, \
                             sm_upiniski, sm_sex, sm_createtime, sm_procreate_time, sm_buy_time, sm_isTrade as isTrade, sm_owner_name as owner_name\
                             from %s.tbl_Account_horseinfo_values" % self._cache_mysql.db_info[dbname]["databaseName"]
                if sm_sex > 0:
                    if sm_sex == 1:
                        sql_select += " where sm_qua > %d and sm_qua <= %d and sm_sex=%d" % (QUA_1[sm_qua_type][0], QUA_1[sm_qua_type][1], sm_sex)
                    elif sm_sex == 2:
                        sql_select += " where sm_qua > %d and sm_qua <= %d and sm_sex=%d" % (QUA_2[sm_qua_type][0], QUA_2[sm_qua_type][1], sm_sex)

                if sm_generation > -1:
                    sql_select += " and sm_generation=%d" % sm_generation
                sql_select += ")"
                if index == 1:
                    sql += sql_select
                else:
                    sql += "union" + sql_select
                index += 1
        sql += " order by sm_qua desc;"
        #DEBUG_MSG("Statistics.horse_info:sql=%s" % sql)
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def luck_draw_log(self, parameter):
        if "accountName" in parameter:
            sql = "select * from luck_draw_log where accountName='%s';" % parameter["accountName"]
        else:
            sql = "select * from luck_draw_log;"
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0, "data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def luck_draw_log_update(self, parameter):
        if "id" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter error"}'
        else:
            receiveTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            sql = "update luck_draw_log set status=1,receiveTime='%s' where id=%s;" % (receiveTime,parameter["id"])
            self._cache_mysql.execute("default", sql)
            reply_str = '{"code":0}'
        return reply_str

    def integral_log(self, parameter):
        dealDate_start = None
        dealDate_end = None
        if "start" in parameter and "end" in parameter:
            if not parameter["start"].isdigit() or not parameter["end"].isdigit():
                reply_str = '{"code":1,"errmsg":"parameter time fail"}'
                return reply_str
            start = int(parameter["start"])
            end = int(parameter["end"])
            if end < start:
                reply_str = '{"code":1,"errmsg":"The start time cannot be greater than the end time"}'
                return reply_str
            elif end - start > 60 * 60 * 24 * 7:
                reply_str = '{"code":1,"errmsg":"Time can not exceed 7 day"}'
                return reply_str
            else:
                dealDate_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
                dealDate_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        accountName = None
        operation = None
        if "accountName" not in parameter and "operation" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no accountName and operation"}'
            return reply_str
        if "accountName" in parameter:
            accountName = parameter["accountName"]
        if "operation" in parameter:
            operation = parameter["operation"]
        data = self.__get_integral_log(accountName, operation, dealDate_start, dealDate_end)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def __init__(self, cache_redis, cache_mysql, background):
        self._cache_redis = cache_redis
        self._cache_mysql = cache_mysql
        self._background = background
        """
        y = 2018
        for m in range(9,13):
            for d in range(1,32):
                ymd = "%d-%02d-%02d" % (y,m,d)
                sql = "insert into statistics_players_register(days)values('%s')" % ymd
                self._cache_mysql.execute("default", sql)
        """
    def __get_register(self, start, end):
        sql = "select count(*) from kbe_accountinfos where regtime>%i and regtime<=%i;" % (start, end)
        #DEBUG_MSG("Statistics.__get_register:sql=%s" % sql)
        count = 0
        for dbname in self._cache_mysql.db_info.keys():
            count += self._cache_mysql.get_count(dbname, sql)
        return count

    def __get_pay(self, start, end):
        sql = "select sum(money) from onlinePay where status=1 and dealDate>'%s' and dealDate<='%s';" % (start, end)
        #DEBUG_MSG("Statistics.__get_pay:sql=%s" % sql)
        return self._cache_mysql.get_count("default", sql)

    def __get_pay_frequency(self, start, end):
        sql = "select count(*) as num from onlinePay where status=1 and dealDate>'%s' and dealDate<='%s' group by accountName order by num desc;" % (start, end)
        #DEBUG_MSG("Statistics.__get_pay_frequency:sql=%s" % sql)
        return self._cache_mysql.get_count_dict("default", sql)

    def __get_pay_total(self, start, end):
        sql = "select sum(money) as num from onlinePay where status=1 and dealDate>'%s' and dealDate<='%s' group by accountName order by num desc;" % (start, end)
        #DEBUG_MSG("Statistics.__get_pay_total:sql=%s" % sql)
        return self._cache_mysql.get_count_dict("default", sql)

    def __get_gold_log(self, accountName, operation, start = None, end = None):
        sql = "select * from ("
        index = 1
        for dbname in self._cache_mysql.db_info.keys():
            sql_select = "(select accountName,gold,fee,reasons,createTime,remarks from {databaseName}.gold_log where ".format(databaseName=self._cache_mysql.db_info[dbname]["databaseName"])
            if accountName is not None:
                sql_select += "accountName=" + "'" + accountName + "'"
            if operation is not None:
                if accountName is not None:
                    sql_select += " and "
                    sql_select += "operation=" + "'" + operation + "'"
                else:
                    sql_select += "operation=" + "'" + operation + "'"
            if start is not None and end is not None:
                sql_select += " and createTime>'%s' and createTime<='%s')" % (start, end)
            else:
                sql_select += ")"

            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as gold order by createTime desc;"
        #DEBUG_MSG("Statistics.__get_gold_log:sql=%s" % sql)
        return self._cache_mysql.get_list("default", sql)

    def __get_integral_log(self, accountName, operation, start = None, end = None):
        sql = "select accountName,integral,reasons,createTime,remarks from integral_log where "
        if accountName is not None:
            sql += "accountName=" + "'" + accountName + "'"
        if operation is not None:
            if accountName is not None:
                sql += " and "
                sql += "operation=" + "'" + operation + "'"
            else:
                sql += "operation=" + "'" + operation + "'"
            if start is not None and end is not None:
                sql += " and createTime>'%s' and createTime<='%s' order by createTime desc;" % (start, end)
            else:
                sql += " order by createTime desc;"
        #DEBUG_MSG("Statistics.__get_gold_log:sql=%s" % sql)
        return self._cache_mysql.get_list("default", sql)

    def __get_recharge_count_sum(self, accountName):
        """
        获得充值次数
        """
        sql = "select count(money) as recharge_count,sum(money) as recharge_sum from onlinePay where accountName='%s' and status=1;" % accountName
        #DEBUG_MSG("Statistics.__get_recharge_count_sum:sql=%s" % sql)
        return self._cache_mysql.get_one("default", sql)

    def __business_horse(self, dbname, dic):
        sql = "select sex,quality,gene from horseMarket"
        # 打开数据库连接
        db = self._cache_mysql.conn_mysql(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        try:
            cur.execute(sql)  # 执行sql语句
            results = cur.fetchall()  # 获取查询的所有记录
            # 遍历结果
            for row in results:
                qua_type,_ = HelpFun.get_qua_type(row[0], row[1], row[2])
                dic[str(qua_type)] += 1
        except Exception as e:
            ERROR_MSG("Statistics.__business_horse:error=%s" % str(e))
        finally:
            db.close()  # 关闭连接

    def __player_recharge_data(self, accountName):
        sql = "select count(money) as recharge_count,sum(money) as recharge_sum,min(dealDate) as first_recharge,max(dealDate) as last_recharge from onlinePay where accountName='%s' and status=1;" % accountName
        recharge_data = self._cache_mysql.get_one("default", sql)
        return recharge_data

    def __player_horse_count(self, dbname, accountName):
        sql = "select count(sm_uuid) from tbl_Account_horseinfo_values where sm_owner_name='%s';" % accountName
        count = self._cache_mysql.get_count(dbname, sql)
        sql = "select count(sm_uuid) from tbl_Account_procreatehinfo_values where sm_owner_name='%s';" % accountName
        procreate_count = self._cache_mysql.get_count(dbname, sql)
        horse_count_dict = {"horse_count":count, "procreate_count":procreate_count}
        return horse_count_dict

    def statistics_players(self):
        """在线玩家数量统计"""
        i = datetime.datetime.now()
        year_month_day = i.strftime("%Y-%m-%d")
        if i.minute <= 15:
            minute = 15
        elif i.minute <= 30:
            minute = 30
        elif i.minute <= 45:
            minute = 45
        else:
            minute = 60
        hour_minute = "%dhour%d" % (i.hour, minute)
        sql = "update statistics_players_online set %s=%d where days='%s'" % (hour_minute, self._background.get_players(), year_month_day)
        self._cache_mysql.execute("default", sql)

    def statistics_players_register(self):
        """玩家每15分钟注册统计"""
        i = datetime.datetime.now()
        year_month_day = "%d-%d-%d" % (i.year, i.month, i.day)
        if i.minute <= 15:
            minute = 15
        elif i.minute <= 30:
            minute = 30
        elif i.minute <= 45:
            minute = 45
        else:
            minute = 60
        hour_minute = "%dhour%d" % (i.hour, minute)

        sql = 'select id from statistics_players_register where days=%s' % year_month_day
        per_day_register_record = self._cache_mysql.get_one("default", sql)

        if per_day_register_record:
            sql = "update statistics_players_register set %s=%s where days='%s'" % (hour_minute, self._background.get_register(), year_month_day)
        else:
            sql = 'insert into `statistics_players_register` (`%s`, days) value (%s, %s)' % (hour_minute, self._background.get_register(), year_month_day)
        self._cache_mysql.execute("default", sql)

    def private_message_log(self, parameter):
        """
        私信数据
        """
        required_params = ("fromAccountName","sort", "page_no", "page_size","startTime","endTime")
        if not HelpFun.check_required_params(required_params, parameter):
            return '{"code":1,"errmsg":"缺少必要参数"}'

        sort = parameter['sort']
        if sort not in ('asc', 'desc'):
            return '{"code":1,"errmsg":"排序类型不正确"}'

        if not parameter['page_no'].isdigit() or not parameter['page_size'].isdigit():
            return '{"code":1,"errmsg":"参数出错"}'

        page_no = int(parameter['page_no'])
        page_size = int(parameter['page_size'])
        startTime = int(parameter.get('startTime'))
        endTime = int(parameter.get('endTime'))
        if page_no <= 0 or page_size > 50:
            return '{"code":1,"errmsg":"参数出错"}'
        if not parameter.get('startTime').isdigit() or not parameter.get('endTime').isdigit():
            return '{"code":1,"errmsg":"参数错误"}'

        if (endTime-startTime)<0:
            return '{"code":1,"errmsg":"参数错误"}'
        fromAccountName = parameter.get('fromAccountName', '').strip()
        sql = "select * from private_message_log where sendTime between %d and %d" % (startTime,endTime)
        if fromAccountName:
            sql +=  " and (fromAccountName like '%{fromAccountName}%' or toAccountName like '%{fromAccountName}%')".format(fromAccountName=fromAccountName)

        sql += " order by id %s limit %d, %d;" % (sort, (page_no-1)*page_size, page_size)
        log_list = self._cache_mysql.get_list("default", sql)
        for x in log_list:
            x['msgContent']=base64.b64decode(x['msgContent']).decode()

        return '{"code":0,"data":%s}' % json.dumps(log_list,ensure_ascii=False)

    def message_log(self, parameter):
        """
        信息数据
        """
        required_params = ("startTime", "endTime", "sort", "page_no", "page_size")
        if not HelpFun.check_required_params(required_params, parameter):
            return '{"code":1,"errmsg":"缺少必要参数"}'

        sort = parameter['sort']
        if sort not in ('asc', 'desc'):
            return '{"code":1,"errmsg":"排序类型不正确"}'

        if not parameter['startTime'].isdigit() or not parameter['endTime'].isdigit():
            return '{"code":1,"errmsg":"参数出错"}'

        if not parameter['page_no'].isdigit() or not parameter['page_size'].isdigit():
            return '{"code":1,"errmsg":"参数出错"}'

        page_no = int(parameter['page_no'])
        page_size = int(parameter['page_size'])
        if page_no <= 0 or page_size > 50:
            return '{"code":1,"errmsg":"参数出错"}'

        begin_send_time = int(parameter['startTime'])
        end_send_time = int(parameter['endTime'])
        if begin_send_time > end_send_time:
            return '{"code":1,"errmsg":"开始时间不能大于结束时间"}'
        sql = "select * from message_log where sendTime between %d and %d" % (begin_send_time,end_send_time)
        if "accountName" in parameter:
            sql += " and accountName='%s'" % parameter["accountName"]
        sql += " order by id %s limit %d, %d;" % (sort, (page_no-1)*page_size, page_size)
        log_list = self._cache_mysql.get_list("default", sql)
        for x in log_list:
            x['msgContent']=base64.b64decode(x['msgContent']).decode()

        return '{"code":0,"data":%s}' % json.dumps(log_list, ensure_ascii=False)

    def reward_gift(self, parameter):
        """
        礼物奖励
        """
        if "start" not in parameter or "end" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no start or end"}'
            return reply_str
        if not parameter["start"].isdigit() or not parameter["end"].isdigit():
            reply_str = '{"code":1,"errmsg":"parameter time fail"}'
            return reply_str
        start = int(parameter["start"])
        end = int(parameter["end"])
        createTime_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        createTime_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        sql = "select accountName,gift_id,prop_id,num,state,redeem_code,uuid,hid,createTime from reward_gift where createTime>'%s' and createTime<='%s'" % (createTime_start, createTime_end)
        if "accountName" in parameter:
            if parameter['accountName']:
                sql += " and accountName='%s'" % parameter["accountName"]
        sql += " order by createTime desc;"
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            val["prop_name"] = HelpFun.get_prop_name(val["prop_id"])
        reply_str = '{"code":0,"data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def share_reward_log(self, parameter):
        """
        礼物奖励
        """
        if "start" not in parameter or "end" not in parameter:
            reply_str = '{"code":1,"errmsg":"parameter no start or end"}'
            return reply_str
        if not parameter["start"].isdigit() or not parameter["end"].isdigit():
            reply_str = '{"code":1,"errmsg":"parameter time fail"}'
            return reply_str
        start = int(parameter["start"])
        end = int(parameter["end"])
        createTime_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        createTime_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        sql = "select accountName,reward_count,createTime from share_reward_log where createTime>'%s' and createTime<='%s'" % (createTime_start, createTime_end)
        if "accountName" in parameter:
            sql += " and accountName='%s'" % parameter["accountName"]
        sql += " order by createTime desc;"
        data = self._cache_mysql.get_list("default", sql)
        reply_str = '{"code":0,"data":%s}' % json.dumps(data, ensure_ascii=False)
        return reply_str

    def account_detail(self,request:dict):
        """用户数据统计：留存，ACU,APU"""
        if not HelpFun.check_required_params(['start','end'],request):
            return {"code": 1, "errmsg": "参数错误"}
        if not request.get('start').isdigit() or not request.get('end').isdigit():
            return {"code": 2, "errmsg": "时间参数错误"}

        start=int(request.get('start',0))
        end=int(request.get('end',0))

        if start>end or int(end-start)/(24*3600)>31:
            return {"code": 1, "errmsg": "参数错误:开始时间大于结束时间或大于31天"}
        listDataSum,listDataDay=self.account_detail_sum_by_time(start,end)

        return {"code":0,"data":{"dataSum":listDataSum,"listDataDay":listDataDay}}

    def account_detail_sum_by_time(self,start:int,end:int):
        """查询期间数据汇总"""
        class cData:
            def __init__(self):
                self.date=0                             # 日期
                self.register_count=0                   # 注册总数
                self.real_register_count=0              # 实际注册总数
                self.login_count=0                      # 登录用户数
                self.active_conv_count=0                # 活跃转化数 查询时间内有（喂养、付费、竞技、买卖，4选3）操作的玩家数量
                self.pay_count=0                        # 付费用户数
                self.max_online=0                       # 在线峰值
                self.acu=0                              # acu 查询时间内每小时同时在线人数（每小时峰值）总和/T的小时数
                self.avg_online_time=0                  # 平均在线时长
                self.pay_rate=0                         # 付费率
                self.active_arpu_val=0                  # 活跃arpu值    【付费金额】 / 【活跃转化数】
                self.pay_arpu_val=0                     # 付费arpu值    【付费金额】 / 【付费用户数】
                self.new_user_pay_count=0               # 新用户付费人数
                self.new_user_pay_sum=0                 # 新用户付费金额
                self.pay_sum=0                          # 付费金额
                self.next_day_retain=0                  # 次日留存
                self.day3_retain=0                      # 3日留存
                self.day7_retain=0                      # 7日留存

        days=int((end-start)/(24*3600))
        if (end-start)<=86400:
            days=1
        classDataSum=cData()
        sumMaxOnlineCount=0
        sumOLTime=0
        listDataDay=[]
        for i in range(days):
            classDataDay=cData()
            classDataDay.date=start+i*24*3600
            each_start=start+i*24*3600
            each_end=start+(i+1)*24*3600
            if days==1:
                each_start=start
                each_end=end
            dictLoginCount=self._cache_mysql.get_one('default',getLoginCountSqlByTime(each_start,each_end))
            active_conv_count=getActiveConvCount(each_start,each_end,self._cache_mysql)
            dictPayInfo=self._cache_mysql.get_one('default',getPayCountSqlByTime(each_start,each_end))
            dictMaxOnline=self._cache_mysql.get_one('default',getMaxOnlineSqlByTime(each_start))
            dictACU=self._cache_mysql.get_one('default',getACUSqlByTime(each_start))
            dictNewPay=self._cache_mysql.get_one('default',getNewPaySqlByTime(each_start,each_end,self._cache_mysql))
            daySumOLTime=getAvgOnlineTimeByTime(each_start,self._cache_mysql)

            #############每日数据##############
            classDataDay.active_conv_count=active_conv_count                                                            # 活跃转化数
            classDataDay.pay_count=dictPayInfo.get('payCount',0)                                                        # 付费用户数
            classDataDay.pay_sum=int(dictPayInfo.get('payNum')) if dictPayInfo.get('payNum') else 0                     # 付费总额
            classDataDay.max_online=dictMaxOnline.get('max_online',0)                                                   # 在线峰值
            classDataDay.login_count=dictLoginCount.get('login_count',0)                                                # 登录用户数
            classDataDay.avg_online_time=int(daySumOLTime/classDataDay.login_count) if classDataDay.login_count else 0  # 平均在线时长:T时间内所有用户在线时间总值/【登入用户数】
            classDataDay.active_arpu_val=0 if classDataDay.active_conv_count==0 else round(classDataDay.pay_sum/classDataDay.active_conv_count,2)   # 活跃arpu值【付费金额】/【活跃转化数】
            dayMaxOnlineCount=0
            if len(dictACU)!=0:
                for count in dictACU.values():
                    dayMaxOnlineCount += count
            classDataDay.acu=round(dayMaxOnlineCount/24)                                                                     # acu 每小时在线峰值总和/24小时
            classDataDay.pay_arpu_val=0 if classDataDay.pay_count==0 else round(classDataDay.pay_sum/classDataDay.pay_count,2) # 付费arpu值【付费金额】/【付费用户数】
            classDataDay.new_user_pay_sum=0 if dictNewPay.get('pay_money') is None else int(dictNewPay.get('pay_money'))       # 新用户付费金额	T时间内新注册的用户充值金额总数
            classDataDay.new_user_pay_count=dictNewPay.get('pay_count',0)                                                      # 新用户付费人数	T时间内新注册的用户充值用户总数，1个用户多次充值算1次计数
            classDataDay.register_count,classDataDay.next_day_retain, classDataDay.day3_retain, classDataDay.day7_retain = getRetainDataByTime(             # 单日注册量,次日，3日，7日留存
                start + i * 24 * 3600, start + (i + 1) * 24 * 3600,self._cache_mysql)
            classDataDay.pay_rate="%.2f%%"%(round(classDataDay.pay_count/classDataDay.login_count,4)*100) if classDataDay.login_count else "0.00%"          #付费率
            classDataDay.real_register_count=classDataDay.register_count                        # todo 没有表数据暂时实际注册总量等于注册总量

            #############汇总数据##############
            classDataSum.register_count += classDataDay.register_count
            classDataSum.real_register_count+=classDataDay.real_register_count
            sumMaxOnlineCount+=dayMaxOnlineCount
            sumOLTime+=classDataDay.avg_online_time
            classDataSum.new_user_pay_sum+=classDataDay.new_user_pay_sum
            classDataSum.new_user_pay_count+=classDataDay.new_user_pay_count
            dictDataDay=convert_to_dict(classDataDay)
            listDataDay.append(dictDataDay)

        # 汇总数据
        classDataSum.acu = sumMaxOnlineCount/(days*24)
        dictSumPayInfo=self._cache_mysql.get_one('default',getPayCountSqlByTime(start,end))
        classDataSum.pay_count = dictSumPayInfo.get('payCount',0)
        classDataSum.pay_sum =int(dictSumPayInfo.get('payNum')) if dictSumPayInfo.get('payNum') else 0
        classDataSum.active_conv_count=getActiveConvCount(start , end,self._cache_mysql)
        classDataSum.login_count =self._cache_mysql.get_one('default',getLoginCountSqlByTime(start,end)).get('login_count',0)
        classDataSum.acu=round(classDataSum.acu/days)
        classDataSum.max_online = max(map(lambda dictDay:dictDay['max_online'],listDataDay))
        classDataSum.active_arpu_val=0 if classDataSum.active_conv_count==0 else round(classDataSum.pay_sum/classDataSum.active_conv_count,2)
        classDataSum.date="{}至{}".format(timestamp2Str(start),timestamp2Str(end))
        classDataSum.pay_rate="%.2f%%"%(round(classDataSum.pay_count/classDataSum.login_count,4)*100) if classDataSum.login_count else "0.00%"
        classDataSum.pay_arpu_val=round(classDataSum.pay_sum/classDataSum.pay_count,2) if classDataSum.pay_count else 0
        classDataSum.avg_online_time=int(sumOLTime/days)

        listDataSum=[]
        dataSum=convert_to_dict(classDataSum)
        dataSum.pop('next_day_retain'),dataSum.pop('day3_retain'),dataSum.pop('day7_retain')
        listDataSum.append(dataSum)
        listDataDay.reverse()
        return  listDataSum,listDataDay

    def get_player_prop_list(self,request:dict):
        """获取用户背包道具"""
        if not HelpFun.check_required_params(['accountName'],request):
            return {'code': 1, 'errmsg': '参数错误'}
        accountName=request.get('accountName','')
        if not accountName:
            return {'code':1,'errmsg':'参数错误'}
        hash_name=NameHash.NameHash(accountName).crcHash()
        sql="select * from {dbname}.tbl_Account_other_prop_list_values where parentID=(select entityDBID from {dbname}.kbe_accountinfos " \
            "where accountName={accountName});".format(dbname=self._cache_mysql.db_info[hash_name]['databaseName'],accountName=accountName)

        return {'code':0,'data':self._cache_mysql.get_list(hash_name,sql)}

    def del_other_prop(self,request:dict):
        """删除用户背包道具"""
        if not HelpFun.check_required_params(['accountName','prop_id','count'],request):
            return {"code": 1, "errmsg": "参数不足"}

        if not request.get('count').isdigit() or not request.get('prop_id').isdigit():
            return {'code':2,'errmsg':'参数错误'}
        prop_id=int(request.get('prop_id'))
        accountName=request.get('accountName','')
        count=int(request.get('count'))
        if not all([prop_id,accountName,count]):
            return {'code': 2, 'errmsg': '参数错误'}

        strData=self._background.del_other_prop(accountName,prop_id,count)
        dictData=json.loads(strData)
        if dictData.get('code','')!=0:
            return {'code': 1, 'errmsg': '删除背包道具失败'}

        return {'code':0,'data':'删除背包道具成功'}

    def get_red_packet(self,request:dict):
        """获取红包数据"""
        if not HelpFun.check_required_params(['accountName','start','end'],request):
            return {"code": 1, "errmsg": "参数不足"}

        accountName=request.get('accountName','')
        if not request.get('start').isdigit() or not request.get('end').isdigit():
            return {"code": 1, "errmsg": "时间参数错误"}
        start=int(request.get('start'))
        end=int(request.get('end'))
        if start>end:
            return {"code":2,"errmsg":"时间错误"}

        p_sql="select * from red_packet where gold<>0 "

        if start and end:
            p_sql+=" and createTime between {} and {} ".format(start,end)

        if accountName:
            p_sql+="and accountName='{}' ".format(accountName)
        p_sql+="order by createTime ;"
        listSendInfo=self._cache_mysql.get_list('default',p_sql)

        return {"code":0,"data":listSendInfo}

    def get_child_packet(self,request:dict):
        """获取抢红包者数据"""
        if not HelpFun.check_required_params(['packet_id'],request):
            return {"code": 1, "errmsg": "参数错误"}

        packet_id=request.get('packet_id')
        if not packet_id:
            return {"code": 1, "errmsg": "参数错误"}
        sql="select * from rob_red_packet_record where red_packet_id='{}';".format(packet_id)
        listData=self._cache_mysql.get_list('default',sql)
        return {"code":0,"data":listData}

    def get_excel_names(self,request:dict):
        """获取所有excel名称"""
        parent_path = os.path.dirname(os.path.realpath(__file__))+"/excel"
        listFileName=[]
        for _,_,fileName in os.walk(parent_path):
            listFileName=fileName
        list1=[]
        for i,strData in enumerate(listFileName):
            dictData=dict()
            dictData['id']=i
            end_str=strData.split('-',1)[1]
            if strData.startswith('gold'):
                dictData['cn_name']="金币流水表-{}".format(end_str)
            elif strData.startswith('charge'):
                dictData['cn_name'] = "充值数据表-{}".format(end_str)
            else:
                dictData['cn_name'] = "积分流水表-{}".format(end_str)
            dictData['name']=strData
            list1.append(dictData)
        return {"code":0,"data":list1}

    def get_match_info(self,request:dict):
        """获取比赛信息"""
        if not HelpFun.check_required_params(['accountName','start','end','coinType','fee'],request):
            return {'code':1,'errmsg':'参数不足'}
        if not request.get('start').isdigit() or not request.get('end').isdigit():
            return {'code':2,'errmsg':'时间参数错误'}
        accountName,start,end=request.get('accountName'),int(request.get('start')),int(request.get('end'))
        coinType,fee=request.get('coinType'),request.get('fee')

        sql="select id,room_id,num,cointype,fee,tax1,tax2,first,second,third,createTime from competition_record where room_id<>'' "
        if accountName:
            sql+=" and "
            for x in range(1,11):
                if x==10:
                    sql += " accountName{}='{}' ".format(x, accountName)
                else:
                    sql += " accountName{}='{}' or".format(x, accountName)
        if start and end:
            if start>end:
                return {'code':2,'errmsg':'时间参数错误'}
            sql+=" and createTime between FROM_UNIXTIME({}) and FROM_UNIXTIME({}) ".format(start,end)
        if coinType:
            sql+=" and coinType={} ".format(coinType)
        if fee:
            sql+=" and fee={} ".format(fee)
        sql+=" order by createTime desc,coinType desc,fee desc ;"
        listMatchInfo=self._cache_mysql.get_list('default',sql)
        return {'code':0,'data':listMatchInfo}

    def get_match_detail(self,request:dict):
        """获取单场比赛排名情况"""
        id=request.get('id','')
        if not id:
            return {'code':1,'errmsg':'参数错误'}
        sql="select "
        for x in range(1,11):
            if x==10:
                sql += " accountName{},uuid{},hid{},runTime{} ".format(x,x,x,x)
            else:
                sql += " accountName{},uuid{},hid{},runTime{},".format(x,x,x,x)
        sql+=" from competition_record where id='{}';".format(id)
        listData=[]
        dictData=self._cache_mysql.get_one('default',sql)
        for x in range(1,11):
            dict_x=dict()
            for tupInfo in dictData.items():
                if tupInfo[0].endswith(str(x)):
                    dict_x['id']=x
                    if x==10:
                        dict_x[tupInfo[0][:-2]] = tupInfo[1]
                    else:
                        dict_x[tupInfo[0][:-1]]=tupInfo[1]

            listData.append(dict_x)

        return {'code':0,'data':listData}

    def get_history_online(self,request:dict):
        """获取历史在线数据"""
        if not HelpFun.check_required_params(['start','end'],request):
            return {'code':1,'errmsg':'参数不足'}
        if not request.get('start').isdigit() or not request.get('end').isdigit():
            return {'code': 2, 'errmsg': '时间参数错误'}
        start=int(request.get('start'))
        end=int(request.get('end'))
        if start>end or end>getNow():
            return {'code': 2, 'errmsg': '时间参数错误'}
        #unix_timestamp('2018-09-01')
        sql="select * from statistics_players_online where unix_timestamp(days) between {} and {} order by days desc;".format(start,end)
        listData=self._cache_mysql.get_list('default',sql)
        return {'code':0,'data':listData}

    def get_history_charge(self,request:dict):
        """获取充值历史数据"""
        if not HelpFun.check_required_params(['start','end'],request):
            return {'code':1,'errmsg':'参数不足'}
        if not request.get('start').isdigit() or not request.get('end').isdigit():
            return {'code': 2, 'errmsg': '时间参数错误'}
        start=int(request.get('start'))
        end=int(request.get('end'))
        days=int((end-start)/86400)
        if start>end or days<1:
            return {'code': 2, 'errmsg': '时间参数错误'}
        listData=[]
        for i in range(days):
            dictInfo=dict()
            sql="select count(id) as counts,count(distinct accountName) as peoples,sum(money) as money,sum(gold) as gold " \
                "from onlinePay where status=1 and create_time between FROM_UNIXTIME({}) and FROM_UNIXTIME({});"\
                .format(start+i*86400,start+(i+1)*86400)
            dictData=self._cache_mysql.get_one('default',sql)
            dictInfo['date']=timestamp2Str(start+i*86400,"%Y-%m-%d")
            dictInfo['count']=dictData.get('counts',0)
            dictInfo['peoples']=int(dictData.get('peoples')) if dictData.get('peoples') else 0
            dictInfo['money']=int(dictData.get('money')) if dictData.get('money') else 0
            dictInfo['gold']=int(dictData.get('gold')) if dictData.get('gold') else 0
            listData.append(dictInfo)

        return {'code':0,'data':listData}

    def get_charge_detail(self,request:dict):
        """获取充值详细数据"""
        if 'date' not in request:
            return {'code':1,'errmsg':'参数错误'}
        date=request.get('date')
        isDate=re.match(r'\d{4}-\d{1,2}-\d{1,2}', date)
        if isDate is None:
            return {'code':1,'errmsg':'时间参数错误'}
        start=strToTimestamp(date,"%Y-%m-%d")
        end=start+86400
        sql="select orderid,accountName,payway,money,gold,create_time,dealDate,trxNo,status_str from onlinePay where status=1 " \
            "and create_time between FROM_UNIXTIME({}) and FROM_UNIXTIME({});".format(start,end)
        listData=self._cache_mysql.get_list('default',sql)
        return {'code':0,'data':listData}

    def daily_lottery_data(self,request:dict):
        """每日抽奖活动数据"""
        start=1538323200   #10月1号开始
        end=start
        listData=[]
        while True:
            dictData=dict()
            dictData['date']=timestamp2Str(end,'%Y-%m-%d')
            dictDailyData=self._cache_redis.get_daily_lottery_data(timestamp2Str(end,'%Y-%m-%d'))
            listEnterUsers=dictDailyData.pop('listEnterUsers')
            if len(listEnterUsers)==0:
                dictData['dailyLoginCount'] =0
            else:
                str1=""
                for i,x in enumerate(listEnterUsers):
                    if i ==len(listEnterUsers)-1:
                        str1 += "'%s'" % x
                    else:
                        str1+="'%s',"%x
                sql = "select count(*) as loginCount from daily_login_statis where accountName in ({}) and lastLoginTime between {} and {};".format(str1,end,end+86400)
                dictLoginCount=self._cache_mysql.get_one('default',sql)
                dictData['dailyLoginCount']=dictLoginCount.get('loginCount')
            dictData.update(dictDailyData)
            # 每日第一次抽奖人数；
            first_count_sql="select count(DISTINCT(account_name)) as firstCount from lottery_record where create_time between {} and {};".format(end,end+86400)
            dictFirstCount=self._cache_mysql.get_one('default',first_count_sql)
            dictData.update(dictFirstCount)
            # 每日第二次抽奖人数；
            second_count_sql="select count(*) as secondCount from (select count(account_name) from lottery_record where " \
                             "create_time between {} and {} group by account_name having count(account_name)>=2) as a;".format(end,end+86400)
            dictSecondCount=self._cache_mysql.get_one('default',second_count_sql)
            dictData.update(dictSecondCount)
            listData.append(dictData)
            if timestamp2Str(end,'%Y-%m-%d')==timestamp2Str(getNow(),'%Y-%m-%d'):
                break
            end+=86400
        # 该活动带来新注册用户数；
        iRegisterCount = self._cache_redis.redis_conn.hlen(self._cache_redis.lottery_register_key)

        return {'code':0,'data':listData,'reg_count':iRegisterCount}

    def daily_lottery_record(self,request:dict):
        """每日中奖记录"""
        if 'date' not in request:
            return {'code':1,'errmsg':'参数错误!'}
        date=request.get('date','')
        if not date:
            return {'code': 1, 'errmsg': '参数错误!'}
        start=strToTimestamp(date,'%Y-%m-%d')
        end=start+86400
        sql="select * from lottery_record where create_time between {} and {} ;".format(start,end)
        listData=self._cache_mysql.get_list('default',sql)
        return {'code':0,'data':listData}


if __name__ == '__main__':
    pass

