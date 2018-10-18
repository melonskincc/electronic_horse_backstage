# -*- coding: utf-8 -*-
import json
import threading
from logger import *

class share:
    share_count_3 = 3
    share_count_5 = 5
    share_count_7 = 7
    share_count_10 = 10
    share_count_15 = 15
    share_reward = {
        share_count_3: {"prop_id": 5001002, "prop_count": 1, "prop_name": "马鞍", "type": 1},
        share_count_5: {"prop_id": 5002002, "prop_count": 1, "prop_name": "耳罩", "type": 1},
        share_count_7: {"prop_id": 5003002, "prop_count": 1, "prop_name": "龙头", "type": 1},
        share_count_10: {"prop_id": 5004002, "prop_count": 1, "prop_name": "护腿", "type": 1},
        share_count_15: {"sex": 1, "count": 1, "level": 1, "generation": 1, "prop_name": "活动1代母马", "type": 2}
    }
    def __init__(self, cache_mysql, background):
        self._cache_mysql = cache_mysql
        self._background = background
        self._needRewardList = list()
        self._mutex = threading.Lock()

    def reward(self, accountName, invitationCode):
        DEBUG_MSG("accountName=%s, invitationCode=%s" % (accountName, invitationCode))
        if not accountName or not invitationCode:
            return False
        self._mutex.acquire()
        self._needRewardList.append({"accountName": accountName, "invitationCode": invitationCode})
        self._mutex.release()
        return True

    def handle_reward(self):
        if len(self._needRewardList) > 0:
            self._mutex.acquire()
            rewardList = self._needRewardList
            self._needRewardList = list()
            self._mutex.release()
            for val in rewardList:
                # 得到该玩家邀请人数
                count = self._get_count(val["invitationCode"])
                DEBUG_MSG("accountName=%s, invitationCode=%s, count=%d" % (val["accountName"], val["invitationCode"], count))
                self._handle_reward(val["accountName"], val["invitationCode"], count)

    def _handle_reward(self, accountName, invitationCode, count):
        if count >= self.share_count_15:
            if not self._is_reward(accountName, self.share_count_3):
                self._reward(accountName, self.share_count_3)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_5):
                self._reward(accountName, self.share_count_5)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_7):
                self._reward(accountName, self.share_count_7)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_10):
                self._reward(accountName, self.share_count_10)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_15):
                self._reward(accountName, self.share_count_15)
        elif count >= self.share_count_10:
            if not self._is_reward(accountName, self.share_count_3):
                self._reward(accountName, self.share_count_3)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_5):
                self._reward(accountName, self.share_count_5)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_7):
                self._reward(accountName, self.share_count_7)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_10):
                self._reward(accountName, self.share_count_10)
        elif count >= self.share_count_7:
            if not self._is_reward(accountName, self.share_count_3):
                self._reward(accountName, self.share_count_3)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_5):
                self._reward(accountName, self.share_count_5)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_7):
                self._reward(accountName, self.share_count_7)
        elif count >= self.share_count_5:
            if not self._is_reward(accountName, self.share_count_3):
                self._reward(accountName, self.share_count_3)
                self.reward(accountName, invitationCode)
                return
            if not self._is_reward(accountName, self.share_count_5):
                self._reward(accountName, self.share_count_5)
        elif count >= self.share_count_3:
            if not self._is_reward(accountName, self.share_count_3):
                self._reward(accountName, self.share_count_3)
        else:
            pass
        return

    def _reward(self, accountName, reward_count):
        share_reward = self.share_reward[reward_count]
        if reward_count == self.share_count_15:
            data = self._background.share_horse_build(accountName, share_reward["count"], share_reward["sex"], share_reward["level"], share_reward["generation"])
        else:
            data = self._background.share_add_prop(accountName, share_reward["prop_id"], share_reward["prop_count"])
        DEBUG_MSG("accountName=%s, data=%s" % (accountName, data))
        data_dict = json.loads(data)
        if data_dict["code"] == 0 and data_dict["success"] == 1:
            sql = "insert into share_reward_log(accountName,reward_count)values('%s',%d);" % (accountName, reward_count)
            self._cache_mysql.execute("default", sql)

    def _get_count(self, invitationCode):
        sql = "select count(sm_accountName) as count from ("
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select sm_accountName from {databaseName}.tbl_Account where sm_invitationCode ='{invitationCode}')".format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"],invitationCode=invitationCode)
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as invitation;"
        count = self._cache_mysql.get_count("default", sql)
        return count

    def _is_reward(self, accountName, reward_count):
        """
        判断是否奖励过
        """
        sql = "select id from share_reward_log where accountName='%s' and reward_count=%d;" % (accountName, reward_count)
        data = self._cache_mysql.get_one("default", sql)
        if data is not None and len(data) > 0 :
            return True
        return False