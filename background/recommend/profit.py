# -*- coding: utf-8 -*-
import time
from lib.gen_excel import GenExcel
from lib.timeHelp import strToTimestamp, timestamp2Str, getNow, monthStartTimestamp
from logger import *
from official.define import  AgentRate,AgentGiftGold


class profit:

    def statistics_agent_register_count(self, accountName, channelCode, start, end):
        """
        统计出一个代理（合伙人也是代理）多少注册用户
        """
        sql = "select count(accountName) as count from ("
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select accountName from {databaseName}.kbe_accountinfos,{databaseName}.tbl_Account \
                         where {databaseName}.kbe_accountinfos.accountName={databaseName}.tbl_Account.sm_accountName and \
                         {databaseName}.tbl_Account.sm_accountName<>'{accountName}' and \
                         {databaseName}.tbl_Account.sm_channelCode='{channelCode}' and {databaseName}.kbe_accountinfos.regtime>={start} and \
                         {databaseName}.kbe_accountinfos.regtime<={end})".\
                         format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"],accountName=accountName,channelCode=channelCode,start=start,end=end)
                         
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as register;"

        count = self._cache_mysql.get_count("default", sql)
        return count

    def statistics_user(self, accountName, channelCode, start, end):
        """
        统计出旗下的用户获得到抽佣人民币（充值）和骑士币（税）
        "Pay":"充值" （gold）
        "Publish":"上架市场"（gold）
        "Purify":"洗练"（gold）
        "ProcreateHorse":"生小马"（fee）
        "Buy":"买马"（fee）
        """
        createTime_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        createTime_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        sql = "select * from ("
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select sum(gold) as gold, sum(fee) as fee, operation,sm_accountName as accountName, sm_uuid as uuid, regtime, sm_channelCode as channelCode\
                         from {databaseName}.gold_log, {databaseName}.tbl_Account,{databaseName}.kbe_accountinfos \
                         where {databaseName}.gold_log.accountName={databaseName}.tbl_Account.sm_accountName \
                         and {databaseName}.kbe_accountinfos.accountName={databaseName}.tbl_Account.sm_accountName \
                         and {databaseName}.tbl_Account.sm_accountName<>'{accountName}' \
                         and {databaseName}.tbl_Account.sm_channelCode='{channelCode}' \
                         and {databaseName}.gold_log.createTime>='{createTime_start}' \
                         and {databaseName}.gold_log.createTime<='{createTime_end}' \
                         group by {databaseName}.tbl_Account.sm_accountName,{databaseName}.gold_log.operation)".\
                         format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"],
                                accountName=accountName,
                                channelCode=channelCode,
                                createTime_start=createTime_start,
                                createTime_end=createTime_end)

            if index == 1:
                sql += sql_select
            else:
                sql += "union all" + sql_select
            index += 1
        sql += ") as gold_fee;"
        dic = dict()
        data = self._cache_mysql.get_list("default", sql)

        for val in data:
            recharge = 0
            tax = 0
            if val["accountName"] not in dic:
                dic[val["accountName"]] = {"recharge":0,"tax":0, "uuid":val["uuid"], "regtime":val["regtime"], "channelCode":val['channelCode'], "accountName":val['accountName']}
            if val["operation"] == "Pay":
                recharge += float(val["gold"])
            elif val["operation"] == "Publish" or val["operation"] == "Purify":
                tax += float(val["gold"])
            elif val["operation"] == "ProcreateHorse" or val["operation"] == "Buy":
                tax += float(val["fee"])
            else:
                pass
            if recharge != 0:
                dic[val["accountName"]]["recharge"] += recharge
            if tax != 0:
                dic[val["accountName"]]["tax"] += tax
        return dic
 
    def statistics_agent(self, accountName, channelCode, start, end):
        """
        统计出一个代理（合伙人也是代理）的获得到抽佣人民币（充值）和骑士币（税）
        "Pay":"充值" （gold）
        "Publish":"上架市场"（gold）
        "Purify":"洗练"（gold）
        "ProcreateHorse":"生小马"（fee）
        "Buy":"买马"（fee）
        """
        createTime_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        createTime_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        sql = "select sum(gold) as gold, sum(fee) as fee, operation from ("
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select gold, fee, operation from {databaseName}.gold_log,{databaseName}.tbl_Account where " \
                         "{databaseName}.gold_log.accountName={databaseName}.tbl_Account.sm_accountName and {databaseName}" \
                         ".tbl_Account.sm_accountName<>'{accountName}' and {databaseName}.tbl_Account.sm_channelCode='{channelCode}' " \
                         "and createTime>='{createTime_start}' and createTime<='{createTime_end}')"\
                .format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"],
                        accountName=accountName,channelCode=channelCode,
                        createTime_start=createTime_start,createTime_end=createTime_end)
            if index == 1:
                sql += sql_select
            else:
                sql += "union all" + sql_select
            index += 1
        sql += ") as gold_fee group by operation;"
        recharge = 0
        tax = 0
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            if val["operation"] == "Pay":
                recharge += float(val["gold"])
            elif val["operation"] == "Publish" or val["operation"] == "Purify":
                tax += float(val["gold"])
            elif val["operation"] == "ProcreateHorse" or val["operation"] == "Buy":
                tax += float(val["fee"])
            else:
                pass
        return recharge, tax

    def insert_agent_statistics(self, agent_id, pid, ppid, level, charge_gold, tax_fee, register_count, statistics_month):
        TaxRate={1:AgentRate.ChildLevel1TaxRate.value,2:AgentRate.ChildLevel2TaxRate.value,3:AgentRate.ChildLevel3TaxRate.value}
        sql = "insert into agent_statistics(agent_id,pid,ppid,level,charge_gold,tax_fee,rec_num,statistics_month,rake_back_amount,rake_back_tax) values('%s','%s','%s','%s',%i,%i,%i,'%s',%i,%i);" %\
              (agent_id, pid, ppid, level, charge_gold, abs(tax_fee), register_count, statistics_month,int((charge_gold/10)*AgentRate.ChildChargeRate.value),int(abs(tax_fee)*TaxRate.get(int(level))))
        self._cache_mysql.execute('default', sql)
 
    def update_agent_tax_statistics(self, agent_id, agent_tax_fee, statistics_month):
        sql = "update agent_statistics set agent_tax_fee=%i where agent_id='%s' and statistics_month='%s';" % (agent_tax_fee, agent_id, statistics_month)
        self._cache_mysql.execute('default', sql)

    def update_agent_agent_tax_statistics(self, agent_id, agent_agent_tax_fee, statistics_month):
        # 统计出间推代理税金收益
        sql = "update agent_statistics set agent_agent_tax_fee=%i where agent_id='%s' and statistics_month='%s';" % (agent_agent_tax_fee, agent_id, statistics_month)
        self._cache_mysql.execute('default', sql)

    def update_agent_amount_statistics(self, agent_id, agent_count, agent_amount, statistics_month):
        sql = "update agent_statistics set agent_count=%i,agent_amount=%i,agent_rake_back_amount=%i where agent_id='%s' and statistics_month='%s';" % (agent_count, agent_amount,agent_amount*AgentRate.AmountRate.value, agent_id, statistics_month)
        self._cache_mysql.execute('default', sql)

    def statistics_all_agent(self, start, end, statistics_month):
        """
        统计所有代理下面所有玩家充值和税金（玩家推荐玩家也算这个代理（渠道号）的用户，但玩家成为代理时该玩家（代理）下面的玩家就不再是上面的代理玩家了）
        """
        sql = "select id, pid, ppid, mobile, level, channelcode, gold_rate, tax_rate,audit_time from agent where apply_status=1;"
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            if val['audit_time']>start:
                start=val['audit_time']
            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            register_count = self.statistics_agent_register_count(val["mobile"], val["channelcode"], start, end)
            self.insert_agent_statistics(val["id"], val["pid"], val["ppid"], val['level'], recharge, tax, register_count, statistics_month)

    def statistics_all_agent_agent_tax(self, statistics_month):
        """
        统计所有下级代理人税金
        """
        sql = "select sum(tax_fee) as tax_fee,pid from agent_statistics where statistics_month='%s' and pid<>''  group by pid;" % statistics_month
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            self.update_agent_tax_statistics(val["pid"],float(val["tax_fee"]),statistics_month)

    def statistics_all_agent_agent_agent_tax(self, statistics_month):
        """
        统计所有下下级代理人税金
        """
        sql = "select sum(tax_fee) as tax_fee,ppid from agent_statistics where statistics_month='%s' and ppid<>''  group by ppid;" % statistics_month
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            self.update_agent_agent_tax_statistics(val["ppid"],float(val["tax_fee"]) ,statistics_month)

    def statistics_all_agent_agent_count_amount(self, start, end, statistics_month):
        """
        统计所有推荐下级代理人数和代理费用
        """
        sql = "select count(*) as count,sum(amount) as amount,pid from agent where apply_status=1 and audit_time>=%i and audit_time<=%i group by pid;" % (start, end)
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            self.update_agent_amount_statistics(val["pid"], float(val["count"]), val["amount"], statistics_month)

    def reward_company_gift_gold(self,statistics_month):
        """  统计每月给每个代理发送公司奖励的数量（分12期） """
        # 寻找符合要求的奖励代理（返币次数在12次以内）并且晋升代理时间是小于统计时间
        agent_sql="select id,level,audit_time from agent where apply_status=1;"
        agent_list=self._cache_mysql.get_list('default',agent_sql)
        for dictAgent in agent_list:
            count_sql="select count(distinct statistics_month) as months from agent_statistics where agent_id='{}' and company_gift_gold>0;".format(dictAgent.get('id'))
            dictCount=self._cache_mysql.get_one('default',count_sql)
            months=int(dictCount.get('months'))
            if months<12 and dictAgent['audit_time']<=strToTimestamp(statistics_month,format='%Y-%m'):
                level=int(dictAgent.get('level'))
                AGENT_GIFT_GOLD={1:AgentGiftGold.Level1.value,2:AgentGiftGold.Level2.value,3:AgentGiftGold.Level3.value}
                update_sql="update agent_statistics set company_gift_gold={} where agent_id='{}' and statistics_month='{}';".\
                    format(AGENT_GIFT_GOLD.get(level)/12,dictAgent.get('id'),statistics_month)
                self._cache_mysql.execute('default',update_sql)

    def statistics(self, start, end, statistics_month):
        self.statistics_all_agent(start, end, statistics_month)
        self.statistics_all_agent_agent_count_amount(start, end, statistics_month)
        self.statistics_all_agent_agent_tax(statistics_month)
        self.statistics_all_agent_agent_agent_tax(statistics_month)
        self.reward_company_gift_gold(statistics_month)
        self.update_all_agent_back(statistics_month)
        self.gen_month_data_excel(start,end)

    def update_all_agent_back(self,statistics_month):
        """计算所有代理该月的月骑士币返佣=代理税金返佣+旗下玩家税金返佣
        月利润汇总=直推代理收益+旗下玩家充值返佣
        """
        agent_sql="select id from agent where apply_status=1;"
        for agent in self._cache_mysql.get_list('default',agent_sql):
            sql="select rake_back_tax,agent_rake_back_amount,rake_back_amount,agent_tax_fee,agent_agent_tax_fee from agent_statistics " \
                "where agent_id='{}' and statistics_month='{}';".format(agent['id'],statistics_month)
            dictBackInfo=self._cache_mysql.get_one('default',sql)
            agent_rake_back_tax = int(dictBackInfo['agent_tax_fee'] * AgentRate.TaxRateB.value + dictBackInfo['agent_agent_tax_fee'] * AgentRate.TaxRateC.value)
            month_gold=agent_rake_back_tax+dictBackInfo['rake_back_tax']
            month_rmb=dictBackInfo['agent_rake_back_amount']+dictBackInfo['rake_back_amount']
            update_sql="update agent_statistics set revenue={},gold={},agent_rake_back_tax={} where agent_id='{}' and statistics_month='{}';"\
                .format(month_rmb,month_gold,agent_rake_back_tax,agent['id'],statistics_month)
            self._cache_mysql.execute('default',update_sql)

    def statistics_one_agent(self, channelCode, start, end, select_data):
        """
        统计所有代理下面所有玩家充值和税金（玩家推荐玩家也算这个代理（渠道号）的用户，但玩家成为代理时该玩家（代理）下面的玩家就不再是上面的代理玩家了）
        """
        sql = "select id, pid, ppid, mobile, apply_time, channelcode, amount,gold_rate,tax_rate from agent where channelCode='%s' and apply_status=1;" % channelCode
        val = self._cache_mysql.get_one("default", sql)
        if val is not None and len(val) > 0:
            select_data["id"] = val["id"]
            select_data["pid"] = val["pid"]
            select_data["ppid"] = val["ppid"]
            select_data["mobile"] = val["mobile"]
            select_data["apply_time"] = val["apply_time"]
            select_data["channelcode"] = val["channelcode"]
            select_data["amount"] = val["amount"]
            select_data["gold_rate"] = val["gold_rate"]
            select_data["tax_rate"] = val["tax_rate"]

            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            register_count = self.statistics_agent_register_count(val["mobile"], val["channelcode"], start, end)

            select_data["charge_gold"] = recharge
            select_data["tax_fee"] = tax
            select_data["rec_num"] = register_count

    def statistics_one_agent_agent_count_amount(self, start, end, select_data):
        """
        统计所有推荐下级代理人数和代理费用
        """
        sql = "select count(*) as count,sum(amount) as amount,pid from agent where apply_status=1 and audit_time>=%i and audit_time<=%i and pid='%s';" % (start, end, select_data["id"])
        val = self._cache_mysql.get_one("default", sql)
        if val is not None and len(val) > 0:
            select_data["agent_count"] = int(val["count"]) if val["count"] else 0
            select_data["agent_amount"] = int(val["amount"]) if val["amount"] else 0
            return True
        return False

    def statistics_one_agent_agent_tax(self, start, end, select_data):
        """
        统计所有下级代理人税金
        """
        sql = "select mobile, channelcode from agent where pid='%s' and apply_status=1;" % select_data["id"]
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            select_data["agent_tax_fee"] += float(tax)

    def statistics_one_agent_agent_agent_tax(self, start, end, select_data):
        """
        统计所有下下级代理人税金
        """
        sql = "select mobile, channelcode from agent where ppid='%s' and apply_status=1;" % select_data["id"]
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            select_data["agent_agent_tax_fee"] += float(tax)

    def statistics_one(self, channelCode, start, end):
        select_data = {"id":"", "mobile":"", "apply_time":0, "channelcode":'', "relation":'', "amount":0.0, "tax_fee":0,
                       "agent_tax_fee":0, "agent_agent_tax_fee":0, "charge_gold":0}

        self.statistics_one_agent(channelCode, start, end, select_data)
        if self.statistics_one_agent_agent_count_amount(start, end, select_data):
            self.statistics_one_agent_agent_tax(start, end, select_data)
            self.statistics_one_agent_agent_agent_tax(start, end, select_data)
        return select_data

    def statistics_month_profit(self, agent, start, end):
        select_data = {"id":agent['id'], "pid":agent['pid'], "ppid":agent['ppid'], "level":agent['level'], "rec_num":0,  "rake_back_amount":0.0, "rake_back_tax":0.0, "agent_rake_back_amount":0.0,
                          "agent_rake_back_tax":0.0, "revenue":0.0, "gold":0.0, "charge_gold":0.0, "tax_fee":0.0, "agent_tax_fee":0.0,
                          "agent_agent_tax_fee":0.0, "agent_count":0, "agent_amount":0, "is_revenue_pay":0, "is_gold_pay":0, "createTime":0,
                          "recharge":0, "tax":0, "player_num":0}

        createTime_start = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start))
        createTime_end = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        sql = "select * from ("
        index = 1
        for db_name in self._cache_mysql.db_info.keys():
            sql_select = "(select sum(gold) as gold, sum(fee) as fee, operation,sm_accountName as accountName, sm_channelCode as channelCode\
                         from {databaseName}.gold_log, {databaseName}.tbl_Account,{databaseName}.kbe_accountinfos \
                         where {databaseName}.gold_log.accountName={databaseName}.tbl_Account.sm_accountName \
                         and {databaseName}.kbe_accountinfos.accountName={databaseName}.tbl_Account.sm_accountName \
                         and {databaseName}.tbl_Account.sm_accountName<>'{accountName}' \
                         and {databaseName}.tbl_Account.sm_channelCode='{channelCode}' \
                         and {databaseName}.gold_log.createTime>='{createTime_start}' \
                         and {databaseName}.gold_log.createTime<='{createTime_end}' \
                         group by {databaseName}.tbl_Account.sm_accountName,{databaseName}.gold_log.operation)".\
                         format(databaseName=self._cache_mysql.db_info[db_name]["databaseName"],
                                accountName=agent['mobile'],
                                channelCode=agent['channelcode'],
                                createTime_start=createTime_start,
                                createTime_end=createTime_end)

            if index == 1:
                sql += sql_select
            else:
                sql += "union all" + sql_select
            index += 1
        sql += ") as gold_fee;"

        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            if val["operation"] == "Pay":
                select_data['recharge'] += float(val["gold"])
            elif val["operation"] == "Publish" or val["operation"] == "Purify":
                select_data['tax'] += float(val["gold"])
            elif val["operation"] == "ProcreateHorse" or val["operation"] == "Buy":
                select_data['tax'] += float(val["fee"])

        select_data['player_num'] =self.statistics_agent_register_count(agent['mobile'],agent['channelcode'],start,end)

        """ 统计所有推荐下级代理人数和代理费用 """
        sql = "select count(*) as count,sum(amount) as amount,pid, mobile, channelcode \
               from agent where apply_status=1 and audit_time>=%i and audit_time<=%i and pid='%s';" %\
               (start, end, select_data["id"])
        val = self._cache_mysql.get_one("default", sql)
        if val is not None and len(val) > 0:
            select_data["agent_count"] = int(val["count"]) if val["count"] else 0
            select_data["agent_amount"] = int(val["amount"]) if val["amount"] else 0

        """ 统计所有下级代理人税金 """
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            select_data["agent_tax_fee"] += float(tax)
 
        """ 统计所有下下级代理人税金 """
        sql = "select mobile, channelcode from agent where ppid='%s' and apply_status=1;" % select_data["id"]
        data = self._cache_mysql.get_list("default", sql)
        for val in data:
            recharge, tax = self.statistics_agent(val["mobile"], val["channelcode"], start, end)
            select_data["agent_agent_tax_fee"] += float(tax)

        return select_data

    def statistics_one_return_info(self,agent_id:str):
        """一个代理的返佣总体情况"""
        sql="select * from agent_statistics where agent_id='{}';".format(agent_id)
        VBC_INFO={'not_re_count':0,'re_num':0,'not_re_num':0}
        for month in self._cache_mysql.get_list('default',sql):
            if month['is_gold_pay']==0:
                VBC_INFO['not_re_count'] +=1
                VBC_INFO['not_re_num'] +=month['gold']
            else:
                VBC_INFO['re_num']+=month['gold']
        return VBC_INFO

    def gen_month_data_excel(self,start:int,end:int):
        """每个月1号生成上个月数据的excel表"""
        self.gen_gold_flow_excel(start,end)
        self.gen_integral_flow_excel(start,end)
        self.gen_charge_data_excel(start,end)

    def gen_gold_flow_excel(self,start:int,end:int):
        """生成统计月的金币流水excel表"""
        limit=1000
        offset=0
        count=0
        sheetName="gold_flow_tbl-{}".format(timestamp2Str(start,"%Y-%m"))
        objExcel = GenExcel(sheetName)
        try:
            while True:
                sql = "select * from ("
                index = 1
                for dbname in self._cache_mysql.db_info.keys():
                    sql_select = "(select id,accountName as '用户名',gold as '金币',fee as '税金',operation as '操作',reasons as '操作描述',createTime as '日期',remarks as '备注'  " \
                                 "from {databaseName}.gold_log where createTime between FROM_UNIXTIME({start}) and FROM_UNIXTIME({end}))".format(
                        databaseName=g_cache_mysql.db_info[dbname]["databaseName"], start=start, end=end)

                    if index == 1:
                        sql += sql_select
                    else:
                        sql += "union all" + sql_select
                    index += 1
                sql += ") as gold order by '日期' desc limit {} offset {};".format(limit,offset)
                dataList=self._cache_mysql.get_list('default',sql)
                if len(dataList)==0:
                    break
                if count==0:
                    objExcel.write_excel_row0(list(dataList[0].keys()))
                objExcel.write_excel_data(dataList)
                offset=limit+offset
                count+=1

        except Exception as e:
            ERROR_MSG(repr(e))
            ERROR_MSG("生成【{}】有误，请手动重新生成！！".format(sheetName))
        finally:
            objExcel.save_file()
            return sheetName

    def gen_integral_flow_excel(self,start:int,end:int):
        """生成统计月的积分流水excel表"""
        limit=1000
        count=0
        offset=0
        sheetName = "integral_flow_tbl-{}".format(timestamp2Str(start, "%Y-%m"))
        objExcel = GenExcel(sheetName)
        try:
            while True:
                sql = "select id,accountName as '用户名',integral as '数量',reasons as '操作描述',createTime as '日期',remarks as '备注' " \
                      "from integral_log where createTime between FROM_UNIXTIME({start}) and FROM_UNIXTIME({end}) order by createTime desc " \
                      "limit {limit} offset {offset};".format(start=start,end=end,limit=limit,offset=offset)
                dataList = self._cache_mysql.get_list('default', sql)
                if len(dataList) == 0:
                    break
                if count == 0:
                    objExcel.write_excel_row0(list(dataList[0].keys()))
                objExcel.write_excel_data(dataList)
                offset = limit + offset
                count += 1
        except Exception as e:
            ERROR_MSG(repr(e))
            ERROR_MSG("生成【{}】有误，请手动重新生成！！".format(sheetName))
        finally:
            del objExcel
            return sheetName

    def gen_charge_data_excel(self,start:int,end:int):
        """生成每月充值账单excel表"""
        limit = 1000
        count = 0
        offset = 0
        sheetName = "charge_data_tbl-{}".format(timestamp2Str(start, "%Y-%m"))
        objExcel = GenExcel(sheetName)
        try:
            while True:
                sql = "select orderid as '订单id',accountName as '用户名',payway as '支付方式',money as '金额',gold as '金币数',create_time as '创建日期',dealDate as '交易日期',trxNo as '三方单号',status_str as '状态' " \
                      "from onlinePay where status=1 and create_time between FROM_UNIXTIME({start}) and FROM_UNIXTIME({end}) order by create_time desc " \
                      "limit {limit} offset {offset};".format(start=start, end=end, limit=limit, offset=offset)
                dataList = self._cache_mysql.get_list('default', sql)
                if len(dataList) == 0:
                    break
                if count == 0:
                    objExcel.write_excel_row0(list(dataList[0].keys()))
                objExcel.write_excel_data(dataList)
                offset = limit + offset
                count += 1
        except Exception as e:
            ERROR_MSG(repr(e))
            ERROR_MSG("生成【{}】有误，请手动重新生成！！".format(sheetName))
        finally:
            objExcel.save_file()
            return sheetName

    def gen_all_horse_info_excel(self):
        """生成所有玩家马匹信息excel表"""
        limit = 1000
        count = 0
        offset = 0
        sheetName = "all_horse_tbl"
        objExcel = GenExcel(sheetName)
        try:
            while True:
                sql="select * from ("
                index = 1
                for dbname, dbvalue in self._cache_mysql.db_info.items():
                    sql_select ="(select * from {dbname}.tbl_Account_horseinfo_values)".format(dbname=dbvalue['databaseName'])
                    if index == 1:
                        sql += sql_select
                    else:
                        sql += "union all" + sql_select
                    index += 1
                sql += ") as a limit {} offset {};".format(limit,offset)

                dataList = self._cache_mysql.get_list('default', sql)
                if len(dataList) == 0:
                    break
                if count == 0:
                    objExcel.write_excel_row0(list(dataList[0].keys()))
                objExcel.write_excel_data(dataList)
                offset = limit + offset
                count += 1
        except Exception as e:
            ERROR_MSG(repr(e))
            ERROR_MSG("生成【{}】有误，请手动重新生成！！".format(sheetName))
        finally:
            objExcel.save_file()
            return sheetName

    def __init__(self, cache_mysql):
        self._cache_mysql = cache_mysql
    pass

if __name__ == '__main__':
    from main import g_cache_mysql
    p=profit(g_cache_mysql)
    # p.gen_month_data_excel(1525104000,1527782400)
    print(p.gen_gold_flow_excel(getNow()-86400,getNow()))
