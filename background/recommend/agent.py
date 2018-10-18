# -*- coding: utf-8 -*-
import json
import time
import datetime
import calendar
import NameHash
import MobileIdentify
import manageToken
from lib.timeHelp import getNow, str2Date
from logger import *
from official.define import AgentRate
from recommend.profit import profit
import HelpFun

"""
http://192.168.1.157:30040/agent/month_profit?mobile=15556456456
"""

class Agent:
    # 代理端
    def __init__(self, cache_redis, cache_mysql, background):
        self.cache_redis         = cache_redis
        self.cache_mysql         = cache_mysql
        self.background          = background
        self.password            = ''
        self.profit              = profit(self.cache_mysql)
        self.statistics_month    = self.get_statistics_month()


    def month_profit(self, params):
        """代理本月收益"""
        mobile = params.get('mobile','')
        now    = datetime.datetime.now()
        sql="select id, pid, ppid, level, amount, mobile, gift_gold,audit_time, gold_rate,comment, channelcode\
                  from agent where apply_status=1"
        if mobile:
            sql += " and mobile='%s' " % mobile

        agent_list = self.cache_mysql.get_list("default", sql)
        days_num = calendar.monthrange(now.year, now.month)[1]  # 获取本月有多少天
        begin_date = '%d-%d-01' % (now.year, now.month)
        begin_time = time.mktime(time.strptime(begin_date,'%Y-%m-%d'))
        end_date   = '%d-%d-%d' % (now.year, now.month, days_num)
        end_time   = time.mktime(time.strptime(end_date,'%Y-%m-%d'))+24*3600
        listData=list()
        for agent in agent_list:
            if begin_time<agent['audit_time']:
                begin_time=agent['audit_time']
            SD = self.profit.statistics_month_profit(agent, begin_time, end_time)
            Rate = {1:AgentRate.ChildLevel1TaxRate.value, 2:AgentRate.ChildLevel2TaxRate.value, 3:AgentRate.ChildLevel3TaxRate.value}
            RateStr = {1:AgentRate.ChildLevel1TaxRateStr.value, 2:AgentRate.ChildLevel2TaxRateStr.value, 3:AgentRate.ChildLevel3TaxRateStr.value}
            DATA = {'agent_id':agent['id'],                                                                   # 玩家ID
                    'mobile':agent['mobile'],                                                                 # 手机号码
                    "amount":agent['amount'],                                                                 # 代理费用
                    'gift_gold':agent['gift_gold'],                                                           # 赠送骑士币（万）
                    'child_amount_rate':'8%',                                                                 # 旗下玩家充值返佣基数（RMB)
                    'child_tax_rate':RateStr.get(agent['level']),                                             # 旗下玩家税金返币基数（反币)
                    'gold_rate':'10%',                                                                        # 直推返佣基数（RMB)
                    'tax_rate':'B10%+C5%',                                                                    # 直推税金返币基数（骑士币）
                    'channelcode':agent['channelcode'],                                                       # 代理人包体标识号
                    'comment':agent['comment'],                                                               # 备注
                    'child_num':self.profit.statistics_agent_register_count(agent['mobile'],agent['channelcode'],0,getNow()), # 旗下玩家总数
                    'sum_child_charge_amount':SD['recharge']/10,                                              # 旗下玩家充值汇总（元）
                    'sum_child_charge_commision':int(AgentRate.ChildAmountRate.value*SD['recharge']/10),      # 旗下玩家充值返佣（元）
                    'sum_child_tax_amount':abs(SD['tax']),                                                    # 旗下玩家税金收益汇总（币）
                    'sum_child_tax_commision':abs(int(Rate.get(agent['level'])*SD['tax'])),                   # 税金收益返币量（返币)

                    'recommend_num':SD['agent_count'],                                                         # 直推人数
                    'sum_recommend_amount':SD['agent_amount'],                                                 # 直推总金额
                    'sum_recommend_gold_amount':int(0.1*SD['agent_amount']),                                   # 直推总收益返佣汇总
                    'sum_recommend_tax_amount':abs(int(SD['agent_tax_fee']*0.1 + SD['agent_agent_tax_fee']*0.05)),   # 直推税金收益返佣（返币）
                   }
            VBC_INFO=self.profit.statistics_one_return_info(agent['id'])
            DATA['unremission_num'] = VBC_INFO['not_re_count']          # 剩余未返期数
            DATA['unremission_amount'] = VBC_INFO['not_re_num']         # 未返骑士币总量（万）

            DATA['remission_amount'] = VBC_INFO['re_num']               # 已返还数量（万）
            listData.append(DATA)
        return {"code":0,"data":listData}

    def child_info(self, params):
        """下级代理人列表"""

        if "start" not in params or "end" not in params or "mobile" not in params or "channelcode" not in params:
            ERROR_MSG('{"code":1,"errmsg":"参数出错"}')
            return '{"code":1,"errmsg":"参数出错"}'

        if not params["start"].isdigit() or not params["end"].isdigit():
            ERROR_MSG('{"code":1,"errmsg":"参数出错"}')
            return '{"code":1,"errmsg":"参数出错"}'

        page        = int(params.get('page', '1'))
        page_size   = int(params.get('page_size', '20'))
        sort        = params.get('sort', 'desc').strip()    # 排序 asc; desc;
        start       = int(params.get('start'))
        end         = int(params.get('end'))
        mobile      = params.get('mobile').strip()      # 查询ID/代理人手机号
        channelcode = params.get('channelcode').strip() # 渠道码

        sql = "select id, pid, mobile, apply_time, channelcode from agent where mobile='%s' and apply_status=1" % mobile
        agent = self.cache_mysql.get_one("default", sql)
        if not agent:
            return '{"code":1,"errmsg":"获取代理信息失败"}'

        sql = "select id, pid, mobile, apply_time,audit_time, channelcode from agent \
              where pid='%s' and mobile<>'%s' and apply_status=1" % (agent['id'], mobile)

        _agent_list = self.cache_mysql.get_list("default", sql)

        STATISTICS = {'agent_num': 0, 'sum_amount':0, 'sum_commision':0, 'sum_tax':0, 'sum_tax_commision':0}
        agent_list = [] 
        for a in _agent_list:
            if start<a['audit_time']:
                start=a['audit_time']
            so = self.profit.statistics_one(a['channelcode'], start, end)
            AGENT = {}
            AGENT['agent_id'] = so['id']                                      # 代理ID
            AGENT['mobile'] = so['mobile']                                    # 手机号码
            AGENT['apply_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(so['apply_time'])))     # 申请代理时间
            AGENT['channelcode'] = so['channelcode']                          # 渠道包标识

            # 层级关系
            if so['pid'] == agent['id']:
                AGENT['relation'] = u'直推代理人'        
            elif so['pid'] and so['pid'] != agent['id']:
                AGENT['relation'] = u'间接代理人'      
            else:
                AGENT['relation'] = u''        

            AGENT['amount'] = so['amount']                                                          # 代理金额
            AGENT['amount_commision'] = int(so['amount']*AgentRate.AmountRate.value)                # 直推返利收益（元）
            AGENT['sum_tax'] = abs(so['tax_fee'] + so['agent_tax_fee'] + so['agent_agent_tax_fee']) # 整包当月税金总数（币）
            AGENT['tax_commision'] = abs(int(AGENT['sum_tax']*AgentRate.TaxRateB.value))            # 直推税金收益（币）
            STATISTICS['agent_num'] += 1                             # 下层代理人数
            STATISTICS['sum_amount'] += so['amount']
            STATISTICS['sum_commision'] += AGENT['amount_commision']
            STATISTICS['sum_tax'] += AGENT['sum_tax']
            STATISTICS['sum_tax_commision'] += AGENT['tax_commision']
            agent_list.append(AGENT)
        return '{"code":0,"num":0,"statistic":%s,"player_list":%s}' % (json.dumps(STATISTICS, ensure_ascii=False), json.dumps(agent_list, ensure_ascii=False))

    def player_info(self, params):
        """玩家列表"""
        if "start" not in params or "end" not in params or "mobile" not in params or "channelcode" not in params:
            ERROR_MSG('{"code":1,"errmsg":"参数出错"}')
            return '{"code":1,"errmsg":"参数出错"}'

        if not params["start"].isdigit() or not params["end"].isdigit():
            ERROR_MSG('{"code":1,"errmsg":"参数出错"}')
            return '{"code":1,"errmsg":"参数出错"}'

        start       = int(params.get('start'))
        end         = int(params.get('end'))
        mobile      = params.get('mobile').strip()
        channelcode = params.get('channelcode', '').strip() # 渠道码

        sql = "select invitecode, mobile, level,audit_time from agent where mobile='%s' and apply_status=1" % mobile
        agent = self.cache_mysql.get_one("default", sql)
        if not agent:
            return '{"code":1,"errmsg":"获取代理信息失败"}'
        if start<agent['audit_time']:
            start=agent['audit_time']
        _player_list = self.profit.statistics_user(mobile, channelcode, start, end)
        player_list = []
        STATISTICS = {'player_num':0,
                      'sum_recharge':0,
                      'sum_recharge_commision':0,
                      'sum_tax':0,
                      'sum_tax_commision':0}

        Rate = {1:AgentRate.ChildLevel1TaxRate.value, 2:AgentRate.ChildLevel2TaxRate.value, 3:AgentRate.ChildLevel3TaxRate.value}
        for k, v in _player_list.items():
            PLAYER = {'tax_sum':0}
            PLAYER['player_id'] = v['uuid']                                                             # 玩家ID
            PLAYER['mobile'] = v['accountName'][:4] + '****' + v['accountName'][-3:]                    # 手机号码
            PLAYER['reg_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(v['regtime'])))  # 注册时间
            PLAYER['channelcode'] = v['channelCode']                                                    # 渠道包标识
            PLAYER['recharge_sum'] = round(v['recharge']/10,2)                                          # 累计充值金额
            PLAYER['recharge_commision'] = abs(PLAYER['recharge_sum']*AgentRate.ChildAmountRate.value)  # 旗下玩家返佣元（RMB)
            PLAYER['tax_sum'] = abs(v['tax'])                                                           # 税收汇总
            PLAYER['tax_commision'] = abs(int(v['tax']*Rate.get(agent['level'])))                       # 代理直推税金返佣（币）
            STATISTICS['player_num'] += 1                                                               # 下层玩家数
            STATISTICS['sum_recharge'] += PLAYER['recharge_sum']                                        # 查询期间累计充值汇总
            STATISTICS['sum_tax'] += abs(PLAYER['tax_sum'])
            player_list.append(PLAYER)

        STATISTICS['sum_recharge_commision'] = int(STATISTICS['sum_recharge']*AgentRate.ChildAmountRate.value)     # 查询期间返佣金额汇总
        STATISTICS['sum_tax_commision'] = int(STATISTICS['sum_tax']*Rate.get(agent['level']))                      # 查询期间税金返币汇总
        return '{"code":0,"num":%d,"statistic":%s,"player_list":%s}' % (len(_player_list), json.dumps(STATISTICS, ensure_ascii=False), json.dumps(player_list, ensure_ascii=False))

    def month_detail(self, params):
        """根据时间查直推代理详细信息/本月直推代理"""

        if "start" not in params or "end" not in params or "agent_id" not in params:
            return '{"code":1,"errmsg":"参数出错"}'

        if not params["start"].isdigit() or not params["end"].isdigit():
            return '{"code":1,"errmsg":"参数出错"}'
        # kind=params.get('kind')
        # if kind not in ('ag','bg'):
        #     return '{"code":1,"errmsg":"参数出错"}'

        agent_id = params.get('agent_id', '').strip()
        sql = "select id, invitecode from agent where id='%s'" % agent_id
        agent = self.cache_mysql.get_one("default", sql)
        if not agent:
            return '{"code":1,"errmsg":"获取代理信息失败"}'

        # 直推玩家列表
        start = int(params.get('start'))
        end   = int(params.get('end'))
        sql = "select id, mobile, amount from agent where \
              pid='%s' and apply_status=1 and audit_time >='%s' and audit_time <'%s'" % (agent_id, start, end)

        player_list = self.cache_mysql.get_list("default", sql)
        sum_amount = 0
        for p in player_list:
            sum_amount += p['amount']
            p['mobile'] = p['mobile'][:4] + '****' + p['mobile'][-3:]
        return '{"code":0,"sum_amount":%d,"data":%s}' % (sum_amount, json.dumps(player_list, ensure_ascii=False))

    def signin(self, params):
        """代理用户登录"""

        mobile        = params.get('mobile', '').strip()
        self.password = params.get('password', '').strip()
        current_time  = int(time.time())

        if not mobile:
            return '{"code":1,"errmsg":"请输入手机号码"}'

        if not HelpFun.ismobile(mobile):
            return '{"code":1, "errmsg":"请输入正确的手机号码"}'

        if not self.password:
            return '{"code":1,"errmsg":"请输入密码"}'

        if len(self.password) > 64:
            return '{"code":1,"errmsg":"密码长度不能太长"}'

        sql = "select id, channelcode from agent where mobile='%s'" % mobile
        agent = self.cache_mysql.get_one('default', sql)
        if not agent:
            return '{"code":1,"errmsg":"用户不存在"}'

        name_hash = NameHash.NameHash(mobile)
        dbname = name_hash.crcHash()
        sql = "select accountName from kbe_accountinfos where accountName='%s' and password='%s'" % (mobile, self.password)
        ka = self.cache_mysql.get_one(dbname, sql)
        if not ka:
            return '{"code":1,"errmsg":"密码错误"}'
        token = manageToken.manageToken.generate_token(mobile)
        return '{"code":0,"msg":"登录成功！","token":"%s","channelcode":"%s"}' % (token,  agent['channelcode'])

    def month_statistics(self):
        """
        每月1号0点统计上个月的数据
        """
        # i=str2Date('2018-10-1','%Y-%m-%d')
        i = datetime.datetime.now()
        if i.month-1<=0:
            lastMonth =12
            lastYear=i.year-1
        else:
            lastYear = i.year
            lastMonth=i.month-1

        statistics_month = "%d-%d" % (lastYear, lastMonth)
        # DEBUG_MSG('statistics_month:%s, self.statistics_month:%s' % (statistics_month, self.statistics_month))
        if self.statistics_month < statistics_month:
            # 上个月的开始时间
            start_date = "%d-%d-%d" % (lastYear, lastMonth, i.day)
            start_time = time.mktime(time.strptime(start_date, '%Y-%m-%d'))

            # 上个月的结束时间
            days_num = calendar.monthrange(lastYear, lastMonth)[1]  # 获取上个月有多少天
            end_date = "%d-%d-%d" % (lastYear, lastMonth, days_num)
            end_time = time.mktime(time.strptime(end_date, '%Y-%m-%d')) + 24*3600
            if i.day == 1 and i.hour == 0 and i.minute == 0:
                self.profit.statistics(start_time, end_time, statistics_month)
                self.statistics_month = statistics_month

    def get_statistics_month(self):
        sql = "select statistics_month from agent_statistics order by statistics_month desc limit 1"
        statistics_info = self.cache_mysql.get_one('default', sql)
        statistics_month = statistics_info['statistics_month'] if statistics_info else ''
        return statistics_month


if __name__ == '__main__':
    from main import g_agent
    print(g_agent.month_profit({}))
