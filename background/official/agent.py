# -*- coding: utf-8 -*-
import json
import random
import time
import calendar
import NameHash
import MobileIdentify
import manageToken
from lib.timeHelp import str2Date, getNow
from logger import *
from official.define import AgentRate, EmpowerUserList, AgentLevelAmount,AgentGiftGold
from login import login
import HelpFun
from recommend.profit import profit

"""
http://192.168.1.157:30040/agent/month_profit?mobile=15556456456
"""

class Agent:
    # 后台：官方代理数据
    def __init__(self, cache_redis, cache_mysql, background):
        self.cache_redis         = cache_redis
        self.cache_mysql         = cache_mysql
        self.background          = background
        self.profit              = profit(self.cache_mysql)


    def get_random_id(self):
        """随机生成代理id"""
        str_num = 7
        num_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        str_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
        random_list = num_list + str_list

        id = ''.join(random.sample(random_list, str_num))
        sql = "select id from agent where id='%s'" % id
        is_id = self.cache_mysql.get_one("default", sql)

        if is_id:
            return self.get_random_id()

        return id

    def audit_list(self, params):
        """代理审核、汇总列表"""
        required_params = ("page_no", "page_size", "sort", "start", "end", "level", "time_type","mobile","kind")
        if not HelpFun.check_required_params(required_params, params):
            return '{"code":1,"errmsg":"缺少必要参数"}'

        kind=params.get('kind')
        if kind not in ('bg','oc'):
            return '{"code":1,"errmsg":"参数错误"}'
        page_no = int(params.get('page_no', '1'))
        if page_no <= 0:
            return '{"code":1,"errmsg":"页码不正确"}'

        page_size = int(params.get('page_size', '20'))
        if page_size <= 0:
            return '{"code":1,"errmsg":"列表尺寸不正确"}'

        sort = params.get('sort', 'desc').strip() # asc 升序排序; desc 降序排序;
        if sort not in ("asc", "desc"):
            return '{"code":1,"errmsg":"排序类型不正确"}'

        if not (params["start"].isdigit() and params["end"].isdigit() and params['level'].isdigit() and params['time_type'].isdigit()):
            return '{"code":1,"errmsg":"时间参数出错"}'

        level = int(params.get('level',1)) # 0.全部; 1.一级代理; 2.二级代理; 3.三级代理;
        if level not in (0, 1, 2, 3):
            return '{"code":1,"errmsg":"代理级别不正确"}'

        time_type = int(params.get('time_type',0)) # 0.全部; 1.注册时间; 2.晋升代理时间;
        if time_type not in (0, 1, 2):
            return '{"code":1,"errmsg":"时间类型不正确"}'

        start     = int(params.get('start'))
        end       = int(params.get('end'))
        mobile    = params.get('mobile').strip()

        sql = "select id,mobile, invitecode,pid,ppid, apply_time, amount, gift_gold, apply_status, auditor, comment,audit_time, is_freezed, level, channelcode\
              from agent where id<>''"

        if level > 0:
            sql += " and level=%d" % level

        if time_type == 1:
            sql += " and regtime between %d and %d" % (start,end)

        if time_type == 2:
            sql += " and audit_time between %d and %d" % (start,end)

        if mobile:
            sql += " and mobile='%s' or id='%s'" % (mobile, mobile)
        if kind=='oc':
            sql+=" and apply_status=1"

        sql += " group by apply_time order by apply_time %s limit %d, %d;" % (sort, (page_no-1)*page_size, page_size)

        # TAX_RATE = {1:AgentRate.ChildLevel1TaxRate.value, 2:AgentRate.ChildLevel2TaxRate.value, 3:AgentRate.ChildLevel3TaxRate.value}
        RateStr = {1:AgentRate.ChildLevel1TaxRateStr.value, 2:AgentRate.ChildLevel2TaxRateStr.value, 3:AgentRate.ChildLevel3TaxRateStr.value}

        _agent_list = self.cache_mysql.get_list("default", sql)
        agent_list = []
        for a in _agent_list:
            PLAYER = dict()
            PLAYER['id']           = a['id']                                                              # 玩家ID
            PLAYER['mobile']       = a['mobile']                                                          # 玩家手机号
            name_hash = NameHash.NameHash(a['mobile'])
            dbname = name_hash.crcHash()
            regtime_sql="select regtime from kbe_accountinfos where accountName='%s';"%a['mobile']
            p=self.cache_mysql.get_one(dbname,regtime_sql)
            PLAYER['regtime']      = p['regtime']                                                         # 注册时间
            PLAYER['amount']       = int(a['amount'])                                                     # 代理费用
            PLAYER['child_charge_rate_str'] = AgentRate.ChildChargeRateStr.value                          # 旗下玩家充值返佣基数（RMB）
            PLAYER['child_tax_rate_str'] = RateStr.get(a['level'])                                        # 旗下玩家税金返币基数（反币)
            PLAYER['gift_gold']    = a['gift_gold']                                                       # 赠送骑士币（万）
            PLAYER['audit_time']   = a['audit_time']   # 修改时间
            PLAYER['child_tax_agent_rate_str'] = '10%'  # 直推代理费返佣基数（RMB)
            PLAYER['child_tax_user_rate_str'] = '10%'  # 直接推荐人税金返佣基数（币）
            PLAYER['child_tax_cuser_rate_str'] = '5%'  # 间接推荐人税金返佣基数（币）
            PLAYER['comment']=a['comment']             # 备注
            if kind=='bg':
                PLAYER['invitecode']   = a['invitecode']                                                      # 邀请码
                PLAYER['apply_time']   = a['apply_time']                                                      # 申请时间（添加时间）
                PLAYER['apply_status'] = a['apply_status']                                                    # 申请状态
                PLAYER['auditor']      = a['auditor']                                                         # 修改操作ID
                PLAYER['is_freezed']   = a['is_freezed']                                                      # 是否冻结 0.否; 1.是;
                PLAYER['level']        = a['level']                                                           # 代理级别 0.默认; 1.等级一; 2.等级二; 3.等级三;
                PLAYER['channelcode']  = a['channelcode']                                                     # 代理人包体标识号
                #后台冻结/解冻代理信息
                freeze_sql="select freeze_status,modify_time,operator_id from agent_freeze where agent_id='%s';" % a['id']
                log_list=self.cache_mysql.get_list("default", freeze_sql)
                PLAYER['freeze_log']=log_list
            else:
                dictAgentInfo=self.profit.statistics_one(a['channelcode'],a['audit_time'],getNow())
                PLAYER['player_count']=self.profit.statistics_agent_register_count(a['mobile'],a['channelcode'],0,getNow()) #旗下用户数
                PLAYER['player_recharge']=int(dictAgentInfo.get('charge_gold',0)/10)                                    #旗下用户充值总数
                PLAYER['player_tax']=abs(int(dictAgentInfo.get('tax_fee',0)))                                           #旗下用户税金汇总
                PLAYER['direct_pushback_rmb']=int(dictAgentInfo.get('agent_amount',0)*0.1)                            #直推返佣金额
                PLAYER['direct_pushback_tax']=abs(int(dictAgentInfo.get('agent_tax_fee',0)*0.1+dictAgentInfo.get('agent_tax_fee',0)*0.05))    #直推税金返佣总额

                VBC_INFO=self.profit.statistics_one_return_info(a['id'])
                PLAYER['returned_quantity']=VBC_INFO['re_num']                                                          #已返还数量（万）
                PLAYER['not_return_num']=VBC_INFO['not_re_count']                                                       #剩余未返期数
                PLAYER['not_return_vbc']=VBC_INFO['not_re_num']                                                         #未返骑士币总量（万）
                PLAYER['rmb_return_count']=PLAYER['direct_pushback_rmb']+int(PLAYER['player_recharge']*AgentRate.ChildChargeRate.value) #现金返佣收益汇总（元） 直推返佣+充值返佣
                PLAYER['vbc_return_count']=PLAYER['direct_pushback_tax']+PLAYER['player_tax']                           #骑士币返佣汇总（币）


            agent_list.append(PLAYER)
        return '{"code":0,"data":%s}' % json.dumps(agent_list, ensure_ascii=False)

    def audit(self, params):
        """代理申请审核"""

        required_params = ("auditor_id", "agent_id")
        if not HelpFun.check_required_params(required_params, params):
            return '{"code":1,"errmsg":"缺少必要参数"}'

        auditor_id = params['auditor_id'].strip()
        # sql = "select id from agent where id='%s'" % auditor_id
        # auditor = self.cache_mysql.get_one("default", sql)

        if auditor_id not in login.background_user:
            return '{"code":1,"errmsg":"找不到审核人"}'

        agent_id = params['agent_id'].strip()
        sql = "select id, apply_status,invitecode,channelcode from agent where id='%s'" % agent_id
        agent = self.cache_mysql.get_one("default", sql)
        if not agent:
            return '{"code":1,"errmsg":"找不到代理"}'

        if agent['apply_status'] == 1:
            return '{"code":1,"errmsg":"该玩家已经是代理了"}'

        sql = "update agent set apply_status=1, auditor='%s', audit_time='%s' where id='%s'" % (auditor_id, int(time.time()), agent_id)
        self.cache_mysql.execute("default", sql)
        DEBUG_MSG("### 代理申请审核 agent_id:'%s'" % agent_id)
        return '{"code":0, "msg":"代理申请审核成功!"}'

    def freeze_modify(self, params):
        """
        代理冻结修改
        """
        required_params = ("agent_id", 'freeze_type', "remark", "operator_id")
        if not HelpFun.check_required_params(required_params, params):
            return '{"code":1,"errmsg":"缺少必要参数"}'

        freeze_type = int(params['freeze_type'])
        if freeze_type not in (0, 1):
            return '{"code":1,"errmsg":"类型不存在"}'

        operator_id = params['operator_id'].strip()
        # sql = "select mobile from agent where id='%s'" % operator_id
        # operator = self.cache_mysql.get_one("default", sql)
        if operator_id not in login.background_user:
            return '{"code":1,"errmsg":"找不到操作人"}'

        agent_id = params['agent_id'].strip()
        sql = "select id from agent where id='%s'" % agent_id
        agent = self.cache_mysql.get_one("default", sql)
        if not agent:
            return '{"code":1,"errmsg":"找不到代理"}'
        
        remark        = params['remark'].strip()
        modify_time = int(time.time())
        # 冻结
        if freeze_type == 1:
            sql = "update agent set is_freezed=1 where id='%s'" % agent_id
            self.cache_mysql.execute("default", sql)
            sql = "insert into `agent_freeze` (`agent_id`,`freeze_status`, `remark`, `modify_time`, `operator_id`, `operator`) \
                   VALUES('{agent_id}', '{freeze_status}', '{remark}', {modify_time}, '{operator_id}', '{operator}') ;"\
                .format(agent_id=agent_id, freeze_status=freeze_type, remark=remark, modify_time=modify_time, operator_id=operator_id, operator=operator_id)
            self.cache_mysql.execute("default", sql)
            return '{"code":0, "msg":"代理冻结成功!"}' 

        # 解冻
        sql = "update agent set is_freezed=0 where id='%s'" % agent_id
        self.cache_mysql.execute("default", sql)
        sql = "insert into `agent_freeze` (`agent_id` , `freeze_status`, `remark`, `modify_time`, `operator_id`, `operator`) \
               VALUES('{agent_id}', '{freeze_status}', '{remark}', {modify_time}, '{operator_id}', '{operator}') ;".\
               format(agent_id=agent_id, freeze_status=freeze_type, remark=remark, modify_time=modify_time, operator_id=operator_id, operator=operator_id)

        self.cache_mysql.execute("default", sql)
        DEBUG_MSG("### 代理冻结修改 operator_id:%s, agent_id:%s, freeze_type:'%s'" % (operator_id, agent_id, freeze_type))
        return '{"code":0, "msg":"代理解冻成功!"}'

    def agent_history_profit(self,request:dict):
        """代理历史收益"""
        agent_id=request.get('agent_id')
        if not agent_id:
            return {"code":1,"errmsg":"参数缺少"}
        sql="select * from agent_statistics where agent_id='%s' order by createTime desc;" % agent_id
        agent_month_list = self.cache_mysql.get_list("default", sql)
        resp_data_list=[]
        for agent_month in agent_month_list:
            agent_month_dict=dict()
            agent_month_dict['date']=agent_month['statistics_month']
            agent_month_dict['charge_count']=int(agent_month['charge_gold']/10)             # 旗下玩家充值汇总(元)
            agent_month_dict['charge_return']=agent_month['rake_back_amount']               # 旗下玩家充值返佣
            agent_month_dict['tax_fee']=agent_month['tax_fee']                              # 税金收益总汇
            agent_month_dict['tax_fee_return']=agent_month['rake_back_tax']                 # 旗下玩家税金返佣
            agent_month_dict['rec_num']=agent_month['agent_count']                          # 直推人数
            agent_month_dict['agent_amount']=agent_month['agent_amount']                    # 直推总额
            agent_month_dict['agent_rake_back_amount']=agent_month['agent_rake_back_amount']# 直推总收益返额
            agent_month_dict['agent_rake_back_tax']=agent_month['agent_rake_back_tax']      # 直推税金返佣
            agent_month_dict['revenue_month']=agent_month['revenue']                        # 本月利润汇总
            agent_month_dict['gold_month']=agent_month['gold']                              # 本月骑士币返佣
            agent_month_dict['is_revenue_pay']=agent_month['is_revenue_pay']                # 现金 0为返 1已返
            agent_month_dict['is_gold_pay']=agent_month['is_gold_pay']                      # 骑士币 0为返 1已返

            resp_data_list.append(agent_month_dict)
        return {"code":0,"data":resp_data_list}

    def agent_apply(self, params):
        """代理申请"""

        mobile = params.get('mobile', '').strip()
        level = int(params.get('level', 0))  # 申请级别 0.默认; 1.等级一; 2.等级二; 3.等级三;
        current_time = int(time.time())

        if level not in (1, 2, 3) or not mobile:
            return '{"code":1,"errmsg":"参数出错"}'

        if not HelpFun.ismobile(mobile):
            return '{"code":1, "errmsg":"手机号码不正确"}'

        player = self.background.select_user(mobile)
        if not player:
            return '{"code":1,"errmsg":"玩家不存在"}'

        sql = "select id from agent where mobile='%s' and apply_status=0" % mobile
        is_apply = self.cache_mysql.get_one("default", sql)
        if is_apply:
            return '{"code":1,"errmsg":"您的申请已经提交过了, 待审核中..."}'

        sql = "select id, mobile, invitecode, channelcode from agent where mobile='%s' and apply_status=1" % mobile
        is_agent = self.cache_mysql.get_one("default", sql)
        if is_agent:
            return '{"code":1,"errmsg":"您已经是代理了"}'

        # 如果玩家有上层邀请码，就查找上层代理人信息
        if player['sm_invitationCode']:
            sql = "select id, pid, mobile, pmobile, amount, level, plevel, apply_status from agent where invitecode='%s'" % \
                  player['sm_invitationCode']
            inviter = self.cache_mysql.get_one("default", sql)
            if not inviter:
                return '{"code":1,"errmsg":"找不到邀请人"}'

            if inviter['apply_status'] == 0:
                return '{"code":1,"errmsg":"邀请人还不是代理"}'

            # 如果邀请人有上层邀请码, 查找上层代理的上层代理（即是自己的上上代理）
            if inviter['pid']:
                sql = "select id, apply_status from agent where pid='%s'" % inviter['pid']
                pinviter = self.cache_mysql.get_one("default", sql)
                if not pinviter:
                    return '{"code":1,"errmsg":"找不到上上层代理"}'

                if pinviter['apply_status'] == 0:
                    return '{"code":1,"errmsg":"上上层邀请人还不是代理"}'
        else:
            inviter = {"id": "", "pid": "", "mobile": "", "pmobile": "", "amount": 0.00, "level": 0, "plevel": 0}

        channelcode = self.get_random_id()
        id = '20' + str(level) + channelcode  # 如 201DNFHERO

        AGENT_AMOUNT = {1: AgentLevelAmount.Level1.value, 2: AgentLevelAmount.Level2.value,
                        3: AgentLevelAmount.Level3.value}
        TAX_RATE = {1: AgentRate.ChildLevel1TaxRate.value, 2: AgentRate.ChildLevel2TaxRate.value,
                    3: AgentRate.ChildLevel3TaxRate.value}
        GIFT_GOLD={1:AgentGiftGold.Level1.value,2:AgentGiftGold.Level2.value,3:AgentGiftGold.Level3.value}

        sql = "insert into agent (`id`, `mobile`, `level`, `invitecode`, `amount`, `tax_rate`, `gold_rate`, `apply_time`,\
              `pid`, `ppid`, `pmobile`, `ppmobile`, `channelcode`,`gift_gold`, `pamount`, `plevel`, `pplevel`) \
              value('%s','%s', '%s', '%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s', '%s', '%s', '%s')" % \
              (id, mobile, level, player['sm_selfInvitationCode'], AGENT_AMOUNT.get(level), TAX_RATE.get(level),
               AgentRate.ChildChargeRate.value,
               current_time, inviter['id'], inviter['pid'], inviter['mobile'], inviter['pmobile'], channelcode,GIFT_GOLD.get(level),
               inviter['amount'], inviter['level'],
               inviter['plevel'])

        result = self.cache_mysql.execute("default", sql)
        if not result:
            return '{"code":1,"errmsg":"申请失败！"}'

        self.background.mdify_player_channel_code(mobile, player['sm_selfInvitationCode'], channelcode)
        self.background.modifyChannelCode(channelcode, mobile)
        return '{"code":0, "msg":"申请成功！"}'

    def modify_agent_info(self,request:dict):
        """修改代理信息"""
        agent_id=request.get('agent_id','')
        if not agent_id and len(request.keys())!=2:
            return {"code":1,"errmsg":"参数错误"}
        agent_sql="select id from agent where id='%s'"%agent_id
        agent=self.cache_mysql.get_one('default',agent_sql)
        if not agent:
            return {"code": 1, "errmsg": "该代理不存在"}
        mobile=request.get('mobile','')
        level=request.get('level','')
        comment=request.get('comment','')
        sql="update agent set "
        if mobile:
            if not HelpFun.ismobile(mobile):
                return {"code": 1, "errmsg": "电话号码错误"}
            sql+=" mobile='%s'" % mobile
        if comment:
            sql+=" comment='%s'" % comment

        if level:
            if not level.isdigit():
                return {"code": 1, "errmsg": "等级不是整数"}
            level=int(level)
            if level not in(1,2,3):
                return {"code": 1, "errmsg": "等级错误"}
            AgentAmountEum={1:AgentLevelAmount.Level1.value,2:AgentLevelAmount.Level2.value,3:AgentLevelAmount.Level3.value}
            AgentGiftGoldEum={1:AgentGiftGold.Level1.value,2:AgentGiftGold.Level2.value,3:AgentGiftGold.Level3.value}
            sql+=" level=%d ,amount=%d,gift_gold=%d "%(level,AgentAmountEum.get(level),AgentGiftGoldEum.get(level))
        sql+=" where id='%s'"%agent_id
        ask=self.cache_mysql.execute('default',sql)
        if not ask:
            return {"code":1,"errmsg":"修改不成功!"}
        return {"code":0,"errmsg":"修改成功!"}
