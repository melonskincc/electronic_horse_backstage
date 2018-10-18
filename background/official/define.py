# -*- coding: utf-8 -*-
from enum import Enum

class AgentType(Enum):
    AgentType_1_level = 1  # 1级代理 2万
    AgentType_2_level = 2  # 2级代理 5万
    AgentType_3_level = 3  # 3级代理 10万

class AgentRate(Enum):
    AmountRate            = 0.1         # 直推返佣基数（RMB)
    TaxRateB              = 0.1         # 直推税金返币B基数（骑士币）
    TaxRateC              = 0.05        # 直推税金返币C基数（骑士币）
    ChildAmountRate       = 0.08        # 旗下玩家充值返佣基数（RMB)
    ChildTaxRate          = 0.07        # 旗下玩家税金返币基数（返币)
    ChildLevel1TaxRate    = 0.07        # 2万代理分成比例
    ChildLevel2TaxRate    = 0.175       # 5万代理分成比例
    ChildLevel3TaxRate    = 0.35        # 10万代理分成比例
    ChildChargeRateStr    = '8%'        # 旗下玩家充值返佣基数（RMB)
    ChildLevel1TaxRateStr = '7%'        # 2万代理分成比例
    ChildLevel2TaxRateStr = '17.5%'     # 5万代理分成比例
    ChildLevel3TaxRateStr = '35%'       # 10万代理分成比例
    ChildChargeRate       = 0.08        # 玩家充值金额8%的提成

class AgentLevelAmount(Enum):
    Level1 = 20000         # 1级代理金额 2万
    Level2 = 50000         # 2级代理金额 5万
    Level3 = 100000        # 3级代理金额 10万

class AgentGiftGold(Enum):
    Level1=240000           # 1级代理赠送骑士币 24万  分12期返还
    Level2=600000           # 2级代理赠送骑士币 60万  分12期返还
    Level3=1200000          # 3级代理赠送骑士币 120万 分12期返还

class EmpowerUserList(Enum):
    """授权登录官方账号列表"""
    UserList = ['18605717559']

#######rw_type:奖励类型0:谢谢惠顾，1:游戏道具，2:实物奖励###########
reward_config_table={1:{"comment":"5元京东卡*1","rate":"4%","daily_count":5,"rw_type":1,"rate_range":(1,4000),"prop_id":7008001,"count":1},
                     2:{"comment":"1元京东卡*1","rate":"7.35%","daily_count":10,"rw_type":1,"rate_range":(4001,11350),"prop_id":7004001,"count":1},
                     3:{"comment":"20元京东卡*1","rate":"1%","daily_count":3,"rw_type":1,"rate_range":(11351,12350),"prop_id":7010001,"count":1},
                     4:{"comment":"华为P20*1","rate":"0.045%","daily_count":0,"rw_type":2,"rate_range":(12351,12395)},
                     5:{"comment":"西伯利亚K1耳机*1","rate":"0.2%","daily_count":1,"rw_type":2,"rate_range":(12396,12595)},
                     6:{"comment":"智能运动手环*1","rate":"0.2%","daily_count":2,"rw_type":2,"rate_range":(12596,12795)},
                     7:{"comment":"小米充电宝*1","rate":"0.2%","daily_count":1,"rw_type":2,"rate_range":(12796,12996)},
                     8:{"comment":"马草*2","rate":"21%","daily_count":9999,"rw_type":1,"rate_range":(12996,33995),"prop_id":6001001,"count":2},
                     9:{"comment":"重生石*1","rate":"1%","daily_count":20,"rw_type":1,"rate_range":(33996,34995),"prop_id":6003001,"count":1},
                     10:{"comment":"谢谢惠顾","rate":"65%","daily_count":9999,"rw_type":0,"rate_range":(34996,99995)},
                     0:{"comment":"iPhone XS*1","rate":"0.005%","daily_count":0,"rw_type":2,"rate_range":(99996,100000)},
                    }