# -*- coding: utf-8 -*-
import hashlib
import time
import datetime
import json
import base64
import random
import ast
import manageToken
from d_items import datas
from lib.timeHelp import getNow, getTimeOClockOfToday, get0ClockOfNextDay, date2Str, date2timestamp, str2Date
from logger import *
import HelpFun
import NameHash
import MobileIdentify
from official.define import reward_config_table


class User(object):
    purify_prop_id = 6001001  # 马草
    give_prop_id = 6002001  # 抵扣券
    renascence_prop_id = 6003001  # 重生石
    integral_prop_id = 6004001  # 宝石
    def __init__(self, cache_redis, cache_mysql, background, share):
        self.cache_redis     = cache_redis
        self.cache_mysql     = cache_mysql
        self.background      = background
        self.mobileIdentify  = MobileIdentify.MobileIdentify(cache_mysql)
        self.share = share
        self.login_user = dict()
 
    def signup(self, params):
        """
        推荐用户注册（从APP分享）
        """
        if "mobile" not in params or "invitecode" not in params or "password1" not in params or "password2" not in params:
            return '{"code":1,"errmsg":"参数错误"}'
        if not HelpFun.ismobile(params["mobile"]):
            return '{"code":1, "errmsg":"请输入正确的手机号码"}'
        errorno = self.mobileIdentify.checkIdentifyCode(params["mobile"], int(params["identify_code"]))
        if errorno == 300:
            return '{"code":1,"errmsg":"验证码发送超时"}'
        if errorno == 301:
            return '{"code":1,"errmsg":"验证码无效"}'
        if errorno == 302:
            return '{"code":1,"errmsg":"验证码不正确"}'
        if params["password1"] != params["password2"]:
            return '{"code":2,"errmsg":"两次密码不相等"}'
        if params["invitecode"] == "":
            return '{"code":2,"errmsg":"邀请码不可以为空"}'
        channelcode   = params.get('partnerid', 'AAAAAA').strip()       # 渠道码
        invitecode = params.get('invitecode', '').strip()
        if invitecode:
            sql = "select * from ("
            index = 1
            for db_name in self.cache_mysql.db_info.keys():
                sql_select = "(select sm_accountName as accountName, sm_selfInvitationCode as selfInvitationCode, sm_channelCode as channelCode \
                             from {databaseName}.tbl_Account where {databaseName}.tbl_Account.sm_selfInvitationCode='{invitecode}')".\
                             format(databaseName=self.cache_mysql.db_info[db_name]["databaseName"], invitecode=invitecode)
                if index == 1:
                    sql += sql_select
                else:
                    sql += "union" + sql_select
                index += 1
            sql += ") as inviter;"
            inviter = self.cache_mysql.get_one("default", sql)
            if not inviter:
                return '{"code":1,"errmsg":"邀请码不正确"}'
        result = self.background.add_user(params["mobile"], params["password1"], invitecode, channelcode)
        if result == 1:
            return '{"code":3,"errmsg":"您已经注册过了， 感谢您的支持"}'
        if result == 2:
            return '{"code":4,"errmsg":"注册失败！"}'
        # 给邀请人奖励
        #if "type" in params and inviter:
        if inviter:
            self.share.reward(inviter["accountName"], invitecode)
        active=params.get('active','').strip()
        if active:
            #抽奖次数+1
            count=self.cache_redis.get_lottery_count(active)
            self.cache_redis.set_lottery_count(active,count+1,get0ClockOfNextDay()-getNow())
            self.cache_redis.push_lottery_register(params['mobile'])

        return '{"code":0, "msg":"注册成功！"}'

    def identify_code(self, params):
        """
        获取验证码
        """
        if len(params) != 1:
            DEBUG_MSG('params:%s' % str(params))
            return
        if "mobile" not in params:
            return '{"code":1,"errmsg":"参数错误"}'
        mobile = base64.b64decode(params["mobile"].encode('utf-8')).decode()
        DEBUG_MSG('mobile:%s' % mobile)
        if not HelpFun.ismobile(mobile):
            return '{"code":1, "errmsg":"手机号码不正确"}'
        errorno, text = self.mobileIdentify.buildIdentifyCode(mobile)  # 发送验证码
        if errorno != 201:
            return '{"code":1,"errmsg":"发送验证码失败"}'

        return '{"code":0, "errmsg":"验证码已经发送，请注意查收!"}'

    def login(self, params):
        """
        用户登录（登录成功返回邀请码和渠道码）
        """
        if "mobile" not in params or "password" not in params:
            return '{"code":1,"errmsg":"参数错误"}'
        mobile = params["mobile"].strip()
        name_hash = NameHash.NameHash(mobile)
        dbname = name_hash.crcHash()
        sql = "select sm_selfInvitationCode as invitecode, sm_channelCode as channelcode, password from tbl_Account, kbe_accountinfos \
              where tbl_Account.sm_accountName=kbe_accountinfos.accountName and tbl_Account.sm_accountName='%s'" % mobile
        player = self.cache_mysql.get_one(dbname, sql)
        if not player:
            return '{"code":6,"errmsg":"您还没有注册游戏, 请先注册账号: %s, 再领取吧！"}' % mobile
        password = params["password"].strip()
        if password != player['password']:
            return '{"code":1, "errmsg":"密码不正确"}'
        # 生成token
        token = manageToken.manageToken.generate_token(mobile)
        del player["password"]
        player["token"] = token
        return '{"code":0, "data":%s}' % json.dumps(player, ensure_ascii=False)

    def get_invitation(self, params):
        """
        邀请玩家信息
        """
        if "invitecode" not in params or "page" not in params or "page_size" not in params or "sort" not in params:
            return '{"code":1,"errmsg":"缺少必须参数"}'
        if not params["page"].isdigit() or not params["page_size"].isdigit():
            return '{"code":1,"errmsg":"参数出错"}'
        page = int(params['page'])
        if page <= 0:
            return '{"code":1,"errmsg":"页数不正确"}'
        page_size = int(params['page_size'])
        if page_size <= 0:
            return '{"code":1,"errmsg":"列表尺寸不正确"}'
        sort = params.get('sort', 'desc').strip() # asc 升序排序; desc 降序排序;
        if sort not in ("asc", "desc"):
            return '{"code":1,"errmsg":"排序类型不正确"}'
        invitecode = params["invitecode"].strip()
        if invitecode == "":
            return '{"code":1,"errmsg":"邀请码为空"}'
        sql = "select * from ("
        index = 1
        for db_name in self.cache_mysql.db_info.keys():
            sql_select = "(select sm_accountName as accountName, regtime " \
                         "from {databaseName}.tbl_Account, {databaseName}.kbe_accountinfos where " \
                         "{databaseName}.tbl_Account.sm_accountName={databaseName}.kbe_accountinfos.accountName and " \
                         "{databaseName}.tbl_Account.sm_loginNum<>0 and " \
                         "{databaseName}.tbl_Account.sm_invitationCode='{invitecode}')". \
                format(databaseName=self.cache_mysql.db_info[db_name]["databaseName"], invitecode=invitecode)
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as player order by regtime %s limit %d, %d;" % (sort, (page-1)*page_size, page_size)
        player_list = self.cache_mysql.get_list('default', sql)
        print("player_list", player_list)
        count = self._get_invitation_count(invitecode)
        return '{"code":0, "count":%d, "data":%s}' % (count, json.dumps(player_list, ensure_ascii=False))

    def check_identify(self):
        self.mobileIdentify.check_identify()

    def _get_invitation_count(self, invitecode):
        sql = "select count(*) as count from ("
        index = 1
        for db_name in self.cache_mysql.db_info.keys():
            sql_select = "(select sm_accountName as accountName " \
                         "from {databaseName}.tbl_Account, {databaseName}.kbe_accountinfos where " \
                         "{databaseName}.tbl_Account.sm_accountName={databaseName}.kbe_accountinfos.accountName and " \
                         "{databaseName}.tbl_Account.sm_loginNum<>0 and " \
                         "{databaseName}.tbl_Account.sm_invitationCode='{invitecode}')". \
                format(databaseName=self.cache_mysql.db_info[db_name]["databaseName"], invitecode=invitecode)
            if index == 1:
                sql += sql_select
            else:
                sql += "union" + sql_select
            index += 1
        sql += ") as inviter;"
        count = self.cache_mysql.get_count('default', sql)
        print("count", count)
        return count

    def get_user_lottery_count(self,request:dict):
        """获取用户抽奖次数"""
        i = datetime.datetime.now()
        i_timestamp = date2timestamp(i)
        if i_timestamp < 1538323200 or i_timestamp > 1538927940:
            return {'code': 0, 'data':{'count':0}}

        accountName=request.get('accountName','')
        if not accountName:
            return {'code':1,'errmsg':'参数错误'}

        sql="select count(id) from lottery_record where create_time between {} and {} and account_name='{}';".format(getTimeOClockOfToday(),getNow(),accountName)
        dictCount=self.cache_mysql.get_one('default',sql)
        lottery_count=dictCount.get('count(id)',0)
        count=self.cache_redis.get_lottery_count(accountName)
        # 判断今天是否抽过奖
        if lottery_count<=0 and count<=0:
            count+=1
            self.cache_redis.set_lottery_count(accountName,count,get0ClockOfNextDay()-getNow())
        self.cache_redis.push_enter_active(accountName)
        return {'code':0,'data':{'count':count}}

    def do_lottery(self,request:dict):
        """用户抽奖"""
        #判断时间，如果是凌晨00:00-00:15不能抽奖
        i=datetime.datetime.now()
        i_timestamp=date2timestamp(i)
        if i_timestamp<1538323200 or i_timestamp>1538927940:
            return {'code': 111, 'errmsg': '活动10月1日0:00-10月7日23:59开放!'}
        if i.hour==0 and i.minute<=5:
            return {'code':1,'errmsg':'请于00:05分之后再来抽奖吧！'}
        accountName=request.get('accountName','')
        if not accountName:
            return {'code':2,'errmsg':'参数错误！'}

        name_hash = NameHash.NameHash(accountName)
        dbname = name_hash.crcHash()
        sql = "select sm_selfInvitationCode as invitecode, sm_channelCode as channelcode, password from tbl_Account, kbe_accountinfos \
                              where tbl_Account.sm_accountName=kbe_accountinfos.accountName and tbl_Account.sm_accountName='%s'" % accountName
        player = self.cache_mysql.get_one(dbname, sql)
        if not player:
            return '{"code":6,"errmsg":"您还没有注册游戏, 请先注册账号: %s, 再抽奖吧！"}' % accountName

        # 判断用户抽奖次数
        count=self.cache_redis.get_lottery_count(accountName)
        is_share=self.cache_redis.get_is_share(accountName)
        if count<=0 and is_share==0:
            return {'code':3,'errmsg':'分享可获得1次抽奖机会!'}
        if count<=0 and is_share==1:
            return {'code':4,'errmsg':'抽奖次数用尽,欢迎下次光临!'}

        code=random.randint(1,100000)

        for k,v in reward_config_table.items():
            rate_tuple=v.get('rate_range')
            if code>=rate_tuple[0] and code<=rate_tuple[1]:
                if k!=8 and k!=10:
                    res=self.cache_redis.modify_award_num(k)
                    if res==0:
                        new_lottery=self.do_lottery(request)
                        if new_lottery:
                            return new_lottery

                # 抽奖成功，mysql写入抽奖记录，redis奖少抽奖次数，redis奖品数量减少，如果type=1,玩家背包增加道具
                ret_sql = "insert into lottery_record (award_name,type,account_name,create_time) values('{}',{},'{}',{});" \
                    .format(v.get('comment'), v.get('rw_type'), accountName, getNow())
                if v.get('rw_type')==1:
                    if v['prop_id'] not in datas:
                        return {'code':10,'errmsg':'暂无该道具!'}
                    dictData=dict()
                    if v['prop_id'] == self.purify_prop_id:
                        #马草
                        dictData = ast.literal_eval(self.background.add_purify({'accountName':accountName,'count':v['count']}))
                    elif v['prop_id'] == self.give_prop_id:
                        #装备
                        dictData = ast.literal_eval(self.background.add_give({'accountName':accountName,'count':v['count']}))
                    elif v['prop_id'] == self.renascence_prop_id:
                        #重生石
                        dictData = ast.literal_eval(self.background.add_renascence({'accountName':accountName,'count':v['count']}))
                    elif v['prop_id']==self.integral_prop_id:
                        #积分
                        pass
                    else:
                        if datas[v['prop_id']]["type"] > 100:
                            #京东券
                            dictData = ast.literal_eval(self.background.add_other_prop({'propId':v['prop_id'], 'accountName':accountName,'count':v['count']}))
                        else:
                            dictData = ast.literal_eval(self.background.add_prop({'propId':v['prop_id'], 'accountName':accountName,'count':v['count']}))

                    if dictData.get('code','')!=0:
                        ERROR_MSG("给用户:[{}]，添加抽奖道具:[{}],数量:[{}],失败！需手动给用户添加！".format(accountName,v['prop_id'],v['count']))
                        ret_sql = "insert into lottery_record (award_name,type,account_name,create_time,is_receive) values('{}',{},'{}',{},0);" \
                            .format(v.get('comment'), v.get('rw_type'), accountName, getNow())
                    else:
                        ret_sql="insert into lottery_record (award_name,type,account_name,create_time,is_receive) values('{}',{},'{}',{},1);"\
                            .format(v.get('comment'),v.get('rw_type'),accountName,getNow())

                ret = self.cache_mysql.execute('default', ret_sql)
                id_sql = "SELECT LAST_INSERT_ID() as record_id;"
                dictId = self.cache_mysql.get_one('default', id_sql)
                if not ret:
                    ERROR_MSG("需手动向表数据添加该列:[{}]".format(ret_sql))

                self.cache_redis.set_lottery_count(accountName,count-1,get0ClockOfNextDay()-getNow())
                is_exists=self.cache_redis.redis_conn.hexists(self.cache_redis.lottery_data_key.format(date2Str(datetime.datetime.now(),"%Y-%m-%d")),accountName)
                if not is_exists:
                    self.cache_redis.set_daily_lottery(accountName,0)

                return {'code':0,'data':{'id':k,'record_id':dictId.get('record_id'),'type':v.get('rw_type')}}

    def get_award_pool_info(self,request:dict):
        """获取奖池奖品信息"""
        listData=[]
        for k,v in reward_config_table.items():
            dictData=dict()
            dictData['comment']=v.get('comment')
            dictData['id']=k
            listData.append(dictData)
        return {'code':0,'data':listData}

    def write_address(self,request:dict):
        """填写收货地址"""
        if not HelpFun.check_required_params(['id','real_name','mobile','address'],request):
            return {'code':1,'errmsg':'参数不足!'}
        if not all([request['id'],request['real_name'],request['mobile'],request['address']]):
            return {'code': 2, 'errmsg': '信息错误!'}
        sql="update lottery_record set real_name='{}',mobile='{}',address='{}',is_checkin=1,checkin_time={} where id={} " \
            "and type=2 and is_checkin=0 and account_name='{}';".\
            format(request['real_name'],request['mobile'],request['address'],getNow(),request['id'],request['g_user'])
        ret=self.cache_mysql.execute('default',sql)
        if not ret:
            return {'code':3,'errmsg':'登记失败！'}
        return {'code':0,'data':'登记成功！'}

    def lottery_record_info(self,request:dict):
        """获取用户中奖信息"""
        accountName=request.get('accountName','')
        sql="select * from lottery_record order by create_time desc;"
        if accountName:
            sql="select * from lottery_record where account_name='{}' order by create_time desc;".format(accountName)

        listData=self.cache_mysql.get_list('default',sql)
        return {'code':0,'data':listData}

    def wx_jsapi_signature(self,request:dict):
        """获取wx_jsapi签名"""
        url=request.get('url','')
        if not url:
            return {'code':1,'errmsg':'参数错误！'}

        noncestr = HelpFun.get_random_string(16)
        jsapi_ticket = self.cache_redis.get_ticket_token('jsapi_ticket')
        timestamp = getNow()
        signature_str = "jsapi_ticket={}&noncestr={}&timestamp={}&url={}".format(jsapi_ticket, noncestr, timestamp, url)
        sha = hashlib.sha1(signature_str.encode('utf-8'))
        encrypts = sha.hexdigest()
        dictData = {"encrypts": encrypts, "timestamp": timestamp, "noncestr": noncestr}

        return {"code":0,"data":dictData}

    def share_success_callback(self,request:dict):
        """分享成功回调:redis中key对应value为1"""
        i = datetime.datetime.now()
        i_timestamp = date2timestamp(i)
        if i_timestamp < 1538323200 or i_timestamp > 1538927940:
            return {'code': 0, 'data': '分享成功!'}

        accountName=request.get('accountName','')
        if not accountName:
            return {'code': 1, 'errmsg': '参数错误！'}
        # 没分享过抽奖次数+1，否则不加
        ret=self.cache_redis.get_daily_lottery(accountName)
        if ret is None or int(ret)==0:
            self.cache_redis.set_daily_lottery(accountName, 1)
            count = self.cache_redis.get_lottery_count(accountName)
            self.cache_redis.set_lottery_count(accountName, count + 1, get0ClockOfNextDay() - getNow())
            return {"code":0,"data":'分享成功!'}
        return {'code':0,'data':'分享成功!'}



if __name__ == '__main__':
    from main import g_user
    print(g_user.do_lottery({'accountName':'15770633066'}))
