# -*- coding: utf-8 -*-
import os
import json
import logging
import tornado.web
import manageToken
import genUUID64
import cache_mysql
import cache_redis
import background
import Statistics
import login
import share
import menu
import recommend.user
import recommend.agent
import official.agent
from logger import *
# from lib import wxTools

# 不需要认证的接口
g_no_authorization__list = ['/agent/signin',"/background_login", "/user/signup", "/user/identify_code", "/user/login", "/get_download_mode","/user/get_award_pool_info","/user/wx_jsapi_signature"]
# 访问de接口需要保存日志
g_operation_log_list = ["/add_vbc","/add_give","/add_purify","/add_renascence","/add_prop","/horse_build","/add_reserve_horse_count","/add_reserve_horse_random",
                        "/official/agent_apply","/del_other_prop"]
# 不需要检查接口的权限
g_no_check_interface_list = ["/agent/signin","/agent/month_profit","/agent/player_info","/agent/child_info",
                             "/agent/month_detail","/user/signup","/user/identify_code", "/user/get_invitation","/official/agent_history_profit",
                             "/user/do_lottery","/user/get_user_lottery_count","/user/write_address","/user/lottery_record_info","/user/wx_jsapi_signature",
                             "/user/share_success_callback"]

g_genUUID64 = genUUID64.genUUID64()
g_upload_head_path = "/home/upload/head"
g_upload_chat_path = "/home/upload/chat"
g_excel_path="./excel/"

g_cache_mysql      = cache_mysql.cache_mysql()
g_cache_redis      = cache_redis.cache_redis()
g_background       = background.background(g_cache_redis, g_cache_mysql)
g_statistics       = Statistics.Statistics(g_cache_redis, g_cache_mysql, g_background)
g_login            = login.login()
g_share            = share.share(g_cache_mysql, g_background)
# g_wx_tools         = wxTools.WxTool(g_cache_redis)
g_user             = recommend.user.User(g_cache_redis, g_cache_mysql, g_background, g_share)
g_agent            = recommend.agent.Agent(g_cache_redis, g_cache_mysql, g_background)
g_official_agent   = official.agent.Agent(g_cache_redis, g_cache_mysql, g_background)
g_menu             = menu.Menu(g_cache_redis, g_cache_mysql)

#g_share.reward("13528816789","6583190910551654417")

g_request_path_dict = {
    "/register"                            : g_statistics.get_register,                        # 获得注册人数
    "/players"                             : g_statistics.get_players,                         # 获得在线人数
    "/pay"                                 : g_statistics.get_pay,                             # 获得充值金额
    "/pay_frequency"                       : g_statistics.pay_frequency,                       # 获得周期内充值用户游戏充值的频次
    "/pay_total"                           : g_statistics.pay_total,                           # 获得周期内充值用户游戏充值的总额
    "/gold_log"                            : g_statistics.gold_log,                            # 金币消费流水
    "/player_info"                         : g_statistics.player_info,                         # 获得用户信息
    "/player_info_count"                   : g_statistics.player_info_count,                   # 获得用户信息
    "/player_horse_detail_info"            : g_statistics.player_horse_detail_info,            # 获得用户马信息
    "/player_procreate_horse_detail_info"  : g_statistics.player_procreate_horse_detail_info,  # 获得用户繁殖马信息
    "/player_detail_info"                  : g_statistics.player_detail_info,                  # 获得用户详细信息
    "/player_gold"                         : g_statistics.player_gold,                         # 获得前100条金币
    "/horse_trade_log"                     : g_statistics.horse_trade_log,                     # 获得交易数据
    "/horse_procreate_log"                 : g_statistics.horse_procreate_log,                 # 获得繁殖数据
    "/player_horse_procreate_log"          : g_statistics.player_horse_procreate_log,          # 获得用户繁殖数据
    "/player_give_log"                     : g_statistics.player_givelog,                      # 获得用户代金卷数据
    "/player_purify_log"                   : g_statistics.player_purify_log,                   # 获得用户马草数据
    "/player_renascence_log"               : g_statistics.player_renascence_log,               # 获得用户重生石数据
    "/business_horse"                      : g_statistics.business_horse,                      # 获得交易市场马数据
    "/business_horse_detail_info"          : g_statistics.business_horse_detail_info,          # 获得交易市场马详细数据
    "/reserve_horse"                       : g_statistics.reserve_horse,                       # 获得预订马数据
    "/horse_info"                          : g_statistics.horse_info,                          # 获得所有马数据
    "/luck_draw_log"                       : g_statistics.luck_draw_log,                       # 抽奖数据
    "/luck_draw_log_update"                : g_statistics.luck_draw_log_update,                # 抽奖数据
    "/integral_log"                        : g_statistics.integral_log,                        # 宝石消费流水
    "/private_message_log"                 : g_statistics.private_message_log,                 # 私信数据
    "/message_log"                         : g_statistics.message_log,                         # 信息数据
    "/reward_gift"                         : g_statistics.reward_gift,                         # 礼物奖励
    "/share_reward_log"                    : g_statistics.share_reward_log,                    # 分享奖励
    "/account_detail"                      : g_statistics.account_detail,                      # 留存、后台数据统计
    "/get_player_prop_list"                : g_statistics.get_player_prop_list,                # 玩家背包道具
    "/del_player_prop"                     : g_statistics.del_other_prop,                      # 删除玩家背包道具
    "/get_red_packet"                      : g_statistics.get_red_packet,                      # 获取玩家红包信息
    "/get_child_packet"                    : g_statistics.get_child_packet,                    # 获取抢红包信息
    "/get_excel_names"                     : g_statistics.get_excel_names,                     # 获取excel名称
    "/get_match_info"                      : g_statistics.get_match_info,                      # 获取比赛信息
    "/get_match_detail"                    : g_statistics.get_match_detail,                    # 获取比赛排名详细信息
    "/get_history_online"                  : g_statistics.get_history_online,                  # 获取历史在线数据
    "/get_history_charge"                  : g_statistics.get_history_charge,                  # 获取历史充值数据
    "/get_charge_detail"                   : g_statistics.get_charge_detail,                   # 获取充值详细数据
    "/daily_lottery_data"                  : g_statistics.daily_lottery_data,                  # 每日抽奖数据
    "/daily_lottery_record"                : g_statistics.daily_lottery_record,                # 每日抽奖记录

    "/is_online"                           : g_background.is_online,                           # 查用户是否在线
    "/add_vbc"                             : g_background.add_vbc,                             # 添加用户金币
    "/add_give"                            : g_background.add_give,                            # 添加用户金币
    "/add_purify"                          : g_background.add_purify,                          # 添加用户马草
    "/add_renascence"                      : g_background.add_renascence,                      # 添加用户重生石
    "/add_prop"                            : g_background.add_prop,                            # 添加用户道具
    "/add_reserve_horse_count"             : g_background.add_reserve_horse_count,             # 添加当前预约0代马数量（为造价）
    "/add_reserve_horse_random"            : g_background.add_reserve_horse_random,            # 添加预约中编码的随机数
    "/horse_build"                         : g_background.horse_build,                         # 添加用户马
    "/send_message"                        : g_background.send_message,                        # 发送消息
    "/send_notice"                         : g_background.send_notice,                         # 发送公告
    "/get_notice"                          : g_background.get_notice,                          # 查看公告
    "/del_notice"                          : g_background.del_notice,                          # 删除公告
    "/mod_notice"                          : g_background.mod_notice,                          # 修改公告
    "/set_chat_mode"                       : g_background.set_chat_mode,                       # 修改用户聊天模式

    "/set_download_mode"                   : g_background.set_download_mode,                   # 设置下载app模式（0为可以下载 1为不可以下载）
    "/get_download_mode"                   : g_background.get_download_mode,                   # 获得下载app模式

    "/background_login"                    : g_login.background_login,                         # 后台登录
    # 代理
    "/agent/signin"                        : g_agent.signin,                                   # 代理用户登录
    "/agent/month_profit"                  : g_agent.month_profit,                             # 代理本月收益/官方
    "/agent/player_info"                   : g_agent.player_info,                              # 代理玩家列表/官方
    "/agent/child_info"                    : g_agent.child_info,                               # 下级代理人列表/官方
    "/agent/month_detail"                  : g_agent.month_detail,                             # 本月直推代理详细信息/官方

    # 用户注册
    "/user/signup"                         : g_user.signup,                                    # 用户注册
    "/user/identify_code"                  : g_user.identify_code,                             # 验证码
    # 用户查自己的邀请码和邀请玩家信息
    "/user/login"                          : g_user.login,                                     # 用户登录（登录成功返回邀请码和渠道码）
    "/user/get_invitation"                 : g_user.get_invitation,                            # 邀请玩家信息
    "/user/do_lottery"                     : g_user.do_lottery,                                # 抽奖
    "/user/get_user_lottery_count"         : g_user.get_user_lottery_count,                    # 获取抽奖次数
    "/user/write_address"                  : g_user.write_address,                             # 填写收获地址
    "/user/get_award_pool_info"            : g_user.get_award_pool_info,                       # 奖池物品信息
    "/user/lottery_record_info"            : g_user.lottery_record_info,                       # 获取中奖记录
    "/user/wx_jsapi_signature"             : g_user.wx_jsapi_signature,                        # 获取获取wxjsapi签名
    "/user/share_success_callback"         : g_user.share_success_callback,                    # 分享成功回调

    "/official/audit"                      : g_official_agent.audit,                           # 代理申请审核
    "/official/agent_apply"                : g_official_agent.agent_apply,                     # 申请代理
    "/official/modify_agent_info"          : g_official_agent.modify_agent_info,               # 修改代理信息
    "/official/audit_list"                 : g_official_agent.audit_list,                      # 代理审核列表
    "/official/freeze_modify"              : g_official_agent.freeze_modify,                   # 冻结/解冻代理
    "/official/agent_history_profit"       : g_official_agent.agent_history_profit,            # 代理历史数据详细数据



    "/menu"                                : g_menu._list,                                     # 获取菜单列表
    "/menu/detail"                         : g_menu.detail,                                    # 菜单详情
    "/menu/save"                           : g_menu.save,                                      # 保存菜单
    "/user_menu"                           : g_menu.user_menu                                  # 获取用户菜单列表
}

def add_operation_log(user, interface, parameter):
    if interface in g_operation_log_list:
        message = json.dumps(json.dumps(parameter))
        sql = "insert into operation_log(user,interface,message)values('%s','%s','%s')" % (user, interface, message)
        g_cache_mysql.execute("default", sql)

def scheduler_10():
    manageToken.manageToken.check_token()
    g_user.check_identify()
    g_share.handle_reward()

def scheduler_20():
    g_cache_redis.set_daily_award_num()
    g_agent.month_statistics()
    # g_statistics.statistics_players()

def scheduler_wxtoken():
    ret=g_wx_tools.get_access_token()
    if not ret:
        tornado.ioloop.PeriodicCallback(g_wx_tools.get_access_token, 1000).start()
    res=g_wx_tools.get_jsapi_ticket()
    if not res:
        tornado.ioloop.PeriodicCallback(g_wx_tools.get_jsapi_ticket, 1000).start()


def init_logging(log_file):
    # 使用TimedRotatingFileHandler处理器
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when="D", interval=1, backupCount=30)
    # 输出格式
    log_formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(funcName)s[%(lineno)d]:%(message)s")

    # 定义handler的输出格式
    file_handler.setFormatter(log_formatter)

    # 将处理器附加到根logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)

class requestHandler(tornado.web.RequestHandler):
    executor = tornado.concurrent.futures.ThreadPoolExecutor(10)

    @tornado.gen.coroutine
    def get(self):
        response = yield self.handler(True)
        self.write(response)
        self.finish()

    @tornado.gen.coroutine
    def post(self):
        response = yield self.handler(False)
        self.write(response)
        self.finish()

    @tornado.concurrent.run_on_executor
    def handler(self, isGet):
        if isGet:
            index = self.request.uri.find("?")
            if index != -1:
                uri = self.request.uri[0:index]
            else:
                uri = self.request.uri
            parameter = dict()
            for key in self.request.arguments:
                parameter[key] = self.get_arguments(key)[0]
        else:
            index = self.request.uri.find("?")
            if index != -1:
                uri = self.request.uri[0:index]
            else:
                uri = self.request.uri
            parameter = dict()
            for key in self.request.arguments:
                parameter[key] = self.get_arguments(key)[0]
        DEBUG_MSG('uri=%s,parameter=%s' % (uri, str(parameter)))
        user = None
        permit_list = g_no_authorization__list
        if uri not in permit_list:
            token = self.request.headers.get('Authorization', None)
            if token is None:
                self.set_status(401)
                reply_str = '{"code":1,"errmsg":"Unauthorized"}'
                ERROR_MSG('user=%s,reply_str=%s' % (user, reply_str))
                return reply_str

            success, user = manageToken.manageToken.certify_token(token)
            if not success:
                self.set_status(401)
                reply_str = '{"code":1,"errmsg":"Unauthorized"}'
                ERROR_MSG('user=%s,reply_str=%s' % (user, reply_str))
                return reply_str
            if uri not in g_no_check_interface_list:
                if not g_login.isAuthorityOfInterface(user, uri):
                    reply_str = '{"code":1,"errmsg":"no Authority"}'
                    ERROR_MSG('user=%s,reply_str=%s' % (user, reply_str))
                    return reply_str
        if uri not in g_request_path_dict:
            reply_str = '{"code":1,"errmsg":"Error request type"}'
        else:
            parameter['g_user']=user
            reply_str = g_request_path_dict[uri](parameter)
        if user is not None:
            add_operation_log(user, uri, parameter)
        return reply_str

    def options(self):
        self.set_status(204)
        self.finish()

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*") # 这个地方可以写域名
        self.set_header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, PUT, OPTIONS, DELETE')

class uploadHandler(tornado.web.RequestHandler):
    executor = tornado.concurrent.futures.ThreadPoolExecutor(10)

    @tornado.gen.coroutine
    def post(self):
        response = yield self.handler()
        self.write(response)
        self.finish()

    @tornado.concurrent.run_on_executor
    def handler(self):
        accountName = self.get_arguments("username")[0]
        file_metas = self.request.files["file"]  # 获取上传文件信息
        content_type = file_metas[0]["content_type"]
        filename = file_metas[0]["filename"]

        if filename == "head":
            path = g_upload_head_path
            writeFileName = accountName + ".png"
        else:
            path = g_upload_chat_path
            if content_type == "image":
                writeFileName = str(g_genUUID64.get()) + ".png"
            else:
                writeFileName = str(g_genUUID64.get()) + ".wav"
        with open(os.path.join(path, content_type, writeFileName), 'wb') as f:  # os拼接文件保存路径，以字节码模式打开
            f.write(file_metas[0]['body'])
        reply_str = '{"code":0,"filename":"%s"}' % writeFileName
        if filename == "head":
            # headUrl = "/download?type=head&content_type=image&filename=" + writeFileName
            data = g_background.mod_user_head(accountName, writeFileName)
            DEBUG_MSG("accountName=%s, data=%s" % (accountName, data))
        return reply_str

class downloadHandler(tornado.web.RequestHandler):
    executor = tornado.concurrent.futures.ThreadPoolExecutor(10)

    @tornado.gen.coroutine
    def get(self):
        response = yield self.handler()
        self.write(response)
        self.finish()

    @tornado.concurrent.run_on_executor
    def handler(self):
        parameter = dict()
        for key in self.request.arguments:
            parameter[key] = self.get_arguments(key)[0]
        content_type = parameter["content_type"]
        readFileName = parameter["filename"]
        if readFileName == "":
            reply_str = "no_url"
        else:
            if parameter["type"] == "head":
                path = g_upload_head_path
            else:
                path = g_upload_chat_path
            fullFileName = os.path.join(path, content_type, readFileName)
            if os.path.exists(fullFileName):
                f = open(os.path.join(path, content_type, readFileName), 'rb')
                reply_str = f.read()
                f.close()
            else:
                reply_str = "no_url"
        return reply_str

    def set_default_headers(self):
        self.set_header("Content-type", "image/png")

class downloadExcelHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type","application/octet-stream")
        parameter = dict()
        for key in self.request.arguments:
            parameter[key] = self.get_arguments(key)[0]
        readFileName = parameter["filename"]
        if readFileName == "":
            self.write("no_url")
            return
        if parameter["type"] == "excel":
            path=g_excel_path
        else:
            self.write("no_url")
            return
        fullFileName = os.path.join(path,readFileName)
        if os.path.exists(fullFileName):
            self.set_header('Content-Disposition', 'attachment; filename='+readFileName)
            buf_size = 4096
            with open(os.path.join(path, readFileName), 'rb') as f:
                while True:
                    data = f.read(buf_size)
                    if not data:
                        break
                    self.write(data)
                self.finish()
        else:
            self.write("no_url")



if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/upload", uploadHandler),
        (r"/download", downloadHandler),
        (r'/download_excel',downloadExcelHandler),
        (r"/.*", requestHandler)
    ])
    # tornado.options.parse_command_line()
    g_prot = 8000
    init_logging("./log/background.log")
    tornado.ioloop.PeriodicCallback(scheduler_10, 10000).start()  # start scheduler 每隔10s执行一次f2s
    tornado.ioloop.PeriodicCallback(scheduler_20, 20000).start()  # start scheduler 每隔20s执行一次f2s
    # tornado.ioloop.PeriodicCallback(scheduler_wxtoken, 300000).start()  # start scheduler 每隔300s执行一次f2s
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(g_prot)
    http_server.start()
    # tornado.autoreload.start()
    INFO_MSG("http_server.start:prot=%d" % g_prot)
    tornado.ioloop.IOLoop.instance().start()
