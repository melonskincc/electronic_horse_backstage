from lib.timeHelp import  timestamp2Str

# def getRegCountSql(start:int,end:int):
#     """获取查询时间内的注册总量和实际注册量"""
#     start=getUnixOfTime(start)
#     end=getUnixOfTime(end)
#     return "select * from statistics_players_register where days between %d and %d"%(start,end)

def getActiveConvCount(start:int,end:int,cache_mysql:object):
    """获取时间内有（喂养、付费、竞技、买卖，4选3）操作的玩家数量Sql 活跃转化数"""
    sql = "select * from ("
    index = 1
    for dbname in cache_mysql.db_info.keys():
        sql_select = "(select distinct accountName,operation from {databaseName}.gold_log where createTime between FROM_UNIXTIME({start}) and FROM_UNIXTIME({end}))".format(
            databaseName=cache_mysql.db_info[dbname]["databaseName"],start=start,end=end)

        if index == 1:
            sql += sql_select
        else:
            sql += "union all" + sql_select
        index += 1
    sql += ") as gold order by accountName;"

    listActiveData = cache_mysql.get_list('default', sql)
    dictActive = dict()
    for dictData in listActiveData:
        if dictData.get('accountName') not in dictActive:
            dictActive[dictData.get('accountName')] = 0
        dictActive[dictData.get('accountName')] += 1

    active_set=set(dictActive.keys())
    for tupActive in dictActive.items():
        if tupActive[1]<3:
            active_set.remove(tupActive[0])

    return len(active_set)

def getPayCountSqlByTime(start:int,end:int):
    """获取时间内充值人数和充值总额"""
    return "select count(distinct accountName) as payCount,sum(money) as payNum from onlinePay where create_time between FROM_UNIXTIME(%d) and FROM_UNIXTIME(%d) " \
        "and status=1 ;"%(start,end)

def getMaxOnlineSqlByTime(start:int):
    """时间内同时在线人数最大值Sql"""
    sql="select GREATEST("
    for x in range(24):
        column=str(x)+"hour"
        for i in range(1,4):
            a_column=column+str(i*15)
            if x==0 and i==1:
                sql+=str(a_column)
            else:
                sql=sql+','+str(a_column)
    date=timestamp2Str(start,"%Y-%m-%d")
    sql+=") as max_online from statistics_players_online where days='%s';"%(date)
    return sql

def getAvgOnlineTimeByTime(start:int,cache_mysql:object):
    """时间内线人数在线总时长Sql"""
    sql="select sum("
    for x in range(24):
        column=str(x)+"hour"
        for i in range(1,4):
            if x==23 and i==3:
                a_column = column + str(i * 15)
            else:
                a_column=column+str(i*15)+"+"
            sql=sql+str(a_column)
    date=timestamp2Str(start,"%Y-%m-%d")
    sql+=") as sum_online from statistics_players_online where days='%s';"%(date)
    dictSumOL=cache_mysql.get_one('default',sql)
    sumOL =dictSumOL.get('sum_online') if dictSumOL.get('sum_online') else 0

    return sumOL*15

def getACUSqlByTime(start:int):
    """ACU时间内每小时在线人数最大值Sql"""
    sql="select "
    for x in range(24):
        sql+="GREATEST("
        column=str(x)+"hour"
        for i in range(1,4):
            a_column=column+str(i*15)
            if i==3:
                sql = sql + str(a_column)
            else:
                sql=sql+str(a_column)+','
        if x==23:
            sql+=") as max_online_%d " % x
        else:
            sql += ") as max_online_%d ," % x
    date=timestamp2Str(start,"%Y-%m-%d")
    sql+=" from statistics_players_online where days='%s';"%(date)
    return sql

def getNewPaySqlByTime(start:int,end:int,cache_mysql:object):
    """获取时间内新用户付费金额，新用户付费人数"""
    sql = "select sum(money) as pay_money ,count(distinct accountName) as pay_count from onlinePay where accountName in(select accountName from("
    index = 1
    for dbname in cache_mysql.db_info.keys():
        sql_select="(select accountName from {databaseName}.kbe_accountinfos where regtime between {start} and {end})"\
            .format(databaseName=cache_mysql.db_info[dbname]["databaseName"],start=start,end=end)
        if index == 1:
            sql += sql_select
        else:
            sql += "union all" + sql_select
        index += 1
    sql += ")as name_table) and create_time between FROM_UNIXTIME({start}) and FROM_UNIXTIME({end}) and status=1;".format(start=start,end=end)
    return sql

def getRetainDataByTime(start:int,end:int,cache_mysql:object):
    # 1.先查出时间段内注册用户数
    # 3.获取次日，3日，7日注册用户中登录用户数
    # 4.计算留存   次日留存：（t-1）日注册的用户在t日登陆用户数/（t-1）日注册用户数*100%	。3日留存：（t-3）日注册的用户在t日登陆用户数/（t-3）日注册用户数*100%
    # 7日留存	（t-7）日注册的用户在t日登陆用户数/（t-7）日注册用户数*100%
    sql="select count(accountName) from ("
    index=1
    for dbname,dbvalue in cache_mysql.db_info.items():
        sql_select="(select accountName from {databaseName}.kbe_accountinfos where regtime between {start} and {end})"\
            .format(databaseName=dbvalue['databaseName'],start=start,end=end)
        if index == 1:
            sql += sql_select
        else:
            sql += "union" + sql_select
        index += 1
    sql+=") as a "
    dictRegCount=cache_mysql.get_one('default',sql)
    regCount=dictRegCount.get('count(accountName)',0)
    if regCount==0:
        return 0,"0.00%","0.00%","0.00%"
    sql=sql.replace(r'count(accountName)','accountName',1)

    login_sql="select count(distinct accountName) as login_count from daily_login_statis where accountName in ("+sql+") " \
    "and lastLoginTime between {start} and {end};"

    dictLogin=cache_mysql.get_one('default',login_sql.format(start=start+24*3600,end=end+24*3600))
    nextDay_login=dictLogin.get('login_count',0)

    dictLogin =cache_mysql.get_one('default',login_sql.format(start=start+24*3600*3,end=end+24*3600*3))
    day3_login = dictLogin.get('login_count', 0)

    dictLogin =cache_mysql.get_one('default',login_sql.format(start=start+24*3600*7,end=end+24*3600*7))
    day7_login = dictLogin.get('login_count', 0)

    return regCount,"%.2f%%"%(round(nextDay_login/regCount,4)*100),"%.2f%%"%(round(day3_login/regCount,4)*100),"%.2f%%"%(round(day7_login/regCount,4)*100)

def getLoginCountSqlByTime(start:int,end:int):
    """获取时间内登录人数"""
    sql="select count(distinct accountName) as login_count from daily_login_statis where lastLoginTime between %d and %d;"%(start,end)
    return sql

def updateSubChannelCode(invitCode:str,channelCode:str,cache_mysql:object,dbnameK:str):
    """递归修改代理树下所有玩家的渠道码"""
    count_sql="select count(id) from tbl_Account where sm_invitationCode='%s' ;"%invitCode
    dictCount=cache_mysql.get_one(dbnameK,count_sql)
    if dictCount['count(id)']==0:
        return 1
    else:
        update_sql="update tbl_Account set sm_channelCode='%s' where sm_invitationCode='%s' ;"%(channelCode,invitCode)
        cache_mysql.execute(dbnameK,update_sql)
        users_sql="select sm_selfInvitationCode as invitcode from tbl_Account where sm_invitationCode='%s' ;"%invitCode
        listUser=cache_mysql.get_list(dbnameK,users_sql)
        for dictUser in listUser:
            return updateSubChannelCode(dictUser['invitcode'],channelCode,cache_mysql,dbnameK)


if __name__ == '__main__':
    # print(getRetainDataByTime(1536422400,1536422400+24*3600))
    from main import g_cache_mysql
    print(getAvgOnlineTimeByTime(1536422400,g_cache_mysql))
    # for dbname in g_cache_mysql.db_info.keys():
    #     updateSubChannelCode('6583190777407668267','Z168497',g_cache_mysql,dbname)
