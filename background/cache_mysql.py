# -*- coding: utf-8 -*-
import pymysql
import datetime
import threading
from logger import *

class cache_mysql:
    db_info = {"default": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse"},
               "hashdb1": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse1"},
               "hashdb2": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse2"},
               "hashdb3": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse3"},
               "hashdb4": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse4"},
               "hashdb5": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse5"},
               "hashdb6": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse6"},
               "hashdb7": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse7"},
               "hashdb8": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse8"},
               "hashdb9": {"username": "kbe", "password": "stringto2782", "databaseName": "BusinessHorse9"}}

    def __init__(self):
        self._mutex = threading.Lock()
        self._connection_pool = {
            "default": [], "hashdb1": [], "hashdb2": [], "hashdb3": [], "hashdb4": [],
            "hashdb5": [], "hashdb6": [], "hashdb7": [], "hashdb8": [], "hashdb9": []
        }
        for dbname in self._connection_pool.keys():
            db = self.conn_mysql(dbname)
            self._connection_pool[dbname].append(db)

    def __put_db(self, dbname, db):
        self._mutex.acquire()
        self._connection_pool[dbname].append(db)
        self._mutex.release()

    def __get_db(self, dbname):
        db = None
        self._mutex.acquire()
        if len(self._connection_pool[dbname]) > 0:
            db = self._connection_pool[dbname].pop()
        self._mutex.release()
        if db is not None:
            try:
                db.ping()
            except Exception as e:
                ERROR_MSG("cache_mysql.__get_db:error=%s" % str(e))
                db = self.conn_mysql(dbname)
        else:
            db = self.conn_mysql(dbname)
        return db

    def get_one(self, dbname, sql):
        DEBUG_MSG("cache_mysql.__get_one:dbname=%s,sql=%s" % (dbname, sql))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        dic = dict()
        try:
            cur.execute(sql)  # 执行sql语句
            results = cur.fetchall()  # 获取查询的所有记录
            field_list = list()
            if len(results) == 1:
                for field in cur.description:
                    field_list.append(field[0])
                row = results[0]
                for i, field in enumerate(field_list):
                    if type(row[i]) == datetime.datetime:
                        dic[field] = str(row[i])
                    else:
                        dic[field] = row[i]
        except Exception as e:
            ERROR_MSG("cache_mysql.__get_one:error=%s" % str(e))
        finally:
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
        return dic

    def get_list(self, dbname, sql):
        DEBUG_MSG("cache_mysql.__get_list:dbname=%s,sql=%s" % (dbname, sql))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        data_list = list()
        try:
            cur.execute(sql)  # 执行sql语句
            results = cur.fetchall()  # 获取查询的所有记录
            field_list = list()
            if len(results) > 0:
                for field in cur.description:
                    field_list.append(field[0])
                # 遍历结果
                for row in results:
                    dic = dict()
                    for i, field in enumerate(field_list):
                        if type(row[i]) == datetime.datetime:
                            dic[field] = str(row[i])
                        else:
                            dic[field] = row[i]
                    data_list.append(dic)
        except Exception as e:
            ERROR_MSG("cache_mysql.__get_list:error=%s" % str(e))
        finally:
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
        return data_list

    def get_count(self, dbname, sql):
        DEBUG_MSG("dbname=%s,sql=%s" % (dbname, sql))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        count = 0
        try:
            cur.execute(sql)  # 执行sql语句
            results = cur.fetchall()  # 获取查询的所有记录
            # 遍历结果
            for row in results:
                count += int(row[0])
        except Exception as e:
            ERROR_MSG("cache_mysql.__get_count:error=%s" % str(e))
        finally:
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
        return count

    def list_count(self, dbname, sql):
        DEBUG_MSG("dbname=%s,sql=%s" % (dbname, sql))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        result = 0
        try:
            result = cur.execute(sql)  # 查询列表总共多少条数据
        except Exception as e:
            ERROR_MSG("cache_mysql.__get_count:error=%s" % str(e))
        finally:
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
        return result

    def get_count_dict(self, dbname, sql):
        #DEBUG_MSG("cache_mysql.__get_count_dict:dbname=%s,username=%s,password=%s,databaseName=%s" % (dbname, self.db_info[dbname]["username"], self.db_info[dbname]["password"],self.db_info[dbname]["databaseName"]))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        count_dict = {}
        all_count = 0
        try:
            cur.execute(sql)  # 执行sql语句
            results = cur.fetchall()  # 获取查询的所有记录
            # 遍历结果
            for row in results:
                count = str(row[0])
                if count not in count_dict:
                    count_dict[count] = 0
                count_dict[count] += 1
                all_count += int(row[0])
        except Exception as e:
            ERROR_MSG("cache_mysql.__get_count_dict:error=%s" % str(e))
        finally:
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
        return all_count, count_dict

    def execute(self, dbname, sql):
        #DEBUG_MSG("cache_mysql.execute:dbname=%s,sql=%s" % (dbname, sql))
        # 打开数据库连接
        db = self.__get_db(dbname)
        # 使用cursor()方法获取操作游标
        cur = db.cursor()
        try:
            ret=cur.execute(sql)
            db.commit()
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
            if ret<=0:
                return False
            return True
        except Exception as e:
            ERROR_MSG("cache_mysql.__execute:error=%s" % str(e))
            cur.close()  # 关闭连接
            self.__put_db(dbname, db)
            return False

    def conn_mysql(self, dbname):
        # 打开数据库连接  120.79.187.94   47.106.205.225
        db = pymysql.connect(host="localhost", user=self.db_info[dbname]["username"],
                             password=self.db_info[dbname]["password"], db=self.db_info[dbname]["databaseName"],
                             port=3306, charset='utf8', autocommit = True)
        return db
