# -*- coding: utf-8 -*-
import random
import string
import re
import time
import base64
import hmac
import d_grade
import d_gene
import d_items
import genUUID64
import json
import socket
from decimal import Decimal
import datetime as dt

def get_gene_level(gene_id):
    """
    概率随机属性
    """
    gene = d_gene.datas.get(gene_id)
    return gene["level"]

def get_qua_type(sex, qua, gene):
    """
    1为普通 2为优秀 3为卓越 4为完美
    """
    grade = d_grade.datas.get(sex)
    if qua <= grade["firstmax"]:
        qua_type = 1
        qua_name = "普通"
    elif qua <= grade["secondmax"]:
        qua_type = 2
        qua_name = "优秀"
    elif qua <= grade["thirdmax"]:
        qua_type = 3
        qua_name = "卓越"
    else:
        isPerfect = True
        gene_list = list()
        fid_list = re.split("[{,}]", gene)
        for _id in fid_list:
            if _id == '':
                continue
            gene_list.append(int(_id))
        if len(gene_list) >= 4:
            for gene_id in gene_list:
                if get_gene_level(gene_id) < 3:
                    isPerfect = False
                    break
        else:
            isPerfect = False
        if isPerfect:
            qua_type = 4
            qua_name = "完美"
        else:
            qua_type = 3
            qua_name = "卓越"
    return qua_type,qua_name


def get_accountname_uuid_nickname():
    uuid64 = genUUID64.genUUID64().get()
    strcase = string.ascii_lowercase.upper()
    uuid = "".join(random.sample(list(strcase), 2))
    uuid += "".join(random.sample(list(str(uuid64)), 5))
    nickname = "骑士" + str(uuid)
    return uuid, nickname, uuid64


def ismobile(mobile):
    """
    是否手机号码
    @param mobile: 手机号码
    @return: boolean
    """
    #为了方便测试，增加8开头的测试手机号码段
    return re.match('^0?[18][34578]\d{9}$', mobile)

def get_random_string(str_num = 7):
    random_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9','A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    return ''.join(random.sample(random_list, str_num))


def check_required_params(param, param_dict):
    """检查必须参数"""

    if isinstance(param, (list, tuple)):
        for p in param:
            if p not in param_dict:
                return False
        return True

    if isinstance(param, dict):
        for k, v in param.items():
            if k not in param_dict:
                return False
        return True

    if param not in param_dict:
        return False
        
    return True

def get_prop_name(prop_id):
    items = d_items.datas.get(int(prop_id))
    if items is not None:
        return items["name"]
    return ""

def convert_to_dict(obj):
    # 对象转字典
    dict = {}
    dict.update(obj.__dict__)
    return dict