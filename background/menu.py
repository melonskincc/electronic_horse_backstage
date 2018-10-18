# -*- coding: utf-8 -*-

from logger import *
import json
class Menu:
    def __init__(self, redis, mysql):
        self.redis   = redis
        self.mysql   = mysql
        self.db_info = {}

    def role_user_menu(self, params):
        """获取玩家在线统计详情"""

        sql = ""
        data = self.mysql.get_list("default", sql)
        reply_str = '{"code":0, "num":%d, "data":%s}' % (len(data), json.dumps(data, ensure_ascii=False))

        return reply_str

    def _list(self, params):
        """菜单列表"""

        page_no   = int(params.get('page_no', '1'))
        page_size = int(params.get('page_size', '20'))
        sort      = params.get('sort', 'desc').strip()
        orderby   = params.get('orderby', 'menu_id').strip()
        menu_id   = int(params.get('menu_id', '0'))
        menu_type = int(params.get('menu_type', '-1'))   # 菜单类型 1.父菜单; 2.子菜单

        if page_no <= 0 or orderby not in ('menu_id') or sort not in ('asc', 'desc'):
            return '{"code":1,"errmsg":"params error"}'

        sql = "select count(menu_id) as num, *from menu where menu_id > 0"
        if menu_id:
            sql += ' and menu_id=%s' % menu_id

        if menu_type == 0:
            sql += ' and parent_id=0'

        if menu_type > 0:
            sql += ' and parent_id>0'

        sql += " order by %s %s limit %d, %d;" % (orderby, sort, (page_no-1)*page_size, page_size)
        data = self.mysql.get_list("default", sql)

        return '{"code":0, "page_no":%d, "page_size":%d, "data":%s}' % (page_no, page_size, json.dumps(data, ensure_ascii=False))


    def detail(self, params):
        """菜单详情"""

        menu_id   = int(params.get('menu_id', '0'))
        if menu_id <= 0:
            return '{"code":1,"errmsg":"params error"}'

        sql = "select*from menu where menu_id=%d" % menu_id
        menu = self.mysql.get_one("default", sql)

        if not menu:
            return '{"code":1,"errmsg":"menu does not exis"}'

        return '{"code":0, "menu":%s}' % json.dumps(menu, ensure_ascii=False)


    def save(self, params):
        """保存菜单"""

        args = request.args
        menu_id    = int(args.get('menu_id', '0'))
        href_html  = args.get('href_html', '').strip()
        menu_name  = args.get('menu_name', '').strip()
        icon       = args.get('icon', '').strip()
        menu_type  = int(args.get('menu_type', '0'))   # 菜单类型 1.父菜单; 2.子菜单
        sort_order = int(args.get('sort_order', '0'))
        parent_id  = int(args.get('parent_id', '0'))

        if menu_type not in (1, 2):
            return '{"code":1,"errmsg":"menu_type must in (1, 2)"}'

        if menu_type == 2 and parent_id <= 0:
            return '{"code":1,"errmsg":"params error"}'

        if menu_type != 2:
            parent_id = 0

        if menu_id <= 0:
            # 判断新增链接html文件是否已经存在
            if menu_type == 1:
                sql = 'select menu_id from menu where href_html=%s and parent_id=0' % href_html
            else:
                sql = 'select menu_id from menu where href_html=%s and parent_id>0' % href_html
            is_href_html = self.mysql.get_one("default", sql)
            if is_href_html:
                return u'新增文件:%s已经存在'% href_html

            # 判断新增菜单名称是否存在
            sql = 'select menu_id from menu where menu_name=%s' % menu_name
            is_menu_name = self.mysql.get_one("default", sql)
            if is_menu_name:
                return u'新增菜单名称:%s已经存在'% menu_name
            sql = "insert into menu (parent_id, href_html, menu_name, icon, sort_order) \
                  value(%s, %s, '%s', '%s', %s)" %\
                  (parent_id, href_html, menu_name, icon, sort_order)
        else:
            sql = 'update `menu` set parent_id=%s, href_html=%s, menu_name=%s, icon=%s, sort_order=%s where menu_id=%s' %\
             (parent_id, href_html, menu_name, icon, sort_order, menu_id)

        cur.execute(sql)
        db.commit()

        return '{"code":0}'


    def user_menu(self, params):
        """角色用户菜单"""

        role_id = int(params.get('role_id', '0'))
        menu_list, href_html_list = [], []
        if role_id > 0:
            # 获取用户权限列表
            sql = "select menu_id, parent_menu_id from role_menu where role_id=%s" % role_id
            rm_list         = self.mysql.get_list("default", sql)
            child_id_list   = [rm['menu_id'] for rm in rm_list]
            parent_id_list  = [rm['parent_menu_id'] for rm in rm_list]

            # 如果是超级管理员，就显示所有菜单
            if role_id == 1:
                sql = "select*from menu where parent_id=0"
            else:
                sql = "select*from menu where parent_id=0 and parent_menu_id in %s" % parent_id_list
            parent_menu_list = self.mysql.get_list("default", sql)

            for pm in parent_menu_list:
                child_list = []
                sql = "select*from menu where parent_id>0 and parent_id=%s" % pm['menu_id']
                child_menu_list = self.mysql.get_list("default", sql)
                for cm in child_menu_list:
                    child_dict = {}
                    child_dict['menu_name'] = u'%s'% cm['menu_name']
                    child_dict['href_html'] = cm['href_html']
                    child_list.append(child_dict)
                    href_html_list.append(cm['href_html'])

                href_html = pm['href_html']
                base_menu_dict ={
                            'menu_name':u'%s'% pm['menu_name'],
                            'href_html':u'%s'% href_html,
                            'icon':u'%s' % pm['icon'],
                            'child_menu_list':child_list
                        }
                menu_list.append(base_menu_dict)
                
                if href_html:
                    href_html_list.append(href_html)
            
        return tojson(0, {'menu_list':menu_list, 'href_html_list':href_html_list})

