# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import pymysql,sys
import traceback
sys.path.append("..")
from config.get_config import GetConf
from Loging import Logging
from contextlib import closing

class InitMyDB(object):
    def __init__(self,mysql_host=None):
        self.mysql_user = GetConf().GetMysqlUser()
        self.mysql_password = GetConf().GetMysqlPassword()
        self.mysql_port = GetConf().GetMysqlPort()
        self.socket_dir = GetConf().GetSocketDir()
        self.mysql_host = mysql_host if mysql_host else '127.0.0.1'

    def Init(self):
        try:
            connection = pymysql.connect(host=self.mysql_host,
                                              user=self.mysql_user,
                                              password=self.mysql_password, port=self.mysql_port,
                                              db='',
                                              charset='utf8mb4',
                                              unix_socket=self.socket_dir,
                                              cursorclass=pymysql.cursors.DictCursor)
            return connection
        except pymysql.Error, e:
            Logging(msg=traceback.format_exc(),level='error')
            return None

    def ExecuteSQL(self,sql_list = None):
        connection = self.Init()
        with closing(connection.cursor()) as cur:
            try:
                for sql in sql_list[::-1]:
                    Logging(msg='Rollback statement: {}'.format(sql),level='info')
                    cur.execute(sql[0],sql[1])
            except pymysql.Error:
                return False
                Logging(msg=traceback.format_exc(),level='error')
                connection.rollback()
            connection.commit()
        connection.close()
        return True
