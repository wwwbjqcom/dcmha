# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import pymysql,sys
import traceback
sys.path.append("..")
from config.get_config import GetConf
from Loging import Logging

class InitMyDB:
    def __init__(self):
        self.mysql_user = GetConf.GetMysqlUser()
        self.mysql_password = GetConf.GetMysqlPassword()
        self.mysql_port = GetConf.GetMysqlPort()
        self.socket_dir = GetConf.GetSocketDir()

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