# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import pymysql,sys
sys.path.append("..")
from config.get_config import GetConf
from lib.InitDB import InitMyDB

class GetStruct:
    def __init__(self):
        self.connection = InitMyDB().Init()
        self.cur = self.connection.cursor()

    def GetColumn(self,*args):
        '''args顺序 database、tablename'''
        column_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY from COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.cur.execute(sql,args=args)
        result = self.cur.fetchall()
        for idex,row in enumerate(result):
            column_list.append(row[0])
            if row[1] == 'PRI':
                pk_idex = idex
        return column_list,pk_idex
