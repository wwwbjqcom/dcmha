# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import pymysql

local_conn = pymysql.connect(host='192.168.212.204', user='root', passwd='bsrt123,./', port=3306, db='xz_test',
                                         charset="utf8")
sql = 'update t2 set a=%s where a=%s'

cur = local_conn.cursor()

cur.executemany(sql,[('cc','c'),('d','c')])

local_conn.commit()
cur.close()
local_conn.close()