# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys,time
import traceback
from zkhandle import ZkHandle
from Loging import Logging
from InitDB import InitMyDB
from OperationDB import Operation
sys.path.append("..")
from config.get_config import GetConf
from Binlog.Replication import ReplicationMysql
from Binlog.ParseEvent import ParseEvent
from Binlog.PrepareStructure import GetStruct



class DBHandle:
    def __init__(self):
        self.local_conn = InitMyDB().Init()

    def check(self):
        return True if self.local_conn else False

    def closing(self):
        self.local_conn.close()



def CheckDB():
    __retry_num = 0
    downed_state = None
    binlog_stat,gtid_stat = ZkHandle().GetReplStatus()

    '''第一次启动检查是否为宕机恢复的master，如果是检查是否有需要回滚的数据'''
    if binlog_stat and gtid_stat:
        if DBHandle.check():
            Operation(binlog_stat=binlog_stat)
        else:
            Logging(msg='mysql server is not running,plase checking your mysql server',level='error')


    while True:
        __retry_num += 1 if DBHandle().check() is None else 0
        if __retry_num >= GetConf().GetServerRetryNum():
            ZkHandle().delete('server')
            ZkHandle.Closing()
            downed_state = True


