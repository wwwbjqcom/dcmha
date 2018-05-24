# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import sys,time
from zkhandle import ZkHandle
from Loging import Logging
from InitDB import InitMyDB
from SaveSql import SaveSql
from OperationDB import Operation
from OperationDB import ChangeMaster
from contextlib import closing
sys.path.append("..")
from config.get_config import GetConf




class DBHandle(InitMyDB):
    def __init__(self):
        super(DBHandle,self).__init__()

    def check(self):
        connection = self.Init()
        if connection:
            connection.close()
            return True
        else:
            False

    def closing(self):
        self.closing()


def RollBbinlog():
    with closing(ZkHandle()) as zkhandle:
        binlog_stat,gtid_stat = zkhandle.GetReplStatus()
    Logging(msg='binlog_stat: {} gtid_stat: {}'.format(binlog_stat,gtid_stat),level='info')
    roll_stat = None
    if binlog_stat and gtid_stat:
        if DBHandle().check():
            transaction_sql_list, rollback_sql_list = Operation(binlog_stat=binlog_stat)    #获取回滚sql及当时执行的sql
            roll_stat = DBHandle().ExecuteSQL(rollback_sql_list)    #回滚数据
            #原sql写入文件
            SaveSql().ToFile(transaction_sql_list) if roll_stat else Logging(msg='execute rollback statement failed')
        else:
            Logging(msg='mysql server is not running,plase checking your mysql server',level='error')

    elif gtid_stat:
        Change(binlog_stat=binlog_stat, gtid_stat=gtid_stat)

    if roll_stat:
        Change(binlog_stat=binlog_stat,gtid_stat=gtid_stat)

def Change(binlog_stat=None,gtid_stat=None):
    groupname = binlog_stat[0] if binlog_stat else gtid_stat[0]
    with closing(ZkHandle()) as zkhandle:
        master_host, my_ip = zkhandle.GetMasterHost(groupname=groupname)
    connction = DBHandle().Init()
    change_stat = ChangeMaster(mysqlconn=connction, master_host=master_host, gtid=gtid_stat[1])
    if change_stat:
        Logging(msg='Change to new master succeed..............', level='info')
        with closing(ZkHandle()) as zkhandle:
            zkhandle.retry_create(type='server')
            zkhandle.DeleteDownStatus()
        return True
    else:
        Logging(msg='Change to new master failed, exit now..............', level='error')


def CheckDB():
    __retry_num = 0
    downed_state = None
    '''第一次启动检查是否为宕机恢复的master，如果是检查是否有需要回滚的数据'''
    RollBbinlog()


    while True:
        __retry_num = __retry_num + 1 if DBHandle().check() is None else 0
        if __retry_num == GetConf().GetServerRetryNum():
            #with closing(ZkHandle()) as zkhandle:
            delete_sate = ZkHandle().delete('server')
            while True:
                if delete_sate:
                    break
                else:
                    delete_sate = ZkHandle().delete('server')

            ZkHandle().close()
            downed_state = True

        time.sleep(1)

        if downed_state and __retry_num == 0:
            RollBbinlog()
            downed_state = None


