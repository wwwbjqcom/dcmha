# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys
sys.path.append("..")
from zk_handle.zkHandler import zkHander
from contextlib import closing
from TaskOb import TaskCh
import logging
logging.basicConfig(filename='zk_client.log', level=logging.INFO)

class Watch:
    def __init__(self):
        pass

    def StartWatch(self,master_hosts):
        '''创建master监听'''
        for group_name in master_hosts:
            with closing(zkHander()) as zkhander:
                host = zkhander.GetMasterMeta(group_name)
            zkHander().CreateWatch(host)



class Entrance:
    def __init__(self):
        pass

    def Init(self):
        with closing(zkHander()) as zkhander:
            zkhander.InitNode()

            master_hosts = zkhander.GetMasterGroupHosts()         #启动检查是否已有活动的master,如已经有活动的就直接监控，并进行任务检查
                                                                    #如没有则直接进行任务检查
        if master_hosts is not None:
            Watch().StartWatch(master_hosts=master_hosts)

        TaskCh().TaskCheck()

