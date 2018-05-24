# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
from zkhandle import ZkHandle
from kazoo.client import KazooState
from CheckDB import RollBbinlog
from CheckDB import DBHandle
import time,sys
sys.path.append("..")
from config.get_config import GetConf


class CreateHear(ZkHandle):
    def __init__(self):
        super(CreateHear,self).__init__()

    def listener(self):
        '''创建监听'''
        RollBbinlog()               #首次启动进行检查
        retry_create_stat = None
        while True:
            state = self.zk.state
            if state.upper() != self.retry_state.upper():
                if state == KazooState.LOST:
                    self.retry_state = ""
                elif state == KazooState.SUSPENDED:
                    self.retry_state = ""
                else:
                    self.retry_state = "Connected"

            if self.retry_state == "Connected" and retry_create_stat is None:
                self.retry_create('client')
                self.retry_create('server')
                retry_create_stat = True
            elif self.retry_state == "Connected" and retry_create_stat:
                pass
            else:
                retry_create_stat = None

            self.__checkdb()
            time.sleep(1)

    def __checkdb(self):
        '''mysql服务检查'''
        self.retry_num = self.retry_num + 1 if DBHandle().check() is None else 0
        if self.retry_num == GetConf().GetServerRetryNum():
            delete_sate = self.delete('server')
            while True:
                if delete_sate:
                    break
                else:
                    delete_sate = self.delete('server')

            self.downed_state = True

        if self.downed_state and self.retry_num == 0:
            RollBbinlog()
            self.downed_state = None
            self.retry_create('server')