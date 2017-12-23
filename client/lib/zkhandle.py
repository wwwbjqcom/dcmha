# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,psutil,time
sys.path.append("..")
from Loging import Logging
from config.get_config import GetConf
from kazoo.client import KazooClient
from kazoo.client import KazooState

class ZkHandle:
    def __init__(self):
        zk_host = GetConf().GetZKHosts()
        self.zk = KazooClient(hosts=zk_host)
        self.retry_state = None

    def listener(self):
        '''创建监听'''
        @self.zk.add_listener
        def my_listener(state):
            if state == KazooState.LOST:
                Logging(msg="LOST", level='error')
            elif state == KazooState.SUSPENDED:
                Logging(msg="SUSPENDED", level='info')
            else:
                Logging(msg="Connected", level='info')
                self.retry_tate = "Connected"
                return self.retry_state

    def retry_create(self,type=None):
        '''创建临时node'''
        if type == 'client':
            online_node = GetConf().GetOnlineClientPath()
        elif type == 'server':
            online_node = GetConf().GetOnlinePath()
        else:
            Logging(msg='not suport this type {},create node if failed '.format(type),level='error')
        node_stat = self.zk.exists(path='{}/{}'.format(online_node,self.__get_netcard()))
        if node_stat is None:
            self.zk.create(path='{}/{}'.format(online_node,self.__get_netcard()), value="", ephemeral=True)
        else:
            self.zk.delete(path='{}/{}'.format(online_node,self.__get_netcard()))
            self.zk.create(path='{}/{}'.format(online_node,self.__get_netcard()), value="", ephemeral=True)

    def delete(self,type=None):
        if type == 'client':
            online_node = GetConf().GetOnlineClientPath()
        elif type == 'server':
            online_node = GetConf().GetOnlinePath()
        else:
            Logging(msg='not suport this type {},create node if failed '.format(type), level='error')
        self.zk.delete(path='{}/{}'.format(online_node,self.__get_netcard()))
        Logging(msg='server {} is down, now deleted this server node on zk'.format(self.__get_netcard()),level='info')


    def __get_netcard(self):
        '''获取IP地址'''
        info = psutil.net_if_addrs()
        for k, v in info.items():
            for item in v:
                if item[0] == 2 and not item[1] == '127.0.0.1' and ':' not in k and '10.' in item[1]:
                    netcard_info = item[1]
        return netcard_info.replace('.', '-')

    def GetReplStatus(self):
        '''获取宕机切换时slave执行到的binlog位置'''
        binlog_status_node = '{}/{}'.format(GetConf().GetPath('readbinlog-status'),self.__get_netcard())
        gtid_status_node = '{}/{}'.format(GetConf().GetPath('execute-gtid'),self.__get_netcard())
        if self.zk.exists(binlog_status_node):
            binlog_value,stat = self.zk.get(binlog_status_node)
            gtid_value,stat = self.zk.get(gtid_status_node)
        else:
            return None,None
        return eval(binlog_value),gtid_value


    def Closing(self):
        self.zk.close()

    def __enter__(self):
        self.listener()
        while True:
            if self.retry_tate == "Connected":
                self.retry_create('client')
                self.retry_create('server')
                self.retry_tate = ""
            time.sleep(1)
