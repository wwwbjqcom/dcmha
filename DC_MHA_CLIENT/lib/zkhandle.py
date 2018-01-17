# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,psutil
sys.path.append("..")
from Loging import Logging
from config.get_config import GetConf
from kazoo.client import KazooClient


retry_state = None

class ZkHandle(object):
    def __init__(self):
        zk_host = GetConf().GetZKHosts()
        self.zk = KazooClient(hosts=zk_host)
        self.zk.start()
        self.retry_state = ""
        self.__retry_num = 0
        self.downed_state = None


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
        Logging(msg='server {} is down, now deleted this server node on zk'.format(self.__get_netcard()), level='info')
        stat = self.zk.exists(path='{}/{}'.format(online_node, self.__get_netcard()))
        if stat:
            self.zk.delete(path='{}/{}'.format(online_node,self.__get_netcard()))

        delete_stat = self.zk.exists(path='{}/{}'.format(online_node,self.__get_netcard()))
        if delete_stat is None:
            Logging(msg='delete successful',level='info')
            return True
        else:
            Logging(msg='delete failed', level='info')
            return False

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
        binlog_status_node = '{}/{}/{}'.format(GetConf().root_dir,'readbinlog-status',self.__get_netcard())
        gtid_status_node = '{}/{}/{}'.format(GetConf().root_dir,'execute-gtid',self.__get_netcard())
        #self.zk.state
        if self.zk.exists(binlog_status_node):
            binlog_value,stat = self.zk.get(binlog_status_node)
            gtid_value,stat = self.zk.get(gtid_status_node)
            return eval(binlog_value), eval(gtid_value)
        elif self.zk.exists(gtid_status_node):
            gtid_value, stat = self.zk.get(gtid_status_node)
            return None,eval(gtid_value)
        else:
            return None,None


    def GetMasterHost(self,groupname=None):
        '''返回当前master IP，本身所在系统的IP'''
        value,_ = self.zk.get(path='/mysql/master/{}'.format(groupname))
        return value,self.__get_netcard()

    def DeleteDownStatus(self):
        '''回滚完成后删除binlog及gtid信息'''
        binlog_status_node = '{}/{}/{}'.format(GetConf().root_dir, 'readbinlog-status', self.__get_netcard())
        gtid_status_node = '{}/{}/{}'.format(GetConf().root_dir, 'execute-gtid', self.__get_netcard())
        binlog_stat = self.zk.exists(path=binlog_status_node)
        if binlog_stat:
            self.zk.delete(path=binlog_status_node)

        gtid_stat = self.zk.exists(path=gtid_status_node)
        if gtid_stat:
            self.zk.delete(path=gtid_status_node)

    def close(self):
        self.zk.stop()
