# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

from kazoo.client import KazooClient
import sys,traceback,time,random
sys.path.append("..")
from lib.get_conf import GetConf
import logging
logging.basicConfig(filename='zk_client.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')


class zkHander(object):
    '''zookeeper系列操作'''
    def __init__(self):
        zk_hosts = GetConf().GetZKHosts()
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()

    def Exists(self,path):
        return self.zk.exists(path=path)
    def Create(self,**kwargs):
        return self.zk.create(path=kwargs["path"],value=kwargs["value"],sequence=kwargs['seq'],
                              makepath=(kwargs['mp'] if 'mp' in kwargs else False))




    def close(self):
        self.zk.stop()