# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import ConfigParser
import os
path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

class GetConf(object):
    '''获取配置项'''
    def __init__(self):
        conf_path = path.replace('\\','/') +'/config/zk.conf'
        self.keys_path = path.replace('\\','/')+'/config/keys/'
        self.section = 'nodepath'
        self.mysqlsection = 'mysqldb'
        self.addition = 'addition'
        self.client = 'client'
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(conf_path)
        self.root_dir = self.conf.get(self.section,'root_path')

    def GetPath(self,name):
        _Path = self.conf.get(self.section, name)
        return self.root_dir.replace('\'','') + '/' + _Path.replace('\'','')

    def GetWhitePath(self):
        return self.GetPath('white_path')

    def GetLockPath(self):
        return self.GetPath('lock_path')

    def GetTaskPath(self):
        return self.GetPath('task_path')

    def GetMetaHost(self):
        return self.GetPath('meta_host')

    def GetMetaGroup(self):
        return self.GetPath('meta_group')

    def GetOnlinePath(self):
        return self.GetPath('online_path')

    def GetMasterPath(self):
        return self.GetPath('master_path')

    def GetZKHosts(self):
        hosts = self.conf.get('zookeeper','hosts')
        return hosts.replace('\'','')

    def GetHaproxy(self):
        return self.GetPath('haproxy_path')

    def GetWatchDown(self):
        return self.GetPath('watch_down')

    def GetRouter(self):
        return self.GetPath('meta_router')

    def GetOnlineClientPath(self):
        return self.GetPath('online_client_path')

    def GetSlaveDown(self):
        return self.GetPath('slave_down')

    def GetMysqlAcount(self):
        user = self.conf.get(self.mysqlsection, 'mysqluser').replace('\'','')
        passwd = self.conf.get(self.mysqlsection, 'mysqlpasswd').replace('\'','')
        return user,passwd

    def GetReplAcount(self,rg=None):
        repluser = self.conf.get(self.addition, 'repluser').replace('\'','') if rg else  self.conf.get(self.mysqlsection, 'repluser').replace('\'','')
        replpasswd = self.conf.get(self.addition, 'replpasswd').replace('\'','') if rg else self.conf.get(self.mysqlsection, 'replpasswd').replace('\'','')
        ssl_ca = self.conf.get(self.mysqlsection, 'ssl_ca').replace('\'','')
        ssl_cert = self.conf.get(self.mysqlsection, 'ssl_cert').replace('\'','')
        ssl_key = self.conf.get(self.mysqlsection, 'ssl_key').replace('\'','')
        return repluser,replpasswd,ssl_ca,ssl_cert,ssl_key

    def GetSSLPath(self,name,slave=None):
        if slave:
            file_name = self.conf.get(self.mysqlsection,name).replace('\'','')
            return file_name
        else:
            file_name = self.conf.get(self.mysqlsection, name).replace('\'','')
            return self.keys_path+file_name
    def GetClientPort(self):
        return int(self.conf.get(self.client,'port'))

    def GetUserSSLCa(self):
        return self.GetSSLPath('ca')
    def GetUserSSLCert(self):
        return self.GetSSLPath('cert')
    def GetUserSSLKey(self):
        return self.GetSSLPath('key')
    def GetSlaveSSLCa(self):
        return self.GetSSLPath('ssl_ca',slave=True)
    def GetSlaveSSLCert(self):
        return self.GetSSLPath('ssl_cert',slave=True)
    def GetSlaveSSLKey(self):
        return self.GetSSLPath('ssl_key',slave=True)

    '''获取附加任务类型'''
    def GetAdditionRPL(self):
        repl = self.conf.get('addition','replication').replace('\'','')
        return self.root_dir.replace('\'','') + '/addition/' + repl
    def GetAdditionRegion(self):
        reg = self.conf.get('addition','region').replace('\'','')
        return self.root_dir.replace('\'','') + '/addition/' + reg


#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
