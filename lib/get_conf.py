# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
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

    def GetMysqlAcount(self):
        user = self.conf.get(self.mysqlsection, 'mysqluser').replace('\'','')
        passwd = self.conf.get(self.mysqlsection, 'mysqlpasswd').replace('\'','')
        return user,passwd

    def GetReplAcount(self):
        repluser = self.conf.get(self.mysqlsection, 'repluser').replace('\'','')
        replpasswd = self.conf.get(self.mysqlsection, 'replpasswd').replace('\'','')
        return repluser,replpasswd

    def GetSSLPath(self,name,slave=None):
        if slave:
            file_name = self.conf.get(self.mysqlsection,name).replace('\'','')
            return file_name
        else:
            file_name = self.conf.get(self.mysqlsection, name).replace('\'','')
            return self.keys_path+file_name

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

#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
