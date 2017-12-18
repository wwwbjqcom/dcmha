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
        conf_path = path.replace('\\','/') +'/client.conf'
        self.keys_path = path.replace('\\','/')+'/config/keys/'
        self.section = 'nodepath'
        self.mysqlsection = 'mysqldb'
        self.zookeeper = 'zookeeper'
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(conf_path)
        self.root_dir = self.conf.get(self.section,'root_path')

    def GetPath(self,name):
        _Path = self.conf.get(self.section, name)
        return self.root_dir.replace('\'','') + '/' + _Path.replace('\'','')

    def GetOnlinePath(self):
        return self.GetPath('online_path')

    def GetOnlineClientPath(self):
        return self.GetPath('online_client_path')

    def GetMysqlUser(self):
        return self.conf.get(self.mysqlsection,'mysqluser')

    def GetMysqlPassword(self):
        return self.conf.get(self.mysqlsection,'mysqlpasswd')

    def GetReplUser(self):
        return self.conf.get(self.mysqlsection, 'repluser')

    def GetPeplPassowd(self):
        return self.conf.get(self.mysqlsection, 'mysqlpasswd')

    def GetZKHosts(self):
        return self.conf.get(self.zookeeper,'hosts')


#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
