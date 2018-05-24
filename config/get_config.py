# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import ConfigParser
import os
path = os.path.abspath(os.path.join(os.path.dirname(__file__)))

class GetConf(object):
    '''获取配置项'''
    def __init__(self):
        conf_path = path.replace('\\','/') +'/client.conf'
        self.section = 'nodepath'
        self.mysqlsection = 'mysqldb'
        self.zookeeper = 'zookeeper'
        self.general = 'global'
        self.socket = 'socket'
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(conf_path)
        self.root_dir = self.conf.get(self.section,'root_path').replace('\'','')

    def Replace(self,value):
        return value.replace('\'','')

    def GetPath(self,name):
        _Path = self.conf.get(self.section, name)
        return self.root_dir.replace('\'','') + '/' + _Path.replace('\'','')

    def GetOnlinePath(self):
        return self.GetPath('online_path')

    def GetOnlineClientPath(self):
        return self.GetPath('online_client_path')

    def GetSocketDir(self):
        return self.Replace(self.conf.get(self.mysqlsection,'sockedir'))

    def GetMysqlUser(self):
        return self.Replace(self.conf.get(self.mysqlsection,'mysqluser'))

    def GetMysqlPassword(self):
        return self.Replace(self.conf.get(self.mysqlsection,'mysqlpasswd'))

    def GetMysqlPort(self):
        return int(self.conf.get(self.mysqlsection,'mysqlport'))

    def GetReplUser(self):
        return self.Replace(self.conf.get(self.mysqlsection, 'repluser'))

    def GetPeplPassowd(self):
        return self.Replace(self.conf.get(self.mysqlsection, 'replpasswd'))

    def GetReplPort(self):
        return int(self.conf.get(self.mysqlsection,'replport'))

    def GetZKHosts(self):
        return self.Replace(self.conf.get(self.zookeeper,'hosts'))

    def GetServerRetryNum(self):
        return int(self.conf.get(self.general,'mysql_check_retry'))

    def GetSockPort(self):
        return int(self.conf.get(self.socket,'port'))

    def GetBinlogDir(self):
        return self.Replace(self.conf.get(self.mysqlsection,'binlogdir'))

#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
