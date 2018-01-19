# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

from contextlib import closing
from dbHandle import dbHandle
from zk_handle.zkHandler import zkHander
from lib.get_conf import GetConf
from lib.log import Logging
import time,traceback
import pymysql


class Addition:
    def __init__(self,host=None):
        if host:
            self.host = host.replace('.','-')

    def __get_groupname(self):
        with closing(zkHander()) as zkhander:
            host_meta = eval(zkhander.GetMeta(name=self.host,type='host'))
        groupname = host_meta['group']
        return groupname

    def __check_repl(self,groupname):
        repl_path = GetConf().GetAdditionRPL()+'/'+groupname
        region_value = {}
        with closing(zkHander()) as zkhander:
            state = zkhander.Exists(repl_path)
            if state:
                region_list = zkhander.GetChildren(repl_path)
                for region in region_list:
                    region_value[region] = zkhander.Get(repl_path+'/'+region)
                return region_value
            else:
                return None

    def __get__region_con(self,region_value):
        addition_master = {}                                            #记录需要连接的节点信息
        with closing(zkHander()) as zkhander:
            for region,host in region_value.items():
                if host != 'None':
                    if zkhander.GetOnlineState(eval(host)['host']):       #replication下region存的当前连接节点格式为{'host':'192.168.212.1','port':3306,'ssl':1/0.....}
                        addition_master[region] = eval(host)
                    else:
                        addition_master[region] = self.__get_online_host(region)
                else:
                    addition_master[region] = self.__get_online_host(region)

            return addition_master


    def __get_online_host(self,region):
        """获取在线列表"""
        reg_path = GetConf().GetAdditionRegion() + '/' + region
        with closing(zkHander()) as zkhander:
            reg_value_dict = eval(zkhander.Get(reg_path))               #region存储格式为{'192-168-212-1':{'port':333,'ssl':0/1}.....}
            if reg_value_dict:
                _reg_online = [host for host in reg_value_dict if zkhander.GetOnlineState(host)]
                if _reg_online:
                    _to_reg = {'host': _reg_online[0].replace('-', '.'), 'port': reg_value_dict[_reg_online[0]]['port'],
                               'ssl': reg_value_dict[_reg_online[0]]['ssl']}
                    return _to_reg
                else:
                    Logging(msg='This group has replication task ,But all region not online',level='warning')
                    return None
            else:
                Logging(msg='This group has replication task ,But not region value',level='warning')
                return None

    def GetRepl(self):
        groupname = self.__get_groupname()
        region_value = self.__check_repl(groupname)
        if region_value:
            addition_master = self.__get__region_con(region_value)
            return addition_master
        else:
            return None

    """-----------------------------------------------------------------------------------------------------------------"""

    def ChangeRepl(self,_content):
        try:
            groupname,region,type = _content[0],_content[1],_content[-1]
            if type == 'dow':                                                           #宕机任务，需重新选择节点并监听同步
                for i in range(0, 3):
                    host,port = self.__get_master_for_region(region,groupname)
                    with closing(dbHandle(host, port)) as dbhandle:
                        mysqlstate = dbhandle.RetryConn()                               # 检测是否能正常连接
                    time.sleep(1)
                if mysqlstate:
                    zkHander().CreateWatch(host=host.replace('.','-'),addition=True,region=region,region_for_groupname=groupname)                           # 重新创建master检测
                else:
                    return self.__change_new_master(region=region,groupname=groupname)

            elif type == 'up':                                                          #只进行监听，用于手动添加了同步任务
                self.__up_watch_master(region=region,groupname=groupname)
            return True
        except:
            Logging(msg='addition task failed!',level='error')
            return False

    def __change_new_master(self,region,groupname):
        with closing(zkHander()) as zkhander:
            addition_master = self.__get_online_host(region)
            cur_master = zkhander.GetMasterMeta(groupname)
            host_meta = zkhander.GetMeta(type='host', name=cur_master)
            host_port = eval(host_meta)['port']
            exe_add = ExecuteAdditionTask(host=cur_master.replace('-', '.'), port=int(host_port))
            return exe_add.Change(region, addition_master)

    def __up_watch_master(self,region,groupname):
        host,_ = self.__get_master_for_region(region,groupname)
        zkHander().CreateWatch(host,addition=True,region=region)

    def __get_master_for_region(self,region,groupname):
        with closing(zkHander()) as zkhander:
            repl_path = GetConf().GetAdditionRPL() + '/' + groupname + '/' + region
            master_content = eval(zkhander.Get(repl_path))
            host,port = master_content['host'],int(master_content['port'])
            return host,port

class ExecuteAdditionTask:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.mysqluser, self.mysqlpasswd = GetConf().GetMysqlAcount()
        self.ssl_set = {'ca': GetConf().GetUserSSLCa(), 'cert': GetConf().GetUserSSLCert(),
                        'key': GetConf().GetUserSSLKey()}

        self.conn = pymysql.connect(host=self.host, user=self.mysqluser, passwd=self.mysqlpasswd, port=self.port, db='',
                                    charset="utf8", ssl=self.ssl_set)

    def Change(self, region, host_content):
        repluser, replpassword, ssl_ca, ssl_cert, ssl_key = GetConf().GetReplAcount(rg=True)
        master_host, master_port = host_content['host'], int(host_content['port'])
        if host_content['ssl']:
            sql = 'change master to master_host="%s",master_port=%d,master_user="%s",master_password="%s",master_ssl=1,' \
                  'master_ssl_ca="%s",' \
                  'master_ssl_cert="%s",master_ssl_key="%s",master_auto_position=1 for channel "%s" ' % (
                  master_host, master_port,
                  repluser, replpassword, ssl_ca,
                  ssl_cert, ssl_key, region)
        else:
            sql = 'change master to master_host="%s",master_port=%d,master_user="%s",master_password="%s",' \
                  'master_auto_position=1 for channel "%s" ' % (master_host, master_port,
                                                                repluser, replpassword, region)
        with closing(self.conn.cursor()) as cur:
            try:
                cur.execute('stop slave for channel "%s"' % region)
                cur.execute('reset slave for channel "%s"' % region)
            except:
                pass
            try:
                cur.execute(sql)
                self.__set_group_region(region,host_content)
            except pymysql.Warning,e:
                Logging(msg=traceback.format_exc(),level='error')
                cur.execute('start slave;')
                self.__set_group_region(region, host_content)
            except pymysql.Error,e:
                Logging(msg=traceback.format_exc(),level='error')
                Logging(msg='addition task for {} failed,master to {} in region {} ! ! !'.format(self.host, host_content['host'], region),level='error')
                return False
            return True
    def __set_group_region(self, region, host_content):
        """设置该groupname连接对应region的master信息"""
        with closing(zkHander()) as zkhander:
            host_meta = eval(zkhander.GetMeta(name=self.host.replace('.','-'),type='host'))
            groupname = host_meta['group']
            path = GetConf().GetAdditionRPL() + '/' + groupname + '/' + region
            zkhander.Set(path=path,value=str(host_content))

        zkHander().CreateWatch(host=host_content['host'].replace('.','-'),addition=True,region=region,region_for_groupname=groupname)
        with closing(zkHander()) as zkhander:
            zkhander.DeleteWatchDown(groupname=groupname+'_'+region)



