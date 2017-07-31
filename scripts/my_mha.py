# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys
sys.path.append("..")
from lib.SendRoute import SendRoute
from kazoo.client import KazooClient
from lib.get_conf import GetConf
from zk_handle.zkHandler import zkHander
from contextlib import closing

zk = KazooClient(hosts=GetConf().GetZKHosts())
zk.start()

class White():
    '''手动操作，先执行setwhite再对master进行操作，完成后需执行setmaster'''
    def __init__(self):
        pass
    def SetWhite(self,groupname):
        path = GetConf().GetWhitePath()
        return zkHander().Create(path=path+'/'+groupname,value='',seq=False)
    def SetMaster(self,groupname,host,onlywatch=None):
        if onlywatch:
            value = [groupname,host.replace('.','-'),'white','onlywatch']
        else:
            value = [groupname, host.replace('.', '-'), 'white']
        path = GetConf().GetTaskPath()
        zkHander().DeleteWhite(groupname)
        return zk.create(path=path+'/task',sequence=True,value=str(value))

def Add(groupname):
    '''初始化空白集群的监控'''
    value = [groupname,'add']
    state = zk.create('/mysql/task/task', sequence=True, value=str(value))

def SetHa(groupname,host):
    '''宕机恢复后修改haproxy配置文件，使恢复的节点接收查询'''
    path = GetConf().GetHaproxy()
    _value = zkHander().Get(path+'/'+groupname)
    value = eval(_value)

    meta_path = GetConf().GetMetaHost()
    _port = zkHander().Get(meta_path+'/'+host.replace('.','-'))
    port = eval(_port)['port']
    _add_host = host+':'+str(port)
    _read_list = eval(value['read'])
    if _add_host not in _read_list:
        _read_list.append(host+':'+str(port))
        value['read'] = _read_list
        print value
        zkHander().SetHaproxyMeta(groupname, value['read'], value['write'], type=1)
        SendRoute(groupname)
        return "OK"
    else:
        print 'this host already exists'

def InsertClusterMeta(groupname,hosts):
    '''插入新集群元数据'''
    _hosts = hosts.split(',')
    _host_list = []
    _host_port = {}
    for host in _hosts:
        _host_port[host.split(':')[0]] = host.split(':')[1]
        _host_list.append(host.split(':')[0])

    group_path = GetConf().GetMetaGroup()
    host_path = GetConf().GetMetaHost()
    with closing(zkHander()) as zkhander:
        if zkhander.Exists(path=group_path+'/'+groupname) is None:
            zkhander.Create(path=group_path+'/'+groupname,value=','.join(_host_list).replace('.','-'),seq=False)
        else:
            print '%s is already exists' % groupname

        for host in _host_port:
            value = {'group':groupname,'port':_host_port[host]}
            zkhander.Create(path=host_path+'/'+host.replace('.','-'),value=str(value),seq=False)

def AddMeta(groupname,hosts):
    '''对已经存在的集群增加节点元数据'''
    _hosts = hosts.split(':')
    group_path = GetConf().GetMetaGroup()
    host_path = GetConf().GetMetaHost()
    with closing(zkHander()) as zkhander:
        group_hosts = zkhander.GetMeta(type='group',name=groupname)
        zkhander.Set(path=group_path+'/'+groupname,value=group_hosts+','+_hosts[0].replace('.','-'))
        value = {'group':groupname,'port':_hosts[1]}
        zkhander.Create(path=host_path+'/'+_hosts[0].replace('.','-'),value=str(value),seq=False)

def AddRoute(groupname,route):
    '''添加路由'''
    route_path = GetConf().GetRouter()
    with closing(zkHander()) as zkhander:
        stat = zkhander.Exists(route_path+'/'+groupname)
        if stat is None:
            zkhander.Create(path=route_path+'/'+groupname,value=route,seq=False)
        else:
            print 'this route already exists'

#example SetHa
#SetHa('test','192.168.1.1')

#example AddRoute
#AddRoute('test','192.168.1.1:9011')

#example AddMeta
#AddMeta('test','192.168.1.1:3306')

#example InsertClusterMeta
#hosts = '192.168.1.1:3306,192.168.1.2:3306,192.168.1.3:3306'
#InsertClusterMeta('test',hosts)

#example set white
#White().SetWhite('test')

#example:  set watch for new master after manual switching
#master_host = '192.168.1.1'
#White().SetMaster('test',master_host)
#White().SetMaster('test',master_host,True)  not restart routing

#example: Add empty group
#Add('test')

#all test is groupname


zk.stop()



