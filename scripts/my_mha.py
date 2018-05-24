#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import sys
sys.path.append("..")
from lib.SendRoute import SendRoute
from kazoo.client import KazooClient
from lib.get_conf import GetConf
from zk_handle.zkHandler import zkHander
from contextlib import closing
import getopt

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

class Addition:
    """设置附加同步任务"""
    def __init__(self,host=None,port=None,ssl=None,host_content=None):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.host_content = host_content

    def Rel_Meta(self,groupname,region):
        """同步任务需要同步的节点信息"""
        host_content = {'host':self.host,'port':self.port,'ssl':self.ssl} if self.host and self.port and self.ssl else None
        with closing(zkHander()) as zkhander:
            path = GetConf().GetAdditionRPL() + '/' + groupname + '/'+ region
            zkhander.Create(path=path,value=str(host_content),seq=False,mp=True)

    def Region_meta(self,region):
        """能提供同步的元数据"""
        _host_content = {}
        for host in self.host_content:
            _host_content[host.replace('.','-')] = self.host_content[host]
        with closing(zkHander()) as zkhander:
            path = GetConf().GetAdditionRegion() + '/' + region
            zkhander.Create(path=path,value=str(_host_content),seq=False,mp=True)

def Usage():
    __usage__ = """
        Usage:
        Options:
            -h [--help] : print help message
            -g [--groupname] : zookeeper中集群的名称
            -H [--host] : 节点IP地址
            -P [--port] : 端口
            --HostList : 批量的host+port,格式如'192.168.127.1:3306,192.168.127.2:3306....'
            --ssl : 同步是否开始ssl验证，用于附加任务的添加任务
            开关类：
                --SetHa : 宕机恢复节点加入到集群中接收查询操作，需指定集群名称及宕机节点IP
                --AddRoute : 添加路由节点信息，需指定该路由所属集群名称及路由所在IP和端口,多个使用--HostList
                --AddMeta : 对已存在的集群添加节点，需指定集群名称及加入的节点IP和端口
                --InsertClusterMeta : 创建集群节点原始数据，需指定集群名称及--HostList
                --White : 白名单开关，用于手动维护
                    --SetWhite : 指定手动维护的机群组，需指定集群名称
                    --SetMaster: 手动维护完成使高可用服务对新master进行监听，需指定集群名称及master的host
                    --ReRoute : 是否同步路由，如果指定该参数会使路由重启
                --Add : 添加一个空集群，需指定集群名称且该集群的元数据需先插入zk，空白集群会自动选举master并监听
                --Addition : 附加同步任务，用于异地多端同步
                    --replmeta : 对集群组添加附加同步任务，需指定groupname、regionname 可选有port、host、ssl，如不指定只有在
                                 主从切换或初始化时会启动附加同步任务
                    --regionname : 同步的分区名称
                    --regionmeta : 添加元数据，后必须指定regionname和元数据列表
                        --regionhost : 元数据列表，格式为 '{"192.168.212.1":{"port":3306,"ssl":0}.....}',ssl可设置0/1,
                                       0代表不使用ssl，1代表使用ssl
            """
    print __usage__

def main(argv):
    _argv = {}
    try:
        opts, args = getopt.getopt(argv[1:], 'hg:H:P:', ['help', 'groupname=', 'host=', 'port=', 'HostList=',
                                    'SetHa', 'AddRoute','AddMeta', 'InsertClusterMeta', 'White', 'SetWhite', 'SetMaster',
                                    'ReRoute', 'Add','Addition','replmeta','regionname=','regionmeta','regionhost=','ssl'])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)
    for o, a in opts:
        if o in ('-h', '--help'):
            Usage()
            sys.exit(1)
        elif o in ('--Add'):
            _argv['add'] = True
        elif o in ('--Addition'):
            _argv['addition'] = True
        elif o in ('--ssl'):
            _argv['ssl'] = True
        elif o in ('--replmeta'):
            _argv['replmeta'] = True
        elif o in ('--regionname'):
            _argv['regionname'] = a
        elif o in ('--regionmeta'):
            _argv['regionmeta'] = True
        elif o in ('--regionhost'):
            _argv['regionhost'] = eval(a)
        elif o in ('-g','--groupname'):
            _argv['groupname'] = a
        elif o in ('-H','--host'):
            _argv['host'] = a
        elif o in ('-P','--port'):
            _argv['port'] = int(a)
        elif o in ('--HostList'):
            _argv['host_list'] = a
        elif o in ('--SetHa'):
            _argv['set_ha'] = True
        elif o in ('--AddRoute'):
            _argv['add_route'] = True
        elif o in ('--AddMeta'):
            _argv['add_meta'] = True
        elif o in ('--InsertClusterMeta'):
            _argv['inser_cluster'] = True
        elif o in ('--White'):
            _argv['white'] = True
        elif o in ('--SetWhite'):
            _argv['set_white'] = True
        elif o in ('--SetMaster'):
            _argv['set_master'] = True
        elif o in ('--ReRoute'):
            _argv['restart_route'] = True
        else:
            print 'unhandled option'
            sys.exit(3)


    _check_list = ['set_ha','add_route','add_meta','inser_cluster','white','add','regionmeta','replmeta']
    _ck_resulte = [a for a in _argv if a in _check_list]
    if len(_ck_resulte) > 1:
        print '开关类参数只能有一个，请重新设置！！！'
        sys.exit()

    if 'set_ha' in _argv:
        SetHa(_argv['groupname'],_argv['host'])
    elif 'add_route' in _argv:
        if 'host_list' in _argv:
            AddRoute(_argv['groupname'], _argv['host_list'])
        else:
            AddRoute(_argv['groupname'],_argv['host']+':'+str(_argv['port']))
    elif 'add_meta' in _argv:
        AddMeta(_argv['groupname'],_argv['host']+':'+str(_argv['port']))
    elif 'inser_cluster' in _argv:
        InsertClusterMeta(_argv['groupname'],_argv['host_list'])
    elif 'white' in _argv:
        if 'set_white' in _argv:
            White().SetWhite(_argv['groupname'])
        elif 'set_master' in _argv:
            if 'restart_route' in _argv:
                White().SetMaster(_argv['groupname'],_argv['host'],False)
            else:
                White().SetMaster(_argv['groupname'], _argv['host'], True)
        else:
            Usage()
            sys.exit()
    elif 'add' in _argv:
        Add(_argv['groupname'])

    elif 'addition' in _argv:
        if 'replmeta' in _argv:
            if 'host' in _argv:
                Addition(host=_argv['host'],port=_argv['port'],ssl=_argv['ssl']).Rel_Meta(groupname=_argv['groupname'],region=_argv['regionname'])
            else:
                Addition().Rel_Meta(groupname=_argv['groupname'],region=_argv['regionname'])
        elif 'regionmeta' in _argv:
            Addition(host_content=_argv['regionhost']).Region_meta(region=_argv['regionname'])
        else:
            Usage()
            sys.exit()



if __name__ == "__main__":
    main(sys.argv)






