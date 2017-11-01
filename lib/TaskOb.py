# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,time
sys.path.append("..")
from zk_handle.zkHandler import zkHander
from db_handle.dbHandle import dbHandle
from SendRoute import SendRoute
from contextlib import closing
from lib.get_conf import GetConf
import logging
logging.basicConfig(filename='zk_client.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')



class TaskClassify:
    def __init__(self):
        pass

    def TaskChange(self,groupname,**kwargs):
        '''集群监控'''
        with closing(zkHander()) as zkhander:
            host_list = zkhander.GetMeta(type='group',name=groupname)
        if host_list:
            _host_list = host_list.split(',')
            if 'type' in kwargs and kwargs['type'] == 'change':                                      #宕机切换排除当前master 节点
                with closing(zkHander()) as zkhander:
                    cur_master = zkhander.GetMasterMeta(groupname)
                _host_list.remove(cur_master)
            _host_list.sort()

            for host in _host_list:                                             #检查是否在线
                with closing(zkHander()) as zkhander:
                    _host_list.remove(host) if zkhander.OnlineExists(host) is False else None


            if 'type' in kwargs and kwargs['type'] == 'change':                     #宕机选择读取master日志最前的slave作为新master
                _read_pos,_read_file = None,None
                for host in _host_list:
                    with closing(zkHander()) as zkhander:
                        host_meta = zkhander.GetMeta(type='host',name=host)
                        host_port = eval(host_meta)['port']
                        with closing(dbHandle(host.replace('-','.'),host_port)) as dbhandle:
                            read_file,read_pos = dbhandle.CheckPos()

                    if read_pos is None and read_file is None:
                        master_host = _host_list[-1]
                    elif read_pos != None and read_file != None and read_file > _read_file:
                        master_host = host
                        _read_pos,_read_file = read_pos,read_file
                    elif read_file == _read_file:
                        if read_pos > _read_pos:
                            master_host = host
                            _read_pos,_read_file = read_pos,read_file
            else:
                master_host = _host_list[-1]                                         #最大位作为master，其余为slave
            _host_list.remove(master_host)

            with closing(zkHander()) as zkhander:
                master_meta = zkhander.GetMeta(type='host', name=master_host)
                master_port = eval(master_meta)['port']

            if _host_list:
                for i in range(len(_host_list)):
                    with closing(zkHander()) as zkhander:
                        host_meta = zkhander.GetMeta(type='host', name=_host_list[i])
                        host_port = eval(host_meta)['port']
                        with closing(dbHandle(_host_list[i].replace('-','.'),host_port)) as dbhandle:
                            stat = dbhandle.ChangeMaster(master_host.replace('-','.'),master_port)
            else:
                stat = True
            if stat:
                with closing(zkHander()) as zkhander:
                    zkhander.SetMasterHost(groupname, master_host)
                    _host_list.append(master_host)
                    zkhander.SetHaproxyMeta(groupname,_host_list,master_host)
                    SendRoute(groupname)

                zkHander().CreateWatch(master_host)
                with closing(dbHandle(master_host.replace('-','.'),master_port)) as resetmaster:
                    resetmaster.ResetMaster(groupname)
                return True
            else:
                return False
        else:
            return False


    def TaskWhite(self,_task_value):
        '''手动操作集群'''
        group_name = _task_value[0]
        master_host = _task_value[1]
        zkHander().CreateWatch(master_host)                                     # 对master创建watch
        with closing(zkHander()) as zkhander:
            zkhander.SetMasterHost(group_name, master_host)                       # 修改集群master指向
            zkhander.DeleteWatchDown(group_name)                                  # 删除该集群未处理的宕机信息
            zkhander.SetHaproxyMeta(group_name,None, master_host)      # 修改haproxy配置信息
            if 'onlywatch' not in _task_value:
                SendRoute(group_name)
        return True


    def TaskDown(self,groupname):
        '''master宕机触发任务'''
        with closing(zkHander()) as zkhander:
            cur_master = zkhander.GetMasterMeta(groupname)

        for i in range(0,3):
            with closing(zkHander()) as zkhander:
                host_meta = zkhander.GetMeta(type='host', name=cur_master)
                host_port = eval(host_meta)['port']
                with closing(dbHandle(cur_master.replace('-','.'),host_port)) as dbhandle:
                    mysqlstate = dbhandle.RetryConn()                               #检测mysql是否能正常连接
            time.sleep(1)
        if mysqlstate:
            zkHander().CreateWatch(cur_master)                                  #重新创建master检测
            return True
        else:                                                                  #宕机重选master
            now_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            logging.error(' %s : group %s  the current master %s  state: down' % (now_time,groupname,cur_master))
            return self.TaskChange(groupname,type='change')



class TaskCh:
    def __init__(self):
        pass

    def TaskCheck(self):
        '''获取task任务列表，执行并删除任务,并创建task任务实时监听'''
        task_path = GetConf().GetTaskPath()
        with closing(zkHander()) as zkhander:
            task_list = zkhander.GetTaskList()
        if task_list:
            for task in task_list:
                self.TaskFunc(task)
            zkHander().CreateChildrenWatch(task_path, self.TaskFunc)
        else:
            zkHander().CreateChildrenWatch(task_path,self.TaskFunc)


    def TaskFunc(self,taskname):
        with closing(zkHander()) as zkhander:                                               #检查其他server是否在执行
            task_stat = zkhander.SetLockTask(taskname)
            if task_stat:
                state = TaskOb(taskname)
                if state:
                    zkhander.DeleteTask(taskname)                                             #删除已执行的任务
                else:
                    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    logging.error(' %s : this task %s failed' % (now_time,taskname))
                time.sleep(2)
                zkhander.DeleteLockTask(taskname)
            else:
                zkhander.CreateLockWatch(taskname)
                logging.info('task : %s  elsewhere in the execution' % taskname)


def TaskOb(task_name):
    '''任务操作'''
    with closing(zkHander()) as zkhander:
        _task_value = eval(zkhander.GetTaskContent(task_name))
    if _task_value is not None:
        if 'add' in _task_value:
            return TaskClassify().TaskChange(_task_value[0])

        elif 'white' in _task_value:
            '''白名单操作'''
            return TaskClassify().TaskWhite(_task_value)

        elif 'down' in _task_value:
            '''宕机需判断是否在白名单列表'''
            with closing(zkHander()) as zkhander:
                if zkhander.GetWhite(_task_value[0]):
                    logging.info('this master %s has been down,but it in whitelist!!' % _task_value[0])
                    return True
                else:
                    return TaskClassify().TaskDown(_task_value[0])
        elif 'append' in _task_value:
            '''附加任务'''
            from db_handle import AdditionTask
            return AdditionTask.Addition().ChangeRepl(_task_value)

        else:
            logging.error('task failed  state: type error')
            return False

