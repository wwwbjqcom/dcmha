# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

from kazoo.client import KazooClient
from kazoo.client import KazooState
import threading, time, os, psutil
import traceback
import logging, MySQLdb

logging.basicConfig(filename='ha_client.log',
                    level=logging.INFO,
                    format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')
import socket

encoding = 'utf-8'
BUFSIZE = 1024

zk_hosts = '10.5.9.14:2288,10.5.9.15:2288,10.5.9.16:2288'
ha_path = '/mysql/haproxy'
listen_port = 9011


class _hb:
    retry_state = ''


"""
普通登录用户，检测服务是否可用
"""
mysql_user = 'login_test'
mysql_password = 'xswert123'


class CheckSer:
    """mysqlrouter服务检测，故障重启
       对只读端口进行连接检测    
    """

    def __init__(self):
        pass

    def run(self):
        while True:
            group_list = zkHandle()
            for groupname in group_list:
                port = self.GetPort(groupname)
                state = self.CheckConn(port)
                if state is None:
                    ReStart(groupname)

            time.sleep(10)

    def CheckConn(self, port):
        retry_num = 0
        while True:
            try:
                local_conn = MySQLdb.connect(host='127.0.0.1', user=mysql_user, passwd=mysql_password, port=int(port),
                                             db='', charset="utf8")
                local_conn.cursor()
                local_conn.close()
                state = True
                break
            except MySQLdb.Error, e:
                logging.error(e)
                state = None
            retry_num += 1
            time.sleep(1)
            if retry_num >= 3:
                break
        return state

    def GetPort(self, groupname):
        conf_path = '/etc/haproxy/%s.cfg' % (groupname)
        with open(conf_path) as f:
            read_conf = None
            for line in f:
                if 'read' in line:
                    read_conf = True
                if read_conf and 'bind' in line:
                    ro_port = line.split(':')[-1]
        return ro_port


def getconf(groupname):
    '''获取原始配置'''
    cur_back = None
    __all = {}
    value_list = []
    with open('/etc/haproxy/{}.cfg'.format(groupname)) as f:
        for line in f:
            if 'listen' in line:
                __all[cur_back] = value_list
                cur_back = line.strip().split(' ')[1]
                value_list = []
            elif 'global' in line:
                cur_back = 'global'
                value_list = []
            elif 'defaults' in line:
                __all[cur_back] = value_list
                cur_back = 'defaults'
                value_list = []
            else:
                value_list.append('{}'.format(line.strip()))
    __all[cur_back] = value_list
    # print __all
    return __all


def ReStart(groupname, pidfile):
    '''重启haproxy'''
    restart_stat = None
    pids = psutil.pids()
    for pid in pids:
        p = psutil.Process(pid)
        cmdline = p.cmdline()
        if groupname + '.cfg' in cmdline:
            try:
                os.kill(pid, 9)
                os.popen('cd /etc/haproxy;haproxy -f {}.cfg'.format(groupname))
                return True
            except Exception, e:
                logging.error(traceback.format_exc())
                return False
            restart_stat = True
            break
    if restart_stat is None:
        os.kill(pid, 9)
        os.popen('cd /etc/haproxy;haproxy -f {}.cfg'.format(groupname))


def AlterConf(groupname, new_conf):
    '''修改mysqlrouter配置并重载'''
    _new_conf = new_conf if type(new_conf) == list else eval(new_conf)
    old_config = getconf(groupname)
    for conf in old_config['global']:
        if 'pidfile' in conf:
            pidfile = conf.split(' ')[-1]

    try:
        conf_path = '/etc/haproxy/%s.cfg' % groupname
        with open(conf_path, 'w+') as newfile:
            for k in old_config:
                if k not in _new_conf:
                    newfile.write('{}\n'.format(k))
                    for v in old_config[k]:
                        newfile.write('{:>4}{}\n'.format('', v))

            for k in old_config:
                if k in _new_conf:
                    newfile.write('listen {}\n'.format(k))
                    for v in old_config[k]:
                        if 'server' in v:
                            pass
                        else:
                            newfile.write('{:>4}{}\n'.format('', v))
                    if k == 'write':
                        newfile.write('{:>4}server {} {} check\n'.format('', groupname, _new_conf[k]))
                    else:
                        hp = _new_conf[k] if type(_new_conf[k]) == list else eval(_new_conf[k])
                        for i in range(len(hp)):
                            newfile.write('{:>4}server {}{} {} check\n'.format('', groupname, i + 1, hp[i]))

        if ReStart(groupname, pidfile):
            return True
        else:
            logging.error('mysqlrouter restart failed')
            raise "mysqlrouter restart failed"
    except:
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        logging.error('%s: %s Change config ERROR  New conf: %s' % (now_time, groupname, new_conf))
        return False


def zkHandle(groupname=None):
    __zk = KazooClient(hosts=zk_hosts)
    __zk.start()
    if groupname:
        path = ha_path + '/' + groupname
        if __zk.exists(path=path):
            conf, stat = __zk.get(path=path)
            result = conf
        else:
            result = False
    else:
        if __zk.exists(path=ha_path):
            group_list = __zk.get_children(path=ha_path)
            result = group_list
    return result


class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client

    def run(self):
        while True:
            data = self.client.recv(BUFSIZE)
            try:
                self.client.send('True')
            except:
                pass
            now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            str = bytes.decode(data, encoding)
            conf = zkHandle(str) if len(str) > 0 else None
            if conf:
                logging.info('%s : %s Start Changed' % (now_time, str))
                '''修改配置'''
                if AlterConf(groupname=str, new_conf=conf):
                    logging.info('%s : %s Changed  State: OK' % (now_time, str))
                    break
                else:
                    logging.info('%s : %s Changed  State: Failed' % (now_time, str))
                    break
        logging.info("close:", self.client.getpeername())


class Listener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(0)

    def run(self):
        logging.info("listener started")
        while True:
            client, cltadd = self.sock.accept()
            Reader(client).start()
            logging.info("accept a connect")


class heartbeat(threading.Thread):
    def __init__(self):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        threading.Thread.__init__(self)

    def get_netcard(self):
        '''获取IP地址'''
        info = psutil.net_if_addrs()
        for k, v in info.items():
            for item in v:
                if item[0] == 2 and not item[1] == '127.0.0.1' and ':' not in k and '10.' not in item[1]:
                    netcard_info = item[1]
        return netcard_info.replace('.', '-')

    def retry_create(self):
        '''创建临时node'''
        node_stat = self.zk.exists(path='/mysql/online-list/' + self.get_netcard())
        if node_stat is None:
            self.zk.create(path="/mysql/online-list/" + self.get_netcard(), value="", ephemeral=True)
        else:
            self.zk.delete(path="/mysql/online-list/" + self.get_netcard())
            self.zk.create(path="/mysql/online-list/" + self.get_netcard(), value="", ephemeral=True)

    def run(self):
        self.add_linsten()
        self.retry_create()
        while True:
            if _hb.retry_state == 'Connected':
                self.retry_create()
            time.sleep(1)

    def add_linsten(self):
        @self.zk.add_listener
        def my_listener(state):
            if state == KazooState.LOST:
                logging.error("LOST")
            elif state == KazooState.SUSPENDED:
                logging.info("SUSPENDED")
            else:
                logging.info("Connected")
                _hb.retry_state = "Connected"


if __name__ == "__main__":
    hb = heartbeat()
    hb.start()
    p = threading.Thread(target=CheckSer, args=())
    p.start()
    lst = Listener(listen_port)
    lst.start()