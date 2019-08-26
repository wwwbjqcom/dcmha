# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

from kazoo.client import KazooClient
from kazoo.client import KazooState
import threading, time, os, psutil
import traceback
import logging, MySQLdb
import ConfigParser
import MySQLdb.cursors
logging.basicConfig(filename='ha_client.log',
                    level=logging.INFO,
                    format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')
import socket

encoding = 'utf-8'
BUFSIZE = 1024

zk_hosts = '10.6.1.19:2181,10.6.10.2:2181,10.6.10.2:2181'
ha_path = '/mysql/haproxy'
listen_port = 9011

'''cetus目录配置'''
cetus_dir = '/usr/local/cetus'
cetus_conf_dir = '/usr/local/cetus/conf/shard.conf'
cetus_proxy = True  #如果使用的haprox请设置为False，如果使用cetus请设置为True
''''''

class _hb:
    retry_state = ''


"""
普通登录用户，检测服务是否可用
"""
mysql_user = ''
mysql_password = ''


class AlterCetus:
    def __init__(self, groupname, conf):
        '''
        cetus修改
        :param groupname:
        :param conf:
        '''
        self.groupname = groupname.split('_')[1]
        self._new_conf = conf if type(conf) == dict else eval(conf)
        self.write_info = self._new_conf['write']
        self.read_info = self._new_conf['write'] if type(self._new_conf['read']) == list else eval(
            self._new_conf['read'])

        for addr in self.read_info:
            if addr == self.write_info:
                self.read_info.remove(addr)

    def star(self):
        '''
        cetus配置修改
        new_conf格式{"write":"host:port","read":"["host:port","host:port"]"}
        '''
        cur,conn = self.__cetus_conn()
        if cur is None:
            return None
        logging.info('connection to db success')
        backends = self.__get_all_backends(cur=cur)
        logging.info('backends:{}'.format(backends))
        master_new_info = None
        if backends is None:
            return None
        '''修改master节点指向'''
        for row in backends:
            if row['type'] == 'rw' and row['address'] != self.write_info:
                cur.execute('delete from backends where backend_ndx={};'.format(row['back_index']))
        _tmp_stats = None
        for row in backends:
            if row['address'] == self.write_info:
                cur.execute('update backends set state="up",type="rw" where backend_ndx={};'.format(row['back_index']))
                master_new_info = self.write_info
                _tmp_stats = True
        if _tmp_stats is None:
            cur.execute('add master "{}@{}" '.format(self.write_info,self.groupname))
            cur.execute('update backends set state="up",type="rw" where address="{}";'.format(self.write_info))
            master_new_info = self.write_info
        '''修改slave节点指向,不在新的可读节点中直接删除'''
        _back_read_list = [backend['address'] for backend in backends if backend['type'] in ('ro','unknown')]
        if self.read_info:
            #_back_read_list = [backend['address'] for backend in backends if backend['type'] == 'ro']
            logging.info('{}:{}'.format(_back_read_list,self.read_info))
            for addr in _back_read_list:
                if addr not in self.read_info:
                    cur.execute('delete from backends where address="{}";'.format(addr))
                    _back_read_list.remove(addr)

            for addr in self.read_info:
                if addr in _back_read_list:
                    cur.execute('update backends set state="up",type="ro" where address="{}";'.format(addr))
                else:
                    cur.execute('add slave "{}@{}"'.format(addr,self.groupname))
                    cur.execute('update backends set state="up",type="ro" where address="{}";'.format(addr))
        else:
            for addr in _back_read_list:
                if master_new_info and master_new_info == addr:
                    continue
                cur.execute('delete from backends where address="{}";'.format(addr))
        cur.execute('save settings')
        try:
            cur.close()
            conn.close()
        except:
            pass
        return True

    def __get_all_backends(self,cur):
        '''
        获取cetus中所有的后端节点
        :param cur:
        :return:
        '''
        try:
            cur.execute('select * from backends;')
            result = cur.fetchall()
            rows = []
            for row in result:
                if row['group'] == self.groupname:
                    rows.append({'address': row['address'], 'state': row['state'], 'type': row['type'], 'back_index': row['backend_ndx']})
            return rows
        except MySQLdb.Error:
            logging.error(traceback.format_list())
            return None

    def __cetus_conn(self):
        '''
        创建管理端口的链接信息
        :return:
        '''
        try:
            admin_user, admin_passwd, admin_port = self.__get_config()
            logging.info('{}:{}:{}'.format(admin_user, admin_passwd, admin_port))
            conn = MySQLdb.connect(host='127.0.0.1', port=admin_port, user=admin_user, passwd=admin_passwd,cursorclass=MySQLdb.cursors.DictCursor)
            cur = conn.cursor()
            return cur,conn
        except:
            logging.error(traceback.format_list())
            return None,None
    def __get_config(self):
        '''
        获取cetus配置文件目录中对应groupname的配置文件的管理地址信息
        :return:
        '''
        self.conf = ConfigParser.ConfigParser()
        #self.conf.read('{}/{}.conf'.format(cetus_conf_dir, self.groupname))
        self.conf.read(cetus_conf_dir)
        admin_address = self.conf.get('cetus', 'admin-address')
        address_port = int(admin_address.split(':')[1])
        admin_username = self.conf.get('cetus', 'admin-username')
        admin_passwd = self.conf.get('cetus', 'admin-password')
        return admin_username, admin_passwd, address_port




class CheckSer:
    """mysqlrouter服务检测，故障重启
       对只读端口进行连接检测
    """

    def __init__(self):
        pass

    def run(self):
        if cetus_proxy:
            return
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
    __zk.stop()
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
                if cetus_proxy:
                    state = AlterCetus(groupname=str,conf=conf).star()
                else:
                    state = AlterConf(groupname=str, new_conf=conf)

                if state:
                    logging.info('%s : %s Changed  State: OK' % (now_time, str))
                    break
                else:
                    logging.info('%s : %s Changed  State: Failed' % (now_time, str))
                    break
        logging.info("close:{}".format(self.client.getpeername()))


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
                if item[0] == 2 and not item[1] == '127.0.0.1' and ':' not in k :
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

