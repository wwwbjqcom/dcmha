# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import psutil,time,MySQLdb
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
logging.basicConfig(filename='/var/log/zk_client.log', level=logging.INFO)

mysql_user='root'
mysql_password = ''
socke_dir = '/usr/local/mysql/mysql.sock'
mysql_port = 3306

retry_num = 10

zk_host = '192.168.1.1:2181，192.168.1.2:2181，192.168.1.3:2181'
zk = KazooClient(hosts=zk_host)
zk.start()
retry_tate = ''

def get_netcard():
    '''获取IP地址'''
    info = psutil.net_if_addrs()
    for k,v in info.items():
        for item in v:
            if item[0] == 2 and not item[1]=='127.0.0.1' and ':' not in k:
                netcard_info = item[1]
    return netcard_info.replace('.','-')


class zk_conn:
    '''创建zk心跳连接'''
    def __init__(self, f):
        f()
        self.f = f
        @zk.add_listener
        def my_listener(state):
            global retry_tate
            if state == KazooState.LOST:
                logging.error("LOST")
            elif state == KazooState.SUSPENDED:
                logging.info("SUSPENDED")
            else:
                logging.info("Connected")
                retry_tate = "Connected"
                return retry_tate

    def __call__(self):
        self.f()

@zk_conn
def retry_create():
    '''创建临时node'''
    node_stat =  zk.exists(path='/mysql/online-list/' + get_netcard())
    if node_stat is None:
        zk.create(path="/mysql/online-list/"+get_netcard(),value="",ephemeral=True)
    else:
        zk.delete(path="/mysql/online-list/"+get_netcard())
        zk.create(path="/mysql/online-list/" + get_netcard(), value="", ephemeral=True)

def checkdb():
    try:
        local_conn = MySQLdb.connect(host='127.0.0.1', user=mysql_user, passwd=mysql_password, port=mysql_port, db='',
                                     charset="utf8",unix_socket=socke_dir)
        local_cur = local_conn.cursor()
        return True
    except MySQLdb.Error,e:
        return False


if __name__ == '__main__':
    while True:
        state = checkdb()
        if state is False:
            for i in range(0,retry_num):
                state = checkdb()
                time.sleep(1)
            if state is False:
                break

        if retry_tate == "Connected":
            retry_create()
            retry_tate = ""
        time.sleep(1)
    zk.stop()