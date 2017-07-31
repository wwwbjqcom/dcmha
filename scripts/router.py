# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

from kazoo.client import KazooClient
import threading,time,os,psutil
import ConfigParser,traceback
import logging
logging.basicConfig(filename='ha_client.log', level=logging.INFO)
import socket
encoding = 'utf-8'
BUFSIZE = 1024


zk_hosts = '192.168.1.2:2181,192.168.1.3:2182,192.168.1.4:2183'
ha_path = '/mysql/haproxy'
listen_port = 9011

def ReStart(groupname):
    '''重启mysqlrouter'''
    restart_stat = None
    pids = psutil.pids()
    for pid in pids:
        p = psutil.Process(pid)
        cmdline = p.cmdline()
        if groupname+'.conf' in cmdline:
            try:
                os.kill(pid, 9)
                os.popen('cd /etc/mysqlrouter;nohup mysqlrouter -c mysqlrouter.conf -a %s.conf &' % groupname)
                return True
            except Exception,e:
                logging.error(traceback.format_exc())
                return False
            restart_stat = True
            break
    if restart_stat is None:
        os.popen('cd /etc/mysqlrouter;nohup mysqlrouter -c mysqlrouter.conf -a %s.conf &' % groupname)




def AlterConf(groupname,new_conf):
    '''修改mysqlrouter配置并重载'''
    conf_path = '/etc/mysqlrouter/%s.conf' % groupname
    _new_conf = eval(new_conf)
    conf = ConfigParser.ConfigParser()
    conf.read(conf_path)
    conf.set('routing:rw', 'destinations',_new_conf['write'])
    reads = ','.join(eval(_new_conf['read']))
    conf.set('routing:ro','destinations',reads)
    try:
        conf.write(open(conf_path,'w'))
        if ReStart(groupname):
            return True
        else:
            logging.error('mysqlrouter restart failed')
            raise "mysqlrouter restart failed"
    except:
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        logging.error('%s: %s Change config ERROR  New conf: %s' % (now_time,groupname,new_conf))
        return False


def zkHandle(groupname):
    zk = KazooClient(hosts=zk_hosts)
    zk.start()
    path = ha_path+'/'+groupname
    if zk.exists(path=path):
        conf,stat = zk.get(path=path)
        return conf
    else:
        return False
    zk.stop()


class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client
    def run(self):
        while True:
            data = self.client.recv(BUFSIZE)
            now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            str = bytes.decode(data, encoding)
            conf = zkHandle(str)
            if conf:
                logging.info('%s : %s Start Changed' % (now_time,str))
                '''修改配置'''
                if AlterConf(str,conf):
                    logging.info('%s : %s Changed  State: OK' % (now_time, str))
                    self.client.send('True')
                else:
                    logging.info('%s : %s Changed  State: Failed' % (now_time, str))
                    self.client.send('False')
            else:
                logging.info('%s : %s Not Found' % (now_time,str))
                self.client.send('False')

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


if __name__ == "__main__":
    lst = Listener(listen_port)
    lst.start()