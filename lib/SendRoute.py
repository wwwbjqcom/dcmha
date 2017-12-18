# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import time,sys,traceback
from contextlib import closing
sys.path.append("..")
from socket import *
from zk_handle.zkHandler import zkHander
import logging
logging.basicConfig(filename='mha_server.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')


def SendRoute(group_name,slavedown=None):
    with closing(zkHander()) as zkhander:
        route_content = zkhander.GetRouter(group_name)  # 传递路由配置修改信息
        if route_content:
            _route_content = route_content.split(',')
            for _content in _route_content:
                try:
                    with closing(TcpClient(_content)) as tcpclient:
                        send_stat = tcpclient.Send(group_name)
                except Exception,e:
                    logging.ERROR(traceback.format_exc())

                if not send_stat:
                    if slavedown:
                        return False
                    else:
                        with closing(zkHander()) as zkhander:
                            zkhander.SetWatchDown(group_name, 'failed')
    return True


class TcpClient:
    def __init__(self,host_content):
        _host_content = host_content.split(':')
        HOST = _host_content[0]
        PORT = int(_host_content[1])
        self.BUFSIZ = 1024
        self.ADDR = (HOST, PORT)
        self.client=socket(AF_INET, SOCK_STREAM)
        self.client.connect(self.ADDR)
        self.client.settimeout(1)

    def Send(self,groupname):
        for i in range(3):
            self.client.send(groupname.encode('utf8'))
            data=self.client.recv(self.BUFSIZ)
            if data:
                send_stat = True
                break
            send_stat = False
            time.sleep(1)
        return send_stat

    def close(self):
        self.client.close()

