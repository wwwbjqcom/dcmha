# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
from socket import *

class TcpClient(object):
    def __init__(self,host=None,port=None):
        self.BUFSIZ = 58720256
        self.ADDR = (host, port)
        self.client = socket(AF_INET, SOCK_STREAM)
        self.client.connect(self.ADDR)
        self.client.settimeout(10)



    def close(self):
        self.client.close()


#TcpClient().Send()