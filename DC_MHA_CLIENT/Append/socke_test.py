# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
from socket import *
import struct

class TcpClient:
    def __init__(self):
        self.BUFSIZ = 1024
        self.ADDR = ('127.0.0.1', 9011)
        self.client=socket(AF_INET, SOCK_STREAM)
        self.client.connect(self.ADDR)
        self.client.settimeout(1)

    def Send(self):
        self.client.send(groupname.encode('utf8'))
        while True:
            data=self.client.recv(self.BUFSIZ)
            if data:
                a = data.read(19)
                print struct.unpack('=IBIIIH', a)

    def close(self):
        self.client.close()

TcpClient().Send()