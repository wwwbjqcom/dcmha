# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys
sys.path.append("..")
from config.get_config import GetConf
from Binlog.ParseEvent import ParseEvent
from Binlog.Metadata import binlog_event_header_len


class GetBinlog:
    def __init__(self,binlog_file=None,start_position=None,socket_client=None):
        self.binlog_file = binlog_file
        self.start_position = start_position
        self.socket = socket_client
        self.binlog_dir = GetConf().GetBinlogDir()
        self.__start()

    def __start(self):
        binlog_file_dir = '{}/{}'.format(GetConf().GetBinlogDir(),self.binlog_file)
        p_event = ParseEvent(filename=binlog_file_dir,startpostion=self.start_position)
        p_event.file_data.seek(p_event.startposition)
        while True:
            code,event_length = p_event.read_header()
            if code is None:
                break
            else:
                p_event.file_data.seek(-binlog_event_header_len,1)
                self.socket.send(p_event.file_data.read(event_length))
