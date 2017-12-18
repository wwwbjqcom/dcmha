# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import struct,pymysql,time
connection = pymysql.connect(host='127.0.0.1',
                                     user='root',
                                     password='root',port=3306,
                                     db='',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
connection.autocommit(1)
cur = connection.cursor()

f = open('/usr/local/mysql/data/bin-00001','rb')
f.seek(154)


def read_header():
    '''binlog_event_header_len = 19
    timestamp : 4bytes
    type_code : 1bytes
    server_id : 4bytes
    event_length : 4bytes
    next_position : 4bytes
    flags : 2bytes
    '''
    read_byte = f.read_bytes(19)
    if read_byte:
        result = struct.unpack('=IBIIIH', read_byte)
        type_code, event_length, timestamp, _next_pos = result[1], result[3], result[0], result[4]

        return event_length
    else:
        return None
while True:
    _len = read_header()
    if _len:
        f.seek(-19,1)

        pack = f.read(_len)

        if pymysql.__version__ < "0.6":
            connection.wfile.write(pack)
            connection.wfile.flush()
        else:
            connection._write_bytes(pack)
            connection._next_seq_id = 1
    else:
        break

