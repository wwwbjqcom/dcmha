# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys,traceback
from Connection import TcpClient
sys.path.append("..")
from lib.log import Logging
from Binlog.ParseEvent import ParseEvent
from Binlog.Metadata import binlog_events,column_type_dict

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None
    table_struct_list = {}
    table_pk_idex_list = {}
    sql_all_list = []


class Append(TcpClient):
    def __init__(self,connection=None,cursor=None,host=None,port=None):
        super(Append,self).__init__(host=host,port=port)
        self.mysql_conn = connection
        self.mysql_cur = cursor
        self.packet = None

    def receive(self,conn_info = None):
        # self.client.send(str({'getbinlog': 10010, 'binlog_file': 'bin.000001', 'start_position': 154}).encode('utf8'))
        self.client.send(conn_info)
        while True:
            data = self.client.recv(self.BUFSIZ)
            try:
                if eval(data)['binlogvalue'] == 10010:
                    Logging(msg='recv OK!',level='info')
                    break
            except:
                pass
            if data:
                recv_stat = {'recv_stat': 119}
                self.client.send(str(recv_stat).encode('utf8'))
                self.packet = data
                stat = self.start()
                if stat is None:
                    break
        self.close()
        return self.__executesql()


    def start(self):
        _parse_event = ParseEvent(packet=self.packet)
        try:
            event_code, event_length = _parse_event.read_header()
        except Exception,e:
            Logging(msg=traceback.format_exc(),level='error')
            return None
        if event_code in (binlog_events.WRITE_ROWS_EVENT, binlog_events.UPDATE_ROWS_EVENT, binlog_events.DELETE_ROWS_EVENT):
            _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                            cloums_type_id_list=tmepdata.cloums_type_id_list,
                                            metadata_dict=tmepdata.metadata_dict)
            self.__GetSQL(_values=_values, event_code=event_code)
        elif event_code == binlog_events.TABLE_MAP_EVENT:
            tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict = _parse_event.GetValue(
                type_code=event_code, event_length=event_length)  # 获取event数据
        return True

    def __GetSQL(self,_values=None, event_code=None):
        table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
        if table_struce_key not in tmepdata.table_struct_list:
            column_list, pk_idex = self.__GetColumn(tmepdata.database_name, tmepdata.table_name)
            tmepdata.table_struct_list[table_struce_key] = column_list
            tmepdata.table_pk_idex_list[table_struce_key] = pk_idex

        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            '''update'''
            __values = [_values[i:i + 2] for i in xrange(0, len(_values), 2)]
            if table_struce_key in tmepdata.table_pk_idex_list:
                __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
            for row_value in __values:
                if __pk_idx is not None:
                    pk, roll_pk_value, cur_pk_value = tmepdata.table_struct_list[table_struce_key][__pk_idx], \
                                                      row_value[1][__pk_idx], row_value[0][__pk_idx]
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}={}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.__WhereJoin(row_value[1], table_struce_key),
                                                                       pk, cur_pk_value)
                else:
                    cur_sql = 'UPATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.__WhereJoin(row_value[1], table_struce_key),
                                                                   self.__WhereJoin(row_value[0], table_struce_key))
                tmepdata.sql_all_list.append(cur_sql)
        elif event_code in (binlog_events.DELETE_ROWS_EVENT,binlog_events.WRITE_ROWS_EVENT):
            for value in _values:
                '''获取sql语句'''
                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   tuple(value))
                    tmepdata.sql_all_list.append(cur_sql)
                elif event_code == binlog_events.DELETE_ROWS_EVENT:
                    if table_struce_key in tmepdata.table_pk_idex_list:
                        __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
                        pk, pk_value = tmepdata.table_struct_list[table_struce_key][__pk_idx], value[__pk_idx]
                        cur_sql = 'DELETE FROM {}.{} WHERE {}={};'.format(tmepdata.database_name, tmepdata.table_name, pk,
                                                                          pk_value)
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.__WhereJoin(value, table_struce_key))
                    tmepdata.sql_all_list.append(cur_sql)


    def __WhereJoin(self,values, table_struce_key):
        __tmp = []
        for idex, col in enumerate(tmepdata.table_struct_list[table_struce_key]):
            if tmepdata.cloums_type_id_list[idex] not in (
            column_type_dict.MYSQL_TYPE_LONGLONG, column_type_dict.MYSQL_TYPE_LONG, column_type_dict.MYSQL_TYPE_SHORT,
            column_type_dict.MYSQL_TYPE_TINY, column_type_dict.MYSQL_TYPE_INT24):
                __tmp.append('{}="{}"'.format(col, values[idex]))
            else:
                __tmp.append('{}={}'.format(col, values[idex]))
        return ','.join(__tmp)

    def __GetColumn(self,*args):
        column_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.mysql_cur.execute(sql,args=args)
        result = self.mysql_cur.fetchall()
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex = idex
        return column_list,pk_idex

    def __executesql(self):
        Logging(msg='Additional unsynchronized data now',level='info')
        for sql in tmepdata.sql_all_list:
            Logging(msg='execute sql -- {}'.format(sql),level='info')
            try:
                self.mysql_cur.execute(sql)
                Logging(msg='state OK!', level='info')
            except Exception,e:
                Logging(msg='state failed',level='error')
                Logging(msg=traceback.format_exc(),level='error')
                self.mysql_conn.rollback()
                return False
        else:
            Logging(msg='There is no data to synchronize.', level='info')
        self.mysql_conn.commit()
        return True

