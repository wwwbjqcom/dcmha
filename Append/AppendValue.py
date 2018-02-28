# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import sys,traceback,MySQLdb
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
    table_struct_type_list = {}  # 字段类型列表
    table_struce_key = None


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
                    self.client.close()
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
                                            metadata_dict=tmepdata.metadata_dict,
                                            unsigned_list=tmepdata.table_struct_type_list[tmepdata.table_struce_key])
            self.__GetSQL(_values=_values, event_code=event_code)
        elif event_code == binlog_events.TABLE_MAP_EVENT:
            tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict = _parse_event.GetValue(
                type_code=event_code, event_length=event_length)  # 获取event数据
            tmepdata.table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
            if tmepdata.table_struce_key not in tmepdata.table_struct_list:
                column_list, pk_idex, column_type_list = self.__GetColumn(tmepdata.database_name,tmepdata.table_name)
                tmepdata.table_struct_list[tmepdata.table_struce_key] = column_list
                tmepdata.table_pk_idex_list[tmepdata.table_struce_key] = pk_idex
                tmepdata.table_struct_type_list[tmepdata.table_struce_key] = column_type_list

        return True

    def __GetSQL(self,_values=None, event_code=None):
        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            '''update'''
            __values = [_values[i:i + 2] for i in xrange(0, len(_values), 2)]
            if tmepdata.table_struce_key in tmepdata.table_pk_idex_list:
                __pk_idx = tmepdata.table_pk_idex_list[tmepdata.table_struce_key]
            for row_value in __values:
                if __pk_idx is not None:
                    pk, cur_pk_value = tmepdata.table_struct_list[tmepdata.table_struce_key][__pk_idx],row_value[0][__pk_idx]
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}=%s'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.__SetJoin(), pk)

                    cur_args = row_value[1] + [cur_pk_value]
                else:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                    self.__SetJoin(),
                                                                    self.__WhereJoin())
                    cur_args = row_value[1] + row_value[0]
                tmepdata.sql_all_list.append([cur_sql, cur_args])
        elif event_code in (binlog_events.DELETE_ROWS_EVENT,binlog_events.WRITE_ROWS_EVENT):
            for value in _values:
                '''获取sql语句'''
                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.__ValueJoin())
                    cur_args = value

                    tmepdata.sql_all_list.append([cur_sql, cur_args])
                elif event_code == binlog_events.DELETE_ROWS_EVENT:
                    if tmepdata.table_struce_key in tmepdata.table_pk_idex_list:
                        __pk_idx = tmepdata.table_pk_idex_list[tmepdata.table_struce_key]
                        pk, pk_value = tmepdata.table_struct_list[tmepdata.table_struce_key][__pk_idx], value[__pk_idx]
                        cur_sql = 'DELETE FROM {}.{} WHERE {}=%s;'.format(tmepdata.database_name, tmepdata.table_name,
                                                                          pk)
                        cur_args = [value[__pk_idx]]
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.__WhereJoin())
                        cur_args = value

                    tmepdata.sql_all_list.append([cur_sql, cur_args])

    def __WhereJoin(self):
        return ' AND '.join(['{}=%s'.format(col) for col in tmepdata.table_struct_list[tmepdata.table_struce_key]])

    def __SetJoin(self):
        return ','.join(['{}=%s'.format(col) for col in tmepdata.table_struct_list[tmepdata.table_struce_key]])

    def __ValueJoin(self):
        return '({})'.format(','.join(['%s' for i in range(len(tmepdata.table_struct_list[tmepdata.table_struce_key]))]))

    def __GetColumn(self,*args):
        column_list = []
        column_type_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.mysql_cur.execute(sql,args=args)
        result = self.mysql_cur.fetchall()
        for idex,row in enumerate(result):
            column_list.append(row[0])
            column_type_list.append(row[2])
            if row[1] == 'PRI':
                pk_idex = idex
        return column_list, pk_idex, column_type_list

    def __executesql(self):
        Logging(msg='Additional unsynchronized data now',level='info')
        for sql in tmepdata.sql_all_list:
            Logging(msg='execute sql -- {}'.format(sql),level='info')
            try:
                self.mysql_cur.execute(sql[0],sql[1])
                Logging(msg='state OK!', level='info')
            except MySQLdb.Warning:
                Logging(msg=traceback.format_exc(), level='warning')
            except Exception,e:
                Logging(msg='state failed',level='error')
                Logging(msg=traceback.format_exc(),level='error')
                self.mysql_conn.rollback()
                tmepdata.sql_all_list = []
                return False
        else:
            Logging(msg='There is no data to synchronize.', level='info')
        self.mysql_conn.commit()
        self.__init_tmepdata()

        return True

    def __init_tmepdata(self):
        tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict = None, None, None, None
        tmepdata.table_struct_list = {}
        tmepdata.table_pk_idex_list = {}
        tmepdata.sql_all_list = []
        tmepdata.table_struct_type_list = {}  # 字段类型列表
        tmepdata.table_struce_key = None


