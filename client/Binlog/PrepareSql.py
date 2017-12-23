# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import struct,types
from Metadata import binlog_events
from Metadata import column_type_dict
import pymysql,traceback


class GetRollStatement(object):
    def __init__(self,databasename,tablename,table_column_list,table_pk_idex_list):
        self.databasename = databasename
        self.tablename = tablename
        self.table_column_list = table_column_list
        self.table_pk_idex_list = table_pk_idex_list

        self.rollback_statement = None
        self.transaction_statement = None
    def __join(self,column,value):
        if type(value) is types.StringType:
            if value == 'Null':
                return ' {}={}'.format(column, value)
            else:
                return ' {}="{}"'.format(column, value)
        else:
            return  ' {}={}'.format(column,value)
    def WriteEvent(self,values):
        rollbal_sql = 'delete from {}.{} where '.format(self.databasename,self.tablename)
        transaction_sql = 'insert into {}.{} values '
        if self._is_pri:
            sql += self.__join(self._is_pri[0][0],values[self._is_pri[0][1]])
        else:
            for i,column in enumerate(self.column_list):
                sql += self.__join(column[0],values[i])
                if column != self.column_list[-1]:
                    sql += ' and'
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ', sql)
        else:
            self.__tmppack(sql,2)
    def DeleteEvent(self,values):
        sql = 'insert into {}.{}({}) values('.format(self.databasename,self.tablename,','.join([a[0] for a in self.table_column_list]))
        for idex,value in enumerate(values):
            if type(value) is types.StringType:
                if value == 'Null':
                    sql += '{}'.format(value)
                else:
                    sql += '"{}"'.format(value)
            else:
                sql += '{}'.format(value)
            if len(values[idex:]) <= 1:
                sql += ')'
            else:
                sql += ','
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ', sql)
        else:
            self.__tmppack(sql, 2)
    def UpateEvent(self,values):
        _set = []
        _where = []
        if self._is_pri:
            _where.append(self.__join(self._is_pri[0][0],after_values[self._is_pri[0][1]]))
        else:
            for i,column in enumerate(self.table_column_list):
                _where.append(self.__join(column[0],after_values[i]))

        for i,column in enumerate(self.table_column_list):
            _set.append(self.__join(column[0],befor_values[i]))
        sql = 'update {}.{} set {} where {}'.format(self.databasename, self.tablename, ','.join(_set).replace(" ",""), ','.join(_where))
        if _remote_filed._rollback_status:
            print '{: >21}{}{}'.format('', '-- ',sql)
        else:
            self.__tmppack(sql, 2)


    def CreateSQL(self,values=None,event_type=None):
        if event_type == binlog_events.WRITE_ROWS_EVENT:
            self.WriteEvent(values)
        elif event_type == binlog_events.UPDATE_ROWS_EVENT:
            self.UpateEvent(values)
        elif event_type == binlog_events.DELETE_ROWS_EVENT:
            self.DeleteEvent(values)

    def SaveGtid(self,gtid=None,xid=None):
        if xid:
            __gtid = _rollback._gtid.split(':')
            tid = int(__gtid[1])
            uuid = str(__gtid[0])
            self.__tmppackgtid(uuid,tid,1)
        elif _rollback._gtid != gtid:
            _rollback._gtid = gtid

    def __tmppackgtid(self,uuid,tid,type):
        s_uuid = struct.Struct('{}s'.format(len(uuid)))
        s_header = struct.Struct('QB')
        _uuid = s_uuid.pack(uuid)
        _header = s_header.pack(tid,type)
        _rollback._myfile.write(_uuid)
        _rollback._myfile.write(_header)
    def __tmppack(self,value,type):
        import re
        _value = re.sub(r"\s{2,}"," ",str(value).strip()) + ';'
        s_value = struct.Struct('{}s'.format(len(_value)))
        s_header = struct.Struct('QB')
        _value = s_value.pack(_value)
        _header = s_header.pack(len(_value),type)
        _rollback._myfile.write(_value)
        _rollback._myfile.write(_header)

    def __checkpk(self):
        return True if '{}:{}'.format(self.databasename,self.tablename) in self.table_pk_idex_list else None
