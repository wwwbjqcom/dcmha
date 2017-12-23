# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,pymysql
from Loging import Logging
sys.path.append("..")
from Binlog.Replication import ReplicationMysql
from Binlog.ParseEvent import ParseEvent
from Binlog.PrepareStructure import GetStruct
from Binlog.Metadata import binlog_events

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None
    table_struct_list = {}
    table_pk_idex_list = {}
    sql_list = []


def GetSQL(_database_name=None,_table_name=None,_cloums_type_id_list=None,
           _metadata_dict=None,_values=None,event_code=None):
    ''''''
    if _database_name:
        tmepdata.database_name,tmepdata.table_name,tmepdata.cloums_type_id_list,tmepdata.metadata_dict=_database_name,_table_name,_cloums_type_id_list,_metadata_dict
        table_struce_key = '{}:{}'.format(_database_name,_table_name)
        if table_struce_key not in tmepdata.table_struct_list:
            column_list, pk_idex = GetStruct().GetColumn(_database_name,_table_name)
            tmepdata.table_struct_list[table_struce_key] = column_list
            tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
    elif _database_name is None and _values:
        '''获取sql语句'''
        if event_code == binlog_events.WRITE_ROWS_EVENT:
            '''delete'''
            print _values
        elif event_code == binlog_events.DELETE_ROWS_EVENT:
            '''insert'''
            print _values
        elif event_code == binlog_events.UPDATE_ROWS_EVENT:
            '''update'''
            __values = [_values[i:i + 2] for i in xrange(0, len(_values), 2)]
            print __values

def Operation(binlog_stat):
    Logging(msg='this master is old master,rollback transactions now', level='info')
    groupname, binlog_file, position = binlog_stat[0], binlog_stat[1], binlog_stat[2]
    Logging(msg='replication to master.............', level='info')
    ReplConn = ReplicationMysql(log_file=binlog_file, log_pos=position).ReadPack()
    if ReplConn:
        Logging(msg='replication succeed................', level='info')
        while True:
            try:
                if pymysql.__version__ < "0.6":
                    pkt = ReplConn.read_packet()
                else:
                    pkt = ReplConn._read_packet()
                event_code, event_length, _ = ParseEvent(packet=pkt).read_header()

                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    _database_name, _table_name, _cloums_type_id_list, _metadata_dict, _values = ParseEvent(
                        packet=pkt).GetValue(tmepdata.cloums_type_id_list,tmepdata.metadata_dict)  # 获取event数据
                else:
                    _database_name, _table_name, _cloums_type_id_list, _metadata_dict, _values = ParseEvent(packet=pkt).GetValue()  # 获取event数据
                GetSQL(_database_name, _table_name, _cloums_type_id_list, _metadata_dict, _values,event_code)
            except:
                ReplConn.close()
                break
    else:
        Logging(msg='replication failed................', level='error')