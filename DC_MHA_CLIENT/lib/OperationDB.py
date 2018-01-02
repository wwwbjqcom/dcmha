# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
import sys,pymysql,traceback
from Loging import Logging
sys.path.append("..")
from config.get_config import GetConf
from Binlog.Replication import ReplicationMysql
from Binlog.ParseEvent import ParseEvent
from Binlog.PrepareStructure import GetStruct
from Binlog.Metadata import binlog_events
from Binlog.Metadata import column_type_dict

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None
    table_struct_list = {}
    table_pk_idex_list = {}
    rollback_sql_list = []
    transaction_sql_list = []

def WhereJoin(values,table_struce_key):
    __tmp = []
    for idex,col in enumerate(tmepdata.table_struct_list[table_struce_key]):
        if tmepdata.cloums_type_id_list[idex] not in (column_type_dict.MYSQL_TYPE_LONGLONG,column_type_dict.MYSQL_TYPE_LONG,column_type_dict.MYSQL_TYPE_SHORT,column_type_dict.MYSQL_TYPE_TINY,column_type_dict.MYSQL_TYPE_INT24):
            __tmp.append('{}="{}"'.format(col, values[idex]))
        else:
            __tmp.append('{}={}'.format(col,values[idex]))
    return ','.join(__tmp)

def GetSQL(_values=None,event_code=None):
    table_struce_key = '{}:{}'.format(tmepdata.database_name,tmepdata.table_name)
    if table_struce_key not in tmepdata.table_struct_list:
        column_list, pk_idex = GetStruct().GetColumn(tmepdata.database_name,tmepdata.table_name)
        tmepdata.table_struct_list[table_struce_key] = column_list
        tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
    for value in _values:
        '''获取sql语句'''
        if event_code == binlog_events.WRITE_ROWS_EVENT:
            '''delete'''
            if table_struce_key in tmepdata.table_pk_idex_list:
                __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
                pk,pk_value = tmepdata.table_struct_list[table_struce_key][__pk_idx],value[__pk_idx]
                rollback_sql = 'DELETE FROM {}.{} WHERE {}={};'.format(tmepdata.database_name,tmepdata.table_name,pk,pk_value)
            else:
                rollback_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,WhereJoin(value,table_struce_key))
            cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name,tmepdata.table_name,tuple(value))
        elif event_code == binlog_events.DELETE_ROWS_EVENT:
            '''insert'''
            rollback_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name,tmepdata.table_name,tuple(value))
            if table_struce_key in tmepdata.table_pk_idex_list:
                __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
                pk, pk_value = tmepdata.table_struct_list[table_struce_key][__pk_idx], value[__pk_idx]
                cur_sql = 'DELETE FROM {}.{} WHERE {}={};'.format(tmepdata.database_name,tmepdata.table_name,pk,pk_value)
            else:
                cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,WhereJoin(value,table_struce_key))
        elif event_code == binlog_events.UPDATE_ROWS_EVENT:
            '''update'''
            __values = [_values[i:i + 2] for i in xrange(0, len(value), 2)]
            print __values
        tmepdata.rollback_sql_list.append(rollback_sql)
        tmepdata.transaction_sql_list.append(cur_sql)

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
                _parse_event = ParseEvent(packet=pkt,remote=True)
                event_code, event_length = _parse_event.read_header()
                if event_code is None:
                    ReplConn.close()
                    break
                if event_code in (binlog_events.WRITE_ROWS_EVENT,binlog_events.UPDATE_ROWS_EVENT,binlog_events.DELETE_ROWS_EVENT):
                     _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,cloums_type_id_list=tmepdata.cloums_type_id_list,metadata_dict=tmepdata.metadata_dict)
                     GetSQL(_values=_values,event_code=event_code)
                elif event_code == binlog_events.TABLE_MAP_EVENT:
                    tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据

            except Exception,e:
                Logging(msg=traceback.format_exc(),level='error')
                ReplConn.close()
                break
        return tmepdata.transaction_sql_list,tmepdata.rollback_sql_list
    else:
        Logging(msg='replication failed................', level='error')


def ChangeMaster(mysqlconn=None,master_host=None,gtid=None):
    repl_user,repl_passwd,repl_port = GetConf().GetReplUser(),GetConf().GetPeplPassowd(),GetConf().GetReplPort()
    sql = 'CHANGE MASTER TO MASTER_HOST="{}",MASTER_PORT={},MASTER_USER="{}",MASTER_PASSWORD="{}",' \
          'MASTER_AUTO_POSITION=1 FOR CHANNEL "default";'.format(master_host.replace('-','.'),repl_port,repl_user,repl_passwd)
    with mysqlconn.cursor() as cur:
        try:
            cur.execute('reset master;')
            cur.execute('set gtid_purged="{}"'.format(gtid))
            cur.execute(sql)
            cur.execute('start slave;')
            return True
        except Exception, e:
            Logging(msg=traceback.format_exc(), level='error')
            return False