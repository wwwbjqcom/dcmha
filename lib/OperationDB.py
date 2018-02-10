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
    table_struct_type_list = {}  # 字段类型列表


def WhereJoin(table_struce_key):
    return ' AND '.join(['{}=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])


def SetJoin(table_struce_key):
    return ','.join(['{}=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])


def ValueJoin(table_struce_key):
    return '({})'.format(','.join(['%s' for i in range(len(tmepdata.table_struct_list[table_struce_key]))]))

def GetSQL(_values=None,event_code=None):
    table_struce_key = '{}:{}'.format(tmepdata.database_name,tmepdata.table_name)

    if table_struce_key in tmepdata.table_pk_idex_list:
        '''获取主键所在index'''
        __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
        pk = tmepdata.table_struct_list[table_struce_key][__pk_idx]
    else:
        __pk_idx = None

    if event_code == binlog_events.UPDATE_ROWS_EVENT:
        __values = [_values[i:i + 2] for i in xrange(0, len(_values), 2)]
        for row_value in __values:
            if __pk_idx is not None:
                roll_pk_value, cur_pk_value =  row_value[1][__pk_idx], row_value[0][__pk_idx]
                rollback_sql = 'UPDATE {}.{} SET {} WHERE {}=%s'.format(tmepdata.database_name, tmepdata.table_name,
                                                                        SetJoin(table_struce_key), pk)
                cur_sql = 'UPDATE {}.{} SET {} WHERE {}=%s'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   SetJoin(table_struce_key), pk)

                rollback_args = row_value[0] + [roll_pk_value]
                cur_args = row_value[1] + [cur_pk_value]
            else:
                rollback_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                     SetJoin(table_struce_key),
                                                                     WhereJoin(table_struce_key))
                cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                               SetJoin(table_struce_key),
                                                               WhereJoin(table_struce_key))
                rollback_args = row_value[0] + row_value[1]
                cur_args = row_value[1] + row_value[0]

            tmepdata.rollback_sql_list.append([rollback_sql,rollback_args])
            tmepdata.transaction_sql_list.append([cur_sql,cur_args])
    else:
        for value in _values:
            '''获取sql语句'''
            if event_code == binlog_events.WRITE_ROWS_EVENT:
                '''delete'''
                if __pk_idx is not None:
                    rollback_sql = 'DELETE FROM {}.{} WHERE {}=%s;'.format(tmepdata.database_name,tmepdata.table_name,pk)
                    rollback_args = [value[__pk_idx]]
                else:
                    rollback_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,WhereJoin(table_struce_key))
                    rollback_args = value

                cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   ValueJoin(table_struce_key))
                cur_args = value

                tmepdata.rollback_sql_list.append([rollback_sql,rollback_args])
                tmepdata.transaction_sql_list.append([cur_sql,cur_args])
            elif event_code == binlog_events.DELETE_ROWS_EVENT:
                '''insert'''
                rollback_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                    ValueJoin(table_struce_key))
                rollback_args = value
                if __pk_idx is not None:
                    cur_sql = 'DELETE FROM {}.{} WHERE {}=%s;'.format(tmepdata.database_name,tmepdata.table_name,pk)
                    cur_args = [value[__pk_idx]]
                else:
                    cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,WhereJoin(table_struce_key))
                    cur_args = value
                tmepdata.rollback_sql_list.append([rollback_sql,rollback_args])
                tmepdata.transaction_sql_list.append([cur_sql,cur_args])



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
                     _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                     metadata_dict=tmepdata.metadata_dict,unsigned_list=tmepdata.table_struct_type_list[table_struce_key])
                     GetSQL(_values=_values,event_code=event_code)
                elif event_code == binlog_events.TABLE_MAP_EVENT:
                    tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据
                    table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
                    if table_struce_key not in tmepdata.table_struct_list:
                        column_list, pk_idex,column_type_list = GetStruct().GetColumn(tmepdata.database_name, tmepdata.table_name)
                        tmepdata.table_struct_list[table_struce_key] = column_list
                        tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
                        tmepdata.table_struct_type_list[table_struce_key] = column_type_list

            except Exception,e:
                Logging(msg=traceback.format_exc(),level='error')
                ReplConn.close()
                break
        transaction_sql_list,rollback_sql_list = tmepdata.transaction_sql_list,tmepdata.rollback_sql_list
        tmepdata.transaction_sql_list, tmepdata.rollback_sql_list = [],[]
        return transaction_sql_list,rollback_sql_list
    else:
        Logging(msg='replication failed................', level='error')


def ChangeMaster(mysqlconn=None,master_host=None,gtid=None):
    repl_user,repl_passwd,repl_port = GetConf().GetReplUser(),GetConf().GetPeplPassowd(),GetConf().GetReplPort()
    sql = 'CHANGE MASTER TO MASTER_HOST="{}",MASTER_PORT={},MASTER_USER="{}",MASTER_PASSWORD="{}",' \
          'MASTER_AUTO_POSITION=1 FOR CHANNEL "default";'.format(master_host.replace('-','.'),repl_port,repl_user,repl_passwd)
    with mysqlconn.cursor() as cur:
        try:
            cur.execute('reset master;')
            cur.execute('set global gtid_purged="{}"'.format(gtid))
            cur.execute(sql)
            cur.execute('start slave;')
            cur.execute('set global read_only=1')
            cur.execute('set global sync_binlog=0')
            cur.execute('set global innodb_flush_log_at_trx_commit=0')
        except pymysql.Warning,e:
            Logging(msg=traceback.format_exc(), level='warning')
            cur.execute('set global read_only=1')
            cur.execute('set global sync_binlog=0')
            cur.execute('set global innodb_flush_log_at_trx_commit=0')
            return True
        except pymysql.Error, e:
            Logging(msg=traceback.format_exc(), level='error')
            cur.execute('set global read_only=1')
            cur.execute('set global sync_binlog=0')
            cur.execute('set global innodb_flush_log_at_trx_commit=0')
            return False
        return True
