# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
from warnings import filterwarnings
import MySQLdb,sys,traceback
sys.path.append("..")
from lib.get_conf import GetConf
from lib.log import Logging
filterwarnings('error', category = MySQLdb.Warning)

class dbHandle:
    def __init__(self,host,port):
        self.host,self.port = host,int(port)
        self.mysqluser,self.mysqlpasswd = GetConf().GetMysqlAcount()
        ssl_set = {'ca':GetConf().GetUserSSLCa(),'cert':GetConf().GetUserSSLCert(),'key':GetConf().GetUserSSLKey()}
        try:
            self.local_conn = MySQLdb.connect(host=self.host, user=self.mysqluser, passwd=self.mysqlpasswd, port=self.port, db='',
                                         charset="utf8",ssl=ssl_set)
            self.mysql_cur = self.local_conn.cursor()
            self.state = True
        except MySQLdb.Error,e:
            Logging(msg=traceback.format_exc(),level='error')
            self.state = False

    def ChangeMaster(self,host,port):
        '''主从指向'''
        repluser,replpassword,ssl_ca,ssl_cert,ssl_key = GetConf().GetReplAcount()
        try:
            sql = 'reset slave all;'
            print self.host
            try:
                self.mysql_cur.execute(sql)
            except:
                self.mysql_cur.execute('stop slave')
                self.mysql_cur.execute(sql)
            change_sql = 'change master to master_host="%s",master_port=%s,master_user="%s",master_password="%s",master_auto_position=1 for channel "default"' % (host,int(port),repluser,replpassword)
            self.mysql_cur.execute(change_sql)
            self.__set_variables(type='slave')
            return True
        except MySQLdb.Warning,e:
            start_sql = 'start slave'
            self.mysql_cur.execute(start_sql)
            self.__set_variables(type='slave')
            Logging(msg='Change master to %s   state : Warning' % host,level='warning')
            Logging(msg=traceback.format_exc(),level='warning')
            return True
        except MySQLdb.Error,e:
            Logging(msg='Change master to %s   state : Error' % host,level='error')
            Logging(msg=traceback.format_exc(),level='error')
            return False

    def ResetMaster(self,groupname):
        try:
            '''获取当前binlog读取位置'''
            append_stat=None
            master_log_file,read_master_log_pos,master_host = self.CheckPos(get_host=True)

            '''================'''
            #用于mysql宕机，服务器在线追加数据
            from zk_handle.zkHandler import zkHander
            from Append.AppendValue import Append
            from lib.get_conf import GetConf
            from contextlib import closing
            with closing(zkHander()) as zkhander:
                client_stat = zkhander.CheckOnlineClient(master_host)
                if client_stat:
                    __get_content = {'getbinlog': 10010, 'binlog_file': master_log_file, 'start_position': read_master_log_pos}
                    Logging(msg='gets the unsynchronized data not. info:{}'.format(__get_content),level='info')
                    append_stat = Append(connection=self.local_conn,cursor=self.mysql_cur,host=master_host,port=GetConf().GetClientPort()).receive(conn_info=str(__get_content))
                    if append_stat:
                        Logging(msg='Append OK',level='info')
                    else:
                        Logging(msg='Append Failed',level='error')

            '''================='''

            if master_host:
                readbinlog_status = str([groupname,master_log_file,read_master_log_pos])
                execute_gtid = str([groupname,self.__CetGtid()])
                with closing(zkHander()) as zkhander:
                    if append_stat:
                        zkhander.SetExecuteGtid(master_host, execute_gtid)
                    else:
                        zkhander.SetReadBinlog(master_host,readbinlog_status)
                        zkhander.SetExecuteGtid(master_host,execute_gtid)
            ''''''

            #self.mysql_cur.execute('set global read_only=0;')
            self.mysql_cur.execute('stop slave')
            self.mysql_cur.execute('reset slave all;')
            self.__set_variables(type='master')

        except MySQLdb.Warning,e:
            Logging(msg=traceback.format_exc(),level='warning')
            self.mysql_cur.execute('reset slave all;')
            self.__set_variables(type='master')
        except MySQLdb.Error,e:
            self.__set_variables(type='master')
            Logging(msg=traceback.format_exc(),level='warning')

        """附加任务,没有可注销，不注销也无影响，只要zk中不添加对应数据"""
        import AdditionTask
        addition = AdditionTask.Addition(self.host)
        addition_master = addition.GetRepl()
        if addition_master:
            exe_addition = AdditionTask.ExecuteAdditionTask(self.host,self.port)
            for region in addition_master:
                exe_addition.Change(region,addition_master[region])
        """"""
    def __set_variables(self,type=None):
        if type == 'slave':
            self.mysql_cur.execute('set global read_only=1')
            self.mysql_cur.execute('set global sync_binlog=0')
            self.mysql_cur.execute('set global innodb_flush_log_at_trx_commit=0')
        elif type == 'master':
            self.mysql_cur.execute('set global read_only=0')
            self.mysql_cur.execute('set global sync_binlog=1')
            self.mysql_cur.execute('set global innodb_flush_log_at_trx_commit=1')

    def RetryConn(self):
        return True if self.state else False

    def CheckPos(self,get_host=None):
        self.mysql_cur.execute('select Master_log_name,master_log_pos,Host from mysql.slave_master_info WHERE Channel_name="default";')
        pos = self.mysql_cur.fetchall()
        if pos:
            if get_host:
                return pos[0][0], int(pos[0][1]),pos[0][2]
            else:
                return pos[0][0], int(pos[0][1])
        else:
            if get_host:
                return None,None,None
            else:
                return None,None

    def __CetGtid(self):
        self.mysql_cur.execute('show master status;')
        result = self.mysql_cur.fetchall()
        return str(result[0][4]).replace('\n','')

    def GetColumn(self,*args):
        '''args顺序 database、tablename,获取表结构用于追加数据'''
        column_list = []
        pk_idex = None

        sql = 'select COLUMN_NAME,COLUMN_KEY from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.cur.execute(sql,args=args)
        result = self.cur.fetchall()
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex = idex
        self.cur.close()
        self.connection.close()
        return column_list,pk_idex

    def close(self):
        try:
            self.mysql_cur.close()
            self.local_conn.close()
        except:
            pass

'''
from contextlib import closing
with closing(dbHandle('192.168.212.205',3306)) as dbhandle:
    a,b,c = dbhandle.CheckPos(get_host=True)
    print a,b,c'''





