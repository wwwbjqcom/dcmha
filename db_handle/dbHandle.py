# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
from warnings import filterwarnings
import MySQLdb,sys,traceback
sys.path.append("..")
from lib.get_conf import GetConf
import logging
logging.basicConfig(filename='mha_server.log',
                    level=logging.INFO,
                    format  = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',
                    datefmt='%Y-%m-%d %A %H:%M:%S')
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
            logging.error(traceback.format_exc())
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
            return True
        except MySQLdb.Warning,e:
            start_sql = 'start slave'
            self.mysql_cur.execute(start_sql)
            self.mysql_cur.execute('set global read_only=1;')
            logging.warning('Change master to %s   state : Warning' % host)
            logging.warning(traceback.format_exc())
            return True
        except MySQLdb.Error,e:
            logging.error('Change master to %s   state : Error' % host)
            logging.error(traceback.format_exc())
            return False

    def ResetMaster(self,groupname):
        try:
            '''获取当前binlog读取位置'''
            master_log_file,read_master_log_pos,master_host = self.CheckPos(get_host=True)
            if master_host:
                readbinlog_status = str([groupname,master_log_file,read_master_log_pos])
                from zk_handle.zkHandler import zkHander
                from contextlib import closing
                with closing(zkHander()) as zkhander:
                    zkhander.SetReadBinlog(master_host,readbinlog_status)
            ''''''

            #self.mysql_cur.execute('set global read_only=0;')
            self.mysql_cur.execute('stop slave')
            self.mysql_cur.execute('reset slave all;')

        except MySQLdb.Warning,e:
            logging.warning(traceback.format_exc())
        except MySQLdb.Error,e:
            logging.warning(traceback.format_exc())

        """附加任务,没有可注销，不注销也无影响，只要zk中不添加对应数据"""
        import AdditionTask
        addition = AdditionTask.Addition(self.host)
        addition_master = addition.GetRepl()
        if addition_master:
            exe_addition = AdditionTask.ExecuteAdditionTask(self.host,self.port)
            for region in addition_master:
                exe_addition.Change(region,addition_master[region])
        """"""

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





