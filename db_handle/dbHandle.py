# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''
from warnings import filterwarnings
import MySQLdb,sys,traceback
sys.path.append("..")
from lib.get_conf import GetConf
import logging
logging.basicConfig(filename='zk_client.log', level=logging.INFO)
filterwarnings('error', category = MySQLdb.Warning)

class dbHandle:
    def __init__(self,host,port):
        self.mysqluser,self.mysqlpasswd = GetConf().GetMysqlAcount()
        ssl_set = {'ca':GetConf().GetUserSSLCa(),'cert':GetConf().GetUserSSLCert(),'key':GetConf().GetUserSSLKey()}
        try:
            self.local_conn = MySQLdb.connect(host=host, user=self.mysqluser, passwd=self.mysqlpasswd, port=int(port), db='',
                                         charset="utf8",ssl=ssl_set)
            self.mysql_cur = self.local_conn.cursor()
            self.state = True
        except MySQLdb.Error,e:
            logging.error(traceback.format_exc())
            self.state = False

    def ChangeMaster(self,host,port):
        '''主从指向'''
        repluser,replpassword = GetConf().GetReplAcount()
        try:
            sql = 'reset slave all;'
            try:
                self.mysql_cur.execute(sql)
            except:
                self.mysql_cur.execute('stop slave')
                self.mysql_cur.execute(sql)
            change_sql = 'change master to master_host="%s",master_port=%s,master_user="%s",master_password="%s",' \
                         'master_auto_position=1 for channel "default"' % (host,int(port),repluser,replpassword)
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

    def ResetMaster(self):
        try:
            self.mysql_cur.execute('set global read_only=0;')
            self.mysql_cur.execute('stop slave')
            self.mysql_cur.execute('reset slave all;')
        except MySQLdb.Warning,e:
            logging.warning(traceback.format_exc())
        except MySQLdb.Error,e:
            logging.warning(traceback.format_exc())

    def RetryConn(self):
        return True if self.state else False

    def CheckPos(self):
        self.mysql_cur.execute('select Master_log_name,master_log_pos from mysql.slave_master_info;')
        pos = self.mysql_cur.fetchall()
        if pos:
            return pos[0][0], int(pos[0][1])
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
with closing(dbHandle('192.168.212.208',3306)) as dbhandle:
    a,b = dbhandle.CheckPos()
    print a,b'''



