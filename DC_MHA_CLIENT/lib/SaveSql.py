# -*- encoding: utf-8 -*-
'''
@author: xiaozhong
'''

import os,time
from System import UsePlatform

class SaveSql:
    def __init__(self):
        self.path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        self.file = None

    def __CheckSystem(self):
        sys = UsePlatform()
        if sys == 'Linux':
            return self.path
        elif sys == 'Windows':
            return self.path.replace('\\','/')

    def __create_file(self):
        date = time.strftime('%Y%m%d',time.localtime(time.time()))
        file_path = '{}/{}.sql'.format(self.__CheckSystem(),date)
        self.file = open(file=file_path,str='w+')

    def ToFile(self,sql_list):
        for sql in sql_list:
            self.file.write('{}\n'.format(sql))

        self.file.close()