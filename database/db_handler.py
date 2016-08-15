#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2015-08-12 
# 描述：数据库DAO


import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import MySQLdb
import time
from exceptions import Exception

class DBHandler(object):

    def __init__(self,host_,port_,db_,user_,passwd_,logger,auto_commit=True):
        self.cursor_ = None
        self.conn_ = None

        self.host_ = host_
        self.port_ = port_
        self.db_ = db_
        self.user_ = user_
        self.passwd_ = passwd_

        self.logger_ = logger
        self.auto_commit = auto_commit

        connect_time = 0
        while self.cursor_ is None and connect_time < 5:
            try:
                self.reconnect()
                connect_time += 1
            except Exception,e:
                 self.logger_.error('connect mysql error'+'\t' + str(e) + '\t' 
                                    + traceback.format_exc().replace("\n",""))
                 time.sleep(3)
        self.exc_num = 0



    def reconnect(self):
        if self.conn_ != None:
            try:
                self.conn_.close()
            except Exception as e:
                 self.logger_.warn("colse conn to mysql failed,msg=%s" % str(e))
        try:
            self.conn_ = MySQLdb.connect(host=self.host_, port=self.port_, user=self.user_,
                passwd=self.passwd_, db=self.db_,connect_timeout = 1)
            self.conn_.set_character_set('utf8')
            if self.auto_commit:
                self.conn_.autocommit(self.auto_commit) 
            self.cursor_ = self.conn_.cursor()
        except Exception as e:
            time.sleep(5)
            self.logger_.warn("connect to mysql failed,msg=%s" % str(e))
    
    
    def execute(self,sql):
        count = -1
        try:
            count = self.cursor_.execute(sql)
            self.exc_num += 1
            if not self.auto_commit or self.exc_num % 100 == 0:
                self.commit()
        except (AttributeError, MySQLdb.OperationalError):
            time.sleep(1)
            self.reconnect()
            try:
                count = self.cursor_.execute(sql)
            except Exception as e:
                time.sleep(1)
                self.reconnect()
                self.logger_.error("%s\t%s" % (sql,str(e)))
        except Exception as e:
             self.logger_.error("%s\t%s" % (sql,str(e)))
        return self.cursor_,count


    # 自格式化
    def execute_ex(self,sql_format,args):
        count = -1
        try:
            count = self.cursor_.execute(sql_format,args)
            self.exc_num += 1
            if not self.auto_commit or self.exc_num % 100 == 0:
                self.commit()
        except (AttributeError, MySQLdb.OperationalError):
            time.sleep(1)
            self.reconnect()
            try:
                count = self.cursor_.execute(sql_format,args)
            except Exception as e:
                time.sleep(1)
                self.reconnect()
                self.logger_.error("%s\t%s" % (sql_format,str(e)))
        except Exception as e:
             self.logger_.error("%s\t%s" % (sql_format,str(e)))
        return self.cursor_,count


    def execute_many(self,sql,arg_lst):
        count = -1
        try:
            count = self.cursor_.executemany(sql,arg_lst)
            self.commit()
        except (AttributeError, MySQLdb.OperationalError):
            time.sleep(1)
            self.reconnect()
            count = self.cursor_.executemany(sql,arg_lst)
        except Exception as e:
            print traceback.format_exc()
            self.logger_.error("execute many sql failed:%s" % (str(e)))
        return self.cursor_,count
   

    def commit(self):
        self.conn_.commit()



if __name__ == '__main__':
    hd = DbHandler()
    pass

