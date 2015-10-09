#!/usr/bin/env python
#coding=UTF-8
'''
    Created on 2014-03-08
    @author: devin
    @desc:
        数据访问
'''
import sys
import MySQLdb
from MySQLdb.cursors import DictCursor
import datetime
#from logger import logger

# MySQL 连接信息

HOST_66 = "182.254.139.66"
USER_66 = "reader"
PASSWD_66 = "miaoji1109"
DB_66 = "onlinedb"
TABLE_66 = "exchange"

def GetConnection(host, user, passwd, db):
    conn = MySQLdb.connect(host, user, passwd, db, charset="utf8")
    return conn

def ExecuteSQL(host,user,passwd,db,sql, args = None):
    '''
        执行SQL语句, 正常执行返回影响的行数，出错返回Flase 
    '''
    ret = 0
    try:
        conn = GetConnection(host, user, passwd, db)
        cur = conn.cursor()

        ret = cur.execute(sql, args)
        conn.commit()
    except MySQLdb.Error, e:
        print str(e)
        #logger.error("ExecuteSQL error: %s" %str(e))
        return False
    finally:
        cur.close()
        conn.close()

    return ret

def ExecuteSQLs(host,user,passwd,db,sql, args = None):
    '''
        执行多条SQL语句, 正常执行返回影响的行数，出错返回Flase 
    '''
    ret = 0
    try:
        conn = GetConnection(host, user, passwd, db)
        cur = conn.cursor()

        ret = cur.executemany(sql, args)
        conn.commit()
    except MySQLdb.Error, e:
        #logger.error("ExecuteSQL error: %s" %str(e))
        print str(e)
        return False
    finally:
        cur.close()
        conn.close()

    return ret

def QueryBySQL(host,user,passwd,db,sql, args = None, size = None):
    '''
        通过sql查询数据库，正常返回查询结果，否则返回None
    ''' 
    results = []
    try:
        conn = GetConnection(host, user, passwd, db)
        cur = conn.cursor(cursorclass = DictCursor)
        
        cur.execute(sql, args)
        rs = cur.fetchall()
        for row in rs : 
            results.append(row)
    except MySQLdb.Error, e:
        print "QueryBySQL error: %s" %str(e)
        #logger.error("QueryBySQL error: %s" %str(e))
        return None
    finally:
        cur.close()
        conn.close()

    return results
if __name__ == "__main__":
    sql = 'select currency_code,rate from exchange'
    print HOST_66,USER_66,PASSWD_66,DB_66,sql
    QueryBySQL(HOST_66,USER_66,PASSWD_66,DB_66,sql)
