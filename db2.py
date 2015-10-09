#!/usr/bin/env python
#coding = utf8
'''
    @author:    verlink
    @date:      2014-10-18
    @desc:      database operate
'''

import sys
import MySQLdb
from MySQLdb.cursors import DictCursor
import datetime

MYSQL_HOST = '182.254.208.203'
MYSQL_PORT = 3306
MYSQL_USER = 'crawldb'
MYSQL_PWD = 'miaoji3202'
MYSQL_DB = 'crawldb'

def getConnection():
    conn = MySQLdb.connect(host = MYSQL_HOST,user = MYSQL_USER,passwd = \
            MYSQL_PWD,db = MYSQL_DB,charset = 'utf8')
    return conn

def ExecuteSQL(sql,args = None):

    try:
        conn = getConnection()
        cur = conn.cursor()

        ret = cur.execute(sql, args)
        conn.commit()
    except:
        return False
    finally:
        conn.close()
        cur.close()
    return ret
def ExecuteSQLs(sql,args = None):

    try:
        conn = getConnection()
        cur = conn.cursor()
        ret = cur.executemany(sql, args)
        conn.commit()

    except Exception,e:
        print e
        return False
    finally:
        conn.close()
        cur.close()

    return ret
def QueryBySQL(sql,args = None,size = None):
    results = []
    try:
        conn = getConnection()
        cur = conn.cursor(cursorclass = DictCursor)
        cur.execute(sql,args)
        rs = cur.fetchall()
        for rows in rs:
            results.append(rows)
    except Exception,e:
        print e
        return None
    finally:
        cur.close()
        conn.close()
    return results

if __name__ == '__main__':
    print getConnection()
