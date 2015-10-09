#!/bin/python
#coding=UTF-8

# BRIEF: 过滤基类

import math
import sys
import time
import random
import redis
import db1
import datetime

HOST_174="10.66.115.222"
USER_174="reader"
PASSWD_174="miaoji1109"
DB_174="workload"

HOST_203 = "182.254.208.203"
USER_203 = "crawldb"
PASSWD_203 = "miaoji3202"
DB_203 = "crawldb"

HOST_66 = "10.66.115.212"
USER_66 = "reader"
PASSWD_66 = "miaoji1109"
DB_66 = "onlinedb"

class baseFilter:

    def __init__(self):
        self.redis = redis.Redis(host='10.131.184.134', port=6379, db=0)

    # 根据match读取redis key,返回dict，只包含出发到达机场
    def read_redis_keys(self, match = None):

        dict = {}
        for key in self.redis.keys(match):
            key_dict = key[7:14]
            dict[key_dict] = 0
        print 'get redis success'
        return dict

    # 排序
    def data_sort(self,dict):

        return sorted(dict.items(), lambda x, y: cmp(x[1], y[1]))
    
    def week_day_to_num(self,week_day):

        dict = {'Sun':0,'Mon':1,'Tues':2,'Wed':3,'Thur':4,'Fri':5,'Sat':6}
        if len(week_day) == 0:
            return []
        return dict[week_day]

    # 读取必需的数据
    def load_data(self,host_postfix,db,sql):

        if host_postfix == '174':
            result = db1.QueryBySQL(HOST_174,USER_174,PASSWD_174,db,sql)
            return result

        elif host_postfix == '66':
            result = db1.QueryBySQL(HOST_66,USER_66,PASSWD_66,db,sql)
            return result

        elif host_postfix == '203':
            result = db1.QueryBySQL(HOST_203,USER_203,PASSWD_203,db,sql)
            return result

    # 执行数据操作，针对单条sql语句
    def operate_data(self,host_postfix,db,sql):

        if host_postfix == '174':
            result = db1.ExecuteSQL(HOST_174,USER_174,PASSWD_174,db,sql)
            return result
        elif host_postfix == '66':
            result = db1.ExecuteSQL(HOST_66,USER_66,PASSWD_66,db,sql)
            return result
        elif host_postfix == '203':
            result = db1.ExecuteSQL(HOST_203,USER_203,PASSWD_203,db,sql)
            return result

    # 执行数据操作，针对多条sql语句
    def operate_datas(self,host_postfix,db,sql,args):
        if host_postfix == '174':
            result = db1.ExecuteSQLs(HOST_174,USER_174,PASSWD_174,db,sql,args)
            return result
        elif host_postfix == '66':
            result = db1.ExecuteSQLs(HOST_66,USER_66,PASSWD_66,db,sql,args)
            return result
        elif host_postfix == '203':
            result = db1.ExecuteSQLs(HOST_203,USER_203,PASSWD_203,db,sql,args)
            return result

    # 计算两城市之间的距离
    def city_distance(self,lng1,lat1,lng2,lat2):
        EARTH_RADIUS = 6378137
        PI = 3.1415927
        def rad(d):
            return d * PI /180.0
        def getDist(lng1, lat1, lng2, lat2):
            radLat1 = rad(lat1)
            radLat2 = rad(lat2)
            a = radLat1 - radLat2
            b = rad(lng1) - rad(lng2)
            s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b/2),2)))
            s = s * EARTH_RADIUS
            s = round(s * 10000) / 10000
            #此处处理为公里单位
            return s/1000
        dist = getDist(lng1,lat1,lng2,lat2)
        return dist

    # 获取城市的经纬度信息
    def city_map_info(self):
        list = []
        sql = 'SELECT tri_code,map_info FROM city where map_info != \'NULL\' and tri_code != \'NULL\''
        result = self.load_data('66','onlinedb',sql)
        for i in range(len(result)):
            tuple = (result[i]['tri_code'],result[i]['map_info'].split(',')[0],result[i]['map_info'].split(',')[1])
            list.append(tuple)
        return list

    def city_distance_all(self):
        list = []
        dict = {}
        city_list = self.city_map_info()
        for i in range(len(city_list)):
            for j in range(len(city_list)):
                if i != j:
                    dist = self.city_distance(float(city_list[i][1]),float(city_list[i][2]),float(city_list[j][1]),float(city_list[j][2]))
                    tuple = (city_list[i][0]+'_'+city_list[j][0],dist)
                    list.append(tuple)
        sql = 'insert into city_dist (city,distance) values(%s,%s)'
        result = self.operate_datas('203','crawldb',sql,list)
        print result

    def city_grade(self,city):

        sql = 'select grade from city where tri_code = \'%s\''%(city)
        result = self.load_data('66','onlinedb',sql)
        return result

    # 根据源统计数据库中任务数
    def count_task_by_source(self,source):

        sourceflight = source+'Flight'
        sql = 'select count(*) from workload_flight_pair where %s = 1'%(sourceflight)
        result = self.load_data('203','crawldb',sql)
        return result

    # 根据可变规则过滤不符合规则的workload_key
    def workload_key_format(self,table,db,signal,key_num):

        sql = 'select workload_key from %s'%(table)
        delete_list = []
        result = self.load_data('174',db,sql)
        for info in result:
            key = info['workload_key'].encode('utf8')
            key_list = key.split(signal)
            for key_pieces in key_list:
                if key_pieces == '':
                    delete_list.append(key)
                    break
            if len(key_list) != key_num and key not in delete_list:
                delete_list.append(key)

            if len(delete_list) == 200:
                delete_str = '('+str(delete_list)[1:len(str(delete_list))-1] + ')'
                print delete_str
                sql = 'delete from %s where workload_key in %s'%(table,delete_str)
                ret = self.operate_data('174',db,sql)
                print ret
                delete_list = []

    #将type(如火车)的(source)源的所有任务的状态全部set为1
    def status_reset_by_type(self,type,source):
        
        if type == 'Rail':
            sql = 'select workload_key,content,source,extra,status from workload_train_bak where status = 0 and source = \'%s\''%(source)
            result = self.load_data('174','workload',sql)
            insert_list = []
            for item in result:
                tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                insert_list.append(tuple)
            sql = 'replace into workload_train_bak (workload_key,content,source,extra,status) values (%s,%s,%s,%s,%s)'
            result = self.operate_datas('174','workload',sql,insert_list)

        elif type == 'Flight':
            sql = 'select dept_airport,dest_airport,%s from workload_flight_pair where %s = 0 '%(source,source)
            result = self.load_data('174','workload',sql)
            for item in result:
                sql = "update workload_flight_pair set %s = %s where dept_airport = '%s' and dest_airport = '%s' "%(source,1,item['dept_airport'],item['dest_airport'])
                self.operate_data('174','workload',sql)
               
        elif type == 'Hotel':
            pass
        else:
            return
    def get_city_info_from_db(self):
        
        sql = 'select name, name_en,tri_code,country from city '
        result = self.load_data('66','onlinedb',sql)
        return result
    def get_airport_info_from_db(self):
        
        sql = 'select iata_code, name,city,country from airport'
        result = self.load_data('66','onlinedb',sql)
        return result
    def get_country_info_from_db(self):

        sql = 'select name,name_en from country '
        result  = self.load_data('66','onlinedb',sql)
        return result

    # 读取各源任务目标
    def read_target(self,target_file):
        pass

    # 读取人工准则key
    def read_rule_keys(self, rule_file):
        pass

    # 读取各源过滤目标
    def read_target(self, target_file):
        pass

if __name__ == '__main__':
    bf = baseFilter()
    sql = 'select content from workload_static where source = \'hoteltravelHotel\''
    print bf.load_data('174','workload',sql)
    #bf.workload_key_format('train_task_monitor','monitor','_',4)
