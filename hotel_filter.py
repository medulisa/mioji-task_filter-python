#!/usr/bin/env python
#coding:utf8
# author: verlink

# BRIEF: 过滤酒店
import sys
import time
import random
import redis
from base_filter import baseFilter
import db1
import ConfigParser

class hotelFilter(baseFilter):
    def __init__(self):
        baseFilter.__init__(self)
        conf = ConfigParser.ConfigParser()
        conf.read("./conf/hotelConf.ini")
        self.a = 1

    #将174上的workload_hotel的status置为0
    def workload_hotel_status_set_num(self,num):

        sql1 = 'select workload_key from workload_hotel'
        result = self.load_data('174','workload',sql1)
        list = []
        for item in result:
            list.append(item['workload_key'].encode('utf8'))
        list_str = '('+str(list)[1:-1]+')'
        sql2 = 'update  workload_hotel set status = %s where workload_key in %s'%(num,list_str)
        result2 = self.operate_data('174','workload',sql2)
        print result2

    # 用于过滤workload_hotel的status
    def filter_status(self,num):

        self.workload_hotel_status_set_num(0)
        dict_66 = {}
        dict_174 = {}
        sql = 'select source,sid,status from hotel_unid_v2 where status = \'%s\''%(num)
        data_66 = self.load_data('66','onlinedb',sql)
        result_list = []
        sql = 'select workload_key,content,source,extra,status from workload_hotel'
        data_174 = self.load_data('174','workload',sql)
        for i in range(len(data_66)):
            dict_66[data_66[i]['sid']+'|'+data_66[i]['source'] +'Hotel'] = data_66[i]['status']
        for i in range(len(data_174)):
            data_raw = data_174[i]['workload_key'].split('|')
            key = data_raw[1]+'|'+data_raw[2]
            dict_174[key] = [data_174[i]['workload_key'],data_174[i]['content'],data_174[i]['source'],\
                    data_174[i]['extra'],data_174[i]['status']]
            if dict_66.has_key(key):
                dict_174[key][4] = num
            else:
                #此处会每次都更新为新值
                if data_raw[1] == 'NULL':
                    dict_174[key][4] = num
            result_list.append(dict_174[key])
        sql = 'replace into workload_hotel(workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
        result = self.operate_datas('174','workload',sql,result_list)
        print result

    # 用于过滤线上任务
    def filter_online_task(self):

        dict_room = {}
        sql = 'select workload_key from room_task_monitor '
        result = self.load_data('174','monitor',sql)

        sql = 'select workload_key from workload_hotel where status = 0'
        result_te = self.load_data('174','workload',sql)
        for i in range(len(result_te)):
            dict_hotel[result[i]['workload_key']] = '1'
        
        delete_list = []
        for key_info in result:
            key_list = key_info['workload_key'].split('|')
            key = key_list[0]+'|'+key_list[1]+'|'+key_list[2]
            if dict_hotel.has_key(key): 
                delete_list.append(key_info['workload_key'].encode('utf8'))
                if len(delete_list) == 200:
                    delete_str = '('+str(delete_list)[1:len(str(delete_list))-1] + ')'
                    print delete_str
                    sql = 'delete from room_task_monitor where workload_key in %s'%(delete_str)
                    ret = self.operate_data('174','workload',sql)
                    delete_list = []
        if len(delete_list) != 0:
            delete_str = '('+str(delete_list)[1:len(str(delete_list))-1] + ')'
            sql = 'delete from room_task_monitor where workload_key in %s'%(delete_str)
            ret = self.operate_data('174','workload',sql)

if __name__ == "__main__":

    hf = hotelFilter()



