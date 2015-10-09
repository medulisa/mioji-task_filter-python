#!/usr/bin/env python
#coding=utf8
'''
    @author:    verlink
    @date:      2014-10-22
    @desc:      wave rate collection by source
'''
import db2
import sys
import datetime
from dict_capicity import dict

def get_data_from_db():

    sql = 'select source,update_time,update_num,change_num from workload_wave where update_num >=5 and update_num < 8'
    data_5_8 = db2.QueryBySQL(sql)
    sql = 'select source,update_time,update_num,change_num from workload_wave where update_num >=8'
    data_8_above = db2.QueryBySQL(sql)
    result = data_5_8 + data_8_above
    return result

def wave_rate_hour():

    data = get_data_from_db()
    date_init = datetime.datetime(2014,10,20,13,36,00)
    date_delta = data[0]['update_time'] - date_init
    hour = date_delta.seconds/3600 + date_delta.days * 24
    dict = { }
    dict['init'] = {'change_num':0,'count':0}
    change_num = 0
    count = 0
    for i in range(len(data)):
        if dict.has_key(data[i]['source']):
            dict[data[i]['source']]['change_num'] = int(dict[data[i]['source']]['change_num']) + int(data[i]['change_num'])
            dict[data[i]['source']]['count'] = dict[data[i]['source']]['count'] + 1
        else:
            dict[data[i]['source']] = {'change_num':int(data[i]['change_num']),'count':1}
    dict_source_wave = {}
    del dict['init']
    for key in dict.keys():
        hour_wave = (hour * dict[key]['count'])/dict[key]['change_num']
        dict_source_wave[key] = hour_wave
    print dict_source_wave
    return dict_source_wave
def count_task_day():

    dict_task_day = {}
    for i in range(len(dict['items'])):
        dict_task_day[dict['items'][i]['name']] = dict['items'][i]['ub'] * 288
    return dict_task_day

def task_total_num():

    dict_result = {}
    dict_hour = count_task_day()
    dict_wave = wave_rate_hour()
    for key in dict_wave.keys():
        keyflight = key+'Flight'
        delta = dict_wave[key]/24.00
        dict_result[key] = dict_hour[keyflight]/delta
    return dict_result
def task_num_per_day():

    dict = task_total_num()
    for key in dict.keys():
        dict[key] = dict[key]/70.00
    print dict
if __name__ == "__main__":
    task_num_per_day()
 #   task_total_num()
 #   wave_rate_hour()
   # count_task_day()

    
