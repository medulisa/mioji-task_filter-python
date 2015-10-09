#!/usr/bin/env python
#coding:UTF-8
'''
    @author:     verlink
    @brief:      filter train
'''
import sys
import time 
import random
import redis
from base_filter import baseFilter
import db1
import ConfigParser
import datetime

class trainFilter(baseFilter):
    
    def __init__(self):
        baseFilter.__init__(self)
        self.conf = ConfigParser.ConfigParser()
        self.conf.read("./conf/trainConf.ini")

    #根据配置文件读取需要处理的任务
    def task_portal(self):
        section_list = self.conf.sections()
        step_list = []

        for item in section_list:
            if self.conf.get(item,'filter_steps') != 'close':
                step_list.append((item,self.conf.get(item,'filter_steps')))
        for task_tuple in step_list:
            task_list = task_tuple[1].split(' ')
            if item == 'close':
                continue
            self.status_reset_by_type('Rail',task_tuple[0])
            bit = 0
            for item in task_list:
                print item
                if item == '&&':
                    bit = 0
                    continue
                elif item == '||':
                    bit = 1
                    continue
                else:
                    pass
                print item,bit
                self.worker(task_tuple[0],item,bit)
    #任务分发器
    def worker(self,source,task,bit):
        
        if task == 'city_distance':
            if self.conf.get(source,'city_distance') == '':
                return
            dis = self.conf.get(source,'city_distance').split(' ')
            self.train_filter_by_distance(source,dis[0],dis[1])
        elif task == 'country_pair':
            if self.conf.get(source,'country_pair') == '':
                return
            raw = self.conf.get(source,'country_pair').split(';')
            flag = len(raw)
            for item in raw:
                dept = item[1:item.find(',')]
                dest = item[item.find(',')+1:-1]
                self.task_allocate_country_pair(source,dept,dest,len(raw),flag)
                flag = flag - 1
        elif task == 'city_pair':
            if self.conf.get(source,'city_pair') == '':
                return
            raw = self.conf.get(source,'city_pair').split(';')
            flag = len(raw)
            for item in raw:
                dept = item[1:item.find(',')]
                dept_list = dept.split(' ')
                dest = item[item.find(',')+1:-1]
                dest_list = dest.split(' ')
                self.task_allocate_city_pair(source,dept_list,dest_list,len(raw),flag,bit)
                flag = flag - 1
        elif task == 'week_day':
            if self.conf.get(source,'week_day') == '':
                return
            raw = self.conf.get(source,'week_day').split(' ')
            self.task_allocate_week_day(source,raw)
            
        elif task == 'error_code_filter':
            if self.conf.get(source,'error_code_filter') == '':
                return
            raw = self.conf.get(source,'error_code_filter').split(' ')
            self.task_allocate_error_code_filter(source,raw)
        else:
            return 
    #根据距离过滤任务，超过800km的任务直接过滤掉，并选择数据库更新
    def train_filter_by_distance(self,source,dis1,dis2):

        update_list = []
        map_base = self.city_map_info()
        dict_map = {}
        print source ,dis1,dis2
        if source == 'basic':
            sql = 'select workload_key,status from workload_train_bak'
        else:
            sql = 'select workload_key,status from workload_train_bak where source = \'%s\' '%(source)
        data = self.load_data('174','workload',sql)

        for i in range(len(map_base)):
            dict_map[map_base[i][0]] = [map_base[i][1],map_base[i][2]]
        for i in range(len(data)):
            list = data[i]['workload_key'].split('_')
            dept_id = list[0]
            dest_id = list[1]
            source = list[2]
            loc_pair_dept = dict_map[dept_id]
            loc_pari_dest = dict_map[dest_id]
            dist = self.city_distance(float(loc_pair_dept[0]),float(loc_pair_dept[1]),\
                    float(loc_pari_dest[0]),float(loc_pari_dest[1]))
            if float(dis2) >= dist >= float(dis1) :
                data[i]['status'] = 1
            else:
                data[i]['status'] = 0
            tuple = (data[i]['workload_key'],dept_id+'&'+dest_id+'&',source,0,data[i]['status'])
            update_list.append(tuple)
        sql = 'REPLACE INTO workload_train_bak(workload_key,content,source,extra,status) values (%s,%s,%s,%s,%s)'
        result = self.operate_datas('174','workload',sql,update_list)
        print 'distance filter',result


    # 过滤线上火车任务,同步于workload_train_bak
    def filter_online_train_task(self):

        sql_monitor = 'select workload_key from train_task_monitor'
        result = self.load_data('174','monitor',sql_monitor)
        
        dict_train = {}
        sql_train = 'select workload_key from workload_train_bak where status = 0'
        ret = self.load_data('174','workload',sql_train)
        for i in range(len(ret)):
            dict_train[ret[i]['workload_key']] = '1'
        delete_list = []
        for key_info in result:
            key_list = key_info['workload_key'].split('_')
            key = key_list[0] + '_' + key_list[1] + '_' + key_list[2]
            if dict_train.has_key(key):
                delete_list.append(key_info['workload_key'].encode('utf8'))
                if len(delete_list) == 200:
                    delete_str = '(' +str(delete_list)[1:len(str(delete_list))-1] + ')'
                    sql = 'delete from train_task_monitor where workload_key in %s'%(delete_str)
                    ret = self.operate_data('174','monitor',sql)
                    delete_list = []

        if len(delete_list) != 0:
            delete_str = '('+str(delete_list)[1:len(str(delete_list))-1] + ')'
            sql = 'delete from train_task_monitor where workload_key in %s'%(delete_str)
            ret = self.operate_data('174','monitor',sql)

    #根据源的不同任务不同  country_pair
    def task_allocate_country_pair(self,source,dept,dest,total,flag):
        
        print source,dept,dest
        city_info = self.get_city_info_from_db()
        country_info = self.get_country_info_from_db()
        dict = {}
        for item in country_info:
            dict[item['name_en']] = item['name']
        list_tri = []
        if dept == 'NULL' or dest == 'NULL':
            return 
        if dept == '*' and dest == '*':
            return
        elif dept == '*':
            for item in city_info:
                if item['country'] == dict[dest]:
                    list_tri.append(item['tri_code'])
            if total == flag:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 0'%(source)
                result = self.load_data('174','workload',sql)
            inser_list = []
            for item in result:
                if total == flag:
                    data = item['workload_key'].split('_')
                    if data[1] not in list_tri:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                        inser_list.append(tuple)
                else:
                    data = item['workload_key'].split('_')
                    if data[1]  in list_tri:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                        inser_list.append(tuple)
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)

        elif dest == '*':
            for item in city_info:
                if item['country'] == dict[dept]:
                    list_tri.append(item['tri_code'])
            if total == flag:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 0'%(source)
                result = self.load_data('174','workload',sql)

            inser_list = []
            for item in result:
                if total == flag:
                    data = item['workload_key'].split('_')
                    if data[0] not in list_tri:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                        inser_list.append(tuple)
                    else:
                        data = item['workload_key'].split('_')
                        if data[0] in list_tri:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                            inser_list.append(tuple)
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)
        else:
            list_tri2 = []
            for item in city_info:
                if item['country'] == dict[dept]:
                    list_tri.append(item['tri_code'])
            for item in city_info:
                if item['country'] == dict[dest]:
                    list_tri2.append(item['tri_code'])
            if total == flag:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 0'%(source)
                result = self.load_data('174','workload',sql)
            inser_list = []
            for item in result:
                if total == flag:
                    data = item['workload_key'].split('_')
                    if data[0] not in list_tri or data[1]  not in list_tri2:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                        inser_list.append(tuple)
                else:
                    data = item['workload_key'].split('_')
                    if data[0] in list_tri and data[1]  in list_tri2:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                        inser_list.append(tuple)
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)
    #
    def task_allocate_city_pair(self,source,dept_list,dest_list,total,flag,bit):

        print dept_list,dest_list
        city_info = self.get_city_info_from_db()
        if bit == 1:
            sql = 'select * from workload_train_bak where source = \'%s\' and status = 0'%(source)
            result = self.load_data('174','workload',sql)
        else:
            if total == flag:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select * from workload_train_bak where source = \'%s\' and status = 0'%(source)
                result = self.load_data('174','workload',sql)
            
        if dept_list[0] == '*' and dest_list[0] == '*':
            return
        elif dept_list[0] == '*':
            inser_list = []
            for item in result:
                if bit == 1:
                    data = item['workload_key'].split('_')
                    if data[1] in dest_list:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                        inser_list.append(tuple)
                else:
                    if total == flag:
                        data = item['workload_key'].split('_')
                        if data[1] not in dest_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                            inser_list.append(tuple)
                    else:
                        data = item['workload_key'].split('_')
                        if data[1] in dest_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                            inser_list.append(tuple)
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)
        elif dest_list[0] == '*':
            inser_list = []
            for item in result:
                data = item['workload_key'].split('_')
                if bit == 1:
                    if data[1] in dept_list:
                        tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                        inser_list.append(tuple)
                else:
                    if total == flag:
                        if data[1] not in dept_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                            inser_list.append(tuple)
                    else:
                        if data[1] in dept_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                            inser_list.append(tuple)
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)
        else:
            inser_list = []
            for item in result:
                if bit == 1:
                    data = item['workload_key'].split('_')
                    if data[0] in dept_list and data[1] in dest_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                            inser_list.append(tuple)
                else:
                    if total == flag:
                        data = item['workload_key'].split('_')
                        if data[0] not in dept_list or data[1] not in dest_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],0)
                            inser_list.append(tuple)
                    else:
                        data = item['workload_key'].split('_')
                        if data[0] in dept_list and data[1] in dest_list:
                            tuple = (item['workload_key'],item['content'],item['source'],item['extra'],1)
                            inser_list.append(tuple)
                            print inser_list
            order = 'replace into workload_train_bak (workload_key,content,source,extra,status) values(%s,%s,%s,%s,%s)'
            print self.operate_datas('174','workload',order,inser_list)
    
    def task_allocate_week_day(self,source,day_list):
        
        week_num = []
        dele = []
        for item in day_list:
            week_num.append(self.week_day_to_num(item))

        sql = 'select * from workload_static_bak where source = \'%s\' '%(source)
        result = self.load_data('174','workload',sql)
        for item in result:
            key_date = item['workload_key'].split('_')[-1]
            da = datetime.datetime(int(key_date[:4]),int(key_date[4:6]),int(key_date[6:8]))
            num = da.weekday()
            if num not in week_num:
                dele.append(item['workload_key'].encode('utf8'))
        if len(dele) == 0:
            return 
        for i in range(len(dele)):
            if i == 5000:
                print 'wuha'
                dele_list = '('+str(dele)[1:-1]+')'
                sql = 'delete from workload_static_bak where workload_key in %s'%(dele_list)
                result = self.operate_data('174','workload',sql)
        dele_list = '('+str(dele)[1:-1]+')'
        sql = 'delete from workload_static_bak where workload_key in %s'%(dele_list)
        result = self.operate_data('174','workload',sql)
        print result

    #根据error_code来进行过滤
    def task_allocate_error_code_filter(self,source,code_list):

        sql = 'select workload_key,error_code from task_error_monitor where workload_key like \'%'+'%s'%(source)+'%\''
        data = self.load_data('174','monitor',sql)
        dict_data = {}
        for item in data:
            item_list = item['workload_key'].split('_')
            key = item_list[0]+'_'+item_list[1]+'_'+item_list[2]
            try:
                if dict_data[key] != []:
                    pass
            except:
                dict_data[key] = [] 
            dict_data[key].append(item['error_code'])
        i = 0
        inser_list = []
        for key in dict_data.keys():
            for item in dict_data[key]:
                if item not in code_list:
                    break
                i = i + 1
            if i == len(dict_data[key]):
                piece = key.split('_')
                tuple = (key,piece[0]+'&'+piece[1]+'&',piece[2],0,0)
                inser_list.append(tuple)
        sql = 'replace into workload_train_bak (workload_key,content,source,extra,status) values (%s,%s,%s,%s,%s)'
        result = self.operate_datas('174','workload',sql,inser_list)
        print result



if __name__ == "__main__":

    tf = trainFilter()
    tf.task_portal()
