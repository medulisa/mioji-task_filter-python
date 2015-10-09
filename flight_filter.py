#!/bin/python
#coding:UTF-8
'''
    @author:    verlink
    @desc:      flight filter
'''
import sys
import time
import random
import datetime
import redis
from base_filter import baseFilter
import db1
from wave_rate import task_num_per_day
import ConfigParser

class flightFilter(baseFilter):

    # 初始化
    def __init__(self):
        baseFilter.__init__(self)
        self.conf = ConfigParser.ConfigParser()
        self.conf.read("./conf/flightConf.ini")


    #根据配置文件读取需要处理的任务  
    def task_portal(self):
        
        section_list = self.conf.sections()
        step_list = []
        result = 0

        for item in section_list:
            if self.conf.get(item,'filter_steps') != 'close':
                step_list.append((item,self.conf.get(item,'filter_steps')))
        for task_tuple in step_list:
            task_list = task_tuple[1].split(' ')
            bit = 'None'
            if item == 'close':
                continue
            print 'hi'
            self.status_reset_by_type('Flight',task_tuple[0])
            bit = '0'
            for item in task_list:
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
        
        if task == 'country_pair':
            if self.conf.get(source,'country_pair') == '':
                return
            raw = self.conf.get(source,'country_pair').split(';')
            print raw
            flag = len(raw)
            for item in raw:
                dept = item[1:item.find(',')]
                dest = item[item.find(',')+1:-1]
                print len(raw),flag
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
                print dept_list,dest_list
                self.task_allocate_city_pair(source,dept_list,dest_list,len(raw),flag,bit)
                flag = flag - 1
        elif task == 'airport_pair':
            if self.conf.get(source,'airport_pair') == '':
                return
            raw = self.conf.get(source,'airport_pair').split(';')
            flag = len(raw)
            for item in raw:
                dept = item[1:item.find(',')]
                dept_list = dept.split(' ')
                dest = item[item.find(',')+1:-1]
                dest_list = dest.split(' ')
                print dept_list,dest_list
                self.task_allocate_airport_pair(source,dept_list,dest_list,len(raw),flag,bit)
                flag = flag - 1
        elif task == 'week_day':
            if self.conf.get(source,'week_day') == '':
                return 
            raw = self.conf.get(source,'week_day').split(' ')
            self.task_allocate_week_day(source,raw,bit)
            
        elif task == 'error_code_filter':
            if self.conf.get(source,'error_code_filter') == '':
                return 
            raw = self.conf.get(source,'error_code_filter').split(' ')
            self.task_allocate_error_code_filter(source,raw,bit)
        else:
            return 
    # 更新现有任务，实现过滤,逐条更新,有待改进
    def update_tasks(self, args, source):

        sourceflight = source + 'Flight'
        for i in range(len(args)):
            sql = "update workload_flight_pair set %s = %s where dept_airport = '%s' and dest_airport = '%s' "%(sourceflight,args[i][2],args[i][0],args[i][1])
            self.operate_data('203','crawldb',sql)
        print 'update success'

    # 从数据库读取现有任务
    def airport_city(self):

        dict_airport = {}
        dict_city = {}
        dict_final = {}
        sql = 'select iata_code, city_id from airport where city_id != \'NULL\''
        result_airport = self.load_data('66','onlinedb',sql)
        sql = 'select id,tri_code from city where tri_code != \'NULL\''
        result_city = self.load_data('66','onlinedb',sql)
        for i in range(len(result_airport)):
            dict_airport[result_airport[i]['iata_code']] = result_airport[i]['city_id']
        for j in range(len(result_city)):
            dict_city[result_city[j]['id']] = result_city[j]['tri_code']
        for key in dict_airport.keys():
            try:
                if dict_city.has_key(dict_airport[key]):
                    dict_final[key] = dict_city[dict_airport[key]]
            except Exception,e:
                pass
        return dict_final

    def update_task_by_source(self,dept_id,dest_id,source):

        for i in source:
            sourceflight = i+ 'Flight'
            sql = "update workload_flight_pair set %s = %s where dept_airport = '%s' and dest_airport = '%s'"%(sourceflight,1,dept_id,dest_id)
            self.operate_data('203','crawldb',sql)
        print 'update success'

    def count_sql_task_by_source(self,source):

        sourceflight = source+'Flight'
        sql = 'select count(*) from workload_flight_pair where %s = 1'%(sourceflight)
        result = self.load_data('203','crawldb',sql)
        print result
        return result

    #   根据源过滤航线 
    def filter_source_by_airline(self, source):

        print 'source = '+source
        match = '*'+source+'*'
        dict_redis = self.read_redis_keys(match)
        dict_mysql = {}
        sourceflight = source + 'Flight'
        sql = 'SELECT dept_airport,dest_airport,'+ sourceflight +' FROM workload_flight_pair'
        list = self.load_data('203','crawldb',sql)
        for i in range(len(list)):
            str = list[i]['dept_airport'] + '_' + list[i]['dest_airport']
            dict_mysql[str] = list[i][sourceflight]
        for i in dict_mysql.keys():
            if dict_redis.has_key(i):
                dict_mysql[i] = 1
            else:
                dict_mysql[i] = 0
        update_list = []
        for key in dict_mysql.keys():
            list = key.split('_')
            tuple = (list[0],list[1],dict_mysql[key])
            update_list.append(tuple)
        self.update_tasks(update_list,source)
        print 'update success'

    # 根据价格过滤一个航线
    def filter_airline_by_price(self, dept_airport, dest_airport):

        dict = self.airport_city()
        try:
            dept_id = dict[dept_airport]
            dest_id = dict[dest_airport]
        except Exception,e:
            print str(e)+'can not find the airport_city pair'
            return 
        print dept_id,dest_id
        source_num = self.analysis_source_num(dept_id,dest_id)
        sorted_source_list = self.read_redis_price(dept_id,dest_id)
        list = sorted_source_list[:source_num]
        list_result = []
        for i in range(len(list)):
            list_result.append(list[i][0])
        print list_result
        return list_result

    # 返回price数据，dict = (key:price_all_source)根据match读取并计算各key的平均价格
    def read_redis_price(self, dept_id,dest_id):
        dict_rate = {}
        sql = 'select currency_code,rate from exchange'
        result = self.load_data('66','onlinedb',sql)
        for i in range(len(result)):
            dict_rate[result[i]['currency_code']] = result[i]['rate']
        dict_key_low = {}
        match ='*'+ dept_id + '_' + dest_id + '*'
        list = self.redis.keys(match)
        #单key最低价
        source_dict = {}
        for key in list:
            price_lowest = 999999
            value = self.redis.get(key)
            list = value.split('\n')
            try:
                for i in range(1,len(list)):
                    data = list[i].split('\t')
                    if float(data[2]) == -1:
                        data[2] = 0
                    price_raw = float(data[0]) + float(data[1]) + float(data[2])
                    exchange = dict_rate[data[3]]

                    price = price_raw * exchange
                    if price < price_lowest:
                        price_lowest = price
                dict_key_low[key] = price_lowest
            except Exception,e:
                continue
        #单source平均价
        source_dict['init'] = {'price_total':0,'count':0}
        for key in dict_key_low.keys():
            if source_dict.has_key(key.split('_')[4]):
                source_dict[key.split('_')[4]]['price_total'] = source_dict[key.split('_')[4]]['price_total'] + dict_key_low[key]
                source_dict[key.split('_')[4]]['count'] = source_dict[key.split('_')[4]]['count'] + 1
            else:
                source_dict[key.split('_')[4]] = {'price_total':dict_key_low[key],'count':1}
        dict_avg = {}
        del source_dict['init']
        for key in source_dict.keys():
            dict_avg[key] = source_dict[key]['price_total']/source_dict[key]['count']
        result_list = self.data_sort(dict_avg)
        return result_list
    def city_dist(self,dept_id,dest_id):

        city = dept_id + '_' + dest_id
        sql = 'select distance from city_dist where city = \'%s\''%(city)
        result = self.load_data('203','crawldb',sql)
        return result

    # 根据热门程度和距离确定source_num 
    def analysis_source_num(self,dept_id,dest_id):
        try:
            grade1 = self.city_grade(dept_id)
            grade2 = self.city_grade(dest_id)
            dist = self.city_dist(dept_id,dest_id)[0]['distance']
            grade = 6 - (grade1[0]['grade'] + grade2[0]['grade'])/2
        except Exception,e:
            print str(e)+'your input is illegal'
            return 1

        if dist < 500:
            dist_weight = 1
        elif 500 <= dist < 1000:
            dist_weight = 2
        elif 1000 <= dist < 2000:
            dist_weight = 3
        elif 2000 <= dist < 3000:
            dist_weight = 4
        else:
            dist_weight = 5
        TOTAL_SOURCE = self.conf.get("flight","total_source_num")
        weight = grade * 0.5 + dist_weight * 0.5
        source_num = int(int(TOTAL_SOURCE) / 5.0 * weight)
        print source_num
        return source_num

    def task_allocate_country_pair(self,source,dept,dest,total,flag):

        city_info = self.get_airport_info_from_db()
        country_info = self.get_country_info_from_db()
        dict = {}
        for item in country_info:
            dict[item['name_en']] = item['name']
        list_tri = []
        if dept == 'NULL' or dest == 'NULL':
            return 
        if dept == '*' and dest == '*':
            pass
        elif dept == '*':
            for item in city_info:
                if item['country'] == dict[dest]:
                    list_tri.append(item['iata_code'])
            if total == flag:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
                result = self.load_data('174','workload',sql)
            inser_list = []
            for item in result:
                if total == flag:
                    if item['dest_airport'] not in list_tri:
                        sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if item['dest_airport'] in list_tri:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)

        elif dest == '*':
            for item in city_info:
                if item['country'] == dict[dept]:
                    list_tri.append(item['iata_code'])
            if total == flag:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select dept_airport,dest_airportfrom workload_flight_pair where %s = 0'%(source)
                result = self.load_data('174','workload',sql)
            inser_list = []
            for item in result:
                if total == flag:
                    if item['dept_airport'] not in list_tri:
                        sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if item['dept_airport'] in list_tri:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)

        else:
            list_tri2 = []
            for item in city_info:
                if item['country'] == dict[dept]:
                    list_tri.append(item['iata_code'])
            for item in city_info:
                if item['country'] == dict[dest]:
                    list_tri2.append(item['iata_code'])
            if total == flag:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
                result = self.load_data('174','workload',sql)
            inser_list = []
            for item in result:
                if total == flag:
                    if item['dept_airport'] not in list_tri or item['dest_airport'] not in list_tri2:
                        sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if item['dept_airport'] in list_tri and item['dest_airport']  in list_tri2:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)


    def task_allocate_city_pair(self,source,dept_list,dest_list,total,flag,signal):
        
        city_info = self.get_airport_info_from_db()
        city_en = self.get_city_info_from_db()
        dict_city_en = {}
        for item in city_en:
            dict_city_en[item['name']] = item['tri_code']
        dict_info = {}
        for item in city_info:
            dict_info[item['iata_code']] = item['city']
        if signal == 1:
            print '||'
            sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
            result = self.load_data('174','workload',sql)
        else:
            print '&&1'
            if total == flag:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
                result = self.load_data('174','workload',sql)

        if dept_list[0] == '*' and dest_list[0] == '*':
            return
        elif dept_list[0] == '*':
            inser_list = []
            for item in result:
                if signal == 1:
                    if dict_city_en[dict_info[item['dest_airport']]] in dest_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if dict_city_en[dict_info[item['dest_airport']]] not in dest_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if dict_city_en[dict_info[item['dest_airport']]] in dest_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
        elif dest_list[0] == '*':
            inser_list = []
            for item in result:
                if signal == 1:
                    if dict_city_en[dict_info[item['dept_airport']]] in dept_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if dict_city_en[dict_info[item['dept_airport']]] not in dept_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if dict_city_en[dict_info[item['dept_airport']]] in dept_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
        else:
            inser_list = []
            for item in result:
                if signal == 1:
                    if dict_city_en[dict_info[item['dept_airport']]] in dept_list and dict_city_en[dict_info[item['dest_airport']]] in dest_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if dict_city_en[dict_info[item['dept_airport']]] not in dept_list or dict_city_en[dict_info[item['dest_airport']]] not in dest_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if dict_city_en[dict_info[item['dept_airport']]] in dept_list and dict_city_en[dict_info[item['dest_airport']]] in dest_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)

    def task_allocate_airport_pair(self,source,dept_list,dest_list,total,flag,signal):
        
        city_info = self.get_airport_info_from_db()
        if signal == 1:
            print '||'
            sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
            result = self.load_data('174','workload',sql)
        else:
            print '&&1'
            if total == flag:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 1'%(source)
                result = self.load_data('174','workload',sql)
            else:
                sql = 'select dept_airport,dest_airport from workload_flight_pair where %s = 0'%(source)
                result = self.load_data('174','workload',sql)

        if dept_list[0] == '*' and dest_list[0] == '*':
            return
        elif dept_list[0] == '*':
            inser_list = []
            for item in result:
                if signal == 1:
                    if item['dest_airport'] in dest_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if item['dest_airport'] not in dest_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if item['dest_airport'] in dest_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
        elif dest_list[0] == '*':
            inser_list = []
            for item in result:
                if signal == 1:
                    if item['dept_airport'] in dept_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if item['dept_airport'] not in dept_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if item['dept_airport'] in dept_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
        else:
            inser_list = []
            for item in result:
                if signal == 1:
                    if item['dept_airport'] in dept_list and item['dest_airport'] in dest_list:
                        sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                        self.operate_data('174','workload',sql)
                else:
                    if total == flag:
                        if item['dept_airport'] not in dept_list or item['dest_airport'] not in dest_list:
                            sql = 'update workload_flight_pair set %s = 0 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)
                    else:
                        if item['dept_airport'] in dept_list and item['dest_airport'] in dest_list:
                            sql = 'update workload_flight_pair set %s = 1 where dept_airport = \'%s\' and dest_airport = \'%s\''%(source,item['dept_airport'],item['dest_airport'])
                            self.operate_data('174','workload',sql)

    def task_allocate_week_day(self,source,day_list,bit):

        week_num = []
        dele = []
        for item in day_list:
            week_num.append(self.week_day_to_num(item))

        sql = 'select * from workload_static_bak where source = \'%s\''%(source)
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
            print i
            if i == 5000:
                dele_list = '('+str(dele)[1:-1]+')'
                sql = 'delete from workload_static_bak where workload_key in %s'%(dele_list)
                result = self.operate_data('174','workload',sql)
        dele_list = '('+str(dele)[1:-1]+')' 
        sql = 'delete from workload_static_bak where workload_key in %s'%(dele_list)
        result = self.operate_data('174','workload',sql)
        print result

    def task_allocate_error_code_filter(self,source,code_list,bit):

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

if __name__ == '__main__':

    ff = flightFilter()
    ff.task_portal()
