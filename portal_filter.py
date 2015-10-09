#!/usr/bin/env python
#coding:utf8
import getopt
import sys
import flight_filter
import hotel_filter
import train_filter
def optSelect():
    list_opt = ['hotel','hotel_status_uuid=','hotel_status_room',\
            'train','train_distance_filter','train_status_monitor',\
            'flight','flight_filter_price=dept_airport&dest_airport','flight_filter_airline=source']
    try:
        opts ,args = getopt.getopt(sys.argv[1:],'h',list_opt)

        for k,v in opts:
            if k == '-h':
                print '\nwarning: 过滤会直接影响数据库，请谨慎操作\n'
                print '--flight <portal of flight filter>'
                print '--train <portal of train filter>'
                print '--hotel <portal of hotel filter>\n'
            if k == '--hotel':
                print '\n'
                print '--hotel_status_uuid=num          <数据库:66-174 status从66数据库hotel_uuid更新到174数据库workload_hotel,默认为174清零后重置>\n'
                print '--hotel_status_room_monitor  <数据库:174 status从workload_hotel更新到room_task_monitor>\n'
            if k == '--hotel_status_uuid':
                hf = hotel_filter.hotelFilter()
                hf.workload_hotel_status_set_num(0)
                hf.filter_status(v)
            if k == '--hotel_status_room':
                hf = hotel_filter.hotelFilter()
                hf.filter_online_task()
            if k == '--train':
                print '\n'
                print '--train_distance_filter     <数据库:174 根据距离过滤任务，并更新到workload_train 距离参数在配置文件中更改>\n'
                print '--train_status_monitor      <数据库:174 根据status从workload_train更新到train_task_monitor>\n'
            if k == '--train_distance_filter':
                tf = train_filter.trainFilter()
                tf.train_filter_by_distance()
            if k == '--train_status_monitor':
                tf = train_filter.trainFilter()
                tf.filter_online_train_task()
            if k == '--flight':
                print '\n'
                print '--flight_filter_airline=source                    <根据redis里的key是否有该源的该航线，在66数据库中通过status过滤掉该源该条航线>\n'
                print '--flight_filter_price=dept_airport&dest_airport   <算出两机场之间价格最低的k个源(k通过热门程度和距离确定)，并通过status更新至任务>\n'
            if k == '--flight_filter_price':
                list = v.split('&')
                ff = flight_filter.flightFilter()
                ff.filter_airline_by_price(list[0],list[1])
            if k == '--flight_filter_airline=source':
                ff = flight_filter.flightFilter()
                ff.filter_all_airline_by_all_source(v)
            else:
                return
    except getopt.GetoptError, e:
        print 'Your order isn\'t in the list , try again!\n '
if __name__ == '__main__':

    optSelect()



