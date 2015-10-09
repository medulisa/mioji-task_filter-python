#!/usr/bin/sh
array=(wego vueling elong ryanair ufeifan ctrip jijitong tongcheng feiquanqiu ceair smartfares csair easyjet lcair expedia airberlin tripsta huifee ebookers mango airtickets24 cheapoair priceline airkx kopu)
for i in ${array[@]}:
do
    python flight_filter.py $i
    sleep 60
done
