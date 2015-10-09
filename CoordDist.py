#--coding:utf-8--

import math

EARTH_RADIUS = 6378137
PI = 3.1415927

def rad(d):
    return d * PI / 180.0

def getDist(lng1, lat1, lng2, lat2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)

    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b/2),2)))

    s = s * EARTH_RADIUS
    s = round(s * 10000) / 10000

    return s

if __name__ == '__main__':
    print getDist(116.39985, 39.914004, 117.236131, 31.853693)
    print getDist(2.309327, 48.872395, 2.3092, 48.872299)
    print getDist(6.13334, 49.60053, 6.13335, 49.60054)
    print getDist(2.51506,49.00094,2.5156,49.0031)
