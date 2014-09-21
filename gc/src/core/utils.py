#!/usr/bin/python
#encoding=utf-8

import time,datetime

def getCurrentDate():
    """ 获取当前实时时间. """
    current = time.time()
    ndt = time.localtime(current)
    return time.strftime("%Y%m%d%H", ndt)

def is_valid_date(date_str):
    """ 验证时间是否为天时间. """
    try:
        time.strptime(date_str, "%Y%m%d%H")
        return True
    except:
        return False

def get_this_month_str():
    today = datetime.date.today()
    return today.strftime('%Y%m')

def get_N_days_ago(dateStr,n):
    """ 获取前n天的日期"""
    imeArray = time.strptime(dateStr,"%Y%m%d")
    bdt = time.mktime(imeArray)
    bdt -= int(n)*24*60*60
    ndt = datetime.datetime.fromtimestamp(bdt)
    return ndt.strftime('%Y%m%d')

def get_N_days_ago_with_hour(dateStr,n):
    """ 获取前n天的日期"""
    imeArray = time.strptime(dateStr,"%Y%m%d%H")
    bdt = time.mktime(imeArray)
    bdt -= int(n)*24*60*60
    ndt = datetime.datetime.fromtimestamp(bdt)
    return ndt.strftime('%Y%m%d%H')

def get_N_hours_ago(statDate,n):
    """ 获取前n小时的数据"""
    imeArray = time.strptime(statDate, "%Y%m%d%H")
    current = time.mktime(imeArray)
    current -= int(n)*60*60
    ndt = time.localtime(current)
    return time.strftime("%Y%m%d%H", ndt)

def getNStr(limit):
    """ 去除设置的删除时间的单位（d,h）"""
    return limit[0:len(limit)-1]

def limitIsValide(limit):
    if limit is not None or "d" in limit or "h" in limit:
        return True
    else:
        return False

def isHour(statDate):
    if len(statDate) == 10:
        return True
    return False
