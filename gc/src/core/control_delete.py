#!/usr/bin/python
#encoding=utf-8
"""Usage: python control_delete.py >> logs"""

import ConfigParser
import os
from sys import argv,exit
import time,datetime

from config import Config
from utils import *


etlLimit = "hour_limit"
dayLimit = "day_limit"
hourLimit = "hour_limit"
monthLimit = "month_limit"

def deligateCommomDelete(dir,dirName,params,envType,statDate,hourFlag,exInput):
    for param in params:
        if "day" in param:
            if exInput is True and hourFlag is True:
                pass
            else:
                limit = config.get(dir,dayLimit)
                if "d" in limit:
                    n = getNStr(limit)
                    rmDate = get_N_days_ago(statDate[0:8],n)
                if limitIsValide(limit):
                    cmd = "./bin/core_delete.sh" + " -d " + dirName + " -r " + rmDate + " -e " + envType + " -t day" 
                    os.system(cmd)
        elif "hour" in param:
            if exInput is True and hourFlag is False:
                pass
            else:
	            limit = config.get(dir,hourLimit)
	            if "d" in limit:
	                statDate1= statDate[0:8]
	                hh = statDate[8:]
	                n = getNStr(limit)
	                rmDate = get_N_days_ago(statDate1,n)
	                rmDate = rmDate + hh
	            if "h" in limit:
	                n = getNStr(limit)
	                rmDate = get_N_hours_ago(statDate,n)
	            if limitIsValide(limit):
	                cmd = "./bin/core_delete.sh" + " -d " + dirName + " -r " + rmDate + " -e " + envType + " -t hour" 
	                os.system(cmd)

def handleDir(dir_options,config,dir_type,statDate,flag,exInput):
    for dir in dir_options:
        if "etl" in dir and flag is True:
            dirName = config.get(dir_type, dir)
            limit = config.get(dir, etlLimit)
            if "d" in limit:
                statDate1= statDate[0:8]
                hh = statDate[8:]
                n = getNStr(limit)
                rmDate = get_N_days_ago(statDate1,n)
                rmDate = rmDate + hh
            elif "h" in limit:
                n = getNStr(limit)
                rmDate = get_N_hours_ago(statDate,n)
            if limitIsValide(limit):        
                cmd = "./bin/core_delete.sh" + " -d " + dirName + " -r " + rmDate + " -e etl" 
                os.system(cmd)
        else:
            dirName = config.get(dir_type, dir)
            params = config.options(dir)
            if "hdfs" in dir:      
                """删除集群目录"""
                deligateCommomDelete(dir,dirName,params,"hdfs",statDate,flag,exInput)
            elif "local" in dir:
                """删除本地目录"""
                deligateCommomDelete(dir,dirName,params,"local",statDate,flag,exInput)           

if __name__ == "__main__":
    
    hourFlag = False
    exInput = False
    if len(argv) == 1:
        statDate = getCurrentDate();
    elif len(argv) == 2:
        statDate = argv[1]
        exInput = True
        if is_valid_date(statDate):
            pass
        else:
            print "DATE FORMAT ERROR ! Please enter the correct date[yyyymmddhh]..."
            exit()
    else:
        print "INPUT ERROR"
        exit()
    
    if isHour(statDate):
        hourFlag = True
    ad_dir_options,config = Config("conf/ad.cfg","ad_dir").getConfigDir()
    handleDir(ad_dir_options,config,"ad_dir",statDate,hourFlag,exInput)
    mc_dir_options,config = Config("conf/mc.cfg","mc_dir").getConfigDir()
    handleDir(mc_dir_options,config,"mc_dir",statDate,hourFlag,exInput)
                             