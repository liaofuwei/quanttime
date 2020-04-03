#-*-coding:utf-8 -*-
__author__ = 'Administrator'

'''
该程序用于更新券商的财经数据,主要涉及pb，pe，成交等，日数据
更新目录为C:\quanttime\data\security_raw\
直接运行，程序会判断上次最后更新的时间
自动获取从最后一次更新到运行当天的数据
数据采取添加到末尾的方式，更新到csv最后
注意：环境为Python3
在anaconda下的虚拟python3环境
'''
from jqdatasdk import *
import pandas as pd
from jqdatasdk import *
import os
from datetime import datetime,timedelta
#授权
auth('13811866763',"sam155")

security_code = get_industry_stocks('J67')
print (security_code)

#券商财经数据所在目录
data_dir = "C:\\quanttime\\data\\security_raw\\"
filelist = os.listdir(data_dir)
for code in security_code:
    filepath = data_dir + code + ".csv"
    filename = code + ".csv"
    if filename not in filelist:
        continue
    security = pd.read_csv(filepath)
    #print (security)
    #获取已下载数据的最后时间日期，该日期的下两天作为更新日期的开始，下一天作为初始日期取第一次数据
    enddate = security['day'].iloc[-1]
    if '/' in enddate:
        startdate = datetime.strptime(security['day'].iloc[-1],"%Y/%m/%d") + timedelta(days=2)
        initialdate = datetime.strptime(security['day'].iloc[-1],"%Y/%m/%d") + timedelta(days=1)
        print (startdate)
    elif '-' in enddate:
        startdate = datetime.strptime(security['day'].iloc[-1],"%Y-%m-%d") + timedelta(days=2)
        initialdate = datetime.strptime(security['day'].iloc[-1],"%Y-%m-%d") + timedelta(days=1)
        print (startdate)
    else:
        print ('code: %r error' % code)

    startdata = startdate.date()
    initialdate = initialdate.date()
    today=datetime.now().date()

    q = query(valuation).filter(valuation.code == code)
    date = pd.date_range(startdata,today)
    df = get_fundamentals(q,initialdate)

    for i in date:
        tmp = get_fundamentals(q,i)
        df = pd.concat([df,tmp])

    if os.path.exists(filepath):
        df.to_csv(filepath,mode='a',header=None)
    else:
        df.to_csv(filepath)

print ("update success")