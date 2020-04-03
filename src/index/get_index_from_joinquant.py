#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
from datetime import datetime, timedelta

'''
readme：
！！！该文件已移到index_maintenance.py中维护，即时不再维护！！！
1、各种指数历史数据的维护
2、存储目录：C:\quanttime\data\index
3、命名按照指数的code命名
4、程序能够自动识别最后更新的日期，按照最后的日期自动更新
'''

auth('13811866763',"sam155") #jqdata 授权
index_dir = "C:\\quanttime\\data\\index\\"

#获取所有的指数标的
index_security = get_all_securities(types=['index'], date=None)
'''
             display_name	name	start_date	    end_date	    type
000001.XSHG	 上证指数	        SZZS	1991-07-15	    2200-01-01	    index
000002.XSHG	 A股指数	        AGZS	1992-02-21	    2200-01-01	     index
'''

today = datetime.today().date().strftime("%Y-%m-%d")
for index_name in index_security.index:
    start_time = index_security.loc[index_name]["start_date"].date()
    today = datetime.today().date().strftime("%Y-%m-%d")
    update_file_name = index_dir + str(index_name) + ".csv"
    if os.path.exists(update_file_name):
        data = pd.read_csv(update_file_name, index_col=["date"])
        d1 = datetime.strptime(data.index[-1], '%Y-%m-%d')
        d2 = datetime.strptime(data.index[0], '%Y-%m-%d')
        delta = d1 - d2
        delta2 = timedelta(days=1)#读取的最后日期往后推一天，作为更新的起始日期
        #判断存储在csv中的日期是顺序还是逆序
        if delta.days > 0:
            d1 = d1 + delta2
            start_time = d1.strftime('%Y-%m-%d')
        else:
            d2 = d2 + delta2
            start_time = d2.strftime('%Y-%m-%d')
        tmp = get_price(index_name, start_date=start_time, end_date=today, frequency='daily', fields=None)
        tmp.to_csv(update_file_name,mode='a',header=None)
        print("update index:%r successful"%str(index_name))
    else:
        tmp = get_price(index_name, start_date=start_time, end_date=today, frequency='daily', fields=None)
        tmp.index.name = "date"
        tmp.to_csv(update_file_name)
        print("first time get index:%r successful"%str(index_name))