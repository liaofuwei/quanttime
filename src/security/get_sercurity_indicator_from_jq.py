__author__ = 'Administrator'
from jqdatasdk import *
import pandas as pd
from jqdatasdk import *
import os
from datetime import datetime,timedelta

#授权
auth('13811866763',"sam155")

#券商的行业编码是J67
security_code = get_industry_stocks('J67')
print(security_code)

'''
从jqdata获取券商专有数据的函数，使用，如下所示，statDate必须使用按年，其他输入参数，获得都是空
该数据频度为：年
q = query(security_indicator).filter(security_indicator.code == '600837.XSHG')
df = get_fundamentals(q, statDate="2007")
'''
dates = ['2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017']
for code in security_code:
    print("===begin to get code=%r"%(code))
    q = query(security_indicator).filter(security_indicator.code == code)
    for date in dates:
        df = get_fundamentals(q, statDate=date)
        if df.empty:
            continue
        filepath = "C:\\quanttime\\data\\security\\security_indicator_"
        filename = filepath + code + '.csv'
        if os.path.exists(filename):
            df.to_csv(filename,mode='a',header=None)
        else:
            df.to_csv(filename)
