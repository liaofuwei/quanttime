#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import pandas as pd
from six import StringIO
from datetime import datetime
import matplotlib.pyplot as plt

'''
本部分是对上面部分的细化，将当日的数据与历史统计数据对比，获得当前数据的历史位置，作为判断一个估计
'''
#对上面获取的股票基本信息进行规则化处理，使得满足joinquant的code规则
data_dir = "C:\\quanttime\\data\\security\\"
code2namefile = "C:\\quanttime\\data\\security\\sercurity2name.csv"
securitycode2name = pd.read_csv(code2namefile,index_col=['code'])
#print securitycode2name

'''
以下部分主要是处理pb数据，将所有券商的pb数据获取
'''

today=datetime.today().date()
stocks_codes = securitycode2name.index

for code in stocks_codes:
    filename=data_dir+code+'.csv'

    try:
        hisdata = pd.read_csv(filename,parse_dates=['day'], dayfirst=True, index_col='day')
    except FileNotFoundError:
        print("code: %r,this file can't find" % code)
        continue

    hisdata=hisdata.ix[:,['code','pe_ratio','pb_ratio']]
    hisdata.duplicated() #标记哪些是重复行
    hisdata=hisdata.drop_duplicates() #除去重复的行数据
    hisdata[['pe_ratio','pb_ratio']] = hisdata[['pe_ratio','pb_ratio']].convert_objects(convert_numeric=True)
    hisdata_stat = hisdata.describe()
    print ("==================")
    print (securitycode2name.ix[code,["name"]])
    print ("current pb: %r "% hisdata.iloc[-1, :])
    print ("1%%pb:%r"%(hisdata_stat["pb_ratio"].quantile(0.01)))
    print (hisdata_stat)
    print ("=================")

    if(code == "600837.XSHG"):
        hisdata[['pb_ratio']].plot()
        plt.show()