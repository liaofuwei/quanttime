#-*-coding:utf-8 -*-
__author__ = 'Administrator'
from futuquant import *
import pandas as pd
from datetime import datetime

'''
获取券商股的A+H历史K线
历史价格数据存储在C:\quanttime\data\security_hisdata目录下
数据来至于futu
'''

data_dir = "C:\\quanttime\\data\\security_hisdata\\"
filelist = os.listdir(data_dir)
#print(filelist)
'''
filelist:如下所示
['AH_600030.csv', 'AH_600837.csv', 'AH_600837.xlsx', 'hk_06030.csv', 'hk_06837.csv', 'SH_600030.csv', 'SH_600837.csv']
H股历史数据存储文件名为hk_code
A股历史数据存储文件名为SH_code, or SZ_code
AH合并历史数据存储文件名为AH_code,code按照A股的代码
'''


#1、获取HK券商code列表
'''
code         stock_name
HK.00111     信达国际控股
HK.00165     中国光大控股
HK.00227       第一上海
HK.00665       海通国际
HK.01375       中州证券
HK.01476       恒投证券
HK.01776       广发证券
HK.01788     国泰君安国际
HK.06030       中信证券
HK.06837       海通证券
HK.06881       中国银河
HK.06886       华泰证券
'''
quote_ctx = OpenQuoteContext(host='127.0.0.1',port=11111)
(ret,df) = quote_ctx.get_plate_stock('HK.BK1121')
print(df[["code","stock_name"]])
hkcode = df["code"]

endtime=datetime.today().date().strftime("%Y-%m-%d")
for code in hkcode:
    (ret1, dfH) = quote_ctx.get_history_kline(code,start='2006-01-01',end=endtime)
    if ret1 == RET_OK:
        pathname = data_dir + str(code) + ".csv"
        print(pathname)
        dfH.to_csv(pathname)
    else:
        print("get hisdata failed error reason:%r"%dfH)
#2、沪深券商列表
'''
     code       stock_name
SZ.000166       申万宏源
SZ.000686       东北证券
SZ.002797       第一创业
SH.600030       中信证券
SH.600061       国投资本
SH.600109       国金证券
'''
print("=====update A stock=========")
(ret,dfp) = quote_ctx.get_plate_stock('SH.BK0047')#SH.BK0047 沪深券商板块代码
Acodes = dfp["code"]
print(dfp[["code","stock_name"]])
for acode in Acodes:
    (ret2, dfA) = quote_ctx.get_history_kline(acode,start='2006-01-01',end=endtime)
    if ret2 == RET_OK:
        pathname =  data_dir + str(acode) + ".csv"
        print(pathname)
        dfA.to_csv(pathname)
    else:
        print("get hisdata failed error reason:%r"%dfA)


print("=====all update end=========")