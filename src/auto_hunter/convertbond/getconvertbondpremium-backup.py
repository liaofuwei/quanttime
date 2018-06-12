# -*- coding: utf-8 -*-
from __future__ import division
import tushare as ts
import pandas as pd
import json
import time
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow
sys.path.append("E:\\pythoncoding\\auto_hunter\\")
import getSinaquotation1 as getsina
from monitorwindows import *

'''
本函数主要是规则化stock的code
从tushare获取stock基本信息存入data\stock_basic_info文件夹内
再从该文件夹读出时，code是int型，如000001这样的code，读出来是1
不符合code规则
'''
def standard_code(x):
    s=str(x)
    if len(s)==2:
        return "0000"+s
    elif len(s)==1:
        return "00000"+s
    elif len(s)==3:
        return "000"+s
    elif len(s)==4:
        return "00"+s
    elif len(s)==5:
        return "0"+s
    elif len(s)==6:
        return s
    else:
        print "code is not standard,code=%r"%(s)

'''
#从配置中读取基本信息
bcode	scode	bname	pb	convertprice	huishoujia	qiangshujia	expire
113016	601127	小康转债	  4.67	23	             16.1	    29.9	    2023/11/6
113015	601012	隆基转债	  3.02	32.35	         22.64	    42.05	    2023/11/1
'''
convertbondinfo=pd.read_csv("E:\\pythoncoding\\auto_hunter\\convertbond\\kezhuanzhai.csv",encoding="GBK")
convertbondinfo["bcode"]=convertbondinfo["bcode"].map(str) #将code 由int类型转换为str类型
convertbondinfo["scode"] = convertbondinfo["scode"].map(standard_code)

#格式化转债的code
szcodelist=[]
shcodelist=[]
for code in convertbondinfo["bcode"]:
    if "12" in str(code)[0:2]:#12开头的是深市转债
        szcodelist.append(str(code))
    else:
        shcodelist.append(str(code))


#print "shcodelist：%r"%(shcodelist)
#print "szcodelist：%r"%(szcodelist)
#根据自有接口标准化转债code
useMyszcodelist = ["sz"+x for x in szcodelist]
useMyshcodelist = ["sh"+x for x in shcodelist]


while(True):
    #获取转债的实时数据，本接口采用的自有数据采集接口
    bondrealtimeprice = getsina.getQuotation(useMyshcodelist)
    #print bondrealtimeprice.head(1)
    #基本配置信息与转债实时价格dataframe合并
    bondinfo_realprice=pd.merge(convertbondinfo,bondrealtimeprice,left_on="bcode",right_on="code")

    #正股stock code
    stock_code_series = convertbondinfo["scode"]
    stock_realtime = ts.get_realtime_quotes(stock_code_series)
    #print stock_realtime.head(1)

    #bond -- stock merge data
    bond2stock = pd.merge(bondinfo_realprice,stock_realtime,left_on="scode",right_on="code")
    bond2stock = bond2stock.set_index("bcode")
    bond2stock["one2amount"]=100/bond2stock["convertprice"]#一张转债对应的股份数量，=100/转股价
    #print bond2stock.head(1)

    bond2stock["bid_y"]=bond2stock["bid_y"].astype(float)
    bond2stock["ask_x"]=bond2stock["ask_x"].astype(float)
    #生成一个正股价对应的债券价格，该价格与实际债券价格进行比较，获得折溢价情况
    bond2stock["stock_convert2_bondprice"]=bond2stock["one2amount"]*bond2stock["bid_y"]

    #相对于卖一价的折溢价比例
    bond2stock["premium_discount"]=(bond2stock["stock_convert2_bondprice"]-bond2stock["ask_x"])\
                                   /bond2stock["stock_convert2_bondprice"]

    #选出折价的转债
    discount_bond=bond2stock[bond2stock["premium_discount"]>0]
    print discount_bond
    time.sleep(3)