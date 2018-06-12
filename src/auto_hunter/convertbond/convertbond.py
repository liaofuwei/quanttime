# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import json
import time
import sys

sys.path.append("E:\\pythoncoding\\auto_hunter\\")
import getSinaquotation1 as getsina


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
convertbondinfo=pd.read_csv("kezhuanzhai.csv",index_col="bcode",encoding="GBK")
convertbondinfo.index=convertbondinfo.index.map(str) #将code 由int类型转换为str类型
convertbondinfo["scode"] = convertbondinfo["scode"].map(standard_code)
codes = convertbondinfo.index
#print "info codes:%r"%(codes)
#code中12开头的深市，其他为沪市
print convertbondinfo

#将沪深两市code分成两个list
szcodelist=[]
shcodelist=[]
for code in codes:
    if "12" in str(code)[0:2]:#12开头的是深市转债
        szcodelist.append(str(code))
    else:
        shcodelist.append(str(code))


#print "shcodelist：%r"%(shcodelist)
#print "szcodelist：%r"%(szcodelist)
#根据自有接口标准化转债code
useMyszcodelist = ["sz"+x for x in szcodelist]
useMyshcodelist = ["sh"+x for x in shcodelist]

#正股stock code
stock_code_series = convertbondinfo["scode"]
#print "stock_code:%r"%(stock_code_series)

#获取转债的实时数据，本接口采用的自有数据采集接口
realtimedata = getsina.getQuotation(useMyshcodelist)
realtimedata=realtimedata.set_index("code")
#print "==============="
#print realtimedata
#print "realtimedata.index:%r"%(realtimedata.index)
convertbond = pd.merge(convertbondinfo,realtimedata,left_index = True,right_index = True)
#print "==============="
#print convertbond
#print "==============="
convertbond = convertbond.set_index("scode")

#获取正股的实时数据，主要采用tushare接口
stock_realtime = ts.get_realtime_quotes(stock_code_series)
stock_realtime=stock_realtime.set_index("code")
stock_realtime = stock_realtime[['name','price','bid','ask','volume','amount','time']]
#print "==============="
# #print "stock_realtime: "
#print stock_realtime
#print "========="

convertbond=pd.merge(convertbond,stock_realtime,left_index = True,right_index = True)#将基本数据与实时数据进行合并
#convertbond["oneBondIncludestocks"]=100/convertbond["convertprice"]#每张债券含有股票数
print "==============="
print convertbond
print "==============="

#按照实时股价折算的债券价格=每张债券含有的股数*正股实时价格（以买一价算）
#convertbond["bondofstock_bid"]=convertbond["oneBondIncludestocks"]*convertbond["bid"]