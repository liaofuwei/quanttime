# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import json



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


#从目录中读取配置路径，这主要是解决不同计算机的路径问题
filePath=file('./filepath.json','r')
pathcontext = json.load(filePath)
#print pathcontext

#通过stock_basic_info.csv表获取code-name的关系
stockbasicinfoPath=str(pathcontext['filepath'])+"\stock_basic_info.csv"


#print stockbasicinfoPath
stock_basic_info=pd.read_csv(stockbasicinfoPath,index_col=['code'])
stock_basic_info.index=stock_basic_info.index.map(standard_code)

#code-name的对应关系。dataFrame格式，通过code获取名称
only_code_name = stock_basic_info.ix[:,["name"]]

securityCode=file('./fenjifundb.json','r')
codeContext=json.load(securityCode)
codeList = codeContext.split(",") #将读取的内容转换为list，如['150020', '601318']

realPrice=ts.get_realtime_quotes(codeList[0])
#print realPrice



