#-*-coding:utf-8 -*-
__author__ = 'Administrator'
import sys
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
import tushare as ts
import time
import json
import pandas as pd

with open("..\\..\\config.json", "r") as configfile:
    configdata = json.loads(configfile.read())
    src_root = configdata["src_dir"]

srcpath=src_root+"\\auto_hunter"
sys.path.append(srcpath)
print srcpath
import getSinaquotation1 as getsina

'''
1.get convertbond realtime data from tushare first
2.use the sina data interface at tradetime for compare
正常运行阶段使用tushare的接口获取实时数据
在进入到套利阶段时，使用sina接口，主要是防止频繁调用接口被封
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

def amount(x):
    if x==0:
        print "x=0 error"
        return 0
    else:
        return 100/x

'''
convert bond column name:
bcode,scode,bname,pb,convertprice,huishoujia,qiangshujia,expire
'''
kezhuanzhai= pd.read_csv("C:\\quanttime\\src\\convertbond\\kezhuanzhai.csv")
bond_codes=kezhuanzhai['bcode'].map(str) #get convertible bond code from csv
stock_codes=kezhuanzhai['scode'].map(standard_code)
conversion_price=kezhuanzhai['convertprice'] #get convertible bond conversion_price
bond2amount=100/conversion_price #get every bond(100yuan) transfer to amount of stock
#print stock_codes
#print bond_codes
kezhuanzhai['scode'] = kezhuanzhai['scode'].map(standard_code)
kezhuanzhai['bond2amount'] = kezhuanzhai['convertprice'].map(amount)

cons = ts.get_apis() #tushare connect context

while True:
    bond_realtime_price=ts.quotes(bond_codes,conn=cons)
    bond_realtime_price=bond_realtime_price.loc[:,['code','price','bid1','bid_vol1','ask1','ask_vol1',\
                                                   'bid2','bid_vol2','ask2','ask_vol2']]
    bond_realtime_price.index=pd.RangeIndex(len(kezhuanzhai.index))

    stock_realtime_price=ts.get_realtime_quotes(stock_codes)
    stock_realtime_price=stock_realtime_price.loc[:,['code','name','price','bid','ask','b2_p','b2_v','a2_p','a2_v']]
    stock_realtime_price = stock_realtime_price.rename(columns={"price":"stockprice",\
                                                                "bid":"stockbid",\
                                                                'ask':"stockask",\
                                                                'b2_p':"stockb2_p",\
                                                                'b2_v':'stockb2_v',\
                                                                'a2_p':'stocka2_p',\
                                                                'a2_v':'stocka2_v'})
    #stock_realtime_price.head(2)
    bond=pd.merge(kezhuanzhai,bond_realtime_price,left_on="bcode",right_on="code")
    kezhuanzhai.set_index("bcode")
    bond_realtime_price.set_index("code")
    bond=pd.merge(kezhuanzhai,bond_realtime_price,left_index=True,right_index=True)

    bond[["pb","convertprice","huishoujia","bond2amount","price","bid1","bid_vol1","ask1","ask_vol1","bid2","bid_vol2",\
      "ask2","ask_vol2"]]=bond[["pb","convertprice","huishoujia","bond2amount","price","bid1","bid_vol1","ask1","ask_vol1",\
                                "bid2","bid_vol2","ask2","ask_vol2"]].convert_objects(convert_numeric=True)
    bond.set_index("scode")
    stock_realtime_price.set_index("code")
    bond=pd.merge(bond,stock_realtime_price,left_index=True,right_index=True)
    bond[["stockprice","stockbid","stockask","stockb2_p","stockb2_v","stocka2_p","stocka2_v"]] = \
        bond[["stockprice","stockbid","stockask","stockb2_p","stockb2_v","stocka2_p","stocka2_v"]].convert_objects(convert_numeric=True)
    bond["bid1_discount"]=bond["bond2amount"]*bond["stockbid"]*10
    bond[["bcode","bid1","bid1_discount","name"]]
    bond["premium"]=(bond["bid1"]-bond["bid1_discount"])/bond["bid1"]
    #bond[["bcode","bid1","bid1_discount","name","premium"]]
    select_bond=bond[bond["premium"]>0.25]
    print select_bond[["bcode","scode","name","bid1","stockbid","premium"]]
    currenttime=time.strftime("%Y-%m-%d %X",time.localtime())
    print "alive:%r "%(currenttime)
    time.sleep(10)