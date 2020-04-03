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
import logging
import ui
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidget ,QTableWidgetItem
#import getSinaquotation1 as getsina

#日志setting
logger = logging.getLogger("convertBondTradeTime")
logger.setLevel(logging.DEBUG)

#日志输出到cmd窗口中
cmdwindows = logging.StreamHandler()
cmdwindows.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
cmdwindows.setFormatter(formatter)

logger.addHandler(cmdwindows)




with open("..\\..\\config.json", "r") as configfile:
    configdata = json.loads(configfile.read())
    src_root = configdata["src_dir"]

srcpath = src_root+"\\auto_hunter"
sys.path.append(srcpath)
logger.debug("srcpath:%r　",srcpath)



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
        logger.error("code is not standard,code=%r ",s)

'''
该函数处理一张转债对应的正股股数
计算方法如下：转债面值（即100）除以转股价
转股价从csv基本信息表中读取
'''
def amount(x):
    if x==0:
        logger.error("x=0 error ")
        return 0
    else:
        return 100/x

'''
convert bond column name(vsv可转债基本信息表中的列名):
bcode,scode,bname,pb,convertprice,huishoujia,qiangshujia,expire
'''
#kezhuanzhai = pd.read_csv("C:\\quanttime\\src\\convertbond\\kezhuanzhai.csv")
kezhuanzhai = pd.read_csv("kezhuanzhai.csv")#kezhuanzhai.csv与程序放到同一目录下，不再读路径
bond_codes=kezhuanzhai['bcode'].map(str) #get convertible bond code from csv
stock_codes=kezhuanzhai['scode'].map(standard_code)
conversion_price=kezhuanzhai['convertprice'] #get convertible bond conversion_price
bond2amount=100/conversion_price #get every bond(100yuan) transfer to amount of stock

kezhuanzhai['scode'] = kezhuanzhai['scode'].map(standard_code)
kezhuanzhai['bond2amount'] = kezhuanzhai['convertprice'].map(amount)

cons = ts.get_apis() #tushare connect context

while True:
    app = QApplication(sys.argv)
    MainWidow = QMainWindow()
    uiWindows = ui.Ui_MainWindow()
    uiWindows.setupUi(MainWidow)
    #获取转债实时价格
    bond_realtime_price=ts.quotes(bond_codes,conn=cons)
    bond_realtime_price=bond_realtime_price.loc[:, ['code','price','bid1','bid_vol1','ask1','ask_vol1',\
                                                   'bid2','bid_vol2','ask2','ask_vol2']]
    bond_realtime_price.index=pd.RangeIndex(len(kezhuanzhai.index))

    #获取对应的正股股价
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

    bond["stockbid2bondprice"]=bond["bond2amount"]*bond["stockbid"]*10 #将正股买一价折算成转债价格
    bond[["bcode","bid1","stockbid2bondprice","name"]]
    bond["premium"]=(bond["bid1"]-bond["stockbid2bondprice"])/bond["stockbid2bondprice"]
    #bond[["bcode","bid1","bid1_discount","name","premium"]]
    select_bond=bond[bond["premium"]>0.25]
    print select_bond[["bcode","scode","name","bid1","stockbid2bondprice","stockbid","bond2amount","premium"]]

    for i in range(7):
        value = select_bond.iloc[0,i]
        newItem = QTableWidgetItem(str(value))
        uiWindows.setItemContext(0,i,newItem)

    MainWidow.show()
    sys.exit(app.exec_())
    currenttime = time.strftime("%Y-%m-%d %X",time.localtime())
    print "alive:%r "%(currenttime)
    time.sleep(10)

if __name__ == 'main':
    app = QtWidgets.QApplication(sys.argv)
    MainWidow = QtWidgets.QMainWindow()
    ui = RealTimeTradeEvent()
    ui.setupUi(MainWidow)
    #MainWidow.show()
    sys.exit(app.exec_())