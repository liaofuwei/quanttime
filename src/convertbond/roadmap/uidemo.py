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
from PyQt5 import QtCore, QtGui, QtWidgets


#日志初始化
logger = logging.getLogger("convertBondTradeTime")
#日志输出到cmd窗口中
cmdwindows = logging.StreamHandler()
cmdwindows.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
cmdwindows.setFormatter(formatter)
logger.addHandler(cmdwindows)

class RealTimeTradeEvent(QtWidgets.QWidget):
    def __init__(self):
        self.setupUi()


    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1120, 820)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.tableWidget = QtWidgets.QTableWidget(self.centralwidget)
        self.tableWidget.setGeometry(QtCore.QRect(30, 50, 951, 161))
        self.tableWidget.setColumnCount(7)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setRowCount(10)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(5, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(6, item)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1120, 26))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        #self.getRealTimePremium()
        MainWidow.show()
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        item = self.tableWidget.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "bcode"))
        item = self.tableWidget.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "scode"))
        item = self.tableWidget.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "bname"))
        item = self.tableWidget.horizontalHeaderItem(3)
        item.setText(_translate("MainWindow", "bondbid1"))
        item = self.tableWidget.horizontalHeaderItem(4)
        item.setText(_translate("MainWindow", "stockbid2bond"))
        item = self.tableWidget.horizontalHeaderItem(5)
        item.setText(_translate("MainWindow", "stockbid1"))
        item = self.tableWidget.horizontalHeaderItem(5)
        item.setText(_translate("MainWindow", "bond2amount"))
        item = self.tableWidget.horizontalHeaderItem(6)
        item.setText(_translate("MainWindow", "premium"))



    '''
    给table中行列元素赋值
    '''
    def setItemContext(self, x, y, context):
        newItem = self.tableWidget.setItem(x, y, context)


    '''
    获取配置文件的路径
    '''
    def getConfigFilePath(self):
        with open("..\\..\\config.json", "r") as configfile:
            configdata = json.loads(configfile.read())
            src_root = configdata["src_dir"]
        configfile.close()

        srcpath = src_root+"\\auto_hunter"
        sys.path.append(srcpath)
        self.logger.debug("srcpath:%r　",srcpath)


    '''
    标准化code代码
    从csv中读取的深市代码为去掉0的，例如平安银行读取的code：0
    标准代码为：000001
    '''
    def standard_code(self,x):
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
            self.logger.error("code is not standard,code=%r ",s)


    '''
        该函数处理一张转债对应的正股股数
        计算方法如下：转债面值（即100）除以转股价
        转股价从csv基本信息表中读取
    '''
    def amount(self,x):
        if x==0:
            self.logger.error("x=0 error ")
            return 0
        else:
            return 100/x


    '''
    获取可转债的实时折溢价情况
    '''
    def getRealTimePremium(self):
        kezhuanzhai = pd.read_csv("kezhuanzhai.csv")#kezhuanzhai.csv与程序放到同一目录下，不再读路径
        bond_codes=kezhuanzhai['bcode'].map(str) #get convertible bond code from csv
        stock_codes=kezhuanzhai['scode'].map(self.standard_code)
        conversion_price=kezhuanzhai['convertprice'] #get convertible bond conversion_price
        bond2amount=100/conversion_price #get every bond(100yuan) transfer to amount of stock

        kezhuanzhai['scode'] = kezhuanzhai['scode'].map(self.standard_code)
        kezhuanzhai['bond2amount'] = kezhuanzhai['convertprice'].map(self.amount)

        #获取转债实时价格
        bond_realtime_price=ts.quotes(bond_codes,conn=self.cons)
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

        bond[["pb","convertprice","huishoujia","bond2amount","price","bid1","bid_vol1","ask1","ask_vol1",\
              "bid2","bid_vol2","ask2","ask_vol2"]]=bond[["pb","convertprice","huishoujia","bond2amount",\
                                                          "price","bid1","bid_vol1","ask1","ask_vol1",\
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

        for j in range(9):
            for i in range(7):
                value = select_bond.iloc[j, i]
                newItem = QtWidgets.QTableWidgetItem(str(value))
                self.setItemContext(j, i, newItem)

if __name__ == 'main':
    app = QtWidgets.QApplication(sys.argv)
    MainWidow = QtWidgets.QMainWindow()
    ui = RealTimeTradeEvent()
    ui.setupUi(MainWidow)
    #MainWidow.show()
    sys.exit(app.exec_())