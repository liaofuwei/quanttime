#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import  quanttimeUI as qui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import tushare as ts
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import logging
from jqdatasdk import *

sys.path.append('C:\\quanttime\\src\\mydefinelib\\')
from getSinaFutureQuotation import sinaFutureData

class quanttimeMainWindows(object):
    def __init__(self):
        app = QtWidgets.QApplication(sys.argv)
        MainWindow = QtWidgets.QMainWindow()
        self.ui = qui.Ui_MainWindow()
        self.ui.setupUi(MainWindow)

        self.AU_price = 0
        self.AG_price = 0
        self.future_realtime_data = sinaFutureData()


        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        self.pro = ts.pro_api()  # ts 授权

        auth('13811866763', "sam155")  # jqdata 授权

        self.update_AU_price()
        self.update_AG_price()
        self.update_AUAG_compare()

        self.stop_auto_process = False

        self.ui.realtime_pushButton.clicked.connect(self.click_refresh)
        self.ui.radioButton.clicked.connect(self.start_auto_fresh)
        self.ui.radioButton_2.clicked.connect(self.stop_auto_fresh)

        MainWindow.show()
        sys.exit(app.exec_())

    def update_AU_price(self):
        #current_date = datetime.today().date().strftime("%Y-%m-%d")
        #self.df_AU_price = get_price('AU9999.XSGE',start_date=current_date, end_date=current_date, frequency='daily', \
        #                          fields=None)
        #self.AU_price = self.df_AU_price.close[0]
        #---------------------------------------------------
        #改新浪接口
        au = self.future_realtime_data.get_future_price("au0")
        print(au)
        self.AU_price = au["AU0"]["new_price"]
        self.ui.AU_price_lineEdit.setText(str(self.AU_price))

    def update_AG_price(self):
        #current_date = datetime.today().date().strftime("%Y-%m-%d")
        #self.df_AG_price = get_price('AG9999.XSGE', start_date=current_date, end_date=current_date, frequency='daily', \
        #                          fields=None)
        #self.AG_price = self.df_AG_price.close[0]
        # ---------------------------------------------------
        # 改新浪接口
        ag = self.future_realtime_data.get_future_price("ag0")
        self.AG_price = ag["AG0"]["new_price"]
        self.ui.AG_price_lineEdit.setText(str(self.AG_price))

    def update_AUAG_compare(self):
        if(self.AG_price>0):
            value = self.AU_price / (self.AG_price / 1000)
            value = round(value, 2)
            self.ui.lineEdit.setText(str(value))

    def start_auto_fresh(self):
        self.stop_auto_process = False

        
        while not (self.stop_auto_process):
            au = self.future_realtime_data.get_future_price("au0")
            self.AU_price = au["AU0"]["new_price"]
            print(self.AU_price)
            ag = self.future_realtime_data.get_future_price("ag0")
            self.AG_price = ag["AG0"]["new_price"]
            if (self.AG_price > 0):
                value = self.AU_price / (self.AG_price / 1000)
                value = round(value, 2)
                print(value)
                self.ui.lineEdit.setText(str(value))
            time.sleep(10)
            QtWidgets.QApplication.processEvents()





    def stop_auto_fresh(self):
        self.stop_auto_process=True



    def click_refresh(self):
        #current_date = datetime.today().date().strftime("%Y-%m-%d")
        #df_AU_price = get_price('AU9999.XSGE', start_date=current_date, end_date=current_date, frequency='daily', \
        #                             fields=None)
        #df_AG_price = get_price('AG9999.XSGE', start_date=current_date, end_date=current_date, frequency='daily', \
        #                             fields=None)
        #if(df_AG_price.close[0]>0):
        #    value = df_AU_price.close[0] / (df_AG_price.close[0] / 1000)
        #    self.ui.lineEdit.setText(str(value.round(2)))
        au = self.future_realtime_data.get_future_price("au0")
        self.AU_price = au["AU0"]["new_price"]
        print(self.AU_price)
        ag = self.future_realtime_data.get_future_price("ag0")
        self.AG_price = ag["AG0"]["new_price"]
        if (self.AG_price>0):
            value = self.AU_price / (self.AG_price / 1000)
            value = round(value, 2)
            print(value)
            self.ui.lineEdit.setText(str(value))

if __name__ == "__main__":
    quanttimeMainWindows()

