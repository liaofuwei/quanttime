#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import  quanttimeUI as quanttimeui
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
        self.ui = quanttimeui.Ui_MainWindow()
        self.ui.setupUi(MainWindow)

        self.AU_price = 0
        self.AG_price = 0
        self.future_realtime_data = sinaFutureData()


        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        self.pro = ts.pro_api()  # ts 授权

        #auth('13811866763', "sam155")  # jqdata 授权

        #启动运行的时候，获取一次金价，银价及比值数据
        self.update_AU_price()
        self.update_AG_price()
        self.update_AUAG_compare()

        #实例化获取金银比的线程
        self.run_processAUAGThread = ProcessAUAGThread(parent=None)

        #pushbutton click signal connect 处理click信号的slot
        self.ui.realtime_pushButton.clicked.connect(self.click_refresh)
        #启动/停止 金银比线程
        self.ui.radioButton.clicked.connect(self.auto_update_AUAG)
        self.ui.radioButton_2.clicked.connect(self.stop_auto_update_AUAG)

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


    def click_refresh(self):
        '''
        实时更新pushbotton对应的处理slot函数，获取金银数据后更新对应的值
        :return:
        '''
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

    def auto_update_AUAG(self):
        '''
        启动自动更新radiobutton对应的处理函数
        该函数主要启动ProcessAUAGThread线程，以及连接ProcessAUAGThread发射的signal
        :return:
        '''
        self.run_processAUAGThread.is_running = True
        self.run_processAUAGThread.start()
        self.run_processAUAGThread.auag_quotation.connect(self.process_quotation)

    def process_quotation(self, list_quotation):
        '''
        处理ProcessAUAGThread 接收的signal数据，该signal是一个list，放在list_quotation中
        :param list_quotation: signal
        :return:
        '''
        if len(list_quotation)==0:
            return
        elif len(list_quotation)==2:
            self.ui.AU_price_lineEdit.setText(str(list_quotation[0]))
            self.ui.AG_price_lineEdit.setText(str(list_quotation[1]))
        elif len(list_quotation)==3:
            self.ui.AU_price_lineEdit.setText(str(list_quotation[0]))
            self.ui.AG_price_lineEdit.setText(str(list_quotation[1]))
            self.ui.lineEdit.setText(str(list_quotation[2]))
        else:
            print("recv the signal error")

    def stop_auto_update_AUAG(self):
        '''
        停止线程ProcessAUAGThread
        :return:
        '''
        try:
            self.run_processAUAGThread.stop()
        except:
            pass



class ProcessAUAGThread(QtCore.QThread):
    '''
    自动获取金银行情数据，计算金银比线程
    行情数据取至新浪sina
    该线程将获取的金银，及比值数据通过signal(auag_quotation)发射出去，发射的signal是一个list
    list按照au,ag,au/ag比值的顺序排列
    '''

    auag_quotation = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(ProcessAUAGThread,self).__init__(parent)
        self.is_running = True
        self.future_realtime_data = sinaFutureData()

    def run(self):
        while self.is_running == True:
            signal_list = []
            au = self.future_realtime_data.get_future_price("au0")
            self.AU_price = au["AU0"]["new_price"]
            signal_list.append(self.AU_price)
            print(self.AU_price)
            ag = self.future_realtime_data.get_future_price("ag0")
            self.AG_price = ag["AG0"]["new_price"]
            signal_list.append(self.AG_price)
            if (self.AG_price > 0):
                value = self.AU_price / (self.AG_price / 1000)
                value = round(value, 2)
                signal_list.append(value)
            self.auag_quotation.emit(signal_list)
            time.sleep(20)
            print("ProcessAUAGThread alive %s"%datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def stop(self):
        self.is_running = False
        print("ProcessAUAGThread stop !")
        self.terminate()





if __name__ == "__main__":
    quanttimeMainWindows()

