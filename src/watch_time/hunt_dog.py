# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore, QtGui, QtWidgets
from quote_api import *
import time
from datetime import datetime
import pandas as pd


'''
不断是探测一些机会股，比如突然的大跌带来的上车机会
通过线程自动去获取行情，判断是否是机会
在目录下有csv文件hunt_dog.csv，包含需要监控的code，name以及市场
行情采用通达信的接口

该监控具有一定特异性，针对不同的stock需要进行修改

'''


class HuntDogThread(QtCore.QThread):
    REIT_Penghua_quotation = QtCore.pyqtSignal(list)
    signal_auag_stat = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(HuntDogThread, self).__init__(parent)
        self.is_running = False
        self.sleep = 60
        self.df_dog_target = pd.DataFrame()
        self.code_list = []
        self.read_hunt_dog_csv()

    def run(self):
        while self.is_running:
            # 1、先获取行情信息，可以统一一次性获取不同的需要监控的stock，然后使用不同的函数进行处理
            df_quote = get_quote_by_tdx2(self.code_list, True)
            if df_quote.empty:
                time.sleep(self.sleep)
                continue
            df_quote = df_quote.set_index("code")
            self.process_REITs_Penghua(df_quote)
            time.sleep(self.sleep)
            print("ProcessAUAGThread alive %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def auto_run(self, status):
        if status == QtCore.Qt.Checked:
            self.is_running = True
            self.start()
        elif status == QtCore.Qt.Unchecked:
            self.stop()
        else:
            print("QCheckbox states unkown, status:%d" % status)

    def stop(self):
        self.is_running = False
        print("HuntDogThread stop !")
        self.terminate()

    def read_hunt_dog_csv(self):
        '''
        读取目录下的hunt_dog.csv文件获取需要监控的stock
        :return:
        '''
        self.df_dog_target = pd.read_csv('hunt_dog.csv', encoding="gbk", index_col=["code"])
        for row in self.df_dog_target.itertuples():
            tmp_tuple = (row.market, str(row.Index))
            self.code_list.append(tmp_tuple)

    def process_REITs_Penghua(self, df_quote):
        '''
        处理鹏华前海REITs的行情（code:184801)
        :param df_quote: 获得行情信息
        :return:
        '''
        if '184801' in df_quote.index:
            # 卖一价
            ask1 = df_quote.loc['184801', ['ask1']]['ask1'] / 10
            name = u"鹏华前海REIT"# df_quote.loc['184801', ['name']]['name']
            tmp_tuple = ()
            for i in self.code_list:
                if i[1] == '184801':
                    finance_data = get_finance_by_tdx(i[0], i[1])
                    net_value = finance_data['meigujingzichan']
                    premium = (ask1 - net_value) / net_value
                    self.REIT_Penghua_quotation.emit(['184801', name, ask1, net_value, premium])

    def set_sleep_time(self, comboText):
        '''
        设置sleep时间，该时间由页面上选择，默认一分钟，由本线程默认设置
        :param comboText:页面选择的sleep状态
        #:param nsleep: 秒
        :return:
        '''
        tmp_sleep = int(comboText)
        if tmp_sleep == 1:
            self.sleep = tmp_sleep * 60
        elif tmp_sleep == 3:
            self.sleep = 3 * 60
        elif tmp_sleep == 5:
            self.sleep = 5 * 60
        elif tmp_sleep == 10:
            self.sleep = 10 *60
        else:
            self.sleep = 60

    def process_by_hand(self):
        '''
        手动刷新的处理过程
        :return:
        '''
        df_quote = get_quote_by_tdx2(self.code_list, True)
        if df_quote.empty:
            return
        df_quote = df_quote.set_index("code")
        self.process_REITs_Penghua(df_quote)




