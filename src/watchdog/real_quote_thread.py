# -*-coding:utf-8 -*-
__author__ = 'liao'

from PyQt5 import QtCore, QtGui, QtWidgets
import time
from datetime import datetime
from quote_api import *
'''
获取实时监控股票的行情信息

'''


class GetQuoteThread(QtCore.QThread):
    '''
    自动获取实时行情信息

    '''

    signal_quotation = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(GetQuoteThread, self).__init__(parent)
        self.is_running = False
        self.need_watch_codes = []

    def run(self):
        while self.is_running:
            self.get_realtime_quote()
            time.sleep(30)
            print("ProcessAUAGThread alive %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def set_run_or_stop(self, bsignal):
        """
        设置运行或者暂停
        :param bsignal: bool，有主界面发射过来的signal，true：启动线程，false：暂停线程
        :return:
        """
        if bsignal:
            self.auto_run()
        else:
            self.stop()

    def auto_run(self):
        self.is_running = True
        self.start()

    def stop(self):
        self.is_running = False
        print("GetQuoteThread stop !")
        self.terminate()

    def get_realtime_quote(self):
        codes = self.need_watch_codes
        df_quote = get_quote_by_tdx2(codes, True)
        print(df_quote)
        if not df_quote.empty:
            prices = df_quote["price"].tolist()
            self.signal_quotation.emit(prices)

    def set_quote_codes(self, code_list):
        """
        设置由主界面信号过来的，需要监控的code
        :param code_list: 主界面signal_codes过来的list
        :return:
        """
        print(code_list)
        self.need_watch_codes = code_list
