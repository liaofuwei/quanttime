# -*-coding:utf-8 -*-
__author__ = 'liao'

from PyQt5 import QtCore
from tdx_api import *
import time
from datetime import datetime
'''
获取实时监控股票的行情信息

'''


class GetQuoteThread(QtCore.QThread):
    """
    处理行情线程
    """
    # signal_quotation:线程中周期性获取的行情后发射的信号
    signal_quotation = QtCore.pyqtSignal(list)
    # signal_one_time_quotation:非周期性获取的（需要时更新用）行情
    signal_one_time_quotation = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(GetQuoteThread, self).__init__(parent)
        self.is_running = False
        self.need_watch_codes = []
        self.scan_freq = 30

    def run(self):
        while self.is_running:
            self.get_realtime_quote()
            time.sleep(self.scan_freq)
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
        df_quote = get_a_quote_by_tdx(codes)
        # print(df_quote)
        if not df_quote.empty:
            prices = df_quote["price"].tolist()
            self.signal_quotation.emit(prices)

    def set_quote_codes(self, code_list):
        """
        设置由主界面信号过来的，需要监控的code
        :param code_list: 主界面signal_codes过来的list
        :return:
        """
        # print(code_list)
        self.need_watch_codes = code_list

    def set_quote_scan_freq(self, sec):
        """
        设置行情扫描的周期
        :param sec: 扫描的周期频率，单位s
        :return:
        """
        tmp = int(sec)
        if tmp <= 0:
            print("行情扫描周期设置不正确，sec：%r" % sec)
            return
        self.scan_freq = tmp
        # print("scan fre:%d" % tmp)

    def get_one_time_quote(self, code_list):
        """
        设置一次性获取行情的code
        :param code_list:
        :return:
        """
        if len(code_list) == 0:
            return
        df_quote = get_a_quote_by_tdx(code_list)
        # print(df_quote)
        if not df_quote.empty:
            prices = df_quote["price"].tolist()
            self.signal_one_time_quotation.emit(prices)

