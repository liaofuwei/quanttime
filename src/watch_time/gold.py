# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore, QtGui, QtWidgets
from quote_api import *
import time
from datetime import datetime


'''
金银比分析
实时行情获取至新浪，封装于quote_api.py
get_auag_quote_by_sina(code)

'''


class ProcessAUAGThread(QtCore.QThread):
    '''
    自动获取金银行情数据，计算金银比线程
    行情数据取至新浪sina
    该线程将获取的金银，及比值数据通过signal(auag_quotation)发射出去，发射的signal是一个list
    list按照au,ag,au/ag比值的顺序排列
    '''

    auag_quotation = QtCore.pyqtSignal(list)
    signal_auag_stat = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(ProcessAUAGThread, self).__init__(parent)
        self.is_running = False

    def run(self):
        while self.is_running:
            self.emit_auag_ratio()
            time.sleep(30)
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
        print("ProcessAUAGThread stop !")
        self.terminate()

    def get_auag_ratio(self):
        '''
        单次获取金银比信息，并emit
        :return:
        '''
        self.emit_auag_ratio()

    def emit_auag_ratio(self):
        '''
        获取金银实时价格，并计算金银比
        按照[au,ag,au/ag,au_bid,au_ask,ag_bid,ag_ask,long_ratio,short_ratio, date]的顺序将list emit
        :return:
        '''
        quote_price = get_auag_quote_by_sina("au0,ag0")
        if quote_price["AG0"]["new_price"] > 0:
            ratio = quote_price["AU0"]["new_price"] / (quote_price["AG0"]["new_price"] / 1000)
            ratio = round(ratio, 2)
            ratio_long = quote_price["AU0"]["ask"] / (quote_price["AG0"]["bid"] / 1000)
            ratio_long = round(ratio_long, 2)
            ratio_short = quote_price["AU0"]["bid"] / (quote_price["AG0"]["ask"] / 1000)
            ratio_short = round(ratio_short, 2)
        emit_list = [quote_price["AU0"]["new_price"],
                     quote_price["AG0"]["new_price"],
                     ratio,
                     quote_price["AU0"]["bid"],
                     quote_price["AU0"]["ask"],
                     quote_price["AG0"]["bid"],
                     quote_price["AG0"]["ask"],
                     ratio_long,
                     ratio_short,
                     quote_price["AU0"]["date"]]
        self.auag_quotation.emit(emit_list)

    def calc_stat(self, stat_list):
        '''
        计算统计值
        计算参数有主页面通过信号发射过来，float与int类型的list
        :param stat_list: 输入的stat信息list，按照[long_buy，long_sell，short_buy，short_sell，back_day]排序
        :return:
        '''
        print("calc_stat,%r" % stat_list)
        if len(stat_list) == 0:
            print("stat info list=0,return")
            return
        if len(stat_list) != 5:
            print("stat list len !=5, list=%r" % stat_list)
            return

        gold_future = "C:\\quanttime\\data\\gold\\sh_future\\gold.csv"
        silver_future = "C:\\quanttime\\data\\gold\\sh_future\\silver.csv"

        stander_dtype = {'open': float, "close": float, "high": float, "low": float, "volume": float, "money": float}
        gold_future_data = pd.read_csv(gold_future, parse_dates=["date"], index_col=["date"],
                                       dtype=stander_dtype)
        gold_future_data = gold_future_data[~gold_future_data.reset_index().duplicated().values]

        silver_future_data = pd.read_csv(silver_future, parse_dates=["date"], index_col=["date"],
                                         dtype=stander_dtype)
        silver_future_data = silver_future_data[~silver_future_data.reset_index().duplicated().values]

        future_data = pd.merge(gold_future_data, silver_future_data, left_index=True, right_index=True,
                               suffixes=('_gold', '_silver'))
        future_data["compare"] = future_data["close_gold"] / future_data["close_silver"] * 1000
        # 去重
        future_data = future_data.dropna()
        future_data_trade_date = future_data.index

        recode_last_date = future_data_trade_date[-1].strftime("%Y-%m-%d")

        back_day_stat = stat_list[4]  # 设置当前日期往前推几天的统计信息
        long_buy_value = stat_list[0]  # 做多金银比，统计买入线，如0.10即10%分位线
        long_sell_value = stat_list[1]  # 做多金银比，统计卖出线，如0.15即15%分位线
        short_buy_value = stat_list[2]  # 做空金银比，统计的买入线，如0.85即85%分位线
        short_sell_value = stat_list[3]  # 做空金银比，统计的卖出线
        df_stat_20 = future_data.iloc[-int(back_day_stat):]  # 如future_data.iloc[-20:]
        # print(df_stat_20)
        df_stat = df_stat_20.loc[:, ["compare"]].describe()
        # print(df_stat)

        long_buyValue = round(long_buy_value, 2)
        long_sellValue = round(long_sell_value, 2)
        short_sellValue = round(short_buy_value, 2)
        short_buyValue = round(short_sell_value, 2)
        # print(self.long_buy_value)
        # long_buyValue = 0.05
        # long_sellValue = 0.10
        # short_sellValue = 0.85
        # short_buyValue = 0.90
        v_5 = df_stat_20.quantile(long_buyValue).compare  # 5%分位
        v_10 = df_stat_20.quantile(long_sellValue).compare  # 10%分位
        v_90 = df_stat_20.quantile(short_sellValue).compare  # 90%分位
        v_95 = df_stat_20.quantile(short_buyValue).compare  # 95%分位

        max_value = round(df_stat.loc["max", ["compare"]].compare, 2)
        min_value = round(df_stat.loc["min", ["compare"]].compare, 2)
        mean_value = round(df_stat.loc["mean", ["compare"]].compare, 2)
        per_25 = round(df_stat.loc["25%", ["compare"]].compare, 2)
        per_50 = round(df_stat.loc["50%", ["compare"]].compare, 2)
        per_75 = round(df_stat.loc["75%", ["compare"]].compare, 2)
        std = round(df_stat.loc["std", ["compare"]].compare, 2)

        result_list = [str(round(v_5, 2)), str(round(v_10, 2)), str(round(v_90, 2)), str(round(v_95, 2)),
                       str(max_value), str(min_value), str(mean_value), str(std),
                       str(per_25), str(per_50), str(per_75),
                       recode_last_date]
        self.signal_auag_stat.emit(result_list)


