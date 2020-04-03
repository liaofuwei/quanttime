# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import sys
import os
import pandas as pd
from PyQt5 import QtCore
import numpy as np
from pyecharts import Line, Kline, Bar
import quote_api as quote


class AHCompareStat(QtCore.QThread):
    html_display_out = QtCore.pyqtSignal(int)
    ah_stat_out = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(AHCompareStat, self).__init__(parent)
        self.is_running = True

    # ============================
    def ah_compare_stat(self, item_code):
        """
        ah统计分析
        :param item_code: str，由主页面combobox发送过来的信号，
        :return:
        """
        if not isinstance(item_code, str):
            return
        code = self.stander_code_2_jq(item_code.split(" ")[0])
        stock_file = 'C:\\quanttime\\data\\AH_ratio\\' + code + '.csv'
        if not os.path.exists(stock_file):
            print("file:%s 不存在" % stock_file)
            return
        df_data = pd.read_csv(stock_file, encoding='gbk', index_col=["day"], parse_dates=True)

        time_axis = np.array(df_data.index.map(str))
        data = np.array(df_data['h_a_comp'])
        min_data = df_data['h_a_comp'].min()
        max_data = df_data['h_a_comp'].max()
        mean_data = df_data['h_a_comp'].mean()
        std_data = df_data['h_a_comp'].std()
        data_5 = df_data['h_a_comp'].quantile(0.05)
        data_10 = df_data['h_a_comp'].quantile(0.1)
        data_75 = df_data['h_a_comp'].quantile(0.75)
        data_90 = df_data['h_a_comp'].quantile(0.9)
        tmp_list_stat = [mean_data, data_5, data_10, data_75, data_90, max_data, min_data, std_data]
        list_stat = [round(d, 2) for d in tmp_list_stat]
        line = Line("H VS A compare", width=1400)
        line_name = item_code + "AH compare"
        line.add(line_name, time_axis, data.round(2), mark_point=["average", "max", "min"], is_stack=True,
                 is_datazoom_show=True, yaxis_min=round(min_data * 0.9, 2), yaxis_max=(max_data * 1.1).round(2))
        line.render("ah_compare_line.html")

        ret_ah_compare = self.calc_ah_compare(item_code)
        if ret_ah_compare == -1:
            return

        self.html_display_out.emit(1)
        self.ah_stat_out.emit([ret_ah_compare, list_stat])

    # ================================
    @staticmethod
    def stander_code_2_jq(code):
        """
        将code标准化为joinquant代码
        注意本方法只能转换常见的6,0,3开头的stock code
        :param code:
        :return:
        """
        if len(code) != 6:
            return
        if code[0] == '6':
            return code + '.XSHG'
        else:
            return code + '.XSHE'
    # ===================================
    @staticmethod
    def calc_ah_compare(item_code):
        """
        计算实时的AH比价
        汇率来至于opendatatools
        股价来至于通达信
        :param item_code: str，由主页面combobox发送过来的信号
        :return:
        """
        tmp_list = item_code.split(" ")
        acode = tmp_list[0]
        name = tmp_list[1]
        hcode = tmp_list[2]
        if len(acode) != 6:
            acode = acode.zfill(6)
        if len(hcode) != 5:
            hcode = hcode.zfill(5)
        code_list = []
        if acode[0] == '6':
            acode = 'SH.' + acode
            code_list.append(acode)
        elif acode[0] == '0':
            acode = 'SZ.' + acode
            code_list.append(acode)
        else:
            print("code error")
            return -1
        hcode = "HK." +hcode
        code_list.append(hcode)
        df_price = quote.get_quote_by_futu(code_list)
        if df_price.empty:
            return -1
        df_price = df_price.set_index("code")
        print(df_price)
        cny_hk = quote.get_cny_spot_from_opendatatool("HKD/CNY")
        if cny_hk == -1:
            print("获取实时汇率失败")
            return -1
        a_price = df_price.loc[acode, ["last_price"]]["last_price"]
        h_price = df_price.loc[hcode, ["last_price"]]["last_price"]
        if h_price == 0:
            return -1
        ah_compare = a_price / (h_price * cny_hk)
        return [name, acode, ah_compare.round(2), a_price, h_price]

