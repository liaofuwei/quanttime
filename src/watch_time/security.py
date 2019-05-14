# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore, QtGui, QtWidgets
from quote_api import *
import os
import pandas as pd

'''
券商估值分析
'''


class SecurityThread(QtCore.QThread):
    df_out = QtCore.pyqtSignal(pd.DataFrame, pd.DataFrame)

    def __init__(self, parent=None):
        super(SecurityThread, self).__init__(parent)
        self.is_running = True

        #main_windows = WatchMainWindows()
        # 主页面的券商分析信号与业务处理逻辑连接
        #main_windows.security_out.connect(self.analyze_security_valuation)

    # =================================================
    def analyze_security_valuation(self, check_state):
        '''
        分析券商的估值信息
        分别读取所有券商valuation表，获取pb，pe等信息
        :param:check_state :int 1--只分析头部券商   0--分析全部券商
        :return:
        '''
        security = pd.DataFrame()
        if check_state == 1:
            # 读取重点关注的券商
            security = pd.read_csv(r"C:\quanttime\src\watch_time\key_security.csv", encoding="gbk",
                                   index_col=["ts_code"])
        else:
            # 从tushare的基本信息表中读取所有证券行业的code和名称
            select_columns = ["ts_code", "name", "industry"]
            security = pd.read_csv(r"C:\quanttime\data\basic_info\all_stock_info_ts.csv",
                                   usecols=select_columns, encoding="gbk", index_col=["ts_code"])
            security = security[security["industry"] == "证券"]

        # loc获取元素时，name是属性，使用loc[index,['name']].name获取的值不对，所有列名这里改个名称
        security = security.rename(columns={"name": "sname"})

        # 读取所有证券行业的valuation数据，该数据来至于finance文件夹下的valuation表，该表每天更新，存到交易日前一天的股指数据
        get_columns = ["code", "day", "pe_ratio", "pb_ratio"]
        basic_path = "C:\\quanttime\\data\\finance\\valuation\\"
        pb_general_result = []
        pe_general_result = []
        test_code_list = ['600030.SH', '000776.SZ']
        # for ts_code in test_code_list:
        for ts_code in security.index:
            jq_code = self.tscode2jqcode(ts_code)
            if jq_code == "000000":
                continue
            file_path = basic_path + jq_code + ".csv"
            if os.path.exists(file_path):
                df_valuation = pd.read_csv(file_path, usecols=get_columns)
                df_valuation = df_valuation.drop_duplicates("day")
                df_valuation = df_valuation.set_index("day")
                if df_valuation.empty:
                    continue

                pb = df_valuation.loc[df_valuation.index[-1], ["pb_ratio"]].pb_ratio
                pb_quantile_5 = df_valuation.pb_ratio.quantile(0.05)
                pb_quantile_10 = df_valuation.pb_ratio.quantile(0.10)
                pb_min = df_valuation.pb_ratio.min()
                pb_min_date = df_valuation.pb_ratio.idxmin()
                pb_max = df_valuation.pb_ratio.max()
                pb_max_date = df_valuation.pb_ratio.idxmax()
                pb_median = df_valuation.pb_ratio.median()
                pb_std = df_valuation.pb_ratio.std()

                pb_name = security.loc[ts_code, ["sname"]].sname
                price = "--"
                pre_close = "--"
                # 从通达信获取行情信息
                df_quote = get_quote_by_tdx([ts_code])
                if not df_quote.empty:
                    df_quote = df_quote.set_index("code")
                    price = df_quote.iloc[0, df_quote.columns.get_loc('price')]
                    pre_close = df_quote.iloc[0, df_quote.columns.get_loc('last_close')]
                pb_tmp = [ts_code, pb_name, price, pre_close, pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10,
                          pb_median, pb_max, pb_max_date, pb_std]
                pb_general_result.append(pb_tmp)

                pe = df_valuation.loc[df_valuation.index[-1], ["pe_ratio"]].pe_ratio
                pe_quantile_5 = df_valuation.pe_ratio.quantile(0.05)
                pe_quantile_10 = df_valuation.pe_ratio.quantile(0.10)
                pe_min = df_valuation.pe_ratio.min()
                pe_min_date = df_valuation.pe_ratio.idxmin()
                pe_max = df_valuation.pe_ratio.max()
                pe_max_date = df_valuation.pe_ratio.idxmax()
                pe_median = df_valuation.pe_ratio.median()
                pe_std = df_valuation.pe_ratio.std()
                pe_tmp = [ts_code, pb_name, price, pre_close, pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10,
                          pe_median, pe_max, pe_max_date, pe_std]
                pe_general_result.append(pe_tmp)
        df_pb_columns_name = ['code', 'name', 'price', 'last_close', 'pb', 'min', 'min_date', '5_per', '10_per', 'mean',
                              'max', 'max_date', 'std']
        df_pb = pd.DataFrame(data=pb_general_result, columns=df_pb_columns_name)
        # df_pb = df_pb.set_index("code")

        df_pe_columns_name = ['code', 'name', 'price', 'last_close', 'pe', 'min', 'min_date', '5_per', '10_per', 'mean',
                              'max', 'max_date', 'std']
        df_pe = pd.DataFrame(data=pe_general_result, columns=df_pe_columns_name)
        # df_pe = df_pe.set_index("code")

        df_pb['pb'] = df_pb['pb'].apply(pd.to_numeric)
        df_pe['pe'] = df_pe['pe'].apply(pd.to_numeric)

        # 小数只保留两位
        columns_2decimals = ["pb", "min", "max", "mean", "std", "5_per", "10_per"]
        df_pb[columns_2decimals] = df_pb[columns_2decimals].round(decimals=2)
        columns_2decimals = ["pe", "min", "max", "mean", "std", "5_per", "10_per"]
        df_pe[columns_2decimals] = df_pe[columns_2decimals].round(decimals=2)

        df_pb = df_pb.sort_values(by=['pb'])
        df_pe = df_pe.sort_values(by=['pe'])
        self.df_out.emit(df_pb, df_pe)
# ==============================================================

    @staticmethod
    def tscode2jqcode(x):
        '''
        tushare code --> joinquant code
        即601318.SH --> 601318.XSHG
        :param x:
        :return:
        '''
        code = str(x)
        ret = code.split('.')

        if ret[0].isnumeric():
            if ret[0][0] == '6':
                return ret[0] + '.XSHG'
            else:
                return ret[0] + '.XSHE'

        else:
            print("code is not standard,code=%r ", code)
            return "000000"


