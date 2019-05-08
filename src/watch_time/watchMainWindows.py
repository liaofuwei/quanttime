#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import ui_watch_time as ui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import os
import pandas as pd
import numpy as np
import tushare as ts
import math

class watchMainWindows(object):
    def __init__(self):
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = ui.Ui_dog()
        self.ui.setupUi(main_window)

        # 银行基本信息目录及文件名
        self.bank_basic_info_dir = r"C:\quanttime\src\watch_time\bank_info.csv"

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        # ts 授权
        self.pro = ts.pro_api()

        # 设定默认的行情数据, 通达信默认行情源，tushare，joinquant可选
        self.ui.checkBox_4.setCheckState(QtCore.Qt.Unchecked)
        self.ui.checkBox_5.setCheckState(QtCore.Qt.Checked)
        self.ui.checkBox_7.setCheckState(QtCore.Qt.Unchecked)

        # 银行基本信息更新按钮信号连接
        self.ui.pushButton.clicked.connect(self.update_bank_industry_table)

        # 银行分红信息及系统排名表刷新信号连接
        self.ui.pushButton_2.clicked.connect(self.update_bank_dividend)

        main_window.show()
        sys.exit(app.exec_())
    # ==============================================================

    def update_bank_industry_table(self):
        '''
        更新银行业信息表
        1、读取stock基本信息目录（C:\quanttime\data\basic_info）下的all_stock_info_ts.csv文件，获取行业为银行的所有内容
        2、将该信息转存到程序运行目录（C:\quanttime\src\watch_time）下，命名为bank_info.csv
        :return:
        '''
        select_columns = ["ts_code", "name", "industry"]
        bank = pd.read_csv(r"C:\quanttime\data\basic_info\all_stock_info_ts.csv",
                           usecols=select_columns, encoding="gbk", index_col=["ts_code"])
        bank = bank[bank["industry"] == "银行"]
        bank[["name"]].to_csv(self.bank_basic_info_dir, encoding="gbk")

    # ==============================================================

    def update_bank_dividend(self):
        '''
        更新分红信息，当前查询分红信息采用的tushare接口
        :return:
        '''
        bank = pd.read_csv(self.bank_basic_info_dir, encoding="gbk", index_col=["ts_code"])
        # 获取分红信息
        df_bank_dividend = self.get_dividend_by_tushare(bank.index.tolist())
        df_bank_dividend = df_bank_dividend.set_index("ts_code")
        df_bank_dividend = pd.merge(df_bank_dividend, bank, left_index=True, right_index=True)

        #获取实时股价
        df_bank_price = self.get_quote(bank.index)
        if df_bank_price.empty:
            print("获取实时行情失败，检查行情接口，再次获取")
            return

        df_bank = pd.merge(df_bank_dividend, df_bank_price, left_on="name", right_on="name")
        columns_need_2_float = ["cash_div_tax", "price"]
        df_bank[columns_need_2_float] = df_bank[columns_need_2_float].apply(pd.to_numeric)
        df_bank["div_rate"] = df_bank["cash_div_tax"] / df_bank["price"]
        df_bank = df_bank.sort_values(by=["div_rate"], ascending=False)
        df_bank["div_rate"] = df_bank["div_rate"].map(self.display_percent_format)
        print(df_bank)

        #获取flitter排名
        df_flitter = self.get_flitter()
        df_flitter = df_flitter.set_index("code")
        print(df_flitter)

        rows = len(df_bank)
        self.ui.tableWidget.setRowCount(rows)
        for i in range(rows):
            item = QtWidgets.QTableWidgetItem(df_bank.iloc[i, df_bank.columns.get_loc("code")])
            self.ui.tableWidget.setItem(i, 0, item)

            item = QtWidgets.QTableWidgetItem(df_bank.iloc[i, df_bank.columns.get_loc("name")])
            self.ui.tableWidget.setItem(i, 1, item)

            item = QtWidgets.QTableWidgetItem(str(df_bank.iloc[i, df_bank.columns.get_loc("price")]))
            self.ui.tableWidget.setItem(i, 2, item)

            item = QtWidgets.QTableWidgetItem(str(df_bank.iloc[i, df_bank.columns.get_loc("cash_div_tax")]))
            self.ui.tableWidget.setItem(i, 4, item)

            item = QtWidgets.QTableWidgetItem(str(df_bank.iloc[i, df_bank.columns.get_loc("div_rate")]))
            self.ui.tableWidget.setItem(i, 5, item)

            item = QtWidgets.QTableWidgetItem(df_bank.iloc[i, df_bank.columns.get_loc("end_date")])
            self.ui.tableWidget.setItem(i, 6, item)

            code = df_bank.iloc[i, df_bank.columns.get_loc("code")]
            if code[0] == '6':
                code = "SH" + code
            elif code[0] == '0':
                code = "SZ" + code
            else:
                code = "000000"
            try:
                pb2 = df_flitter.loc[code, ["pb2"]].pb2
                item = QtWidgets.QTableWidgetItem(str(pb2))
                self.ui.tableWidget.setItem(i, 3, item)

                rank = df_flitter.loc[code, ["index_value9"]].index_value9
                item = QtWidgets.QTableWidgetItem(str(rank))
                self.ui.tableWidget.setItem(i, 7, item)

            except KeyError:
                continue




    # ==============================================================

    def get_dividend_by_tushare(self, ts_code_list):
        '''
        tushare接口获取分红信息
        :param ts_code_list: code list，需要判断是否满足tushare的code格式要求
        :return: 包含分红信息的df
        '''
        # end_date:分红年度 cash_div_tax：每股分红税前 record_date：股权登记日 pay_date：派息日
        columns_name = ["ts_code", "end_date", "cash_div_tax", "record_date", "pay_date"]
        get_feilds = 'ts_code,end_date,cash_div_tax,record_date,pay_date'
        df_bank_dividend = pd.DataFrame(columns=columns_name)

        # df_bank_dividend = self.pro.dividend(ts_code=ts_code_list.pop(0), fields=get_feilds)
        for ts_code in ts_code_list:
            df_tmp = self.pro.dividend(ts_code=ts_code, fields=get_feilds)
            if df_tmp.empty:
                continue
            # 只取第一行最新的记录，旧的分红记录不需要
            df_tmp = df_tmp.loc[df_tmp.index[0], columns_name]
            df_bank_dividend = df_bank_dividend.append(df_tmp, ignore_index=True)
        return df_bank_dividend

    # ==============================================================

    def get_quote(self, code_list):
        '''
        获取股票实时行情
        :param:code_list 需要获取实时行情的代码
        :return:df
        '''
        if len(code_list) == 0:
            return pd.DataFrame()

        # checkBox_4通达信接口，checkBox_5 tushare接口 checkBox_7 joinquant接口
        check_status = self.ui.checkBox_4.checkState()
        if check_status == QtCore.Qt.Checked:
            print("暂时没有接入通达信接口，这里选择tushare查询")
            # return pd.DataFrame()

        check_status = self.ui.checkBox_5.checkState()
        if check_status == QtCore.Qt.Checked:
            # 获取对应的正股股价
            """
            Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
           'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
           'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
           'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
            dtype='object')
            """
            ts_code = [str(x).split('.')[0] for x in code_list]
            stock_rt_price = ts.get_realtime_quotes(ts_code)
            stock_rt_price = stock_rt_price.loc[:, ["code", "price", "name"]]
            return stock_rt_price

        check_status = self.ui.checkBox_7.checkState()
        if check_status == QtCore.Qt.Checked:
            print("暂时没有接入joinquant接口，这里选择tushare查询")
            # return pd.DataFrame()

    # ==============================================================
    @staticmethod
    def get_flitter():
        '''
        获取flitter的排名信息，该排名暂时用txt存储在运行目录内
        C:\quanttime\src\watch_time\flitter.txt
        code	name	price	net_value	pb2	ret9	index_value9
        HK000998	中信银行	4.214	8.67	0.48	0.13	-0.3
    	HK003618	重庆农商	3.792	7.13	0.53	0.14	-0.2
        :return:df
        '''
        columns_name = ["code", "name", "price", "net_value", "pb2", "ret9", "index_value9"]
        if os.path.exists(r"C:\quanttime\src\watch_time\flitter.txt"):
            flitter_data = pd.read_csv(r"C:\quanttime\src\watch_time\flitter.txt",
                                       encoding="gbk", delim_whitespace=True,names=columns_name, header=0)
            return flitter_data
        else:
            return pd.DataFrame()

    # ==============================================================

    @staticmethod
    def display_percent_format(x):
        '''
        功能：小数按照百分数%显示，保留两位小数
        '''
        try:
            data = float(x)
        except:
            print("input is not numberic")
            return 0

        return "%.2f%%"%(data * 100)



if __name__ == "__main__":
    watchMainWindows()