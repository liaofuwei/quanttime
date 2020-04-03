#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import ui_convertbond as ui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import os
import pandas as pd
import numpy as np
import tushare as ts
import easytrader
import math

class BondMainWindows(object):
    def __init__(self):
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = ui.Ui_ant()
        self.ui.setupUi(main_window)

        convert_bond_raw_path = r"C:\quanttime\src\convertbond\raw_data.csv"
        self.ui.lineEdit.setText(convert_bond_raw_path)
        convert_bond_raw_after_process_path = r"C:\quanttime\src\convertbond\raw_data_after_process.csv"
        self.ui.lineEdit_2.setText(convert_bond_raw_after_process_path)

        # 可转债到转股期的信息更新文件目录及文件名,放在初始化中，主要是好修改配置
        self.converion_period_file = r"C:\quanttime\src\convertbond\conversion_period.csv"

        # 生信息转换pushbutton的signal连接
        self.ui.pushButton.clicked.connect(self.process_convertbond_raw_basic_info)

        # 显示可转债基本信息表pushbotton的signal连接
        self.ui.pushButton_2.clicked.connect(self.display_convert_basic_info)

        # 可转债实时折溢价情况刷新pushbutton的signal连接
        self.ui.pushButton_4.clicked.connect(self.display_premium)

        # 更新是否到转股期到csv表格
        self.ui.pushButton_3.clicked.connect(self.update_bond2stock_period)

        # 重点监测可转债checkboxsignal
        self.ui.checkBox.stateChanged.connect(self.select_or_cancel_all_bond_status)
        # premium table的checkbox signal
        self.ui.checkBox_8.stateChanged.connect(self.select_or_cancel_premium_table_state)

        # 转债的基本信息，从本地csv中读取的信息
        self.df_convertbond_basic_info = pd.DataFrame()

        # 可转债的实时行情df
        self.df_bond_rt_quotes = pd.DataFrame()
        self.quotes_bond_code = []

        # 正股的实时行情df
        self.df_stock_rt_quotes = pd.DataFrame()
        self.quotes_stock_code = []

        # 转债基本信息以及转债实时行情，股票实时行情
        self.df_bond_total = pd.DataFrame()

        # 点击premium表的单元格，发射信号，生成转债与正股的2档价量信息
        self.ui.tableWidget_2.cellClicked.connect(self.display_buy_sell_info)

        # 点击可转债折溢价交易按钮触发交易信号
        self.ui.pushButton_5.clicked.connect(self.trade_convert_bond_premium)

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        # ts 授权
        self.pro = ts.pro_api()

        # 设定默认的行情数据, 通达信默认行情源，tushare，joinquant可选
        self.ui.checkBox_4.setCheckState(QtCore.Qt.Checked)
        self.ui.checkBox_5.setCheckState(QtCore.Qt.Unchecked)
        self.ui.checkBox_7.setCheckState(QtCore.Qt.Unchecked)

        # 注册交易接口,初始化时，登录交易接口默认为不登录，需要勾选才登录
        self.user = 0
        self.ui.checkBox_6.setCheckState(QtCore.Qt.Unchecked)
        self.ui.checkBox_6.stateChanged.connect(self.register_trade)

        # 显示基本信息表
        self.display_convert_basic_info()

        main_window.show()
        sys.exit(app.exec_())
    # ==============================================================

    def process_convertbond_raw_basic_info(self):
        '''
        处理可转债的基本信息，基本信息的生数据来源于集思录，存放在csv中，该方法主要是从生数据中提取有用的关键信息用于下一步使用
        :return:
        '''
        columns_name = ["code", "bond_name", "bond_price", "u1", "stock_name", "stock_price", "u2", "PB",
                        "convert_price", "u3", "u4", "u5", "rank", "u6", "back_sell_price", "130_sell_price",
                        "bond_percentage", "u7", "expire", "remaining_time", "pretax_return", "tax_return",
                        "back_sell_return", "u8", "u9"]
        raw_data = pd.read_csv(self.ui.lineEdit.text(), encoding="gbk", skiprows=1, names=columns_name)
        raw_data = raw_data.set_index("code")
        raw_data["convert_price"] = raw_data["convert_price"].map(self.format_convert_price)
        raw_data.to_csv(self.ui.lineEdit_2.text(), encoding="gbk")

    # ==============================================================
    def register_trade(self, state):
        '''
        注册交易接口，当选定时，登录华泰的交易接口，避免长时间登录
        :return:
        '''
        # 注册交易接口
        if state == QtCore.Qt.Checked:
            self.user = easytrader.use('ht_client')
            # self.user.prepare(user='666626218349', password='836198', comm_password='sam155')
            self.user.prepare(user='666628601648', password='836198', comm_password='sam155')
    # ==============================================================

    @staticmethod
    def format_convert_price(x):
        '''
        格式化转股价，处理集思录为了标记转股价修改次数标记星号，形如（4.56**）
        :return:
        '''
        convert_price = str(x)
        if '*' in convert_price:
            return convert_price[0:convert_price.find('*')]
        else:
            return convert_price

    # ==============================================================
    def display_convert_basic_info(self):
        '''
        显示可转债基本信息
        :return:
        '''
        if not os.path.exists(self.ui.lineEdit_2.text()):
            QtWidgets.QMessageBox.warning(self, "message", "可转债基本信息csv不存在",
                                          QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                                          QtWidgets.QMessageBox.Yes)
            return

        use_col = ["code", "bond_name", "stock_name", "PB", "convert_price", "rank", "back_sell_price", "130_sell_price",
                   "expire", "remaining_time"]
        col_type = {"convert_price": np.float16}

        df = pd.read_csv(self.ui.lineEdit_2.text(), encoding="gbk", usecols=use_col,
                         parse_dates=["expire"], dtype=col_type)
        self.df_convertbond_basic_info = df
        # 读取stock基本信息表，主要获取stock name
        stock_basic_file = r"C:\quanttime\data\basic_info\all_stock_info.csv"
        df_stock_basic_info = pd.read_csv(stock_basic_file, encoding="gbk")

        df_bond_basic = pd.merge(df, df_stock_basic_info, left_on="stock_name", right_on="display_name",
                                 suffixes=["_bond", "_stock"])
        # 结合可转债基本信息与正股信息
        self.df_convertbond_basic_info = df_bond_basic

        if len(df) != len(df_bond_basic):
            print("转债基本信息表与股票基本信息表合并过程中有数据丢失")

        # 读取是否到转股期表，获得转股期信息
        converion_period_file = r"C:\quanttime\src\convertbond\conversion_period.csv"
        if os.path.exists(converion_period_file):
            df_converion_period = pd.read_csv(converion_period_file, encoding="gbk", index_col=["bond_code"])
        else:
            df_converion_period = pd.DataFrame(columns=["bond_code", "bond_name", "if_conversion"])

        self.ui.tableWidget.setRowCount(len(df_bond_basic))

        for i in range(len(df_bond_basic)):
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('code_bond')]))
            self.ui.tableWidget.setItem(i, 1, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('bond_name')]))
            self.ui.tableWidget.setItem(i, 2, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('stock_name')]))
            self.ui.tableWidget.setItem(i, 3, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('code_stock')]))
            self.ui.tableWidget.setItem(i, 4, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('PB')]))
            self.ui.tableWidget.setItem(i, 5, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('rank')]))
            self.ui.tableWidget.setItem(i, 6, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('convert_price')]))
            self.ui.tableWidget.setItem(i, 7, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('back_sell_price')]))
            self.ui.tableWidget.setItem(i, 8, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('130_sell_price')]))
            self.ui.tableWidget.setItem(i, 9, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('expire')]))
            self.ui.tableWidget.setItem(i, 10, newItem)
            newItem = QtWidgets.QTableWidgetItem(
                str(df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('remaining_time')]))
            self.ui.tableWidget.setItem(i, 11, newItem)

            newItem = QtWidgets.QCheckBox()
            newItem1 = QtWidgets.QCheckBox()
            bond_code = df_bond_basic.iloc[i, df_bond_basic.columns.get_loc('code_bond')]
            try:
                if_convesion = df_converion_period.loc[bond_code, ["if_conversion"]].if_conversion
                if str(if_convesion) == "Y":
                    newItem.setCheckState(QtCore.Qt.Checked)
                    newItem1.setCheckState(QtCore.Qt.Checked)
                else:
                    newItem.setCheckState(QtCore.Qt.Unchecked)
                    newItem1.setCheckState(QtCore.Qt.Unchecked)
            except:
                newItem.setCheckState(QtCore.Qt.Unchecked)
                newItem1.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget.setCellWidget(i, 12, newItem)
            self.ui.tableWidget.setCellWidget(i, 0, newItem1)

    # ==============================================================

    def select_or_cancel_all_bond_status(self):
        '''
        勾选或者取消全部的checkbox状态
        :return:
        '''
        if self.ui.tableWidget.rowCount() == 0:
            return

        if self.ui.checkBox.checkState() == QtCore.Qt.Checked:
            for i in range(self.ui.tableWidget.rowCount()):
                newItem = QtWidgets.QCheckBox()
                newItem.setCheckState(QtCore.Qt.Checked)
                self.ui.tableWidget.setCellWidget(i, 0, newItem)
        else:
            for i in range(self.ui.tableWidget.rowCount()):
                newItem = QtWidgets.QCheckBox()
                newItem.setCheckState(QtCore.Qt.Unchecked)
                self.ui.tableWidget.setCellWidget(i, 0, newItem)

    # ===============================================================

    def select_or_cancel_premium_table_state(self):
        '''
        勾选或者取消premium表的checkbox状态
        :return:
        '''
        if self.ui.tableWidget_2.rowCount() == 0:
            return

        if self.ui.checkBox_8.checkState() == QtCore.Qt.Checked:
            for i in range(self.ui.tableWidget_2.rowCount()):
                item = QtWidgets.QCheckBox()
                item.setCheckState(QtCore.Qt.Checked)
                self.ui.tableWidget_2.setCellWidget(i, 0, item)
        else:
            for i in range(self.ui.tableWidget_2.rowCount()):
                item = QtWidgets.QCheckBox()
                item.setCheckState(QtCore.Qt.Unchecked)
                self.ui.tableWidget_2.setCellWidget(i, 0, item)

    # ===============================================================

    def update_bond2stock_period(self):
        '''
        更新是否到转股期的记录，是否到转股期是可转债套利的核心条件之一
        该方法是将可转债基本信息表的内容更新到conversion_period.csv中
        列名：bond_code，bond_name, if_conversion
        :return:
        '''
        columns_name = ["bond_code", "bond_name", "if_conversion"]
        list_record = []
        for i in range(self.ui.tableWidget.rowCount()):
            tmp_list = []
            item = self.ui.tableWidget.item(i, 1).text()
            tmp_list.append(item)
            item = self.ui.tableWidget.item(i, 2).text()
            tmp_list.append(item)
            item = self.ui.tableWidget.cellWidget(i, 12).checkState()
            if item == QtCore.Qt.Checked:
                tmp_list.append("Y")
            else:
                tmp_list.append("N")

            list_record.append(tmp_list)
        df = pd.DataFrame(data=list_record, columns=columns_name)
        df.to_csv(self.converion_period_file, encoding="gbk")

    # ===============================================================

    def get_premium(self):
        '''
        获得实时折溢价情况
        :return:
        '''
        if self.df_convertbond_basic_info.empty:
            print("先获取可转债基本信息表！")
            return

        columns_name = ["bond_code", "bond_name", "convert_price", "stock_code", "stock_name"]
        record_list = []
        for i in range(self.ui.tableWidget.rowCount()):
            item = self.ui.tableWidget.cellWidget(i, 0).checkState()
            if item == QtCore.Qt.Checked:
                # 转债代码，转债名称，正股代码，正股名称
                tmp = [self.ui.tableWidget.item(i, 1).text(), self.ui.tableWidget.item(i, 2).text(),
                       self.ui.tableWidget.item(i, 7).text(), self.ui.tableWidget.item(i, 4).text(),
                       self.ui.tableWidget.item(i, 3).text()]
                record_list.append(tmp)
        # 获取转债实时价格，正股价格所需要的code以及算折溢价需要的convert_price
        df_premium = pd.DataFrame(data=record_list, columns=columns_name)

        bond_codes = df_premium["bond_code"]
        # 获取转债实时价格
        """
        Index(['code', 'price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
       'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
       'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
       'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
       'bid_vol5', 'ask_vol5'],
        dtype='object')
        """
        bond_rt_price = ts.quotes(bond_codes, conn=self.cons)
        # print(type(bond_rt_price))
        print(bond_rt_price)
        if bond_rt_price.empty:
            print("获取转债实时行情为空")
            return
        # 只取一些列
        bond_rt_columns = ['code', 'price', 'bid1', 'bid_vol1', 'ask1',
                           'ask_vol1', 'bid2', 'bid_vol2', 'ask2', 'ask_vol2']
        bond_rt_price = bond_rt_price.loc[:, bond_rt_columns]
        dic_rename = {
            'code':      'bond_code',
            'price':     'bond_price',
            'bid1':      'bond_bid1',
            'bid_vol1':  'bond_bid_vol1',
            'ask1':      'bond_ask1',
            'ask_vol1':  'bond_ask_vol1',
            'bid2':      'bond_bid2',
            'bid_vol2':  'bond_bid_vol2',
            'ask2':      'bond_ask2',
            'ask_vol2':  'bond_ask_vol2'
        }
        bond_rt_price = bond_rt_price.rename(columns=dic_rename)

        df_premium["stock_code"] = df_premium["stock_code"].map(self.standard_code)
        # 获取对应的正股股价
        """
        Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
       'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
       'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
       'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
        dtype='object')
        """
        stock_rt_price = ts.get_realtime_quotes(df_premium["stock_code"])
        pd.set_option('display.max_columns', None)

        if stock_rt_price.empty:
            print("获取正股实时行情为空")
            return
        stock_columns = ['code', 'name', 'price', 'bid', 'ask', 'b2_p', 'b2_v', 'a2_p', 'a2_v', 'a1_v', 'b1_v']
        stock_rt_price = stock_rt_price.loc[:, stock_columns]
        dic_stock_rename = {
            'code':  "stock_code",
            "price": "stock_price",
            "bid":   "stock_bid",
            'ask':   "stock_ask",
            'b2_p':  "stock_b2_p",
            'b2_v':  'stock_b2_v',
            'a2_p':  'stock_a2_p',
            'a2_v':  'stock_a2_v'
        }
        stock_rt_price = stock_rt_price.rename(columns=dic_stock_rename)
        # 1 基本信息表与bond实时数据的合并
        bond = pd.merge(df_premium, bond_rt_price, on="bond_code")
        # 2 上面合并信息与stock实时数据的合并
        bond_total = pd.merge(bond, stock_rt_price, on="stock_code")
        columns_need_2_float = ["convert_price", "bond_price", "bond_bid1", "bond_ask1", "bond_bid2", "bond_ask2",
                                "stock_price", "stock_bid", "stock_ask", "stock_b2_p", "stock_a2_p"]
        bond_total[columns_need_2_float] = bond_total[columns_need_2_float].apply(pd.to_numeric)
        bond_total["bond2amount"] = bond_total["convert_price"].map(self.amount)

        # stockbid2bondprice > bond 价格才有套利价值，此时买入转债，卖出正股，可以实现无风险实时套利
        # 将正股买一价折算成转债价格
        bond_total = bond_total[bond_total["stock_price"] != 0]
        bond_total["stockbid2bondprice"] = bond_total["bond2amount"] * bond_total["stock_price"] * 10
        for i in range(len(bond_total)):
            if bond_total.iloc[i, bond_total.columns.get_loc("stockbid2bondprice")] == 0:
                print("正股价折算成债券价格为0，return，原因可能是没有获取到正股价格，在刷新一遍")
                return
        bond_total["premium"] = (bond_total["bond_bid1"] - bond_total["stockbid2bondprice"]) / \
                                bond_total["stockbid2bondprice"]

        # 按照折溢价情况排序
        bond_total = bond_total.sort_values(by=["premium"])
        self.df_bond_total = bond_total

    # ==============================================================

    def display_premium(self):
        '''
        显示实时折溢价情况
        :return:
        '''
        self.get_premium()
        rows = len(self.df_bond_total)
        self.ui.tableWidget_2.setRowCount(rows)
        print(self.df_bond_total.head(1))
        for i in range(rows):
            item = QtWidgets.QCheckBox()
            if self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("premium")] < 0:
                item.setCheckState(QtCore.Qt.Checked)
            else:
                item.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget_2.setCellWidget(i, 0, item)

            item = QtWidgets.QTableWidgetItem(
                self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("bond_code")])
            self.ui.tableWidget_2.setItem(i, 1, item)

            item = QtWidgets.QTableWidgetItem(
                self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("bond_name")])
            self.ui.tableWidget_2.setItem(i, 2, item)

            premium = self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("premium")]
            item = QtWidgets.QTableWidgetItem("%.2f%%" % (premium * 100))
            self.ui.tableWidget_2.setItem(i, 3, item)

            item = QtWidgets.QTableWidgetItem(
                self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("stock_code")])
            self.ui.tableWidget_2.setItem(i, 4, item)

            item = QtWidgets.QTableWidgetItem(
                self.df_bond_total.iloc[i, self.df_bond_total.columns.get_loc("stock_name")])
            self.ui.tableWidget_2.setItem(i, 5, item)
        self.ui.textBrowser_2.clear()

    # ===========================================================

    def display_key_premium_table(self):
        '''
        重点监视premium<0的转债
        :return:
        '''

    # ===========================================================

    def display_buy_sell_info(self, row, column):
        '''
        显示转债和正股的两档买卖信息及量
        :param row:来至于信号，即行坐标
        :param column:来至于信号，即列坐标
        :return:
        '''
        self.ui.textBrowser_2.clear()
        bond_code = self.ui.tableWidget_2.item(row, 1).text()
        bond_name = self.ui.tableWidget_2.item(row, 2).text()
        stock_code = self.ui.tableWidget_2.item(row, 4).text()
        stock_name = self.ui.tableWidget_2.item(row, 5).text()
        bond_label = bond_code + "      " + bond_name
        self.ui.lineEdit_3.setText(bond_label)
        stock_label = stock_code + "     " + stock_name
        self.ui.lineEdit_4.setText(stock_label)

        bond_value = self.df_bond_total[self.df_bond_total["bond_code"] == bond_code]
        # b1-->bid1,b1v-->bid1 vol, a1-->ask1, a1v-->ask1 vol
        b1 = bond_value.iloc[0, bond_value.columns.get_loc('bond_bid1')]
        b2 = bond_value.iloc[0, bond_value.columns.get_loc('bond_bid2')]
        a1 = bond_value.iloc[0, bond_value.columns.get_loc('bond_ask1')]
        a2 = bond_value.iloc[0, bond_value.columns.get_loc('bond_ask2')]
        b1v = bond_value.iloc[0, bond_value.columns.get_loc('bond_bid_vol1')]
        b2v = bond_value.iloc[0, bond_value.columns.get_loc('bond_bid_vol2')]
        a1v = bond_value.iloc[0, bond_value.columns.get_loc('bond_ask_vol1')]
        a2v = bond_value.iloc[0, bond_value.columns.get_loc('bond_ask_vol2')]

        item = QtWidgets.QTableWidgetItem(str(a2/10))
        self.ui.tableWidget_3.setItem(0, 0, item)
        item = QtWidgets.QTableWidgetItem(str(a2v/10))
        self.ui.tableWidget_3.setItem(0, 1, item)
        item = QtWidgets.QTableWidgetItem(str(a1/10))
        self.ui.tableWidget_3.setItem(1, 0, item)
        item = QtWidgets.QTableWidgetItem(str(a1v/10))
        self.ui.tableWidget_3.setItem(1, 1, item)
        item = QtWidgets.QTableWidgetItem(str(b1/10))
        self.ui.tableWidget_3.setItem(2, 0, item)
        item = QtWidgets.QTableWidgetItem(str(b1v/10))
        self.ui.tableWidget_3.setItem(2, 1, item)
        item = QtWidgets.QTableWidgetItem(str(b2/10))
        self.ui.tableWidget_3.setItem(3, 0, item)
        item = QtWidgets.QTableWidgetItem(str(b2v/10))
        self.ui.tableWidget_3.setItem(3, 1, item)
        # ====stock
        b1 = bond_value.iloc[0, bond_value.columns.get_loc('stock_bid')]
        b2 = bond_value.iloc[0, bond_value.columns.get_loc('stock_b2_p')]
        a1 = bond_value.iloc[0, bond_value.columns.get_loc('stock_ask')]
        a2 = bond_value.iloc[0, bond_value.columns.get_loc('stock_a2_p')]
        b1v = bond_value.iloc[0, bond_value.columns.get_loc('b1_v')]
        b2v = bond_value.iloc[0, bond_value.columns.get_loc('stock_b2_v')]
        a1v = bond_value.iloc[0, bond_value.columns.get_loc('a1_v')]
        a2v = bond_value.iloc[0, bond_value.columns.get_loc('stock_a2_v')]

        item = QtWidgets.QTableWidgetItem(str(a2))
        self.ui.tableWidget_4.setItem(0, 0, item)
        item = QtWidgets.QTableWidgetItem(str(a2v))
        self.ui.tableWidget_4.setItem(0, 1, item)
        item = QtWidgets.QTableWidgetItem(str(a1))
        self.ui.tableWidget_4.setItem(1, 0, item)
        item = QtWidgets.QTableWidgetItem(str(a1v))
        self.ui.tableWidget_4.setItem(1, 1, item)
        item = QtWidgets.QTableWidgetItem(str(b1))
        self.ui.tableWidget_4.setItem(2, 0, item)
        item = QtWidgets.QTableWidgetItem(str(b1v))
        self.ui.tableWidget_4.setItem(2, 1, item)
        item = QtWidgets.QTableWidgetItem(str(b2))
        self.ui.tableWidget_4.setItem(3, 0, item)
        item = QtWidgets.QTableWidgetItem(str(b2v))
        self.ui.tableWidget_4.setItem(3, 1, item)

        # 计算每张转债对应的正股数量，以及套利概况
        bond2stockvol = bond_value.iloc[0, bond_value.columns.get_loc('bond2amount')]
        stock2bondprice = bond_value.iloc[0, bond_value.columns.get_loc('stockbid2bondprice')]
        diff = stock2bondprice - bond_value.iloc[0, bond_value.columns.get_loc('bond_bid1')]
        self.ui.textBrowser_2.append("一张转债对应的正股数量：%.2f" % bond2stockvol)
        self.ui.textBrowser_2.append("买入10张转债，卖出对应数量正股，获利(为正可获利)：%.2f" % diff)

        # 从账户中读取仓位信息
        if self.user:
            position = self.user.position
            # 持仓正股的最大可卖数量
            max_sell_amount = 0
            for stock in position:
                if stock['证券代码'] == stock_code[0:6]:
                    print("可用余额： %s" % stock['可用余额'])
                    self.ui.textBrowser_2.append("持仓可用股票余额：%s" % stock['可用余额'])
                    if stock['可用余额'] > 0:
                        max_sell_amount = float(stock['可用余额'])
            if max_sell_amount >= 100:
                buy_bond_vol = self.position_stock2bond(max_sell_amount,
                                                        bond_value.iloc[0, bond_value.columns.get_loc('bond2amount')])
                self.ui.textBrowser_2.append("持仓可用股票余额对应需要买入转债手数：%d" % buy_bond_vol)
                self.ui.lineEdit_5.setText(str(int(buy_bond_vol)))
                self.ui.lineEdit_6.setText(str(int(max_sell_amount)))
                self.ui.lineEdit_7.setText(bond_code)
                self.ui.lineEdit_8.setText(str(bond_value.iloc[0, bond_value.columns.get_loc('bond_ask1')]/10))
                self.ui.lineEdit_9.setText(stock_code)
                self.ui.lineEdit_10.setText(str(bond_value.iloc[0, bond_value.columns.get_loc('stock_bid')]))

            else:
                self.ui.textBrowser_2.append("持仓中不包含选中转债对应的股票")

    # =============================================================

    def trade_convert_bond_premium(self):
        '''
        可转债折溢价交易的触发
        # :param df_bond_value:进行交易的转债和股票信息df
        :return:
        '''

        stock_code = self.ui.lineEdit_9.text()
        stock_price = float(self.ui.lineEdit_10.text())
        stock_amount = int(self.ui.lineEdit_6.text())
        # self.user.sell(stock_code, price=stock_price, amount=stock_amount)

        bond_code = self.ui.lineEdit_7.text()
        bond_price = float(self.ui.lineEdit_8.text())
        if '6' in stock_code:
            bond_amount = int(self.ui.lineEdit_5.text()) * 10
        else:
            bond_amount = int(self.ui.lineEdit_5.text())
        # self.user.buy(bond_code, price=bond_price, amount=bond_amount)

        self.ui.textBrowser.append("卖出正股：%s" % stock_code)
        self.ui.textBrowser.append("价格为：%f" % stock_price)
        self.ui.textBrowser.append("数量为：%d" % stock_amount)
        self.ui.textBrowser.append("=========================")
        self.ui.textBrowser.append("买入转债：%s" % bond_code)
        self.ui.textBrowser.append("价格：%f" % bond_price)
        self.ui.textBrowser.append("数量：%d" % bond_amount)

    # ==============================================================
    def get_quotes(self):
        '''
        获取实时行情信息，把行情剥离出业务逻辑，单独处理，方便后续增加数据行情源
        :return:
        '''
        check_status = self.ui.checkBox_4.checkState()
        if check_status == QtCore.Qt.Checked:
            print("tongdaxin")

    # ==============================================================

    @staticmethod
    def standard_code(x):
        '''
        标准化code代码. joinquant code --> tushare code
        convert_bond_basic_info.csv中的stock code 是joinquant格式的stock code
        需要转化为tushare格式的stock code
        :param x: str
        :return: 标准code代码
        '''
        jq_code = str(x)
        ret = jq_code.split('.')

        if ret[0].isnumeric():
            return ret[0]

        else:
            print("code is not standard,code=%r ", jq_code)
            return -1

    # ==============================================================
    @staticmethod
    def amount(x):
        '''
        该函数处理一张转债对应的正股股数
        计算方法如下：转债面值（即100）除以转股价
        转股价从csv基本信息表中读取
        '''
        try:
           x = float(x)
        except:
            return 0
        if x == 0:
            print("convert price is zero please check ")
            return -1
        else:
            return 100/x

    # ========================================================

    @staticmethod
    def position_stock2bond(position_stock, bond2stock_vol):
        '''
        根据持仓的正股数量，计算最佳的转债买入量
        :param position_stock: 正股持仓量
        :param bond2stock_vol:一张转债对应的正股数量
        :return:转债的买入手数（注意是买入的手数，即10张的整数倍)
        '''

        if position_stock <= 0:
            return 0
        if bond2stock_vol <= 0:
            return 0
        # 持仓正股对应的需要买入的转债张数
        position_stock2bond_vol = position_stock / bond2stock_vol

        integer = math.modf(position_stock2bond_vol)[1]

        # 取舍基准，当小数部分大于该数值则向上进一位，小于则不进位, 如stander = 0.8表示0.8手，买一手转债，小于0.8则不买，该值可以调整
        stander = 0.7

        # 如果持仓正股数量对应买入的转债量小于9张（即小于1手）,返回0，这个数量的正股不满足最小的转债买入量
        if position_stock2bond_vol < 9:
            return 0
        # 如果持仓正股数量对应买入的转债量在9与10张之间，则返回1，即一手转债
        elif 9 <= position_stock2bond_vol < 10:
            return 1
        elif 10 <= position_stock2bond_vol < 100:
            tmp_int = integer / 10
            if math.modf(tmp_int)[0] > stander:
                return math.modf(tmp_int)[1] + 1
            else:
                return math.modf(tmp_int)[1]
        else:
            tmp_int = integer / 10
            if math.modf(tmp_int)[0] > stander:
                return math.modf(tmp_int)[1] + 1
            else:
                return math.modf(tmp_int)[1]


if __name__ == "__main__":
    BondMainWindows().display_convert_basic_info()