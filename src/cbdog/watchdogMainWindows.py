# -*-coding:utf-8 -*-
__author__ = 'liao'

from ui_watchdog import Ui_watchdog
from PyQt5 import QtCore, QtWidgets, QtGui
import sys
import os
import pandas as pd
from datetime import datetime
from real_quote_thread import GetQuoteThread
from convert_bond_volatility_analyse import CBCalc
from jqdatasdk import *

# jqdata context
auth('13811866763', "sam155")  # jqdata 授权


class WatchDogMainWindows(QtCore.QObject):
    signal_thread_run = QtCore.pyqtSignal(bool)
    # signal_codes：监控表中周期性监控code信号
    signal_codes = QtCore.pyqtSignal(list)
    signal_scan_freq = QtCore.pyqtSignal(int)
    # signal_one_time_codes:一次性获取非周期code信号（当前用于转债基本信息表计算折溢价使用）
    signal_one_time_codes = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(WatchDogMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_watchdog()
        self.ui.setupUi(main_window)

        # 周期性监控转债表cbwatchdog.csv，该csv在运行目录下
        self.csv = "cbwatchdog.csv"

        # 周期性监控code_list
        self.need_watch_codes = []

        # 当前为初始化时调用jq 一次性获取所有股票基本信息,该操作必须在windows窗口初始化之前，后续初始化需要用到股票基本信息表
        # self.df_stock_basic jq获取的股票基本信息表
        self.df_stock_basic = pd.DataFrame()
        self.get_stock_basic_info()

        # 初始化监控窗口
        self.init_alarm_windows()

        # self.df_cb_stock_total该df为转债与正股信息合并形成的一个完整df，包含了转债code与正股code，用于获取行情使用
        # 初始化转债基本信息表窗口
        self.df_cb_stock_total = pd.DataFrame()
        self.init_basic_cb_table()

        #
        # 监控信息表相关设置，信号与slot函数关系设置
        # 设置默认扫描周期为30秒
        # pushButton_4：增加监控表一行
        # pushButton_5：完成监控信息添加
        # pushButton：行情获取线程运行按钮
        # pushButton_2：行情获取线程暂停按钮
        # pushButton_11：更新周期性获取行情的code
        # pushButton_10：删除监控表中的行
        # pushButton_3：将监控窗口的内容转存到csv，本地保存
        #
        self.ui.comboBox.setCurrentIndex(3)

        self.ui.pushButton_4.clicked.connect(self.add_one_row)
        self.ui.pushButton_5.clicked.connect(self.finish_add_info)

        the_quote_thread = GetQuoteThread()
        self.signal_codes.connect(the_quote_thread.set_quote_codes)
        self.signal_thread_run.connect(the_quote_thread.set_run_or_stop)
        self.ui.comboBox.currentTextChanged.connect(the_quote_thread.set_quote_scan_freq)

        the_quote_thread.signal_quotation.connect(self.update_realtime_price)

        self.ui.pushButton.clicked.connect(self.click_to_start_thread)
        self.ui.pushButton_2.clicked.connect(self.click_to_stop_thread)

        self.ui.pushButton_11.clicked.connect(self.update_watch_code)
        self.ui.pushButton_10.clicked.connect(self.del_watch_stock_record)

        # 将监控窗口的信息转存到csv
        self.ui.pushButton_3.clicked.connect(self.save_watch_stock)

        #
        # 转债基本信息表相关信号与slot设置
        # pushButton_8：手动刷新转债基本信息表（更新折溢价情况）
        # 非周期获取code list signal连接行情获取线程的get_one_time_quote
        self.ui.pushButton_8.clicked.connect(self.emit_cb_basic_code)
        self.signal_one_time_codes.connect(the_quote_thread.get_one_time_quote)
        the_quote_thread.signal_one_time_quotation.connect(self.update_cb_basic_table)

        #
        # pushButton_9:将转债基本行情表中的转债添加到监控栏目
        #
        self.ui.pushButton_9.clicked.connect(self.add_bond_to_watch)

        self.emit_init_code_info()

        main_window.show()
        sys.exit(app.exec_())
    # ====================================

    def init_alarm_windows(self):
        """
        初始化监控预警窗口
        读取运行目录下watchdog.csv文件
        初始到预警窗口中
        :return:
        """
        if not os.path.exists(self.csv):
            msgbox = QtWidgets.QMessageBox()
            msgbox.setText("without cbwatchdog.csv in dir")
            msgbox.exec_()
            return
        column_name = ["code", "name", "low", "high", "if_win", "c1"]
        df_csv = pd.read_csv(self.csv, index_col=["code"], encoding="gbk", header=0, names=column_name)
        df_csv.index = df_csv.index.map(self.standard_code)
        if df_csv.empty:
            print("csv 文件为空")
            return

        # 添加需要监控的股票code，即将读取的csv的code转换成list作为初始监控股票code list
        self.need_watch_codes = df_csv.index.tolist()

        self.ui.tableWidget.setRowCount(len(df_csv))
        irow = 0
        for index, row in df_csv.iterrows():
            item = QtWidgets.QTableWidgetItem(str(index))
            self.ui.tableWidget.setItem(irow, 0, item)

            item = QtWidgets.QTableWidgetItem(str(row["name"]))
            self.ui.tableWidget.setItem(irow, 1, item)

            item = QtWidgets.QTableWidgetItem(str(row["low"]))
            self.ui.tableWidget.setItem(irow, 2, item)

            item = QtWidgets.QTableWidgetItem(str(row["high"]))
            self.ui.tableWidget.setItem(irow, 3, item)
            item = QtWidgets.QTableWidgetItem(str(row["if_win"]))
            self.ui.tableWidget.setItem(irow, 5, item)

            item = QtWidgets.QTableWidgetItem()
            if row["if_win"] == 1:
                item.setCheckState(QtCore.Qt.Checked)
                self.ui.tableWidget.setItem(irow, 5, item)
            else:
                # 即没有明确弹窗的，一律认为不做弹窗处理
                item.setCheckState(QtCore.Qt.Unchecked)
                self.ui.tableWidget.setItem(irow, 5, item)
            irow = irow + 1

    # ==============
    def init_basic_cb_table(self):
        """
        读取可转债基本信息表
        :return:
        """
        convert_basic_file = r'raw_data.csv'
        columns_name = ['bond_code', 'bond_name', 'stock_name', 'pb', 'convert_price', 'expire_date']
        df_convert = pd.read_csv(convert_basic_file, usecols=[0, 1, 4, 7, 8, 18], encoding="gbk", names=columns_name,
                                 header=0, index_col=["bond_code"])
        df_convert["stock_name"] = df_convert["stock_name"].map(lambda x: str(x).replace("?R", ""))
        df_convert["convert_price"] = df_convert["convert_price"].map(lambda x: str(x).split("*")[0])
        df_convert["convert_price"] = pd.to_numeric(df_convert["convert_price"])
        df_convert.index = df_convert.index.map(self.standard_code)
        #
        # 计算波动性需要查询200多只股票的数年k线，会占用大量的时间和数据流量，波动率不是实时数据，所以这里读取计算好的波动率数据
        #
        # 波动率表
        # 列名："bond_code", "bond_name", "stock_name", "parkinson", "garmanKlass",
        # "rogersSatchell", "garmanKlassYang", "yangZhang", "normal"
        #
        # 实际没有读取"bond_name", "stock_name"，这两列信息在转债基本信息表中有
        #
        df_volatility = pd.read_csv(r'bond_volatility.csv', index_col=["bond_code"], encoding="gbk",
                                    usecols=[0, 3, 4, 5, 6, 7, 8])
        df_volatility.index = df_volatility.index.map(self.standard_code)

        df_total = pd.merge(df_convert, df_volatility, left_index=True, right_index=True)
        #
        # 再次合并，加入stock_name 到stock_code的关系
        # df_stock_basic: stock_code,display_name
        #
        df_total = pd.merge(df_total, self.df_stock_basic, left_on="stock_name", right_index=True)
        # print(df_total.columns)
        # 去掉不许留存用于计算溢价等的列
        drop_list = ['bond_name', 'stock_name', 'pb', 'expire_date', "parkinson", "garmanKlass",
                     "rogersSatchell", "garmanKlassYang", "yangZhang", "normal"]

        #
        # self.df_cb_stock_total 列名：
        # bond_code, stock_code, convert_price
        #
        self.df_cb_stock_total = df_total.drop(columns=drop_list)

        if len(df_convert) != len(df_total):
            msgbox = QtWidgets.QMessageBox()
            msgbox.setText("convertbond表合并波动率表及股票基本信息表过程中有信息丢失，转债基本信息表：%d,合并后：%d" %
                           (len(df_convert), len(df_total)))
            msgbox.exec_()

        volatility_75 = df_volatility["normal"].quantile(0.75)
        if df_convert.empty:
            print("csv 文件为空")
            return
        irow = 0
        self.ui.tableWidget_2.setRowCount(len(df_total))
        for index, row in df_total.iterrows():
            item = QtWidgets.QTableWidgetItem(str(index))
            self.ui.tableWidget_2.setItem(irow, 0, item)
            item = QtWidgets.QTableWidgetItem(row["bond_name"])
            self.ui.tableWidget_2.setItem(irow, 1, item)
            item = QtWidgets.QTableWidgetItem(str(row["convert_price"]))
            self.ui.tableWidget_2.setItem(irow, 3, item)
            item = QtWidgets.QTableWidgetItem(str(row["pb"]))
            self.ui.tableWidget_2.setItem(irow, 4, item)
            item = QtWidgets.QTableWidgetItem(row["expire_date"])
            self.ui.tableWidget_2.setItem(irow, 5, item)
            item = QtWidgets.QTableWidgetItem(str(round(row["normal"], 4)))
            if row["normal"] > volatility_75:
                brush = QtGui.QBrush(QtGui.QColor(170, 0, 0))
                brush.setStyle(QtCore.Qt.NoBrush)
                item.setForeground(brush)
            self.ui.tableWidget_2.setItem(irow, 7, item)
            irow = irow + 1

    # ==============
    def emit_cb_basic_code(self):
        """
        将转债基本信息表中的转债code与正股code 发送到行情获取线程
        :return:
        """
        code_list = self.df_cb_stock_total.index.tolist() + self.df_cb_stock_total["stock_code"].tolist()
        self.signal_one_time_codes.emit(code_list)

    # ============
    def update_cb_basic_table(self, price_list):
        """
        更新转债基本信息表，实际是一个slot函数，接收到行情线程返回的行情信息后更新
        :param price_list:行情线程发射（signal_one_time_quotation）过来的实时价格list
        :return:
        """
        rows = self.ui.tableWidget_2.rowCount()
        if len(price_list) != 2*rows:
            print("行情返回的价格行情数量与转债基本信息表不匹配，转债表：%d行，返回价格数量:%d" % (rows, len(price_list)/2))
            return
        bond_price_list = price_list[0: int(len(price_list)/2)]
        stock_price_list = price_list[int(len(price_list)/2):]
        self.df_cb_stock_total["bond_price"] = bond_price_list
        self.df_cb_stock_total["stock_price"] = stock_price_list
        # 转股价值=（100/转股价 * 实时正股价格 ）
        # premium=（转债实时价格 - 转股价值 ）/ 转债实时价格
        self.df_cb_stock_total["convert_value"] = 100 / self.df_cb_stock_total["convert_price"] * \
                                                  self.df_cb_stock_total["stock_price"]
        self.df_cb_stock_total["premium"] = (self.df_cb_stock_total["bond_price"] -
                                             self.df_cb_stock_total["convert_value"]) / \
                                            self.df_cb_stock_total["convert_value"]
        # print(self.df_cb_stock_total.head(3))
        i_row = 0
        for index, row in self.df_cb_stock_total.iterrows():
            bond_code = self.ui.tableWidget_2.item(i_row, 0).text()
            if bond_code != index:
                print("获取的行情返回bond_code：%s, 表格中的bond_code:%s,不一致" % (index, bond_code))
                continue
            item = QtWidgets.QTableWidgetItem(str(row["bond_price"]))
            self.ui.tableWidget_2.setItem(i_row, 2, item)

            premium = round(row["premium"]*100, 2)
            item = QtWidgets.QTableWidgetItem(str(premium))
            # 折价的转债标红
            if row["premium"] < 0:
                brush = QtGui.QBrush(QtGui.QColor(170, 0, 0))
                brush.setStyle(QtCore.Qt.NoBrush)
                item.setForeground(brush)
            self.ui.tableWidget_2.setItem(i_row, 6, item)
            i_row = i_row + 1

    # =============

    def get_stock_basic_info(self):
        """
        获取股票基本信息
        这里暂时选用的jq获取股票基本信息
        :return:
        stock_code         display_name  name start_date   end_date   type
    0  000001.XSHE         平安银行  PAYH 1991-04-03 2200-01-01  stock
    1  000002.XSHE          万科A   WKA 1991-01-29 2200-01-01  stock
    2  000004.XSHE         国农科技  GNKJ 1990-12-01 2200-01-01  stock
    3  000005.XSHE         世纪星源  SJXY 1990-12-10 2200-01-01  stock
    4  000006.XSHE         深振业A  SZYA 1992-04-27 2200-01-01  stock
        """
        df_stock = get_all_securities(types=['stock'], date=None)
        df_stock.index.name = "stock_code"
        df_stock = df_stock.reset_index()
        df_stock = df_stock.drop(columns=["name", "start_date", "end_date", "type"])
        df_stock["stock_code"] = df_stock["stock_code"].map(lambda x: str(x)[0:6])
        self.df_stock_basic = df_stock.set_index("display_name")
        # print(self.df_stock_basic.head(5))

    # ==================
    def emit_init_code_info(self):
        """
        将初始化后的code信息发射出去
        :return:
        """
        self.signal_codes.emit(self.need_watch_codes)

    # ===============

    def add_one_row(self):
        """
        添加一只需要监控的股票
        该方式适用于在运行中临时增加一只需要监控的股票
        注意：
        1、添加一行监控内容
        2、自主选择是否存到如csv保存
        :return:
        """
        curr_row = self.ui.tableWidget.rowCount()
        self.ui.tableWidget.insertRow(curr_row)
        item = QtWidgets.QCheckBox()
        item.setCheckState(QtCore.Qt.Unchecked)
        self.ui.tableWidget.setCellWidget(curr_row, 5, item)

    # ====================
    def finish_add_info(self):
        """
        完成临时监控信息的添加
        :return:
        """
        item = self.ui.tableWidget.item(self.ui.tableWidget.rowCount()-1, 0)
        # 把新添加的股票code放到监控code list中
        self.need_watch_codes.append(item.text())

        # self.signal_codes.emit(self.need_watch_codes)
        # print(self.need_watch_codes)
        # self.signal_thread_run.emit(True)

    # =================
    def update_watch_code(self):
        """
        更新监控列表到行情监控线程
        :return:
        """
        self.signal_thread_run.emit(False)
        self.signal_codes.emit(self.need_watch_codes)
        self.signal_thread_run.emit(True)

    # ======================

    def click_to_start_thread(self):
        self.signal_thread_run.emit(True)
    # ===================

    def click_to_stop_thread(self):
        self.signal_thread_run.emit(False)

    # =======
    def set_quote_scan_freq(self):
        self.signal_scan_freq.emit(int(self.ui.comboBox.currentText()))

    # =========
    @staticmethod
    def standard_code(x):
        """
        标准化code代码
        深市代码补充0
        :param x: str
        :return: 标准code代码
        """
        return str(x).zfill(6)

    # ========
    def update_realtime_price(self, price_list):
        """
        将行情获取的实时价格更新到表格，并判断是否到达预警价格
        :param price_list:行情线程发射（signal_quotation）过来的实时价格list
        :return:
        """
        curr_row = self.ui.tableWidget.rowCount()
        if len(price_list) != curr_row:
            print("实时行情获取的价格数据与表格行数不匹配，表格：%d行，price个数：%d" % (curr_row, len(price_list)))
            box = QtWidgets.QMessageBox()
            box.setText("实时行情获取的价格数据与表格行数不匹配")
            box.exec_()
            return
        alarm_list = []
        curr_time = datetime.now().time().strftime("%H:%M:%S")
        set_red = False
        for i in range(len(price_list)):
            price = price_list[i]

            item = self.ui.tableWidget.item(i, 2)
            alarm_price_low = float(item.text())

            if price < alarm_price_low:
                set_red = True
                if_windows = self.ui.tableWidget.item(i, 5).checkState()
                # print("%s, if_windows:%d" % (self.ui.tableWidget.item(i, 1).text(), if_windows))
                if if_windows == QtCore.Qt.Checked:
                    item = self.ui.tableWidget.item(i, 1)
                    alarm_list.append(item.text() + "当前价格：" + str(price) + "< 低价预警:" + str(alarm_price_low) +
                                      ", " + "买入！！！")

            item = self.ui.tableWidget.item(i, 3)
            alarm_price_high = float(item.text())
            if price > alarm_price_high:
                set_red = True
                if_windows = self.ui.tableWidget.item(i, 5).checkState()
                # print("%s, if_windows:%d" % (self.ui.tableWidget.item(i, 1).text(), if_windows))
                if if_windows == QtCore.Qt.Checked:
                    item = self.ui.tableWidget.item(i, 1)
                    alarm_list.append(item.text() + "当前价格：" + str(price) + "> 高价预警:" + str(alarm_price_high) +
                                      ", " + "卖出！！！")
            item = QtWidgets.QTableWidgetItem(str(price))
            if set_red:
                brush = QtGui.QBrush(QtGui.QColor(170, 0, 0))
                brush.setStyle(QtCore.Qt.NoBrush)
                item.setForeground(brush)
            self.ui.tableWidget.setItem(i, 4, item)
            set_red = False

            item = QtWidgets.QTableWidgetItem(curr_time)
            self.ui.tableWidget.setItem(i, 6, item)

        if len(alarm_list) > 0:
            msgbox = QtWidgets.QMessageBox()
            alarm_info = "\\n"
            for tmp in alarm_list:
                alarm_info = alarm_info + tmp + ","
            msgbox.setText(alarm_info)
            msgbox.exec_()

    # ============================
    def add_bond_to_watch(self):
        """
        添加转债到监控列表
        :return:
        """
        position = self.ui.tableWidget_2.selectedItems()
        if len(position) == 0:
            return
        self.add_select_item_to_watch(position)
        self.ui.tableWidget_2.clearSelection()
    # =============================

    def add_select_item_to_watch(self, qlist_items):
        """
        将选中的items添加到监控窗口
        :param qlist_items:
        :return:
        """
        row = int(len(qlist_items) / 8)
        for i in range(row):
            code = qlist_items[8 * i]
            name = qlist_items[8 * i + 1]
            # print("code:%s" % str(code.text()))
            if str(code.text()) in self.need_watch_codes:
                msg_box = QtWidgets.QMessageBox()
                msg_box.setText("name:%s已在监控窗口中" % name.text())
                msg_box.exec_()
                continue
            curr_row = self.ui.tableWidget.rowCount()
            self.ui.tableWidget.insertRow(curr_row)
            item = QtWidgets.QTableWidgetItem(code.text())
            self.ui.tableWidget.setItem(curr_row, 0, item)
            self.need_watch_codes.append(code.text())
            item = QtWidgets.QTableWidgetItem(name.text())
            self.ui.tableWidget.setItem(curr_row, 1, item)
            # 预设一个高价1000，低价1预警
            item = QtWidgets.QTableWidgetItem("0.1")
            self.ui.tableWidget.setItem(curr_row, 2, item)
            item = QtWidgets.QTableWidgetItem("1000")
            self.ui.tableWidget.setItem(curr_row, 3, item)

            item = QtWidgets.QTableWidgetItem()
            item.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget.setItem(curr_row, 5, item)

    # ==================
    def save_watch_stock(self):
        """
        将监控窗口的股票存到csv文件保存
        :return:
        """
        column_name = ["code", "name", "低价预警", "高价预警", "是否弹窗"]
        data_list = []
        # print("watch table row:%d" % self.ui.tableWidget.rowCount())
        for i in range(self.ui.tableWidget.rowCount()):
            row_record = []
            for j in range(self.ui.tableWidget.columnCount()-3):
                item = self.ui.tableWidget.item(i, j).text()
                row_record.append(item)
            # print("watch table:%r" % type(self.ui.tableWidget.item(i, 5)))
            # print("watch code:%s" % self.ui.tableWidget.item(i, 1).text())
            if self.ui.tableWidget.item(i, 5).checkState() == QtCore.Qt.Checked:
                row_record.append("1")
            else:
                row_record.append("0")
            data_list.append(row_record)
        df = pd.DataFrame(data=data_list, columns=column_name)
        # print(df)
        df.to_csv("watchdog.csv", index=False, encoding="gbk")

    # ================
    def del_watch_stock_record(self):
        """
        删除监控窗口的记录
        :return:
        """
        record = self.ui.tableWidget.selectedItems()
        if len(record) == 0:
            return
        del_row = []
        for i in range(len(record)):
            row = record[i].row()
            del_row.append(row)

        del_row = set(del_row)
        for row in del_row:
            item = self.ui.tableWidget.item(row, 0).text()
            self.need_watch_codes.remove(item)
        for row in del_row:
            self.ui.tableWidget.removeRow(row)
        # 删除完手动更新列表，防止在更新实时价格的时候同时更新监控列表
        # self.update_watch_code()


if __name__ == "__main__":
    watchdog = WatchDogMainWindows()