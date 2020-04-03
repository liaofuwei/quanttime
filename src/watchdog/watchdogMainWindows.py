# -*-coding:utf-8 -*-
__author__ = 'liao'

from ui_watchdog import Ui_watchdog
from PyQt5 import QtCore, QtWidgets
import sys
import os
import pandas as pd
from datetime import datetime
from real_quote_thread import GetQuoteThread


class WatchDogMainWindows(QtCore.QObject):
    signal_thread_run = QtCore.pyqtSignal(bool)
    signal_codes = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(WatchDogMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_watchdog()
        self.ui.setupUi(main_window)
        self.csv = "watchdog.csv"
        self.need_watch_codes = []
        self.init_alarm_windows()

        self.ui.pushButton_4.clicked.connect(self.add_one_row)
        self.ui.pushButton_5.clicked.connect(self.finish_add_info)

        the_quote_thread = GetQuoteThread()
        self.signal_codes.connect(the_quote_thread.set_quote_codes)
        self.signal_thread_run.connect(the_quote_thread.set_run_or_stop)

        self.emit_init_code_info()

        the_quote_thread.signal_quotation.connect(self.update_realtime_price)

        self.ui.pushButton.clicked.connect(self.click_to_start_thread)
        self.ui.pushButton_2.clicked.connect(self.click_to_stop_thread)

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
            msgbox.setText("without watchdog.csv in dir")
            msgbox.exec_()
            return
        df_csv = pd.read_csv(self.csv, index_col=["code"], encoding="gbk")
        df_csv.index = df_csv.index.map(self.standard_code)
        if df_csv.empty:
            print("csv 文件为空")
            return
        for index, row in df_csv.iterrows():
            self.need_watch_codes.append((row["market"], index))

        self.ui.tableWidget.setRowCount(len(df_csv))
        irow = 0
        for index, row in df_csv.iterrows():
            item = QtWidgets.QTableWidgetItem(str(index))
            self.ui.tableWidget.setItem(irow, 0, item)
            item = QtWidgets.QTableWidgetItem(str(row["market"]))
            self.ui.tableWidget.setItem(irow, 1, item)
            item = QtWidgets.QTableWidgetItem(str(row["name"]))
            self.ui.tableWidget.setItem(irow, 2, item)
            item = QtWidgets.QTableWidgetItem(str(row["alarm price low"]))
            self.ui.tableWidget.setItem(irow, 3, item)
            item = QtWidgets.QTableWidgetItem(str(row["alarm price high"]))
            self.ui.tableWidget.setItem(irow, 4, item)
            item = QtWidgets.QTableWidgetItem(str(row["div10"]))
            self.ui.tableWidget.setItem(irow, 6, item)

            item = QtWidgets.QCheckBox()
            item.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget.setCellWidget(irow, 7, item)
            irow = irow + 1

    # ============

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
        1、添加时，先暂停实时行情的运行
        2、在添加完成后，自动运行实时行情
        3、自主选择是否存到如csv保存
        :return:
        """
        self.signal_thread_run.emit(False)
        curr_row = self.ui.tableWidget.rowCount()
        self.ui.tableWidget.insertRow(curr_row)
        item = QtWidgets.QCheckBox()
        item.setCheckState(QtCore.Qt.Unchecked)
        self.ui.tableWidget.setCellWidget(curr_row, 5, item)

    # ====================
    def finish_add_info(self):
        """
        完成临时监控信息的添加
        启动实时行情线程
        :return:
        """
        item = self.ui.tableWidget.item(self.ui.tableWidget.rowCount()-1, 0)
        market = self.ui.tableWidget.item(self.ui.tableWidget.rowCount()-1, 1)
        self.need_watch_codes.append((int(market.text()), item.text()))
        self.signal_codes.emit(self.need_watch_codes)
        print(self.need_watch_codes)
        self.signal_thread_run.emit(True)

    # ======================

    def click_to_start_thread(self):
        self.signal_thread_run.emit(True)
    # ===================

    def click_to_stop_thread(self):
        self.signal_thread_run.emit(False)

    # =======
    @staticmethod
    def standard_code(x):
        '''
        标准化code代码
        深市代码补充0
        :param x: str
        :return: 标准code代码
        '''
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
            return
        alarm_list = []
        curr_time = datetime.now().time().strftime("%H:%M:%S")
        for i in range(len(price_list)):
            price = price_list[i]
            item = self.ui.tableWidget.item(i, 3)
            alarm_price_low = float(item.text())

            if int(self.ui.tableWidget.item(i, 6).text()) == 1:
                price = price / 10

            if price < alarm_price_low:
                item = self.ui.tableWidget.item(i, 2)
                alarm_list.append(item.text()+"low alarm")
            item = self.ui.tableWidget.item(i, 4)
            alarm_price_high = float(item.text())
            if price > alarm_price_high:
                item = self.ui.tableWidget.item(i, 2)
                alarm_list.append(item.text() + "high alarm")
            item = QtWidgets.QTableWidgetItem(str(price))
            self.ui.tableWidget.setItem(i, 5, item)

            item = QtWidgets.QTableWidgetItem(curr_time)
            self.ui.tableWidget.setItem(i, 8, item)

        if len(alarm_list) > 0:
            msgbox = QtWidgets.QMessageBox()
            alarm_info = ""
            for tmp in alarm_list:
                alarm_info = alarm_info + tmp + ","
            msgbox.setText(alarm_info)
            msgbox.exec_()


if __name__ == "__main__":
    watchdog = WatchDogMainWindows()