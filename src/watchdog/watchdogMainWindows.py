# -*-coding:utf-8 -*-
__author__ = 'liao'

from ui_watchdog import Ui_watchdog
from PyQt5 import QtCore, QtWidgets, QtGui
import sys
import os
import pandas as pd
from datetime import datetime
from real_quote_thread import GetQuoteThread


class WatchDogMainWindows(QtCore.QObject):
    signal_thread_run = QtCore.pyqtSignal(bool)
    signal_codes = QtCore.pyqtSignal(list)
    signal_scan_freq = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        super(WatchDogMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_watchdog()
        self.ui.setupUi(main_window)
        self.csv = "watchdog.csv"
        self.need_watch_codes = []
        self.init_alarm_windows()

        self.process_position()

        # 设置默认扫描周期为30秒
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
        self.ui.pushButton_9.clicked.connect(self.add_position_stock_to_watch)
        self.ui.pushButton_10.clicked.connect(self.del_watch_stock_record)

        # 将监控窗口的信息转存到csv
        self.ui.pushButton_3.clicked.connect(self.save_watch_stock)

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
            msgbox.setText("without watchdog.csv in dir")
            msgbox.exec_()
            return
        column_name = ["code", "name", "low", "high", "if_win"]
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

    # ============================
    def add_position_stock_to_watch(self):
        """
        添加持仓股票到监控列表
        :return:
        """
        self.add_ht48_position_to_watch()
        self.add_gs_position_to_watch()
        self.add_ht49_position_to_watch()
        self.add_zs_position_to_watch()

        # 添加完手动更新列表，防止在更新实时价格的时候同时更新监控列表
        # self.update_watch_code()

    # ===========================

    def add_ht48_position_to_watch(self):
        """
        添加华泰持仓到监控窗口
        :return:
        """
        position = self.ui.tableWidget_2.selectedItems()
        if len(position) == 0:
            return
        self.add_select_item_to_watch(position)
        self.ui.tableWidget_2.clearSelection()

    # ============================
    def add_select_item_to_watch(self, qlist_items):
        """
        将选中的items添加到监控窗口
        :param qlist_items: 获取的选中的items
        :return:
        """
        # print("selected item:%d" % len(qlist_items))
        row = int(len(qlist_items) / 5)
        for i in range(row):
            code = qlist_items[5 * i]
            name = qlist_items[5 * i + 1]
            # print("code:%s" % str(code.text()))
            if str(code.text()) in self.need_watch_codes:
                continue
            curr_row = self.ui.tableWidget.rowCount()
            self.ui.tableWidget.insertRow(curr_row)
            item = QtWidgets.QTableWidgetItem(code.text())
            self.ui.tableWidget.setItem(curr_row, 0, item)
            self.need_watch_codes.append(code.text())
            item = QtWidgets.QTableWidgetItem(name.text())
            self.ui.tableWidget.setItem(curr_row, 1, item)
            # 预设一个高价1000，低价1预警
            item = QtWidgets.QTableWidgetItem("1")
            self.ui.tableWidget.setItem(curr_row, 2, item)
            item = QtWidgets.QTableWidgetItem("1000")
            self.ui.tableWidget.setItem(curr_row, 3, item)

            item = QtWidgets.QTableWidgetItem()
            item.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget.setItem(curr_row, 5, item)

    # ===========================

    def add_ht49_position_to_watch(self):
        """
        添加华泰持仓到监控窗口
        :return:
            """
        position = self.ui.tableWidget_3.selectedItems()
        if len(position) == 0:
            return
        self.add_select_item_to_watch(position)
        self.ui.tableWidget_3.clearSelection()
    # ============================

    def add_zs_position_to_watch(self):
        """
        添加招商持仓到监控窗口
        :return:
        """
        position = self.ui.tableWidget_5.selectedItems()
        if len(position) == 0:
            return
        self.add_select_item_to_watch(position)
        self.ui.tableWidget_5.clearSelection()
    # =============================

    def add_gs_position_to_watch(self):
        """
        添加国盛持仓到监控
        :return:
        """
        position = self.ui.tableWidget_4.selectedItems()
        if len(position) == 0:
            return
        self.add_select_item_to_watch(position)
        self.ui.tableWidget_4.clearSelection()

    # ============================

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

    # ===============
    def process_position(self):
        """
        处理持仓信息
        :return:
        """
        self.get_ht48_position()
        self.get_ht49_position()
        self.get_gs_position()
        self.get_zs_position()

    # ===========
    def get_ht48_position(self):
        """
        读取ht48持仓信息
        :return:
        """
        ht_48 = "ht_48.csv"
        ht_columns_names = ["code", "name", "amount", "available_amount", "c1", "cost_price", "c3", "c4", "c5", "c6",
                            "total_values", "c7", "c8", "c9"]
        display_column = ["code", "name", "cost_price", "amount", "total_values"]

        if os.path.exists(ht_48):
            df_position = pd.read_csv(ht_48, header=0, names=ht_columns_names, encoding="gbk")
            df_position = df_position.sort_values(by=["total_values"], ascending=False)
            self.ui.tableWidget_2.setRowCount(len(df_position))
            i_row = 0
            for index, row in df_position.iterrows():
                for column_index, column in enumerate(display_column):
                    if column == "cost_price":
                        item = QtWidgets.QTableWidgetItem(str(round(row[column], 3)))
                    else:
                        item = QtWidgets.QTableWidgetItem(str(row[column]))
                    self.ui.tableWidget_2.setItem(i_row, column_index, item)
                i_row = i_row + 1
        else:
            print("%s 文件不存在" % ht_48)

    # ================
    def get_ht49_position(self):
        """
        获取ht49持仓信息
        :return:
        """
        ht_49 = "ht_49.csv"
        ht_columns_names = ["code", "name", "amount", "available_amount", "c1", "cost_price", "c3", "c4", "c5", "c6",
                            "total_values", "c7", "c8", "c9"]
        display_column = ["code", "name", "cost_price", "amount", "total_values"]

        if os.path.exists(ht_49):
            df_position = pd.read_csv(ht_49, header=0, names=ht_columns_names, encoding="gbk")
            df_position = df_position.sort_values(by=["total_values"], ascending=False)
            self.ui.tableWidget_3.setRowCount(len(df_position))
            i_row = 0
            for index, row in df_position.iterrows():
                for column_index, column in enumerate(display_column):
                    if column == "cost_price":
                        item = QtWidgets.QTableWidgetItem(str(round(row[column], 3)))
                    else:
                        item = QtWidgets.QTableWidgetItem(str(row[column]))
                    self.ui.tableWidget_3.setItem(i_row, column_index, item)
                i_row = i_row + 1
        else:
            print("%s 文件不存在" % ht_49)

    # =====================

    def get_gs_position(self):
        """
        获取国盛的持仓信息
        :return:
        """
        gs = "gs.csv"
        gs_columns_names = ["code", "name", "amount", "available_amount", "cost_price", "c3",
                            "total_values", "c5", "c6", "c7", "c8", "c9"]
        display_column = ["code", "name", "cost_price", "amount", "total_values"]
        if os.path.exists(gs):
            df_position = pd.read_csv(gs, header=0, names=gs_columns_names, encoding="gbk", skiprows=3)
            df_position = df_position.sort_values(by=["total_values"], ascending=False)
            self.ui.tableWidget_4.setRowCount(len(df_position))
            i_row = 0
            for index, row in df_position.iterrows():
                for column_index, column in enumerate(display_column):
                    if column == "cost_price":
                        item = QtWidgets.QTableWidgetItem(str(round(row[column], 3)))
                    else:
                        item = QtWidgets.QTableWidgetItem(str(row[column]))
                    self.ui.tableWidget_4.setItem(i_row, column_index, item)
                i_row = i_row + 1
        else:
            print("%s 文件不存在" % gs)

    # ===========================
    def get_zs_position(self):
        """
        获取招商的持仓信息
        :return:
        """
        gs = "zs.csv"
        gs_columns_names = ["name", "amount", "available_amount", "c1", "cost_price", "c3", "c4", "c5",
                            "total_values",  "c6", "c7", "c8", "code", "c9", "c10", "c11"]
        display_column = ["code", "name", "cost_price", "amount", "total_values"]
        if os.path.exists(gs):
            df_position = pd.read_csv(gs, header=0, names=gs_columns_names, encoding="gbk", skiprows=3)
            df_position = df_position.sort_values(by=["total_values"], ascending=False)
            self.ui.tableWidget_5.setRowCount(len(df_position))
            # print(df_position)
            i_row = 0
            for index, row in df_position.iterrows():
                for column_index, column in enumerate(display_column):
                    if column == "cost_price":
                        item = QtWidgets.QTableWidgetItem(str(round(row[column], 3)))
                    else:
                        item = QtWidgets.QTableWidgetItem(str(row[column]))
                    self.ui.tableWidget_5.setItem(i_row, column_index, item)
                i_row = i_row + 1
        else:
            print("%s 文件不存在" % gs)

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