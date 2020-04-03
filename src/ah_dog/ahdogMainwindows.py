# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from ui_ahdogMainWindows import Ui_ahdog
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWebEngineWidgets import QWebEngineView
import sys
import os
import pandas as pd
import tushare as ts
import math
from datetime import datetime
from pyecharts import Line,Kline,Bar
import numpy as np
from ahcompare_stat import AHCompareStat


class AHdogMainWindows(QtCore.QObject):

    def __init__(self, parent=None):
        super(AHdogMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_ahdog()
        self.ui.setupUi(main_window)
        #tab2中的相关操作
        self.get_all_AH_stock()
        ah_stat = AHCompareStat()
        self.ui.comboBox.currentTextChanged.connect(ah_stat.ah_compare_stat)
        ah_stat.html_display_out.connect(self.display_ah_stat_line)
        ah_stat.ah_stat_out.connect(self.display_ah_stat_table)

        main_window.show()
        sys.exit(app.exec_())
    # ====================================

    def get_all_AH_stock(self):
        """
        初始化复选框中的AHcode
        :return:
        """
        ah_info = r'C:\quanttime\data\AH_ratio\AH_code.csv'
        columns_name = ["name", "a_code", "h_code", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
        df_ah_code = pd.read_csv(ah_info, encoding="gbk", header=None, names=columns_name)
        df_ah_code["a_code"] = df_ah_code["a_code"].map(self.stander_code)
        df_ah_code = df_ah_code.set_index("a_code")
        for row in df_ah_code.itertuples():
            item = str(row.Index) + ' ' + row.name + ' ' + str(row.h_code)
            self.ui.comboBox.addItem(item)
    # =======================================

    def display_ah_stat_line(self, int):
        """
        显示程序当前目录下分析生成的html文件
        :return:
        """
        ah_stat_html = QWebEngineView(self.ui.tab_2)
        ah_stat_html.load(QtCore.QUrl("file:///ah_compare_line.html"))
        ah_stat_html.setGeometry(QtCore.QRect(50, 310, 1491, 421))
        ah_stat_html.show()

    # ========================================

    def display_ah_stat_table(self, list_stat):
        """
        显示ah统计信息
        :param list_stat: 由计算函数发射过来的信号
        list 的结构为[[],[]]
        其中第一个[]：name, acode, ah_compare.round(2), a_price, h_price
        第二个[]: mean_data, data_5, data_10, data_75, data_90, max_data, min_data, std_data
        :return:
        """
        self.ui.tableWidget_2.setRowCount(1)
        self.ui.tableWidget_3.setRowCount(1)
        if len(list_stat) != 2:
            print("ah统计计算发射的信号结构有误，长度不为2")
        for i, j in enumerate(list_stat[0]):
            item = QtWidgets.QTableWidgetItem(str(j))
            self.ui.tableWidget_2.setItem(0, i, item)
        for i, j in enumerate(list_stat[1]):
            item = QtWidgets.QTableWidgetItem(str(j))
            self.ui.tableWidget_3.setItem(0, i, item)

    # ========================================

    @staticmethod
    def stander_code(code):
        """
        标准化code
        该方法仅限于深市0开头标准化
        :return:
        """
        code = str(code)
        if len(code) != 6:
            return code.zfill(6)
        else:
            return code

# =========================================


if __name__ == "__main__":
    AHdogMainWindows()