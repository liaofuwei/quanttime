# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from ui_finance_indicator_analyse import Ui_MainWindow
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWebEngineWidgets import QWebEngineView
import sys
import os
import pandas as pd
from finance_indicator_analyse_report import FinanceAnalyseThread


class FinanceAnalyseMainWindows(QtCore.QObject):
    signal_list_indicator = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(FinanceAnalyseMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(main_window)
        self.ui.lineEdit_5.setPlaceholderText(u"请输入股票代码")
        self.ui.pushButton.clicked.connect(self.select_target_stock)

        analyse_thread = FinanceAnalyseThread()
        self.signal_list_indicator.connect(analyse_thread.select_ts_target_stock)
        analyse_thread.df_select_indicator_out.connect(self.display_selected_stock)

        main_window.show()
        sys.exit(app.exec_())
    # ====================================

    def select_target_stock(self):
        """
        将主页面的设置的筛选条件信号发射到筛选线程进行操作
        signal:[pe,pb,roe,净利润增速]的顺序,不设置某个选项
        :return:
        """
        pe = self.ui.lineEdit.text()
        pb = self.ui.lineEdit_2.text()
        roe = self.ui.lineEdit_3.text()
        net_profile_increase = self.ui.lineEdit_4.text()
        signal = [pe, pb, roe, net_profile_increase]
        self.signal_list_indicator.emit(signal)
    # ==========================================

    def display_selected_stock(self, df_select):
        """
        显示精选后的股票
        :param df_select: 由筛选处理后发射的信号，type为pd.DataFrame
        ['code', 'name', 'pb', 'pe', 'pe_ttm', 'roe3', 'roe5', 'roe', 'net_profit', 'net_profit3',
        'net_profit5', 'debt_asset', 'ocfps']
        :return:
        """
        self.ui.tableWidget.clearContents()
        if df_select.empty:
            return
        self.ui.tableWidget.setRowCount(len(df_select))

        for index, row in df_select.iterrows():
            j = 0
            for row_item in row:
                if isinstance(row_item, float):
                    row_item = round(row_item, 2)
                item = QtWidgets.QTableWidgetItem(str(row_item))
                self.ui.tableWidget.setItem(index, j, item)
                j = j+1



if __name__ == "__main__":
    FinanceAnalyseMainWindows()