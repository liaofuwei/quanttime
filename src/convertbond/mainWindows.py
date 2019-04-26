#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import ui_convertbond as ui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import os
import pandas as pd
import numpy as np


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

        # 生信息转换pushbutton的signal连接
        self.ui.pushButton.clicked.connect(self.process_convertbond_raw_basic_info)

        # 显示可转债基本信息表pushbotton的signal连接
        self.ui.pushButton_2.clicked.connect(self.display_convert_basic_info)

        self.df_convertbond_basic_info = pd.DataFrame()

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

    def format_convert_price(self, x):
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
        print("====")
        if not os.path.exists(self.ui.lineEdit_2.text()):
            QtWidgets.QMessageBox.warning(self, "message", "可转债基本信息csv不存在",
                                          QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                                          QtWidgets.QMessageBox.Yes)
            return
        print("1111")
        use_col = ["code", "bond_name", "stock_name", "PB", "convert_price", "rank", "back_sell_price", "130_sell_price",
                   "expire", "remaining_time"]
        col_type = {"convert_price": np.float16}
        print(self.ui.lineEdit_2.text())
        df = pd.read_csv(self.ui.lineEdit_2.text(), encoding="gbk", index_col=["code"],
                         usecols=use_col, parse_dates=["expire"], dtype=col_type)
        print(df)

        self.df_convertbond_basic_info = df




if __name__ == "__main__":
    BondMainWindows().display_convert_basic_info()