# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from ui_index_selected import Ui_Dialog
from PyQt5 import QtCore, QtWidgets
import pandas as pd
import os
from opendatatools import swindex


class IndexSelected(QtCore.QObject):

    signal_select_index = QtCore.pyqtSignal(pd.DataFrame, pd.DataFrame)

    def __init__(self, parent=None):
        super(IndexSelected, self).__init__(parent)
        self.index_dialog = QtWidgets.QDialog()
        self.ui = Ui_Dialog()
        self.ui.setupUi(self.index_dialog)

        self.basic_path = r'C:\quanttime\data\index'
        self.basic_info = r'\basic_index_info'
        self.ts = r'\tushare'

        # 精选出分析的指数，set。保证唯一性
        self.set_selected_index = []

        # 从csv中读取的上次存储的精选指数列表
        self.df_init_selected_index = pd.DataFrame()
        # 从csv中读取上次存储的精选指数列表
        self.init_selected_index()

        self.init_combobox()
        self.ui.comboBox.activated.connect(self.get_index_kind_info)
        # 将左侧checkbox选定的指数经过计算后，emit到主界面
        self.ui.pushButton_2.clicked.connect(self.calc_selected_index_valuation)
        # 右侧index浏览框中，选中的index添加到左侧
        self.ui.tableWidget_2.cellClicked.connect(self.add_selected_index)
        # self.ui.tableWidget_2.cellDoubleClicked.connect(self.add_selected_index)
        # 将精选index存到csv的signal与slot连接
        self.ui.pushButton_4.clicked.connect(self.save_selected_index_2_csv)

    # =======================================

    def show_dialog(self):
        self.index_dialog.show()

    # =======================================

    def init_combobox(self):
        '''
        初始化下拉选择中的选项信息
        :return:
        '''
        self.ui.comboBox.addItem('中证指数')
        self.ui.comboBox.addItem('申万指数')
        self.ui.comboBox.addItem('上交所指数')
        self.ui.comboBox.addItem('深交所指数')
        self.ui.comboBox.addItem('jq指数')

    # =======================================

    def get_index_kind_info(self, index_info):
        '''
        combobox下拉选择后对应的slot函数，该函数用于显示某一大类指数信息
        eg：中证指数下的所有指数，申万指数，深交所指数等。。。
        :return:
        '''
        print(index_info)
        if index_info == 1:
            # 中证指数
            print("中证指数暂时不列出，太多且无估值信息")
            return
        if index_info == 2:
            # 申万指数
            df_index = pd.read_csv(r'C:\quanttime\data\index\basic_index_info\basic_index_info_sw.csv',
                                   index_col=["ts_code"], encoding="gbk")
            if df_index.empty:
                print("读取的index基本信息表为空")
                return
            self.ui.tableWidget_2.clear()
            self.ui.tableWidget_2.setRowCount(len(df_index))
            for i in range(len(df_index)):
                item = QtWidgets.QTableWidgetItem(str(df_index.index[i]))
                self.ui.tableWidget_2.setItem(i, 0, item)
                item = QtWidgets.QTableWidgetItem(str(df_index.iloc[i, 0]))
                self.ui.tableWidget_2.setItem(i, 1, item)
                item = QtWidgets.QCheckBox()
                item.setCheckState(QtCore.Qt.Unchecked)
                self.ui.tableWidget_2.setCellWidget(i, 2, item)

    # =======================================
    def init_selected_index(self):
        '''
        初始化精选指数列表
        该列表读取运行文件夹内的selected_index.csv
        :return:
        '''
        self.df_init_selected_index = pd.read_csv(r"C:\quanttime\src\watch_time\selected_index.csv",
                                               encoding="gbk", index_col=["ts_code"])
        self.ui.tableWidget.setRowCount(len(self.df_init_selected_index))

        for i in range(len(self.df_init_selected_index)):
            item = QtWidgets.QTableWidgetItem(self.df_init_selected_index.index[i])
            self.ui.tableWidget.setItem(i, 0, item)
            item = QtWidgets.QTableWidgetItem(
                self.df_init_selected_index.iloc[i, self.df_init_selected_index.columns.get_loc('sname')])
            self.ui.tableWidget.setItem(i, 1, item)
            item = QtWidgets.QCheckBox()
            item.setCheckState(QtCore.Qt.Checked)
            self.ui.tableWidget.setCellWidget(i, 2, item)

            self.set_selected_index.append(self.df_init_selected_index.index[i])
    # =======================================

    def add_selected_index(self, row):
        '''
        将右侧选择的具体指数添加到左侧框中
        然后将左侧框中index信息，用signal发射到index.py进行分析
        :param:row, 添加的是第几行的index，signal传递过来
        :return:
        '''
        item = self.ui.tableWidget_2.cellWidget(row, 2).checkState()
        if item == QtCore.Qt.Checked:
            item_code = self.ui.tableWidget_2.item(row, 0).text()
            if item_code in self.set_selected_index:
                return
            self.ui.tableWidget.insertRow(0)
            self.set_selected_index.append(item_code)
            item = QtWidgets.QTableWidgetItem(item_code)
            self.ui.tableWidget.setItem(0, 0, item)
            name = self.ui.tableWidget_2.item(row, 1).text()
            item = QtWidgets.QTableWidgetItem(name)
            self.ui.tableWidget.setItem(0, 1, item)
            item = QtWidgets.QCheckBox()
            item.setCheckState(QtCore.Qt.Unchecked)
            self.ui.tableWidget.setCellWidget(0, 2, item)

    # =======================================

    def save_selected_index_2_csv(self):
        '''
        同步精选指数到csv存储
        :return:
        '''
        rows = self.ui.tableWidget.rowCount()
        columns_name = ['ts_code', 'sname']
        list_result = []
        for i in range(rows):
            code = self.ui.tableWidget.item(i, 0).text()
            name = self.ui.tableWidget.item(i, 1).text()
            list_result.append([code, name])
        df = pd.DataFrame(data=list_result, columns=columns_name)
        df.to_csv(r"C:\quanttime\src\watch_time\selected_index.csv", encoding="gbk", index=False)


    # =======================================

    def emit_selected_index(self):
        '''
        将左侧所有选定的indexs 发射到主界面窗口
        :return:
        '''
        rows = self.ui.tableWidget.rowCount()
        signal_selectd_index = []
        for i in range(rows):
            item = self.ui.tableWidget.cellWidget(i, 2).checkState()
            if item == QtCore.Qt.Checked:
                signal_selectd_index.append(self.ui.tableWidget.item(i, 0).text())

        self.signal_select_index.emit(signal_selectd_index)

    # =======================================

    def calc_selected_index_valuation(self):
        '''
        对精选指数进行估值分析
        :return:
        '''
        rows = self.ui.tableWidget.rowCount()
        basic_path = 'C:\\quanttime\\data\\index\\sw\\valuation\\'
        pb_general_result = []
        pe_general_result = []
        columns_name = ['code', 'name', 'close', 'pb', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        columns_name1 = ['code', 'name', 'close', 'pe', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        for i in range(rows):
            item = self.ui.tableWidget.cellWidget(i, 2).checkState()
            if item == QtCore.Qt.Checked:
                code = self.ui.tableWidget.item(i, 0).text().split('.')[0]
                file_path = basic_path + code + '.csv'
                print("file_path:%s" % file_path)
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path, index_col=["date"], parse_dates=True,
                                     usecols=['date', 'close', 'index_name', 'pb', 'pe'], encoding="gbk")
                    if df.empty:
                        continue
                    index_name = df['index_name'][0]
                    pb_min = df['pb'].min()
                    pb_min_date = df['pb'].idxmin()
                    pb_quantile_5 = df['pb'].quantile(0.05)
                    pb_quantile_10 = df['pb'].quantile(0.10)
                    pb_mean = df['pb'].mean()
                    pb_max = df['pb'].max()
                    pb_max_date = df['pb'].idxmax()
                    pb_std = df['pb'].std()
                    pb = df['pb'][-1]
                    pb_tmp = [code, index_name, '--', pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10, pb_mean,
                              pb_max, pb_max_date, pb_std]
                    pb_general_result.append(pb_tmp)

                    pe_min = df['pe'].min()
                    pe_min_date = df['pe'].idxmin()
                    pe_quantile_5 = df['pe'].quantile(0.05)
                    pe_quantile_10 = df['pe'].quantile(0.10)
                    pe_mean = df['pe'].mean()
                    pe_max = df['pe'].max()
                    pe_max_date = df['pe'].idxmax()
                    pe_std = df['pe'].std()
                    pe = df['pe'][-1]
                    pe_tmp = [code, index_name, '--', pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10, pe_mean,
                              pe_max, pe_max_date, pe_std]
                    pe_general_result.append(pe_tmp)
        df_pb = pd.DataFrame(data=pb_general_result, columns=columns_name)
        df_pe = pd.DataFrame(data=pe_general_result, columns=columns_name1)
        # 小数只保留两位
        columns_2decimals = ["pb", "min", "max", "mean", "std", "5%", "10%"]
        df_pb[columns_2decimals] = df_pb[columns_2decimals].round(decimals=2)
        columns_2decimals = ["pe", "min", "max", "mean", "std", "5%", "10%"]
        df_pe[columns_2decimals] = df_pe[columns_2decimals].round(decimals=2)
        print(df_pb)
        print(df_pe)
        self.signal_select_index.emit(df_pb, df_pe)

    # ================================

    def get_sw_weight(self, code):
        '''
        获取申万指数的权重
        :param code: sw code
        :return: df
        '''
        df, msg = swindex.get_index_cons(code)
        return












