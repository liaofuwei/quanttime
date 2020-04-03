# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from ui_index_selected import Ui_Dialog
from PyQt5 import QtCore, QtWidgets
import pandas as pd
import os
from opendatatools import swindex
from quote_api import *
sys.path.append(('C:\\quanttime\\src\\comm'))
import trade_date_util


class IndexSelected(QtCore.QObject):

    signal_select_index = QtCore.pyqtSignal(pd.DataFrame, pd.DataFrame)
    signal_rotation_result = QtCore.pyqtSignal(list, list)

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

        # 分析sw的所有一二三级指数 signal与slot连接
        self.ui.pushButton.clicked.connect(self.analyse_sw_index_invaluation)

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
                    pb_min_date = df['pb'].idxmin().date()
                    pb_quantile_5 = df['pb'].quantile(0.05)
                    pb_quantile_10 = df['pb'].quantile(0.10)
                    pb_mean = df['pb'].mean()
                    pb_max = df['pb'].max()
                    pb_max_date = df['pb'].idxmax().date()
                    pb_std = df['pb'].std()
                    pb = df['pb'][-1]
                    tmp_date = str(df.index[-1].date())
                    pb_tmp = [code, index_name, tmp_date, pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10, pb_mean,
                              pb_max, pb_max_date, pb_std]
                    pb_general_result.append(pb_tmp)

                    pe_min = df['pe'].min()
                    pe_min_date = df['pe'].idxmin().date()
                    pe_quantile_5 = df['pe'].quantile(0.05)
                    pe_quantile_10 = df['pe'].quantile(0.10)
                    pe_mean = df['pe'].mean()
                    pe_max = df['pe'].max()
                    pe_max_date = df['pe'].idxmax().date()
                    pe_std = df['pe'].std()
                    pe = df['pe'][-1]
                    pe_tmp = [code, index_name, tmp_date, pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10, pe_mean,
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
        tmp = self.calc_basic_index()
        tmp_pb = tmp[0]
        tmp_pe = tmp[1]
        df_pb = pd.concat([df_pb, tmp_pb])
        df_pe = pd.concat([df_pe, tmp_pe])
        self.signal_select_index.emit(df_pb, df_pe)

    # ================================
    def calc_basic_index(self):
        '''
        计算大盘综合指数
        :return: df
        '''
        index_list = ["000001.SH", "000300.SH", "000905.SH", "399001.SZ",
                      "399005.SZ", "399006.SZ", "399016.SZ", "399300.SZ",
                      "000005.SH", "000006.SH", "000016.SH"]
        index_name = [u"上证指数", u"沪深300", u"中证500", u"深圳成指",
                      u"中小板指数", u"创业板指数", u"深圳创新指数", u"沪深300",
                      u"商业指数", u"地产指数", u"上证50"]
        basic_dir = 'C:\\quanttime\\data\\index\\tushare\\'
        pb_general_result = []
        pe_general_result = []
        columns_name = ['code', 'name', 'close', 'pb', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        columns_name1 = ['code', 'name', 'close', 'pe', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date',
                         'std']
        for i in range(len(index_list)):
            file = basic_dir + index_list[i] + '.csv'
            if os.path.exists(file):
                df = pd.read_csv(file, index_col=["trade_date"], parse_dates=True, usecols=['trade_date', 'pb', 'pe'])
                if df.empty:
                    continue
                index_code = index_list[i].split('.')[0]
                tmp_name = index_name[i]
                pb_min = df['pb'].min()
                pb_min_date = df['pb'].idxmin().date()
                pb_quantile_5 = df['pb'].quantile(0.05)
                pb_quantile_10 = df['pb'].quantile(0.10)
                pb_mean = df['pb'].mean()
                pb_max = df['pb'].max()
                pb_max_date = df['pb'].idxmax().date()
                pb_std = df['pb'].std()
                pb = df['pb'][-1]
                tmp_date = str(df.index[-1].date())
                pb_tmp = [index_code, tmp_name, tmp_date, pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10, pb_mean,
                          pb_max, pb_max_date, pb_std]
                pb_general_result.append(pb_tmp)

                pe_min = df['pe'].min()
                pe_min_date = df['pe'].idxmin().date()
                pe_quantile_5 = df['pe'].quantile(0.05)
                pe_quantile_10 = df['pe'].quantile(0.10)
                pe_mean = df['pe'].mean()
                pe_max = df['pe'].max()
                pe_max_date = df['pe'].idxmax().date()
                pe_std = df['pe'].std()
                pe = df['pe'][-1]
                pe_tmp = [index_code, tmp_name, tmp_date, pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10, pe_mean,
                          pe_max, pe_max_date, pe_std]
                pe_general_result.append(pe_tmp)
        df_pb = pd.DataFrame(data=pb_general_result, columns=columns_name)
        df_pe = pd.DataFrame(data=pe_general_result, columns=columns_name1)
        # 小数只保留两位
        columns_2decimals = ["pb", "min", "max", "mean", "std", "5%", "10%"]
        df_pb[columns_2decimals] = df_pb[columns_2decimals].round(decimals=2)
        columns_2decimals = ["pe", "min", "max", "mean", "std", "5%", "10%"]
        df_pe[columns_2decimals] = df_pe[columns_2decimals].round(decimals=2)
        return df_pb, df_pe

    # ==================================
    def analyse_sw_index_invaluation(self):
        '''
        分析所有的申万一级二级三级指数
        分析结果通过signal_select_index发射到主界面
        :return:
        '''
        sw_index_info_dir = r'C:\quanttime\data\index\basic_index_info\basic_index_info_sw.csv'
        df_sw_index_info = pd.read_csv(sw_index_info_dir, encoding="gbk", usecols=["ts_code", "category"],
                                       index_col=["ts_code"])
        # 选出指数中一，二，三级指数进行分析
        tmp1 = df_sw_index_info[df_sw_index_info["category"] == u"一级行业指数"]
        tmp2 = df_sw_index_info[df_sw_index_info["category"] == u"二级行业指数"]
        tmp3 = df_sw_index_info[df_sw_index_info["category"] == u"三级行业指数"]
        df_sw_index_info = pd.concat([tmp1, tmp2, tmp3])
        sw_valuation_dir = 'C:\\quanttime\\data\\index\\sw\\valuation\\'
        pb_general_result = []
        pe_general_result = []
        columns_name = ['code', 'name', 'close', 'pb', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        columns_name1 = ['code', 'name', 'close', 'pe', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date',
                         'std']
        for index_code in df_sw_index_info.index:
            valuation_file = sw_valuation_dir + index_code[0:6] + '.csv'
            if not os.path.exists(valuation_file):
                print("sw code:%s 不存在对应的valuation文件" % index_code)
                continue
            df = pd.read_csv(valuation_file, index_col=["date"], parse_dates=True,
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
            tmp_date = str(df.index[-1].date())
            pb_tmp = [index_code, index_name, tmp_date, pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10, pb_mean,
                      pb_max, pb_max_date, pb_std]
            pb_general_result.append(pb_tmp)

            pe_min = df['pe'].min()
            pe_min_date = df['pe'].idxmin().date()
            pe_quantile_5 = df['pe'].quantile(0.05)
            pe_quantile_10 = df['pe'].quantile(0.10)
            pe_mean = df['pe'].mean()
            pe_max = df['pe'].max()
            pe_max_date = df['pe'].idxmax().date()
            pe_std = df['pe'].std()
            pe = df['pe'][-1]
            pe_tmp = [index_code, index_name, tmp_date, pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10, pe_mean,
                      pe_max, pe_max_date, pe_std]
            pe_general_result.append(pe_tmp)
        df_pb = pd.DataFrame(data=pb_general_result, columns=columns_name)
        df_pe = pd.DataFrame(data=pe_general_result, columns=columns_name1)
        # 小数只保留两位
        columns_2decimals = ["pb", "min", "max", "mean", "std", "5%", "10%"]
        df_pb[columns_2decimals] = df_pb[columns_2decimals].round(decimals=2)
        columns_2decimals = ["pe", "min", "max", "mean", "std", "5%", "10%"]
        df_pe[columns_2decimals] = df_pe[columns_2decimals].round(decimals=2)
        print(df_pb.head(2))
        print(df_pe.head(2))
        self.signal_select_index.emit(df_pb, df_pe)

    # ==================================
    def analyse_sw_index_invaluation_by_period(self, period):
        '''
        计算申万指数估值的历史分位数
        通过period 增加时段选择
        period：
        0--代表分析所有时段
        1--代表只分析5年
        2--代表分析从2010年开始
        3--代表分析从2011年开始
        :param period:主页面signal值
        :return:
        '''
        print("analyse_sw_index_invaluation_by_period")
        if period == 0:
            self.analyse_sw_index_invaluation()
        elif period == 1:
            df_pb, df_pe = self.calc_sw_index_invaluation_by_period(datetime.today().year - 5)
            if isinstance(df_pe, pd.DataFrame) and isinstance(df_pb, pd.DataFrame):
                self.signal_select_index.emit(df_pb, df_pe)
        elif period == 2:
            df_pb, df_pe = self.calc_sw_index_invaluation_by_period(2010)
            if isinstance(df_pe, pd.DataFrame) and isinstance(df_pb, pd.DataFrame):
                self.signal_select_index.emit(df_pb, df_pe)
        elif period == 3:
            df_pb, df_pe = self.calc_sw_index_invaluation_by_period(2011)
            if isinstance(df_pe, pd.DataFrame) and isinstance(df_pb, pd.DataFrame):
                self.signal_select_index.emit(df_pb, df_pe)

    # ====================================
    def calc_sw_index_invaluation_by_period(self, nyears):
        '''
        计算从当前日期算的n年申万指数估值历史分位
        :param nyears: 从哪一年开始计算，例如计算2016开始的估值
        :return:(df,df)
        '''
        if nyears < 0:
            return 0, 0

        pre_date = datetime(nyears, datetime.today().month, datetime.today().day).date()

        sw_index_info_dir = r'C:\quanttime\data\index\basic_index_info\basic_index_info_sw.csv'
        df_sw_index_info = pd.read_csv(sw_index_info_dir, encoding="gbk", usecols=["ts_code", "category"],
                                       index_col=["ts_code"])
        # 选出指数中一，二，三级指数进行分析
        tmp1 = df_sw_index_info[df_sw_index_info["category"] == u"一级行业指数"]
        tmp2 = df_sw_index_info[df_sw_index_info["category"] == u"二级行业指数"]
        tmp3 = df_sw_index_info[df_sw_index_info["category"] == u"三级行业指数"]
        df_sw_index_info = pd.concat([tmp1, tmp2, tmp3])
        sw_valuation_dir = 'C:\\quanttime\\data\\index\\sw\\valuation\\'
        pb_general_result = []
        pe_general_result = []
        columns_name = ['code', 'name', 'close', 'pb', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        columns_name1 = ['code', 'name', 'close', 'pe', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date',
                         'std']
        for index_code in df_sw_index_info.index:
            valuation_file = sw_valuation_dir + index_code[0:6] + '.csv'
            if not os.path.exists(valuation_file):
                print("sw code:%s 不存在对应的valuation文件" % index_code)
                continue
            df = pd.read_csv(valuation_file, index_col=["date"], parse_dates=True,
                             usecols=['date', 'close', 'index_name', 'pb', 'pe'], encoding="gbk")
            if df.empty:
                continue
            period_index_start = [d for d in df.index if d.date() >= pre_date][0]

            index_name = df['index_name'][0]
            pb_min = df.loc[period_index_start:, ['pb']]['pb'].min()
            pb_min_date = df.loc[period_index_start:, ['pb']]['pb'].idxmin()
            pb_quantile_5 = df.loc[period_index_start:, ['pb']]['pb'].quantile(0.05)
            pb_quantile_10 = df.loc[period_index_start:, ['pb']]['pb'].quantile(0.10)
            pb_mean = df.loc[period_index_start:, ['pb']]['pb'].mean()
            pb_max = df.loc[period_index_start:, ['pb']]['pb'].max()
            pb_max_date = df.loc[period_index_start:, ['pb']]['pb'].idxmax()
            pb_std = df.loc[period_index_start:, ['pb']]['pb'].std()
            pb = df.loc[period_index_start:, ['pb']]['pb'][-1]
            tmp_date = str(df.index[-1].date())
            pb_tmp = [index_code, index_name, tmp_date, pb, pb_min, pb_min_date, pb_quantile_5, pb_quantile_10, pb_mean,
                      pb_max, pb_max_date, pb_std]
            pb_general_result.append(pb_tmp)

            pe_min = df.loc[period_index_start:, ['pe']]['pe'].min()
            pe_min_date = df.loc[period_index_start:, ['pe']]['pe'].idxmin()
            pe_quantile_5 = df.loc[period_index_start:, ['pe']]['pe'].quantile(0.05)
            pe_quantile_10 = df.loc[period_index_start:, ['pe']]['pe'].quantile(0.10)
            pe_mean = df.loc[period_index_start:, ['pe']]['pe'].mean()
            pe_max = df.loc[period_index_start:, ['pe']]['pe'].max()
            pe_max_date = df.loc[period_index_start:, ['pe']]['pe'].idxmax()
            pe_std = df.loc[period_index_start:, ['pe']]['pe'].std()
            pe = df.loc[period_index_start:, ['pe']]['pe'][-1]
            pe_tmp = [index_code, index_name, tmp_date, pe, pe_min, pe_min_date, pe_quantile_5, pe_quantile_10, pe_mean,
                      pe_max, pe_max_date, pe_std]
            pe_general_result.append(pe_tmp)
        df_pb = pd.DataFrame(data=pb_general_result, columns=columns_name)
        df_pe = pd.DataFrame(data=pe_general_result, columns=columns_name1)
        # 小数只保留两位
        columns_2decimals = ["pb", "min", "max", "mean", "std", "5%", "10%"]
        df_pb[columns_2decimals] = df_pb[columns_2decimals].round(decimals=2)
        columns_2decimals = ["pe", "min", "max", "mean", "std", "5%", "10%"]
        df_pe[columns_2decimals] = df_pe[columns_2decimals].round(decimals=2)
        return df_pb, df_pe
    # ==================================

    def get_sw_weight(self, code):
        '''
        获取申万指数的权重
        :param code: sw code
        :return: df
        '''
        df, msg = swindex.get_index_cons(code)

    # =========================================
    def index_rotation(self):
        """
        实数轮动分析
        历史k线数据读取至C:\quanttime\data\index\jq  .XSHG
        实时行情获取指数当前点位来之与tdx
        :return:
        """

        index_compare = [["399300", "399905"], ["399300", "399673"], ["399300", "399006"]]
        index_name = {
            "399300": "沪深300",
            "399905": "中证500",
            "399673": "创业板50",
            "399006": "创业板指数"}

        basic_dir = r"C:\quanttime\data\index\jq"
        quote_list = ["399300"]
        # 获取实时股价，先从通达信获取
        df_price = get_quote_by_tdx(quote_list)
        if df_price.empty :
            print("tdx获取实时指数点位失败")
            return
        df_price = df_price.set_index("code")
        price_300 = df_price.loc["399300", ["price"]].price

        today = datetime.today().date().strftime("%Y-%m-%d")
        day_20 = trade_date_util.get_appoint_trade_date(today, -20)
        if day_20 == "":
            print("获取前20天日期为空")
            return
        day_20 = datetime.strptime(day_20, "%Y-%m-%d")
        day_19 = trade_date_util.get_appoint_trade_date(today, -19)
        if day_19 == "":
            print("获取前19天日期为空")
            return
        day_19 = datetime.strptime(day_19, "%Y-%m-%d")
        day_21 = trade_date_util.get_appoint_trade_date(today, -21)
        if day_21 == "":
            print("获取前21天日期为空")
            return
        day_21 = datetime.strptime(day_21, "%Y-%m-%d")

        index_300_csv = basic_dir + r'\399300.XSHE' + '.csv'
        if not os.path.exists(index_300_csv):
            print("%s 不存在" % index_300_csv)
            return
        index_300 = pd.read_csv(index_300_csv, index_col=["date"], parse_dates=True, usecols=["date", "close"])
        index_300 = index_300[~index_300.reset_index().duplicated().values]
        try:
            index_300_20 = float(index_300.loc[day_20, ["close"]].close)
            index_300_19 = float(index_300.loc[day_19, ["close"]].close)
            index_300_21 = float(index_300.loc[day_21, ["close"]].close)
        except:
            print("沪深300指数，获取前20天，19天，21天close数据失败")
            return
        print("当前点位：%f,20天前点位：%f，19天前点位：%f，21天前点位：%f" %
              (price_300, index_300_20, index_300_19, index_300_21))

        curr_300_close_20_result = price_300 > index_300_20
        curr_300_close_20_increase = 0
        if index_300_20 == 0:
            print("300指数前20天close=0")
            return
        # 计算标准版的指数涨幅，当前指数-20天前指数，除以20天前的指数
        if curr_300_close_20_result:
            curr_300_close_20_increase = (price_300 - index_300_20) / index_300_20
        index_300_mean = (index_300_20 + index_300_19 + index_300_21) / 3
        curr_300_plus = price_300 > index_300_mean
        curr_300_plus_increase = 0
        # 计算plus的指数涨幅
        if curr_300_plus:
            curr_300_plus_increase = (price_300 - index_300_mean) / index_300_mean

        small_index_list = ["399905", "399673", "399006"]
        general_result_stander = []
        general_result_plus = []
        index_20 = 0
        index_19 = 0
        index_21 = 0
        index_mean = 0
        for index_code in small_index_list:
            index_csv = basic_dir + '\\' + index_code + '.XSHE.csv'
            if not os.path.exists(index_csv):
                print("%s 历史kline数据不存在" % index_csv)
                continue
            index = pd.read_csv(index_csv, index_col=["date"], parse_dates=True, usecols=["date", "close"])
            index = index[~index.reset_index().duplicated().values]
            try:
                index_20 = float(index.loc[day_20, ["close"]].close)
                index_19 = float(index.loc[day_19, ["close"]].close)
                index_21 = float(index.loc[day_21, ["close"]].close)
                index_mean = (index_20 + index_19 + index_21) / 3
            except:
                print("%s指数，获取前20天，19天，21天close数据失败" % index_code)
                continue
            df_price_tmp = get_quote_by_tdx([index_code])
            if df_price_tmp.empty:
                print("tdx获取实时指数点位失败")
                continue
            df_price_tmp = df_price_tmp.set_index("code")
            curr_price = df_price_tmp.loc[index_code, ["price"]].price
            print("指数%s ：当前点位：%f,20天前点位：%f，19天前点位：%f，21天前点位：%f" %
                  (index_code, curr_price, index_20, index_19, index_21))
            result = ""
            if curr_300_close_20_result and (curr_price > index_20):
                # 300指数与当前指数都比20日前close高，比较涨幅
                index_crease = (curr_price - index_20) / index_20
                if curr_300_close_20_increase > index_crease:
                    # 300指数涨幅大
                    result = "buy 沪深300"
                elif curr_300_close_20_increase < index_crease:
                    # 小盘涨的多
                    result = "buy %s" % index_name[index_code]
                else:
                    # 涨幅一样
                    result = "涨幅一样"
            elif curr_300_close_20_result and (curr_price < index_20):
                result = "buy 沪深300"
            elif (not curr_300_close_20_result) and (curr_price > index_20):
                result = "buy %s" % index_name[index_code]
            else:
                result = "sell"
            curr = "沪深300：" + str(price_300) + " vs " + index_name[index_code] + ":" + str(curr_price)
            pre_20 = "沪深300：" + str(index_300_20) + " vs " + index_name[index_code] + ":" + str(index_20)
            general_result_stander.append([index_code, result, curr, pre_20])
            # ==============stander================
            # ===============plus======================
            if curr_300_plus and (curr_price > index_mean):
                index_crease = (curr_price - index_mean) / index_mean
                if curr_300_plus_increase > index_crease:
                    # 300指数涨幅大
                    result = "buy 沪深300"
                elif curr_300_plus_increase < index_crease:
                    # 小盘涨的多
                    result = "buy %s" % index_name[index_code]
                else:
                    # 涨幅一样
                    result = "涨幅一样"
            elif curr_300_plus and (curr_price < index_mean):
                result = "buy 沪深300"
            elif (not curr_300_plus) and (curr_price > index_mean):
                result = "buy %s" % index_name[index_code]
            else:
                result = "sell all"
            curr = "沪深300：" + str(price_300) + " vs " + index_name[index_code] + ":" + str(curr_price)
            pre_mean = "沪深300：" + str(round(index_300_mean, 2)) + " vs " + index_name[index_code] + ":" + \
                       str(round(index_mean, 2))
            general_result_plus.append([index_code, result, curr, pre_mean])

        self.signal_rotation_result.emit(general_result_stander, general_result_plus)





















