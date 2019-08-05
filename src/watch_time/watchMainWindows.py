# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from ui_watch_time import Ui_dog
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import os
import pandas as pd
import tushare as ts
import math
import pysnooper
from bank import BankThread
from security import SecurityThread
from gold import ProcessAUAGThread
from hunt_dog import HuntDogThread
from datetime import datetime
from dialog_index_selected import IndexSelected
from opendatatools import swindex

'''
功能：
1、银行分红率排名，flitter排名
2、券商估值分析
'''


class WatchMainWindows(QtCore.QObject):
    # 券商分析信号，该信号发送给券商分析业务security.py，包含一个int类型参数，用于表示是否只读取头部券商
    security_out = QtCore.pyqtSignal(int)
    # 计算金银比需要的设置参数signal
    agau_stat_info_out = QtCore.pyqtSignal(list)
    # 指数估值分析，选择时间段
    index_valuation_period_out = QtCore.pyqtSignal(int)

    def __init__(self, parent=None):
        super(WatchMainWindows, self).__init__(parent)
        app = QtWidgets.QApplication(sys.argv)
        main_window = QtWidgets.QMainWindow()
        self.ui = Ui_dog()
        self.ui.setupUi(main_window)

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        # ts 授权
        self.pro = ts.pro_api()

        # 银行基本信息更新按钮信号连接
        self.hkd_rmb = 0.87909
        self.ui.lineEdit_5.setText(str(self.hkd_rmb))
        bank_thread = BankThread()
        self.ui.pushButton.clicked.connect(bank_thread.update_bank_industry_table)
        # 银行分红信息及系统排名表刷新信号连接
        self.ui.pushButton_2.clicked.connect(bank_thread.process_bank_dividend)
        bank_thread.df_bank_out.connect(self.update_bank_dividend)
        self.ui.pushButton_6.clicked.connect(bank_thread.get_AH_premium)
        bank_thread.signal_df_ah_premium.connect(self.display_bank_AH_premium)

        # 设定默认的行情数据, 通达信默认行情源，tushare，joinquant可选
        self.ui.checkBox_4.setCheckState(QtCore.Qt.Unchecked)
        self.ui.checkBox_5.setCheckState(QtCore.Qt.Checked)
        self.ui.checkBox_7.setCheckState(QtCore.Qt.Unchecked)

        # 默认只分析重点关注的券商标的
        self.ui.checkBox.setCheckState(QtCore.Qt.Checked)
        # 券商估值分析pushbutton按钮signal连接
        # signal发射信号流为：pushbutton3 触发本class的获取ui中是否只是分析重点券商，然后把获取的ui信息发射到security.py中取分析
        self.ui.pushButton_3.clicked.connect(self.analyse_security_by_choice)

        # 券商业务处理线程信号与主页面券商估值分析表更新slot连接
        security_thread = SecurityThread()
        security_thread.df_out.connect(self.analyze_security_valuation)
        self.security_out.connect(security_thread.analyze_security_valuation)

        # 金银业务, 默认手动刷新，自动刷新会启动对应的处理线程，这里默认不自动关联
        self.ui.checkBox_3.setCheckState(QtCore.Qt.Unchecked)
        self.gold_thread = ProcessAUAGThread()
        self.gold_thread.auag_quotation.connect(self.display_auag_ratio)
        self.gold_thread.get_auag_ratio()
        self.ui.pushButton_4.clicked.connect(self.gold_thread.get_auag_ratio)
        # checkbox自动运行状态选定
        self.ui.checkBox_3.stateChanged.connect(self.gold_thread.auto_run)
        # 设置做多做空买入卖出起始线
        self.back_day_stat = 20  # 设置当前日期往前推几天的统计信息
        self.long_buy_value = 0.10  # 做多金银比，统计买入线，如0.10即10%分位线
        self.long_sell_value = 0.20  # 做多金银比，统计卖出线，如0.15即15%分位线
        self.short_buy_value = 0.90  # 做空金银比，统计的买入线，如0.85即85%分位线
        self.short_sell_value = 0.80  # 做空金银比，统计的卖出线
        self.init_buy_sell_info()
        self.ui.lineEdit_015.editingFinished.connect(self.emit_auag_stat_info)
        self.agau_stat_info_out.connect(self.gold_thread.calc_stat)
        self.gold_thread.signal_auag_stat.connect(self.display_auag_buysell_table)

        # 指数分析
        index_select_dialog = IndexSelected()
        self.ui.pushButton_5.clicked.connect(index_select_dialog.show_dialog)
        index_select_dialog.signal_select_index.connect(self.display_selected_index_analyse)
        self.ui.tableWidget_4.cellClicked.connect(self.get_display_weight)
        self.ui.tableWidget_4.cellDoubleClicked.connect(self.get_display_weight)
        self.ui.pushButton_7.clicked.connect(index_select_dialog.index_rotation)
        index_select_dialog.signal_rotation_result.connect(self.display_ratation_table)
        # 增加指数估值分位数分析的时间段选择
        self.ui.pushButton_8.clicked.connect(self.analyse_index_by_period)
        self.index_valuation_period_out.connect(index_select_dialog.analyse_sw_index_invaluation_by_period)

        # 监控票的处理
        # 设置默认的前海鹏华告警阈值
        self.ui.lineEdit_8.setText("-0.3")
        # 监控线程的sleep时间设置
        hunt_dog_thread = HuntDogThread()
        self.ui.comboBox.currentTextChanged.connect(hunt_dog_thread.set_sleep_time)
        hunt_dog_thread.REIT_Penghua_quotation.connect(self.display_REIT_Penghua)
        self.ui.checkBox_9.stateChanged.connect(hunt_dog_thread.auto_run)
        self.ui.checkBox_10.stateChanged.connect(hunt_dog_thread.stop)
        self.ui.pushButton_9.clicked.connect(hunt_dog_thread.process_by_hand)

        main_window.show()
        sys.exit(app.exec_())
    # ==============================================================

    def update_bank_dividend(self, df_bank):
        '''
        更新分红信息，当前查询分红信息采用的tushare接口
        :return:
        '''

        # 获取flitter排名
        df_flitter = self.get_flitter()
        df_flitter = df_flitter.set_index("code")

        rows = len(df_bank)
        self.ui.tableWidget.setRowCount(rows)
        for i in range(rows):
            # item = QtWidgets.QTableWidgetItem(df_bank.iloc[i, df_bank.columns.get_loc("code")])
            item = QtWidgets.QTableWidgetItem(df_bank.index[i])
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

            code = df_bank.index[i]
            if code[0] == '6':
                code = "SH" + code.split('.')[0]
            elif code[0] == '0':
                code = "SZ" + code.split('.')[0]
            elif code[0] == '3':
                code = "SZ" + code.split('.')[0]
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
    # ========================================

    def display_bank_AH_premium(self, df):
        '''
        获取银行股的AH折溢价情况，并更新数据表
        :param df 从银行业务处理线程获取的AH折溢价情况
        :return:
        '''
        if df.empty:
            print("获取银行AH折溢价失败")
            return
        self.ui.tableWidget_9.setRowCount(len(df))
        exchange_rate = float(self.ui.lineEdit_5.text())
        for i, row in df.iterrows():
            item = QtWidgets.QTableWidgetItem(row['name'])
            self.ui.tableWidget_9.setItem(i, 0, item)
            item = QtWidgets.QTableWidgetItem(row['a_code'])
            self.ui.tableWidget_9.setItem(i, 1, item)
            item = QtWidgets.QTableWidgetItem(row['hk_code'])
            self.ui.tableWidget_9.setItem(i, 2, item)
            premium = "0"
            if row['last_price_a'] == 0:
                premium = "--"
            else:
                premium = (row['last_price_h'] * exchange_rate - row['last_price_a']) / row['last_price_a']
                premium = str(round(premium * 100, 2)) + '%'
            item = QtWidgets.QTableWidgetItem(premium)
            self.ui.tableWidget_9.setItem(i, 3, item)

    # ========================================
    def analyse_security_by_choice(self):
        '''
        根据券商页面的选择，是进行头部券商的分析还是所有券商的分析
        该函数根据checkbox不同的状态发射不同的signal到sercury.py
        1--代表只分析头部券商
        0--代表分析所有券商
        :return: wu
        '''
        check_status = self.ui.checkBox.checkState()
        if check_status == QtCore.Qt.Checked:
            # 读取重点关注的券商
            self.security_out.emit(1)
        else:
            # 所有券商
            self.security_out.emit(0)

    # ========================================

    # @pysnooper.snoop()
    def analyze_security_valuation(self, df_pb, df_pe):
        '''
        分析券商的估值信息
        分别读取所有券商valuation表，获取pb，pe等信息
        :return:
        '''
        rows = len(df_pb)
        self.ui.tableWidget_2.setRowCount(rows)
        for i in range(rows):
            item = QtWidgets.QTableWidgetItem(df_pb.iloc[i, df_pb.columns.get_loc("code")])
            self.ui.tableWidget_2.setItem(i, 0, item)
            item = QtWidgets.QTableWidgetItem(df_pb.iloc[i, df_pb.columns.get_loc("name")])
            self.ui.tableWidget_2.setItem(i, 1, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("price")]))
            self.ui.tableWidget_2.setItem(i, 2, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("last_close")]))
            self.ui.tableWidget_2.setItem(i, 3, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("pb")]))
            self.ui.tableWidget_2.setItem(i, 4, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("min")]))
            self.ui.tableWidget_2.setItem(i, 5, item)
            item = QtWidgets.QTableWidgetItem(df_pb.iloc[i, df_pb.columns.get_loc("min_date")])
            self.ui.tableWidget_2.setItem(i, 6, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("5_per")]))
            self.ui.tableWidget_2.setItem(i, 7, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("10_per")]))
            self.ui.tableWidget_2.setItem(i, 8, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("mean")]))
            self.ui.tableWidget_2.setItem(i, 9, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("max")]))
            self.ui.tableWidget_2.setItem(i, 10, item)
            item = QtWidgets.QTableWidgetItem(df_pb.iloc[i, df_pb.columns.get_loc("max_date")])
            self.ui.tableWidget_2.setItem(i, 11, item)
            item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, df_pb.columns.get_loc("std")]))
            self.ui.tableWidget_2.setItem(i, 12, item)

        rows = len(df_pe)
        self.ui.tableWidget_3.setRowCount(rows)
        for i in range(rows):
            item = QtWidgets.QTableWidgetItem(df_pe.iloc[i, df_pe.columns.get_loc("code")])
            self.ui.tableWidget_3.setItem(i, 0, item)
            item = QtWidgets.QTableWidgetItem(df_pe.iloc[i, df_pe.columns.get_loc("name")])
            self.ui.tableWidget_3.setItem(i, 1, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("price")]))
            self.ui.tableWidget_3.setItem(i, 2, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("last_close")]))
            self.ui.tableWidget_3.setItem(i, 3, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("pe")]))
            self.ui.tableWidget_3.setItem(i, 4, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("min")]))
            self.ui.tableWidget_3.setItem(i, 5, item)
            item = QtWidgets.QTableWidgetItem(df_pe.iloc[i, df_pe.columns.get_loc("min_date")])
            self.ui.tableWidget_3.setItem(i, 6, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("5_per")]))
            self.ui.tableWidget_3.setItem(i, 7, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("10_per")]))
            self.ui.tableWidget_3.setItem(i, 8, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("mean")]))
            self.ui.tableWidget_3.setItem(i, 9, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("max")]))
            self.ui.tableWidget_3.setItem(i, 10, item)
            item = QtWidgets.QTableWidgetItem(df_pe.iloc[i, df_pe.columns.get_loc("max_date")])
            self.ui.tableWidget_3.setItem(i, 11, item)
            item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, df_pe.columns.get_loc("std")]))
            self.ui.tableWidget_3.setItem(i, 12, item)

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
                                       encoding="gbk", delim_whitespace=True, names=columns_name, header=0)
            return flitter_data
        else:
            return pd.DataFrame()

    # ===================================================
    def display_auag_ratio(self, quote_list):
        '''
        更新金银比信息
        :param quote_list 接收的金银比线程emit的list，
        list：按照[au,ag,au/ag,au_bid,au_ask,ag_bid,ag_ask,long_ratio,short_ratio, date]的顺序
        :return:
        '''
        print(quote_list)
        if len(quote_list) == 0:
            print("接收到金银比list==[]")
            return
        if len(quote_list) != 10:
            print("接收的金银比list len!=10, list=%r" % quote_list)
            return
        self.ui.lineEdit.setText(str(quote_list[0]))
        self.ui.lineEdit_2.setText(str(quote_list[1]))
        self.ui.lineEdit_3.setText(str(quote_list[2]))
        self.ui.lineEdit_4.setText(str(quote_list[9]))
        self.ui.lineEdit_6.setText(str(quote_list[4]))
        self.ui.lineEdit_7.setText(str(quote_list[3]))
        self.ui.lineEdit_9.setText(str(quote_list[6]))
        self.ui.lineEdit_11.setText(str(quote_list[5]))
        self.ui.lineEdit_10.setText(str(quote_list[7]))
        self.ui.lineEdit_12.setText(str(quote_list[8]))

    # =====================================================
    def process_auag_auto_quote(self):
        '''
        金银比的自动获取处理
        :return:
        '''
        check_status = self.ui.checkBox_3.checkState()
        if check_status == QtCore.Qt.Checked:
            self.ui.checkBox_2.setCheckState(QtCore.Qt.Unchecked)
    # ============================================
    def init_buy_sell_info(self):
        '''
        设置做多，做空金银比的起始线及统计周期
        :return:
        '''
        self.ui.lineEdit_016.setText(str(self.back_day_stat))# 设置当前日期往前推几天的统计信息
        self.ui.lineEdit_08.setText(str(self.long_buy_value))# 做多金银比，统计买入线，如0.10即10%分位线
        self.ui.lineEdit_013.setText(str(self.long_sell_value))# 做多金银比，统计卖出线，如0.15即15%分位线
        self.ui.lineEdit_014.setText(str(self.short_buy_value))# 做空金银比，统计的买入线，如0.85即85%分位线
        self.ui.lineEdit_015.setText(str(self.short_sell_value))# 做空金银比，统计的卖出线
        self.ui.lineEdit_018.setText(datetime.today().date().strftime("%Y-%m-%d"))

    # =================================================
    def emit_auag_stat_info(self):
        '''
        将界面设置的金银比统计信息的数据发射到gold线程进行处理
        signal：发射是list类型，按照[long_buy，long_sell，short_buy，short_sell，back_day]排序
        :return:
        '''
        long_buy = self.ui.lineEdit_08.text()
        long_sell = self.ui.lineEdit_013.text()
        short_buy = self.ui.lineEdit_014.text()
        short_sell = self.ui.lineEdit_015.text()
        back_day = self.ui.lineEdit_016.text()
        signal_list = [float(long_buy), float(long_sell), float(short_buy), float(short_sell), int(back_day)]
        self.agau_stat_info_out.emit(signal_list)

    # =================================================
    def display_auag_buysell_table(self, stat_list):
        '''
        更新计算后的统计值买卖信息表
        :param stat_list: gold处理线程发射的经过计算后的统计信息值
        按照[long_buy，long_sell，short_buy，short_sell，max, min, mean, std, 25%, 50%, 75%,记录表最后时间]排序
        :return:
        '''
        print("display_auag_buysell_table, %r" % stat_list)
        if len(stat_list) == 0:
            print("接收的经计算的统计值list为空")
            return
        if len(stat_list) != 12:
            print("接收的经计算的统计值len list!=11, list=%r" % stat_list)
            return
        self.ui.lineEdit_017.setText(stat_list[11])
        self.ui.tableWidget_6.setRowCount(1)
        for i in range(len(stat_list) - 1):
            item = QtWidgets.QTableWidgetItem(stat_list[i])
            self.ui.tableWidget_6.setItem(0, i, item)
    # ======================================================

    def display_selected_index_analyse(self, df_pb, df_pe):
        '''
        显示由index分析模块发射过来的pb，pe信息
        :param df_pb: pb分析结果 df
        columns_name = ['code', 'name', 'close', 'pb', 'min', 'min_date', '5%', '10%', 'mean', 'max', 'max_date', 'std']
        :param df_pe: pe分析结果 df
        :return:
        '''
        print(df_pb)
        print(df_pe)
        if len(df_pb) == 0:
            print("精选指数为空")
            return

        '''
        item = QtWidgets.QTableWidgetItem()
        brush = QtGui.QBrush(QtGui.QColor(170, 0, 0))
        brush.setStyle(QtCore.Qt.NoBrush)
        item.setForeground(brush)
        self.tableWidget_4.setItem(0, 0, item)
        '''

        # 清除pb表
        self.ui.tableWidget_4.clearContents()
        self.ui.tableWidget_4.setRowCount(len(df_pb))
        for i in range(len(df_pb)):
            for column in range(len(df_pb.columns)):
                item = QtWidgets.QTableWidgetItem(str(df_pb.iloc[i, column]))
                self.ui.tableWidget_4.setItem(i, column, item)
            curr_pb = float(self.ui.tableWidget_4.item(i, 3).text())
            pb_5 = float(self.ui.tableWidget_4.item(i, 6).text())
            if curr_pb <= pb_5:
                item = QtWidgets.QTableWidgetItem()
                brush = QtGui.QBrush(QtGui.QColor(170, 0, 0))
                brush.setStyle(QtCore.Qt.NoBrush)
                item.setForeground(brush)
                item.setText(str(self.ui.tableWidget_4.item(i, 3).text()))
                self.ui.tableWidget_4.setItem(i, 3, item)


        # 清除pe表
        self.ui.tableWidget_5.clearContents()
        self.ui.tableWidget_5.setRowCount(len(df_pe))
        for i in range(len(df_pe)):
            for column in range(len(df_pe.columns)):
                item = QtWidgets.QTableWidgetItem(str(df_pe.iloc[i, column]))
                self.ui.tableWidget_5.setItem(i, column, item)

    # ===========================================
    def get_display_weight(self, row):
        '''
        获取并显示sw指数的权重，该slot由table cell 单击或双击触发
        :param row: 单击或者双击的table的行号
        :return:
        '''
        code = self.ui.tableWidget_4.item(row, 0).text()
        code = code.split('.')[0]
        df, msg = swindex.get_index_cons(code)
        if df.empty:
            return
        self.ui.tableWidget_7.clearContents()
        self.ui.tableWidget_7.setRowCount(len(df))
        df['weight'] = df['weight'].apply(pd.to_numeric)
        df = df.sort_values(by=['weight'], ascending=False)
        for row in range(len(df)):
            item = QtWidgets.QTableWidgetItem(str(df.iloc[row, df.columns.get_loc('stock_code')]))
            self.ui.tableWidget_7.setItem(row, 0, item)
            item = QtWidgets.QTableWidgetItem(str(df.iloc[row, df.columns.get_loc('stock_name')]))
            self.ui.tableWidget_7.setItem(row, 1, item)
            item = QtWidgets.QTableWidgetItem(str(df.iloc[row, df.columns.get_loc('weight')]))
            self.ui.tableWidget_7.setItem(row, 2, item)

    # =========================================
    def display_ratation_table(self, stander_list, plus_list):
        '''
        显示指数轮动的结果，该结果由指数处理的py处理后emit过来，按照两个list
        ["399905", "399673", "399006"]排列结果
        "399300": "沪深300",
        "399905": "中证500",
        "399673": "创业板50",
        "399006": "创业板指数"
        :param stander_list: 标准指数轮动模型的结果
        :param plus_list: plus指数轮动结果
        :return:
        '''
        print("stander result:%r" % stander_list)
        print("plus result:%r" % plus_list)

        for result in stander_list:
            if result[0] == "399905":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_8.setItem(0, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_8.setItem(0, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_8.setItem(0, 2, item)
            if result[0] == "399006":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_10.setItem(0, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_10.setItem(0, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_10.setItem(0, 2, item)
            if result[0] == "399673":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_11.setItem(0, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_11.setItem(0, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_11.setItem(0, 2, item)

        for result in plus_list:
            if result[0] == "399905":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_8.setItem(1, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_8.setItem(1, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_8.setItem(1, 2, item)
            if result[0] == "399006":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_10.setItem(1, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_10.setItem(1, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_10.setItem(1, 2, item)
            if result[0] == "399673":
                item = QtWidgets.QTableWidgetItem(str(result[1]))
                self.ui.tableWidget_11.setItem(1, 0, item)
                item = QtWidgets.QTableWidgetItem(str(result[2]))
                self.ui.tableWidget_11.setItem(1, 1, item)
                item = QtWidgets.QTableWidgetItem(str(result[3]))
                self.ui.tableWidget_11.setItem(1, 2, item)

    # ========================================
    def analyse_index_by_period(self):
        '''
            根据checkstatus的选择，设置不同的分析时段
            当前可选的时段为：只分析5年，从2010年开始分析，2011年开始（创业板开始于2010年10月）
            0--代表分析所有时段
            1--代表只分析5年
            2--代表分析从2010年开始
            3--代表分析从2011年开始
            :return: wu
        '''
        # 代表只分析最近5年
        check_status5 = self.ui.checkBox_2.checkState()
        # 分析从2010年开始
        check_status10 = self.ui.checkBox_6.checkState()
        # 分析从2011年开始
        check_status11 = self.ui.checkBox_8.checkState()

        if check_status5 == QtCore.Qt.Checked:
            self.index_valuation_period_out.emit(1)
        elif check_status10 == QtCore.Qt.Checked:
            self.index_valuation_period_out.emit(2)
        elif check_status11 == QtCore.Qt.Checked:
            self.index_valuation_period_out.emit(3)
        else:
            self.index_valuation_period_out.emit(0)

    # ===========================================
    def display_REIT_Penghua(self, list_info):
        '''
        显示监控线程发过来的鹏华前海的信息，并判断是否到了阈值
        list_info为发射过来的信号，list，[code,name,ask1,net_value,premium]的顺序
        :return:
        '''
        print("display_penghua")
        print(list_info)
        alarm_limit = float(self.ui.lineEdit_8.text())
        if len(list_info) != 5:
            return
        self.ui.tableWidget_13.setRowCount(1)
        for i, j in enumerate(list_info):
            if isinstance(j, float):
                j = round(j, 2)
            item = QtWidgets.QTableWidgetItem(str(j))
            self.ui.tableWidget_13.setItem(0, i, item)
        if list_info[4] <= alarm_limit:
            qmessagebox = QtWidgets.QMessageBox()
            qmessagebox.setText("价格提醒")
            qmessagebox.setInformativeText("184801鹏华前海REIT到达买入价格！")
            qmessagebox.exec()
            # qmessagebox.information("Information", "184801鹏华前海REIT到达买入价格！")


if __name__ == "__main__":
    WatchMainWindows()