#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import  quanttimeUI as quanttimeui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import tushare as ts
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import logging
from jqdatasdk import *
import easytrader
import convertBondUIlogic as bond

sys.path.append('C:\\quanttime\\src\\mydefinelib\\')
from getSinaFutureQuotation import sinaFutureData

class quanttimeMainWindows(object):
    def __init__(self):
        app = QtWidgets.QApplication(sys.argv)
        MainWindow = QtWidgets.QMainWindow()
        self.ui = quanttimeui.Ui_MainWindow()
        self.ui.setupUi(MainWindow)

        self.AU_price = 0
        self.AG_price = 0
        self.future_realtime_data = sinaFutureData()


        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        self.pro = ts.pro_api()  # ts 授权

        #auth('13811866763', "sam155")  # jqdata 授权

        # 注册交易接口
        self.user = easytrader.use('ht_client')
        # self.user.prepare(user='666626218349', password='836198', comm_password='sam155')
        self.user.prepare(user='666628601648', password='836198', comm_password='sam155')

        #设置做多做空买入卖出起始线
        self.back_day_stat = 20  # 设置当前日期往前推几天的统计信息
        self.long_buy_value = 0.10  # 做多金银比，统计买入线，如0.10即10%分位线
        self.long_sell_value = 0.20  # 做多金银比，统计卖出线，如0.15即15%分位线
        self.short_buy_value = 0.90  # 做空金银比，统计的买入线，如0.85即85%分位线
        self.short_sell_value = 0.80  # 做空金银比，统计的卖出线
        self.init_buy_sell_info()
        self.ui.lineEdit_11.editingFinished.connect(self.calc_stat_info)

        #启动运行的时候，获取一次金价，银价及比值数据
        self.update_AU_price()
        self.update_AG_price()
        self.update_AUAG_compare()

        #实例化获取金银比的线程
        self.run_processAUAGThread = ProcessAUAGThread(parent=None)

        #pushbutton click signal connect 处理click信号的slot
        self.ui.realtime_pushButton.clicked.connect(self.click_refresh)
        #启动/停止 金银比线程
        self.ui.radioButton.clicked.connect(self.auto_update_AUAG)
        self.ui.radioButton_2.clicked.connect(self.stop_auto_update_AUAG)

        #实例化可转债premium线程
        self.df_premium_all = pd.DataFrame()
        self.run_processConvertBondThread = bond.convertBondProcessThread(parent=None)
        self.ui.pushButton.clicked.connect(self.click_update_convert_bond_premium)
        self.run_processConvertBondThread.premium_info.connect(self.update_convert_bond_premium_by_hand)

        #获取bond和stock的tick数据，当输入bond code 和stock code完成后，及send lineedit edittingfinished信号
        self.kezhuanzhai = pd.read_csv("C:\\quanttime\\data\\basic_info\\convert_bond_basic_info.csv", index_col=["bond_code"],encoding="gbk")
        self.kezhuanzhai.index = self.kezhuanzhai.index.map(str)
        self.kezhuanzhai['stock_code'] = self.kezhuanzhai['stock_code'].map(self.standard_code)
        #print(self.kezhuanzhai.loc["123016", ["bond_name"]].bond_name)
        self.ui.lineEdit_2.editingFinished.connect(self.get_bond_stock_tick)
        self.ui.pushButton_3.clicked.connect(self.get_bond_stock_tick)

        #可转债的折价交易激发
        self.ui.pushButton_2.clicked.connect(self.zhuanzhai_trade)

        MainWindow.show()
        sys.exit(app.exec_())

    def init_buy_sell_info(self):
        '''
        设置做多，做空金银比的起始线及统计周期
        :return:
        '''
        self.ui.lineEdit_12.setText(str(self.back_day_stat))# 设置当前日期往前推几天的统计信息
        self.ui.lineEdit_8.setText(str(self.long_buy_value))# 做多金银比，统计买入线，如0.10即10%分位线
        self.ui.lineEdit_9.setText(str(self.long_sell_value))# 做多金银比，统计卖出线，如0.15即15%分位线
        self.ui.lineEdit_10.setText(str(self.short_buy_value))# 做空金银比，统计的买入线，如0.85即85%分位线
        self.ui.lineEdit_11.setText(str(self.short_sell_value))# 做空金银比，统计的卖出线




    def calc_stat_info(self):
        '''
        通过设置的参数信息，计算统计周期内的统计信息
        :return:
        '''
        gold_future = "C:\\quanttime\\data\\gold\\sh_future\\gold.csv"
        silver_future = "C:\\quanttime\\data\\gold\\sh_future\\silver.csv"

        stander_dtype = {'open': float, "close": float, "high": float, "low": float, "volume": float, "money": float}
        gold_future_data = pd.read_csv(gold_future, parse_dates=["date"], index_col=["date"],
                                       dtype=stander_dtype)
        gold_future_data = gold_future_data[~gold_future_data.reset_index().duplicated().values]

        silver_future_data = pd.read_csv(silver_future, parse_dates=["date"], index_col=["date"],
                                         dtype=stander_dtype)
        silver_future_data = silver_future_data[~silver_future_data.reset_index().duplicated().values]

        future_data = pd.merge(gold_future_data, silver_future_data, left_index=True, right_index=True,
                               suffixes=('_gold', '_silver'))
        future_data["compare"] = future_data["close_gold"] / future_data["close_silver"] * 1000
        # 去重
        future_data = future_data.dropna()
        future_data_trade_date = future_data.index

        today = datetime.today().date()
        self.ui.lineEdit_14.setText(today.strftime("%Y-%m-%d"))
        self.ui.lineEdit_13.setText(future_data_trade_date[-1].strftime("%Y-%m-%d"))
        columns_name = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
        # df_empty = pd.DataFrame(columns=columns_name)
        self.back_day_stat = self.ui.lineEdit_12.text()  # 设置当前日期往前推几天的统计信息
        self.long_buy_value = self.ui.lineEdit_8.text()  # 做多金银比，统计买入线，如0.10即10%分位线
        self.long_sell_value = self.ui.lineEdit_9.text()  # 做多金银比，统计卖出线，如0.15即15%分位线
        self.short_buy_value = self.ui.lineEdit_10.text()  # 做空金银比，统计的买入线，如0.85即85%分位线
        self.short_sell_value = self.ui.lineEdit_11.text()  # 做空金银比，统计的卖出线
        df_stat_20 = future_data.iloc[-int(self.back_day_stat):] #如future_data.iloc[-20:]
        #print(df_stat_20)
        df_stat = df_stat_20.loc[:, ["compare"]].describe()
        #print(df_stat)

        long_buyValue = round(float(self.long_buy_value), 2)
        long_sellValue = round(float(self.long_sell_value), 2)
        short_sellValue = round(float(self.short_buy_value), 2)
        short_buyValue = round(float(self.short_sell_value), 2)
        # print(self.long_buy_value)
        # long_buyValue = 0.05
        # long_sellValue = 0.10
        # short_sellValue = 0.85
        # short_buyValue = 0.90
        v_5 = df_stat_20.quantile(long_buyValue).compare  # 5%分位
        v_10 = df_stat_20.quantile(long_sellValue).compare  # 10%分位
        v_90 = df_stat_20.quantile(short_sellValue).compare  # 90%分位
        v_95 = df_stat_20.quantile(short_buyValue).compare  # 95%分位

        value = round(v_5, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(0, 0, newItem)
        value = round(v_10, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(1, 0, newItem)
        value = round(v_90, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(2, 0, newItem)
        value = round(v_95, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(3, 0, newItem)

        value = round(df_stat.loc["max", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(4, 0, newItem)

        value = round(df_stat.loc["min", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(5, 0, newItem)

        value = round(df_stat.loc["mean", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(6, 0, newItem)

        value = round(df_stat.loc["25%", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(7, 0, newItem)

        value = round(df_stat.loc["50%", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(8, 0, newItem)

        value = round(df_stat.loc["75%", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(9, 0, newItem)

        value = round(df_stat.loc["std", ["compare"]].compare, 2)
        newItem = QtWidgets.QTableWidgetItem(str(value))
        self.ui.tableWidget_2.setItem(10, 0, newItem)


    def update_AU_price(self):
        #current_date = datetime.today().date().strftime("%Y-%m-%d")
        #self.df_AU_price = get_price('AU9999.XSGE',start_date=current_date, end_date=current_date, frequency='daily', \
        #                          fields=None)
        #self.AU_price = self.df_AU_price.close[0]
        #---------------------------------------------------
        #改新浪接口
        au = self.future_realtime_data.get_future_price("au0")
        print(au)
        self.AU_price = au["AU0"]["new_price"]
        self.ui.AU_price_lineEdit.setText(str(self.AU_price))

    def update_AG_price(self):
        #current_date = datetime.today().date().strftime("%Y-%m-%d")
        #self.df_AG_price = get_price('AG9999.XSGE', start_date=current_date, end_date=current_date, frequency='daily', \
        #                          fields=None)
        #self.AG_price = self.df_AG_price.close[0]
        # ---------------------------------------------------
        # 改新浪接口
        ag = self.future_realtime_data.get_future_price("ag0")
        self.AG_price = ag["AG0"]["new_price"]
        self.ui.AG_price_lineEdit.setText(str(self.AG_price))

    def update_AUAG_compare(self):
        if(self.AG_price>0):
            value = self.AU_price / (self.AG_price / 1000)
            value = round(value, 2)
            self.ui.lineEdit.setText(str(value))


    def click_refresh(self):
        '''
        实时更新pushbotton对应的处理slot函数，获取金银数据后更新对应的值
        :return:
        '''
        au = self.future_realtime_data.get_future_price("au0")
        self.AU_price = au["AU0"]["new_price"]
        print("AU price: %f" % self.AU_price)
        self.ui.AU_price_lineEdit.setText(str(self.AU_price))
        ag = self.future_realtime_data.get_future_price("ag0")
        self.AG_price = ag["AG0"]["new_price"]
        print("AG price: %d" % self.AG_price)
        self.ui.AG_price_lineEdit.setText(str(self.AG_price))
        if (self.AG_price>0):
            value = self.AU_price / (self.AG_price / 1000)
            value = round(value, 2)
            print(value)
            self.ui.lineEdit.setText(str(value))

    def auto_update_AUAG(self):
        '''
        启动自动更新radiobutton对应的处理函数
        该函数主要启动ProcessAUAGThread线程，以及连接ProcessAUAGThread发射的signal
        :return:
        '''
        self.run_processAUAGThread.is_running = True
        self.run_processAUAGThread.start()
        self.run_processAUAGThread.auag_quotation.connect(self.process_quotation)

    def process_quotation(self, list_quotation):
        '''
        处理ProcessAUAGThread 接收的signal数据，该signal是一个list，放在list_quotation中
        :param list_quotation: signal
        :return:
        '''
        if len(list_quotation)==0:
            return
        elif len(list_quotation)==2:
            self.ui.AU_price_lineEdit.setText(str(list_quotation[0]))
            self.ui.AG_price_lineEdit.setText(str(list_quotation[1]))
        elif len(list_quotation)==3:
            self.ui.AU_price_lineEdit.setText(str(list_quotation[0]))
            self.ui.AG_price_lineEdit.setText(str(list_quotation[1]))
            self.ui.lineEdit.setText(str(list_quotation[2]))
        else:
            print("recv the signal error")

    def stop_auto_update_AUAG(self):
        '''
        停止线程ProcessAUAGThread
        :return:
        '''
        try:
            self.run_processAUAGThread.stop()
        except:
            pass


    def update_convert_bond_premium_by_hand(self, df_premium):
        '''
        手动刷新可转债的premium表
        更新数据来至于convertBondUIlogic.py的init_get_premium的信号，信号类型是一个有pd.dataframe转换的dict
        dict_premium:从可转债接收的信号，由pd.dataframe转的dict
        :return:
        '''
        self.df_premium_all = df_premium
        print(self.df_premium_all)
        df_premium = df_premium.sort_values(by="premium")
        df_premium["premium"] = df_premium["premium"].map(self.display_percent_format)
        nRow = len(df_premium)
        print("nRows: %d" % nRow)
        self.ui.premium_tableWidget.setRowCount(nRow)

        for i in range(nRow):
            newItem1 = QtWidgets.QTableWidgetItem(df_premium.iloc[i, 0])
            self.ui.premium_tableWidget.setItem(i, 0, newItem1)
            #self.ui.premium_tableWidget.item(i, 0).itemDoubleClicked.connect(self.get_detail_quoation_info)

            newItem2 = QtWidgets.QTableWidgetItem(df_premium.iloc[i, 1])
            self.ui.premium_tableWidget.setItem(i, 1, newItem2)
            #self.ui.premium_tableWidget.item(i, 1).itemDoubleClicked.connect(self.get_detail_quoation_info)

            newItem3 = QtWidgets.QTableWidgetItem(df_premium.iloc[i, 2])
            self.ui.premium_tableWidget.setItem(i, 2, newItem3)
            #self.ui.premium_tableWidget.item(i, 2).itemDoubleClicked.connect(self.get_detail_quoation_info)

            newItem4 = QtWidgets.QTableWidgetItem(df_premium.iloc[i, 3])
            self.ui.premium_tableWidget.setItem(i, 3, newItem4)
            #self.ui.premium_tableWidget.item(i, 3).itemDoubleClicked.connect(self.get_detail_quoation_info)

            newItem5 = QtWidgets.QTableWidgetItem(df_premium.iloc[i, 4])
            self.ui.premium_tableWidget.setItem(i, 4, newItem5)
            #self.ui.premium_tableWidget.item(i, 4).itemDoubleClicked.connect(self.get_detail_quoation_info)



    def display_percent_format(self, x):
        '''
        功能：小数按照百分数%显示，保留两位小数
        '''
        try:
            data = float(x)
        except:
            print("input is not numberic")
            return 0

        return "%.2f%%"%(data * 100)


    def standard_code(self, x):
        '''
        标准化code代码
        convert_bond_basic_info.csv中的stock code 是joinquant格式的stock code
        需要转化为tushare格式的stock code
        :param x: str
        :return: 标准code代码
        '''
        joinquant_code = str(x)
        ret = joinquant_code.split('.')

        if ret[0].isnumeric():
            return ret[0]

        else:
            logging.error("code is not standard,code=%r ",joinquant_code)
            return "code error"




    def click_update_convert_bond_premium(self):
        '''
        可转债的手动更新按钮信号
        :return:
        '''

        self.run_processConvertBondThread.init_get_premium()

    def get_bond_stock_tick(self):
        '''
        在获取到premium信息后，双击premium表的某行，获取转债与正股的bid-ask信息，判断是否进行套利
        :return:
        '''
        bond_code = self.ui.lineEdit_2.text()
        stock_code = self.kezhuanzhai.loc[bond_code, ["stock_code"]].stock_code

        self.ui.lineEdit_3.setText(stock_code)
        self.ui.lineEdit_5.setText(self.kezhuanzhai.loc[bond_code, ["bond_name"]].bond_name)
        self.ui.lineEdit_4.setText(self.kezhuanzhai.loc[bond_code, ["stock_name"]].stock_name)
        bond_realtime_price = ts.quotes(bond_code, conn=self.cons)
        bond_realtime_price = bond_realtime_price.set_index("code")
        stock_realtime_price = ts.get_realtime_quotes(stock_code)
        #print("stock rt price:%r "%stock_realtime_price)
        stock_realtime_price = stock_realtime_price.set_index("code")
        if not bond_realtime_price.empty:
            #bond bid2
            item = str(round(bond_realtime_price.loc[bond_code, ["bid2"]].bid2 / 10, 3))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(3, 0, newItem1)
            #bond bid1
            item = str(round(bond_realtime_price.loc[bond_code, ["bid1"]].bid1 / 10, 3))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(2, 0, newItem1)
            #bond ask1
            item = str(round(bond_realtime_price.loc[bond_code, ["ask1"]].ask1 / 10, 3))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(1, 0, newItem1)
            #bond ask2
            item = str(round(bond_realtime_price.loc[bond_code, ["ask2"]].ask2 / 10, 3))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(0, 0, newItem1)
            # bond bid_vol2
            item = str(int(bond_realtime_price.loc[bond_code, ["bid_vol2"]].bid_vol2))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(3, 1, newItem1)
            # bond bid_vol1
            item = str(int(bond_realtime_price.loc[bond_code, ["bid_vol1"]].bid_vol1))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(2, 1, newItem1)
            # bond ask_vol1
            item = str(int(bond_realtime_price.loc[bond_code, ["ask_vol1"]].ask_vol1))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(1, 1, newItem1)
            # bond ask_vol2
            item = str(int(bond_realtime_price.loc[bond_code, ["ask_vol2"]].ask_vol2))
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(0, 1, newItem1)

        if not stock_realtime_price.empty:
            #stock ask2
            #print(stock_realtime_price)
            item = str(stock_realtime_price.loc[stock_code,["a2_p"]].a2_p)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(0, 3, newItem1)

            # stock ask1
            item = str(stock_realtime_price.loc[stock_code, ["a1_p"]].a1_p)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(1, 3, newItem1)
            # stock bid1
            item = str(stock_realtime_price.loc[stock_code, ["b1_p"]].b1_p)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(2, 3, newItem1)
            # stock bid2
            item = str(stock_realtime_price.loc[stock_code, ["b2_p"]].b2_p)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(3, 3, newItem1)

            # stock ask_vol2
            item = str(stock_realtime_price.loc[stock_code, ["a2_v"]].a2_v)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(0, 4, newItem1)
            # stock ask_vol1
            item = str(stock_realtime_price.loc[stock_code, ["a1_v"]].a1_v)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(1, 4, newItem1)
            # stock bid_vol1
            item = str(stock_realtime_price.loc[stock_code, ["b1_v"]].b1_v)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(2, 4, newItem1)
            # stock bid_vol2
            item = str(stock_realtime_price.loc[stock_code, ["b2_v"]].b2_v)
            newItem1 = QtWidgets.QTableWidgetItem(item)
            self.ui.tableWidget.setItem(3, 4, newItem1)

        position = self.user.position
        max_sell_amount = 0
        print(position)
        for stock in position:
            if stock['证券代码'] == self.ui.lineEdit_3.text():
                print("可用余额： %s" % stock['可用余额'])
                if stock['可用余额'] > 0:
                    max_sell_amount = stock['可用余额']
                    stock_bid1 = float(self.ui.tableWidget.item(2, 3).text())
                    max_buy_bond_amount = stock_bid1 * int(max_sell_amount) / float(
                        self.ui.tableWidget.item(1, 0).text())
                    self.ui.lineEdit_7.setText(str(int(max_buy_bond_amount)))




    def zhuanzhai_trade(self):
        '''
        可转债的折价出现后，交易动作
        该接口使用easytrader
        :return:
        '''
        position = self.user.position
        max_sell_amount = 0
        print(position)
        for stock in position:
            if stock['证券代码'] == self.ui.lineEdit_3.text():
                print("可用余额： %s" % stock['可用余额'])
                if stock['可用余额'] > 0:
                    max_sell_amount = stock['可用余额']
                    stock_bid1 = float(self.ui.tableWidget.item(2, 3).text())
                    #max_buy_bond_amount = stock_bid1 * int(max_sell_amount) / float(self.ui.tableWidget.item(1, 0).text())
                    #self.ui.lineEdit_7.setText(str(int(max_buy_bond_amount)))
                    print("===========")
                    print("卖出正股名称：%s" % self.ui.lineEdit_4.text())
                    print("股票代码：%s" % self.ui.lineEdit_3.text())
                    print("卖出价格：%f" % stock_bid1)
                    print("卖出数量：%d" % max_sell_amount)
                    print("=============")

                    if self.ui.lineEdit_6.text() == "":
                        QtWidgets.QMessageBox(self, "提示", "没有输入买入的转债数量")
                        return

                    if int(self.ui.lineEdit_6.text()) > 0:
                        bond_ask1 = float(self.ui.tableWidget.item(1, 0).text())
                        print("***********************")
                        print("买转债名称：%s, , , " % self.ui.lineEdit_5.text())
                        print("转债代码：%s" % self.ui.lineEdit_2.text())
                        print("买入价格：%f" % bond_ask1)
                        print("买入数量：%d"% int(self.ui.lineEdit_6.text()))
                        print("************************")

                    else:
                        QtWidgets.QMessageBox(self, "提示", "没有输入买入的转债数量")
                        return
                    self.ui.textBrowser.append("卖出价格：%f" % stock_bid1)
                    self.ui.textBrowser.append("卖出数量：%d" % max_sell_amount)
                    self.ui.textBrowser.append("正股卖出总金额：%d" % (stock_bid1 * int(max_sell_amount)))
                    self.ui.textBrowser.append("==========")
                    if '6' in self.ui.lineEdit_3.text():
                        self.ui.textBrowser.append("买入价格：%f" % bond_ask1)
                        self.ui.textBrowser.append("买入数量：%d 张" % (int(self.ui.lineEdit_6.text())*10))
                        self.ui.textBrowser.append("转债买入总金额：%d" % (bond_ask1 * int(self.ui.lineEdit_6.text()) * 10))
                    else:
                        self.ui.textBrowser.append("买入价格：%f" % bond_ask1)
                        self.ui.textBrowser.append("买入数量：%d 张" % int(self.ui.lineEdit_6.text()))
                        self.ui.textBrowser.append("转债买入总金额：%d" % (bond_ask1 * int(self.ui.lineEdit_6.text())))

                    # self.user.sell(self.ui.lineEdit_3.text(), stock_bid1, max_sell_amount)
                    # self.user.buy(self.ui.lineEdit_2.text(), bond_ask1, self.ui.lineEdit_6.text())
                    print("正股卖出总金额：%d" % (stock_bid1 * int(max_sell_amount)))
                    print("转债买入总金额：%d" % (bond_ask1 * int(self.ui.lineEdit_6.text())))




class ProcessAUAGThread(QtCore.QThread):
    '''
    自动获取金银行情数据，计算金银比线程
    行情数据取至新浪sina
    该线程将获取的金银，及比值数据通过signal(auag_quotation)发射出去，发射的signal是一个list
    list按照au,ag,au/ag比值的顺序排列
    '''

    auag_quotation = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super(ProcessAUAGThread,self).__init__(parent)
        self.is_running = True
        self.future_realtime_data = sinaFutureData()

    def run(self):
        while self.is_running == True:
            signal_list = []
            au = self.future_realtime_data.get_future_price("au0")
            self.AU_price = au["AU0"]["new_price"]
            signal_list.append(self.AU_price)
            print(self.AU_price)
            ag = self.future_realtime_data.get_future_price("ag0")
            self.AG_price = ag["AG0"]["new_price"]
            signal_list.append(self.AG_price)
            if (self.AG_price > 0):
                value = self.AU_price / (self.AG_price / 1000)
                value = round(value, 2)
                signal_list.append(value)
            self.auag_quotation.emit(signal_list)
            time.sleep(20)
            print("ProcessAUAGThread alive %s"%datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def stop(self):
        self.is_running = False
        print("ProcessAUAGThread stop !")
        self.terminate()





if __name__ == "__main__":
    quanttimeMainWindows()

