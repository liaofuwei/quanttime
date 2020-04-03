#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import  quanttimeUI as quanttimeui
from PyQt5 import QtCore, QtGui, QtWidgets
import sys
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta

class convertBondProcessThread(QtCore.QThread):
    premium_info = QtCore.pyqtSignal(pd.DataFrame)
    bid_ask_info = QtCore.pyqtSignal(dict)

    def __init__(self, parent=None):
        super(convertBondProcessThread, self).__init__(parent)
        self.is_running = True

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        self.pro = ts.pro_api()  # ts 授权

        self.kezhuanzhai = pd.read_csv("C:\\quanttime\\data\\basic_info\\convert_bond_basic_info.csv", encoding="gbk")
        self.selected_kezhuanzhai = self.kezhuanzhai


    def run(self):
        while self.is_running == True:
            pass

    def init_get_premium(self):
        """
        获取可转债的实时折溢价情况
        可转债csv的columns:
        stock_name	bond_code stock_code bond_name	convert_price pb shuishou_price	qiangshu_price	expire	year_to_end
        """
        bond_codes = self.kezhuanzhai['bond_code'].map(str)  # get convertible bond code from csv
        stock_codes = self.kezhuanzhai['stock_code'].map(self.standard_code)
        conversion_price = self.kezhuanzhai['convert_price']  # get convertible bond conversion_price

        self.kezhuanzhai['bond_code'] = self.kezhuanzhai['bond_code'].map(str)
        self.kezhuanzhai['stock_code'] = self.kezhuanzhai['stock_code'].map(self.standard_code)
        self.kezhuanzhai['bond2amount'] = self.kezhuanzhai['convert_price'].map(self.amount)

        # 获取转债实时价格
        """
        Index(['code', 'price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
       'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
       'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
       'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
       'bid_vol5', 'ask_vol5'],
        dtype='object')
        """
        bond_realTime_price = ts.quotes(bond_codes, conn=self.cons)
        bond_rt_columns = ['code', 'price', 'bid1', 'bid_vol1', 'ask1',
                           'ask_vol1', 'bid2', 'bid_vol2', 'ask2', 'ask_vol2']
        bond_realTime_price = bond_realTime_price.loc[:, bond_rt_columns]
        dic_rename = {
            'price': 'bond_price',
            'bid1': 'bond_bid1',
            'bid_vol1': 'bond_bid_vol1',
            'ask1': 'bond_ask1',
            'ask_vol1': 'bond_ask_vol1',
            'bid2': 'bond_bid2',
            'bid_vol2': 'bond_bid_vol2',
            'ask2': 'bond_ask2',
            'ask_vol2': 'bond_ask_vol2'
        }
        bond_realTime_price = bond_realTime_price.rename(columns=dic_rename)

        bond_realTime_price.index = pd.RangeIndex(len(self.kezhuanzhai.index))

        # 获取对应的正股股价
        """
        Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
       'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
       'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
       'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
        dtype='object')
        """
        stock_realTime_price = ts.get_realtime_quotes(stock_codes)
        stock_realTime_price = stock_realTime_price.loc[:,
                               ['code', 'name', 'price', 'bid', 'ask', 'b2_p', 'b2_v', 'a2_p', 'a2_v', \
                                'a1_v', 'b1_v']]
        stock_realTime_price = stock_realTime_price.rename(columns={"price": "stock_price", \
                                                                    "bid": "stock_bid", \
                                                                    'ask': "stock_ask", \
                                                                    'b2_p': "stockb2_p", \
                                                                    'b2_v': 'stockb2_v', \
                                                                    'a2_p': 'stocka2_p', \
                                                                    'a2_v': 'stocka2_v'})

        # stock_realtime_price.head(2)
        bond = pd.merge(self.kezhuanzhai, bond_realTime_price, left_on="bond_code", right_on="code")
        bond = bond.drop(columns=["code"])  # 去掉一个重复的code列,这个code是可转债实时行情的code

        '''
        bond:index
        stock_name	bond_code stock_code bond_name	convert_price pb shuishou_price	qiangshu_price	expire	year_to_end
        ['price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
       'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
       'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
       'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
       'bid_vol5', 'ask_vol5']

        '''
        bond[["pb", "convert_price", "shuishou_price", "bond2amount", "bond_price", "bond_bid1", "bond_bid_vol1",
              "bond_ask1", "bond_ask_vol1", \
              "bond_bid2", "bond_bid_vol2", "bond_ask2", "bond_ask_vol2"]] = bond[
            ["pb", "convert_price", "shuishou_price", "bond2amount", \
             "bond_price", "bond_bid1", "bond_bid_vol1", "bond_ask1", "bond_ask_vol1", \
             "bond_bid2", "bond_bid_vol2", "bond_ask2", "bond_ask_vol2"]].convert_objects(convert_numeric=True)

        # bond.set_index("scode")
        # stock_realTime_price.set_index("code")
        # print("bond index:%r"%bond.index)
        # print("stock_realTime_price index:%r"%stock_realTime_price.index)
        '''
        bond:index
        stock_name	bond_code stock_code bond_name	convert_price pb shuishou_price	qiangshu_price	expire	year_to_end
        ['price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
        'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
        'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
        'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
        'bid_vol5', 'ask_vol5']
        Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
       'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
       'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
       'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
        dtype='object')
        '''
        bond = pd.merge(bond, stock_realTime_price, left_on="stock_code", right_on="code")
        bond = bond.drop(columns=["code"])  # 去掉一个重复的code列，这个code是正股实时行情的code

        # print("========")
        # print(stock_realTime_price)

        bond[["stock_price", "stock_bid", "stock_ask", "stockb2_p", "stockb2_v", "stocka2_p", "stocka2_v"]] \
            = bond[["stock_price", "stock_bid", "stock_ask", "stockb2_p", "stockb2_v", "stocka2_p",
                    "stocka2_v"]].convert_objects(convert_numeric=True)

        # print(bond)
        # stockbid2bondprice > bond 价格才有套利价值，此时买入转债，卖出正股，可以实现无风险实时套利
        bond["stockbid2bondprice"] = bond["bond2amount"] * bond["stock_bid"] * 10  # 将正股买一价折算成转债价格

        # bond[["bcode","bid1","stockbid2bondprice","name"]]
        # print("bond index:%r" % bond.index)
        bond["premium"] = (bond["bond_bid1"] - bond["stockbid2bondprice"]) / bond["stockbid2bondprice"]

        # 将premium < 0 有套利机会的选出来，作为重点观察对象
        #!!!!!!!!通过修改bond.premium < -0.0X来调整选择的溢价范围！！！！！！！！
        self.selected_kezhuanzhai = bond[bond.premium < -0.01]
        print(self.selected_kezhuanzhai)

        bond["premium"] = bond["premium"].map(self.display_percent_format)
        bond = bond.round(4)


        '''
        需要显示的折价情况，为了减少显示的内容，当前只显示'bond_name', 'bond_code', 'stock_code', 'premium','stock_name'
        需要增加可以自由添加
        display_table_columns = ['bond_name', 'bond_code', 'stock_code', 'premium', 'bond_bid1', 'bond_ask1', 'stock_bid',
                         'stock_ask', 'stock_name',…………]
        '''
        display_table_columns = ['bond_name', 'bond_code', 'premium', 'stock_code', 'stock_name']
        #table = PrettyTable(table_columns)
        display_df = self.selected_kezhuanzhai.loc[:, display_table_columns]
        #self.premium_info.emit(display_df.to_dict('index'))
        self.premium_info.emit(display_df)


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
            print("code is not standard,code=%r ", joinquant_code)
            return -1



    def amount(self, x):
        '''
        该函数处理一张转债对应的正股股数
        计算方法如下：转债面值（即100）除以转股价
        转股价从csv基本信息表中读取
        '''
        try:
           x = float(x)
        except:
            return 0
        if x == 0:
            print("convert price is zero please check ")
            return -1
        else:
            return 100/x

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


    def buy_stock_amount(self, x):
        '''
        功能：根据转股价，计算需要购买正股量的最小数量
        '''
        try:
            stock_amount = float(x)
        except:
            print("input bond2amount is not numberic")
            return 0
        if stock_amount < 0:
            print("input bond2amount < 0 ")
            return 0

        if (stock_amount > 1) and (stock_amount < 10) :
            return round(round(stock_amount * 100)/100)*100
        elif (stock_amount > 10) and (stock_amount < 100) :
            return  round(round(stock_amount * 10)/100)*100
        elif stock_amount > 100:
            return round(round(stock_amount)/100)*100
        else:
            return round(stock_amount)
