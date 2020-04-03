#-*-coding:utf-8 -*-
__author__ = 'Administrator'
import sys

import tushare as ts
from datetime import datetime, timedelta
import json
import pandas as pd
import logging
from jqdatasdk import *
from prettytable import PrettyTable



class RealTimeTradeEvent(object):

    def __init__(self):
        #初始化日志
        self.log_format = "%(asctime)s - %(levelname)s - %(message)s"
        self.date_format = "%Y-%m-%d %H:%M:%S %p"
        self.log_file_name = "C:\\quanttime\\log\\realTimeTradeEvent.log"

        logging.basicConfig(filename=self.log_file_name, level=logging.DEBUG, format=self.log_format, \
                                 datefmt=self.date_format)

        #tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        self.pro = ts.pro_api()  # ts 授权

        #joinquant 授权
        #auth('13811866763', "sam155")  # jqdata 授权

        #读取可转债基本basic info table
        #kezhuanzhai.csv path C:\quanttime\data\basic_info
        '''
        stock_name	bond_code	stock_code	bond_name	convert_price pb	shuishou_price	qiangshu_price	expire	year_to_end
        ST辉丰	    128012	    002496.XSHE	辉丰转债	    7.71	      0.89    5.4	        10.02	         2022/4/21	3.334

        '''
        self.kezhuanzhai = pd.read_csv("C:\\quanttime\\data\\basic_info\\convert_bond_basic_info.csv",encoding="gbk")

        self.selected_kezhuanzhai = self.kezhuanzhai

    '''
    获取配置文件的路径
    '''
    def getConfigFilePath(self):
        with open("..\\..\\config.json", "r") as configfile:
            configdata = json.loads(configfile.read())
            src_root = configdata["src_dir"]
        configfile.close()

        srcpath = src_root+"\\auto_hunter"
        sys.path.append(srcpath)
        logging.debug("srcpath:%r　",srcpath)



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
            logging.error("x=0 error ")
            return 0
        else:
            return 100/x

    def display_percent_format(self, x):
        '''
        功能：小数按照百分数%显示，保留两位小数
        '''
        try:
            data = float(x)
        except:
            logging.debug("input is not numberic")
            return 0

        return "%.2f%%"%(data * 100)


    def buy_stock_amount(self, x):
        '''
        功能：根据转股价，计算需要购买正股量的最小数量
        '''
        try:
            stock_amount = float(x)
        except:
            logging.debug("input bond2amount is not numberic")
            return 0
        if stock_amount < 0:
            logging.debug("input bond2amount < 0 ")
            return 0

        if (stock_amount > 1) and (stock_amount < 10) :
            return round(round(stock_amount * 100)/100)*100
        elif (stock_amount > 10) and (stock_amount < 100) :
            return  round(round(stock_amount * 10)/100)*100
        elif stock_amount > 100:
            return round(round(stock_amount)/100)*100
        else:
            return round(stock_amount)


    def getRealTimePremiumFirst(self):

        """
        获取可转债的实时折溢价情况
        可转债csv的columns:
        stock_name	bond_code stock_code bond_name	convert_price pb shuishou_price	qiangshu_price	expire	year_to_end
        """
        bond_codes = self.kezhuanzhai['bond_code'].map(str) #get convertible bond code from csv
        stock_codes = self.kezhuanzhai['stock_code'].map(self.standard_code)
        conversion_price = self.kezhuanzhai['convert_price'] #get convertible bond conversion_price

        self.kezhuanzhai['bond_code'] = self.kezhuanzhai['bond_code'].map(str)
        self.kezhuanzhai['stock_code'] = self.kezhuanzhai['stock_code'].map(self.standard_code)
        self.kezhuanzhai['bond2amount'] = self.kezhuanzhai['convert_price'].map(self.amount)

        #获取转债实时价格
        """
        Index(['code', 'price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
       'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
       'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
       'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
       'bid_vol5', 'ask_vol5'],
        dtype='object')
        """
        bond_realTime_price = ts.quotes(bond_codes, conn=self.cons)
        bond_realTime_price = bond_realTime_price.loc[:, ['code','price','bid1','bid_vol1','ask1','ask_vol1',\
                                                          'bid2','bid_vol2','ask2','ask_vol2']]
        bond_realTime_price = bond_realTime_price.rename(columns={'price':'bond_price',\
                                                                  'bid1':'bond_bid1', \
                                                                  'bid_vol1':'bond_bid_vol1', \
                                                                  'ask1':'bond_ask1', \
                                                                  'ask_vol1':'bond_ask_vol1', \
                                                                  'bid2': 'bond_bid2', \
                                                                  'bid_vol2': 'bond_bid_vol2', \
                                                                  'ask2': 'bond_ask2', \
                                                                  'ask_vol2': 'bond_ask_vol2'
                                                                  })

        bond_realTime_price.index = pd.RangeIndex(len(self.kezhuanzhai.index))


        #获取对应的正股股价
        """
        Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
       'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
       'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
       'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
        dtype='object')
        """
        stock_realTime_price = ts.get_realtime_quotes(stock_codes)
        stock_realTime_price = stock_realTime_price.loc[:, ['code','name','price','bid','ask','b2_p','b2_v','a2_p','a2_v',\
                                                            'a1_v','b1_v']]
        stock_realTime_price = stock_realTime_price.rename(columns={"price":"stock_price",\
                                                                    "bid":"stock_bid",\
                                                                    'ask':"stock_ask",\
                                                                    'b2_p':"stockb2_p",\
                                                                    'b2_v':'stockb2_v',\
                                                                    'a2_p':'stocka2_p',\
                                                                    'a2_v':'stocka2_v'})

        #stock_realtime_price.head(2)
        bond = pd.merge(self.kezhuanzhai, bond_realTime_price, left_on="bond_code", right_on="code")
        bond = bond.drop(columns=["code"])#去掉一个重复的code列,这个code是可转债实时行情的code
        """====for debug===
        print("================")
        print(type(self.kezhuanzhai["bcode"][0]))
        print("================")
        print(type(bond_realTime_price["code"][0]))
        print("================")
        print(bond)
        print("================")
        #self.kezhuanzhai.set_index("bcode")
        #bond_realtime_price.set_index("code")
        #bond = pd.merge(self.kezhuanzhai,bond_realtime_price,left_index=True,right_index=True)
        """

        '''
        bond:index
        stock_name	bond_code stock_code bond_name	convert_price pb shuishou_price	qiangshu_price	expire	year_to_end
        ['price', 'last_close', 'open', 'high', 'low', 'vol', 'cur_vol',
       'amount', 's_vol', 'b_vol', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1',
       'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3',
       'ask_vol3', 'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5',
       'bid_vol5', 'ask_vol5']
       
        '''
        bond[["pb","convert_price","shuishou_price","bond2amount","bond_price","bond_bid1","bond_bid_vol1","bond_ask1","bond_ask_vol1",\
              "bond_bid2","bond_bid_vol2","bond_ask2","bond_ask_vol2"]] = bond[["pb","convert_price","shuishou_price","bond2amount",\
                                                          "bond_price","bond_bid1","bond_bid_vol1","bond_ask1","bond_ask_vol1",\
                                                          "bond_bid2","bond_bid_vol2","bond_ask2","bond_ask_vol2"]].convert_objects(convert_numeric=True)


        #bond.set_index("scode")
        #stock_realTime_price.set_index("code")
        #print("bond index:%r"%bond.index)
        #print("stock_realTime_price index:%r"%stock_realTime_price.index)
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

        #print("========")
        #print(stock_realTime_price)

        bond[["stock_price","stock_bid","stock_ask","stockb2_p","stockb2_v","stocka2_p","stocka2_v"]]\
            = bond[["stock_price","stock_bid","stock_ask","stockb2_p","stockb2_v","stocka2_p","stocka2_v"]].convert_objects(convert_numeric=True)

        #print(bond)
        #stockbid2bondprice > bond 价格才有套利价值，此时买入转债，卖出正股，可以实现无风险实时套利
        bond["stockbid2bondprice"] = bond["bond2amount"]*bond["stock_bid"]*10 #将正股买一价折算成转债价格

        #bond[["bcode","bid1","stockbid2bondprice","name"]]
        #print("bond index:%r" % bond.index)
        bond["premium"] = (bond["bond_bid1"]-bond["stockbid2bondprice"]) / bond["stockbid2bondprice"]

        #将premium < 0 有套利机会的选出来，作为重点观察对象
        self.selected_kezhuanzhai = bond[bond.premium < -0.01]
        print(self.selected_kezhuanzhai)

        bond["premium"] = bond["premium"].map(self.display_percent_format)
        bond = bond.round(4)

        table_columns = ['bond_name','bond_code','stock_code','premium','bond_bid1','bond_ask1','stock_bid','stock_ask','stock_name']
        table = PrettyTable(table_columns)
        display_df = bond.loc[:, table_columns]
        for display_index in display_df.index:
            table.add_row(bond.loc[display_index, table_columns].tolist())

        print(table)
        """
        设置筛选条件，将折价<1%的选出来，每次轮询的时候，不在全部轮询，只查询范围内的转债
        
        select_bond = bond[bond["premium"] < 0.01] #把折价3%的找出来
        self.select = select_bond[["bcode","scode","convertprice","bond2amount"]]
        print(select_bond)

        #select_bond.to_csv("C:\\quanttime\\src\\convertbond\\kezhuanzhai_tmp.csv")
        """

    #---------------------------------------------------------------------
    def getRealTimePremium(self):
        """
        对于首次获取的折溢价情况，刷选出重点关注的bond，在此方法中持续获取折溢价行情
        :return:
        """
        if self.selected_kezhuanzhai.empty:
            print("current time select bond table is empty")
            return

        #去掉包含实时数据的列
        static_columns = ['stock_name', 'bond_code', 'stock_code', 'bond_name', 'convert_price', 'pb', \
                          'expire', 'bond2amount']
        self.selected_kezhuanzhai = self.selected_kezhuanzhai.loc[:, static_columns]

        bond_codes = self.selected_kezhuanzhai.bond_code
        stock_codes = self.selected_kezhuanzhai.stock_code
        bond_realTime_price = ts.quotes(bond_codes, conn=self.cons)
        bond_realTime_price = bond_realTime_price.loc[:, ['code', 'price', 'bid1', 'bid_vol1', 'ask1', 'ask_vol1', \
                                                          'bid2', 'bid_vol2', 'ask2', 'ask_vol2']]
        bond_realTime_price = bond_realTime_price.rename(columns={'price': 'bond_price', \
                                                                  'bid1': 'bond_bid1', \
                                                                  'bid_vol1': 'bond_bid_vol1', \
                                                                  'ask1': 'bond_ask1', \
                                                                  'ask_vol1': 'bond_ask_vol1', \
                                                                  'bid2': 'bond_bid2', \
                                                                  'bid_vol2': 'bond_bid_vol2', \
                                                                  'ask2': 'bond_ask2', \
                                                                  'ask_vol2': 'bond_ask_vol2'
                                                                  })

        stock_realTime_price = ts.get_realtime_quotes(stock_codes)
        stock_realTime_price = stock_realTime_price.loc[:, ['code', 'name', 'price', 'bid', 'ask', 'b2_p', 'b2_v', \
                                                            'a2_p', 'a2_v', 'a1_v', 'b1_v']]
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

        bond = pd.merge(bond, stock_realTime_price, left_on="stock_code", right_on="code")
        bond = bond.drop(columns=["code"])  # 去掉一个重复的code列，这个code是正股实时行情的code


        bond["stockbid2bondprice"] = pd.to_numeric(bond["bond2amount"]) * pd.to_numeric(bond["stock_bid"]) * 10  # 将正股买一价折算成转债价格

        # bond[["bcode","bid1","stockbid2bondprice","name"]]
        # print("bond index:%r" % bond.index)
        bond["premium"] = (pd.to_numeric(bond["bond_bid1"]) - bond["stockbid2bondprice"]) / pd.to_numeric(bond["stockbid2bondprice"])

        bond["premium"] = bond["premium"].map(self.display_percent_format)
        bond = bond.round(4)

        bond["buy_stock_vol"] = pd.to_numeric(bond["bond2amount"]) * 10
        bond["buy_stock_amount"] =pd.to_numeric(bond["stock_bid"]) * pd.to_numeric(bond["buy_stock_vol"])
        bond["buy_bond_vol"] = 1#pd.to_numeric(bond["buy_stock_amount"]) / pd.to_numeric(bond["bond_ask1"])
        #bond["buy_bond_vol"] = bond["buy_bond_vol"].map(round)
        bond["buy_bond_amount"] = bond["buy_bond_vol"] * pd.to_numeric(bond["bond_ask1"])


        table_columns = ['bond_name', 'bond_code', 'stock_code', 'premium', 'bond_bid1', 'bond_ask1', 'stock_bid',
                         'stock_ask', 'stock_name']
        table = PrettyTable(table_columns)
        display_df = bond.loc[:, table_columns]
        for display_index in display_df.index:
            table.add_row(bond.loc[display_index, table_columns].tolist())

        print(table)

        print("====")
        table_columns = ['bond_name' ,"buy_bond_vol","buy_bond_amount","buy_stock_vol", "buy_stock_amount", 'stock_name']
        buy_sell_table = PrettyTable(table_columns)
        display_df = bond.loc[:, table_columns]
        for display_index in display_df.index:
            buy_sell_table.add_row(bond.loc[display_index, table_columns].tolist())
        print(buy_sell_table)

        print("=======")
        table_columns = ['bond_name', "buy_bond_1手", "buy_bond_2手", "buy_bond_3手", "buy_bond_4手", "buy_bond_5手", \
                        "buy_bond_6手", "buy_bond_7手", "buy_bond_8手", "buy_bond_9手", 'stock_name']
        buy_vol_table = PrettyTable(table_columns)
        for i in range(1,10):
            bond[table_columns[i]] = bond["buy_stock_vol"] * i

        display_df = bond.loc[:, table_columns]
        display_df = display_df.round()
        for display_index in display_df.index:
            buy_vol_table.add_row(bond.loc[display_index, table_columns].tolist())
        print(buy_vol_table)
        print("buy_bond_1手表示：1手转债对应的正股数量，其他以此类推")










if __name__ == "__main__":
    bond_premium = RealTimeTradeEvent()
    all_last_time = datetime(2018, 12, 1, 1, 1, 1, 000)
    select_last_time = datetime(2018, 12, 1, 1, 1, 1, 000)


    while True:
        current_time = datetime.now()
        #if (current_time.hour == 11) and (current_time.minute == 30)

        delta1 = current_time - all_last_time
        if delta1.seconds >= 5*60: #5分钟全表获取一次
            bond_premium.getRealTimePremiumFirst()
            all_last_time = current_time

        delta2 = current_time - select_last_time
        if delta2.seconds > 5: #5s循环获取一次
            bond_premium.getRealTimePremium()
            select_last_time = current_time