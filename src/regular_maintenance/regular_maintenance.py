#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
from futuquant import *
import logging
'''
日常维护使用：
1、更新历史k线数据，该数据使用时利用futu接口获取，因为该数据已存在futu历史数据中，属于本地获取

2、更新finance数据，finance数据按照valuation，income，cash_flow，balance，indicator5个文件夹分别存储，
   其中20180730之前的数据已经批量获取存储在本地，之后只需要更新维护即可
   valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
   balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
   cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
   income_file_path = "C:\\quanttime\\data\\finance\\income\\"
   indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"


3、重点研究或者需要研究的股票，所有文件夹按照如下命名格式：600001，000001，00700，不保留中文命名
   所在目录为：C:\quanttime\data\stock

4、该文件夹内存放历史K线数据，财经数据等该只股票的所有相关数据

5、

'''

class regularMaintenance:
    def __init__(self):
        #初始化日志
        self.regularlog = logging.getLogger("regular_maintenance")
        self.cmdconsole =  logging.StreamHandler()
        self.cmdconsole.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.cmdconsole.setFormatter(self.formatter)
        self.regularlog.addHandler(self.cmdconsole)

        #jqdata context
        self.quote_ctx = OpenQuoteContext(host='127.0.0.1',port=11111)#futu context
        auth('13811866763',"sam155") #jqdata 授权

        self.data_dir = "C:\\quanttime\\data\\stock\\"
        self.finance_dir = "C:\\quanttime\\data\\finance\\"
        all_files = os.listdir(self.data_dir)
        self.regularlog.debug("code: %r", all_files)
        if len(all_files) == 0:
            self.regularlog.warning("stock dir is empty,please check!!!")
        self.codes = [] #所有需要更新的代码list
        for p in all_files:
            tmpfile = self.data_dir + p
            if os.path.isdir(tmpfile): #判断是否为文件夹，如果是输出所有文件就改成： os.path.isfile()
                code = p.split(".")
                self.codes.append(code[0])
                #print(self.codes)

        self.regularlog.debug("本次已存在文件夹中需要更新的code：%r", self.codes)

    '''
    从futu中获取历史K线数据
    '''
    def get_K_hisdata_from_futu(self):
        end_time = datetime.today().date().strftime("%Y-%m-%d")
        for code in self.codes:
            (ret, df) = self.quote_ctx.get_history_kline(code[0:9], start='2006-01-01', end=end_time)
        if ret == RET_OK:
            pathname = self.data_dir + code + + ".csv"
            print(pathname)
            df.to_csv(pathname)

    '''
    该方法为第一次获取该股票数据时，在数据根目录下创建对应的文件夹
    及获取该只股票的所有历史K线数据（当前只取日线级别数据)，财经数据等
    code按如下格式："sh.600001"
    '''
    def first_get_data(self, code):
        #code:eg SH.601318
        #1、创建目录
        create_dir_name = self.data_dir + code[3:9] #路径中去除中文
        if not os.path.exists(create_dir_name):
            os.mkdir(create_dir_name)
            self.regularlog.warning("create dir name %r", create_dir_name)
        else:
            self.regularlog.warning("the dir is existed!!!")
            #return

        #2、从futu获取K线历史数据
        end_time = datetime.today().date().strftime("%Y-%m-%d")
        start_time = datetime.strptime("2006-01-01", "%Y-%m-%d")
        (ret, df) = self.quote_ctx.get_history_kline(code, start='2006-01-01', end=end_time)
        pathname = self.data_dir + code[3:9] + "\\" + code[3:9] + ".csv"
        if ret == RET_OK:
            self.regularlog.debug("the pathname:%r", pathname)
            df.to_csv(pathname)
        else:
            self.regularlog.warning("first get data failed,error code:%r", df)
            self.regularlog.warning("path:%r", pathname)

        #3、从聚宽获取财务数据
        table_name = ['valuation', 'balance', 'cash_flow', 'income', 'indicator']

        #valuation表，每日更新
        q = query(valuation).filter(valuation.code == code)
        dates = pd.date_range(start_time, end_time)
        init_date = datetime.strptime("2006-01-01", "%Y-%m-%d")
        for date in dates:
            df_valuation = get_fundamentals(q, date)
            if df_valuation.empty:
                continue
            else:
                init_date = date
                print("df_valuation:%r " %(df_valuation))
                break
        print("init_data:%r" % (init_date))
        df_valuation = get_fundamentals(q,init_date)
        dates = pd.date_range(init_date, end_time, freq='d')
        for date in dates:
            tmp = get_fundamentals(q, date)
            if tmp.empty:
                continue
            else:
                df_valuation = pd.concat([df_valuation, tmp])
        valuation_file = self.data_dir + code[3:9] + "\\" + "valuation_" + code[3:9] +'.csv'
        df_valuation.to_csv(valuation_file)
        self.regularlog.warning("get valuation table success")



    def update_data(self,code):
        return

    '''
    statdate 该参数主要用于更新balance，cash_flow，income，indicator表，明确更新的年份季度
    参数类型为str 例如“2018q1”表示季度数据或者“2018”表示年度数据
    数据已更新到2018q1
    '''
    def update_all_finance(self, strDate):
        #update数据的文件夹路径
        balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
        cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
        income_file_path = "C:\\quanttime\\data\\finance\\income\\"
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"

        #获取所有stock信息
        stock_info = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_stock_info.csv",encoding='gbk')

        #获取balance表
        for i in range(0,len(stock_info)):
            file_path = balance_file_path + str(stock_info.iloc[i,0]) + ".csv"
            q = query(balance).filter(balance.code == stock_info.iloc[i,0])
            df_balance = get_fundamentals(q,statDate = strDate)
            if os.path.exists(file_path):
                df_balance.to_csv(file_path,mode='a',header=None)
            else:
                df_balance.to_csv(file_path)
            print("get balance code:%r successful"%(stock_info.iloc[i,0]))

        #获取cash_flow表
        for i in range(0,len(stock_info)):
            file_path = cash_flow_file_path + str(stock_info.iloc[i,0]) + ".csv"
            q = query(cash_flow).filter(cash_flow.code==stock_info.iloc[i,0])
            df_cash = get_fundamentals(q, statDate = strDate)
            if os.path.exists(file_path):
                df_cash.to_csv(file_path,mode='a',header=None)
            else:
                df_cash.to_csv(file_path)
            print("get cash flow code:%r successful"%(stock_info.iloc[i,0]))

        #获取income表
        for i in range(0,len(stock_info)):
            file_path = income_file_path + str(stock_info.iloc[i,0]) + ".csv"
            q = query(income).filter(income.code==stock_info.iloc[i,0])
            df_income = get_fundamentals(q, statDate = strDate)
            if os.path.exists(file_path):
                df_income.to_csv(file_path,mode='a',header=None)
            else:
                df_income.to_csv(file_path)
            print("get income code:%r successful"%(stock_info.iloc[i,0]))

        #获取indicator表
        for i in range(0,len(stock_info)):
            file_path = indicator_file_path + str(stock_info.iloc[i,0]) + ".csv"
            q = query(indicator).filter(indicator.code==stock_info.iloc[i,0])
            df_indicator = get_fundamentals(q, statDate = strDate)
            if os.path.exists(file_path):
                df_indicator.to_csv(file_path,mode='a',header=None)
            else:
                df_indicator.to_csv(file_path)
            print("get indicator code:%r successful"%(stock_info.iloc[i,0]))
        return

    '''
    获取valuation表数据，valuation表属于高频数据，按日更新，所以单独做成一个方法
    nCount:从今天起，往前取几天的数据，例如nCount=10，表示从今天起，往前取10个交易日的数据
    '''
    def update_all_valuation(self,nCount):
        #update数据的文件夹路径
        valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"

        #获取所有stock信息
        stock_info = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_stock_info.csv",encoding='gbk')
        #1、更新 valuation表
        end_time = datetime.today().date().strftime("%Y-%m-%d")
        for i in range(0,len(stock_info)):
            file_path = valuation_file_path + str(stock_info.iloc[i, 0]) + ".csv"
            q = query(valuation).filter(valuation.code == stock_info.iloc[i, 0])
            df_valuation = get_fundamentals_continuously(q,end_date=end_time, count=nCount)
            if os.path.exists(file_path):
                df_valuation.minor_xs(str(stock_info.iloc[i,0])).to_csv(file_path,mode='a',header=None)
            else:
                df_valuation.minor_xs(str(stock_info.iloc[i,0])).to_csv(file_path)
        print("get valuation code:%r successful"%(stock_info.iloc[i,0]))


if __name__ == "__main__":
    regular = regularMaintenance()
    regular.first_get_data("SH.601318")