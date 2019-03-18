#-*-coding:utf-8 -*-
__author__ = 'Administrator'
import sys

import tushare as ts
from datetime import datetime, timedelta
import pandas as pd
import logging
from jqdatasdk import *
from prettytable import PrettyTable
sys.path.append('C:\\quanttime\\src\\mydefinelib\\')
import mydefinelib as mylib


class AUAGRadioTest(object):

    def __init__(self):

        self.position = {"ag": 0, "au": 0, "status": "empty"}

        self.gold_future = "C:\\quanttime\\data\\gold\\sh_future\\gold.csv"
        self.silver_future = "C:\\quanttime\\data\\gold\\sh_future\\silver.csv"

        self.gold_future_data = pd.read_csv(self.gold_future, parse_dates=["date"], index_col=["date"])
        self.gold_future_data = self.gold_future_data[~self.gold_future_data.reset_index().duplicated().values]

        self.silver_future_data = pd.read_csv(self.silver_future, parse_dates=["date"], index_col=["date"])
        self.silver_future_data = self.silver_future_data[~self.silver_future_data.reset_index().duplicated().values]

        self.future_data = pd.merge(self.gold_future_data, self.silver_future_data, left_index=True, right_index=True,
                               suffixes=('_gold', '_silver'))
        self.future_data["compare"] = self.future_data["close_gold"] / self.future_data["close_silver"] * 1000

        #金银合并以后的交易日序列
        self.future_data_trade_date = self.future_data.index
        #print(self.future_data_trade_date[-1])
        #print(type(self.future_data_trade_date[-1]))

        self.gold_future_mins = "C:\\quanttime\\data\\gold\\sh_future\\gold_mins.csv"
        self.silver_future_mins = "C:\\quanttime\\data\\gold\\sh_future\\silver_mins.csv"
        self.gold_future_mins_data = pd.read_csv(self.gold_future_mins, parse_dates=["datetime"], index_col=["datetime"])
        #print("gold future mins data before duplicated:%d" % len(self.gold_future_mins_data))
        self.gold_future_mins_data = self.gold_future_mins_data[~self.gold_future_mins_data.reset_index().duplicated().values]
        #print("gold future mins data after duplicated:%d" % len(self.gold_future_mins_data))
        self.silver_future_mins_data = pd.read_csv(self.silver_future_mins, parse_dates=["datetime"], index_col=["datetime"])
        self.silver_future_mins_data = self.silver_future_mins_data[~self.silver_future_mins_data.reset_index().duplicated().values]

        self.future_data_mins = pd.merge(self.gold_future_mins_data, self.silver_future_mins_data, left_index=True, right_index=True,
                                    suffixes=('_gold', '_silver'))
        #print(self.future_data_mins.index)

    def get_appoint_date_stat_info(self, strDate, nDays):
        '''
        获取指定日期前推或者往后推的几天的统计信息，统计信息来源为合并后的self.future_data
        :param strDate:input 日期，如2010-10-10
        :param nDays:往前或者往后的n天,<0 往前推，>0往后推
        :return:统计信息的pd.dataframe
        '''
        inputDate = pd.to_datetime(strDate)
        if nDays < 0:
            if self.future_data.index.tolist().index(inputDate) + nDays >= 0:
                pre_date = self.future_data.index[self.future_data.index.tolist().index(inputDate) + nDays]
                pre_date_1 = self.future_data.index[self.future_data.index.tolist().index(inputDate) -1]
                #print("input date: %s, stat begin:%s, end:%s" % (strDate, pre_date, pre_date_1))
                return self.future_data.loc[pre_date:pre_date_1, ["compare"]].describe()
            else:
                return -1
        else:
            if self.future_data.index.tolist().index(inputDate) + nDays <= len(self.future_data.index) - 1:
                after_date = self.future_data.index[self.future_data.index.tolist().index(inputDate) + nDays]
                after_date_1 = self.future_data.index[self.future_data.index.tolist().index(inputDate) + 1]
                #print("input date: %s, stat begin:%s, end:%s" % (strDate, after_date_1, after_date))
                return self.future_data.loc[after_date_1:after_date,["compare"]].describe()
            else:
                return -1

    def get_close_trade_date(self,strDate):
        '''
        获取输入日期最近的交易日，如果输入日期就是交易日则为本身，否则为退后一天，直至为交易日
        :param strDate: str类型的日期
        :return: 返回self.future_data_trade_date中的某一交易日
        '''
        inputDatelist = pd.date_range(start=strDate, periods=100)
        for date in inputDatelist:
            try:
                return self.future_data.index[self.future_data.index.tolist().index(date)]
            except:
                continue
        print("erro")



    def run_back_test(self):
        '''

        :return:
        '''
        for trade_day in self.future_data_trade_date:
            pass



if __name__ == "__main__":
    test = AUAGRadioTest()
    result = test.get_appoint_date_stat_info("2019-03-11",-20)
    print(result)