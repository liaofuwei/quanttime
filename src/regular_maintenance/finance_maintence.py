# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta

from futuquant import *
import logging

'''
日常维护使用：
1、数据来源：joinquant

2、更新finance数据，finance数据按照valuation，income，cash_flow，balance，indicator5个文件夹分别存储，
   其中20180730之前的数据已经批量获取存储在本地，之后只需要更新维护即可
   valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
   balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
   cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
   income_file_path = "C:\\quanttime\\data\\finance\\income\\"
   indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"

3、valuetion表数据是按日获取

4、income，cash_flow,balance,indicator表分为批量更新与指定code更新

'''


class financeMaintenance:
    def __init__(self):
        # 初始化日志
        self.regularlog = logging.getLogger("finance_maintenance")
        self.cmdconsole = logging.StreamHandler()
        self.cmdconsole.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.cmdconsole.setFormatter(self.formatter)
        self.regularlog.addHandler(self.cmdconsole)

        self.finance_dir = "C:\\quanttime\\data\\finance\\"

        # 从C:\quanttime\data\basic_info文件夹获取所有的code，删除退市的代码
        self.stock_info = pd.read_csv(
            "C:\\quanttime\\data\\basic_info\\all_stock_info.csv",
            index_col=["code"],
            encoding="gbk")
        # 退市代码，end_time!=2200/1/1
        stock_code = self.stock_info[self.stock_info["end_date"] == "2200/1/1"]
        self.stock_code = stock_code.index
        if len(self.stock_code) == 0:
            self.regularlog.warning("stock code is empty,please check!!!")
            sys.exit(0)

        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        self.end_time = yesterday.strftime("%Y-%m-%d")

        self.errorCodeList = []

        # jqdata context
        auth('13811866763', "sam155")  # jqdata 授权
    # ==============================================================================

    def batchstanderUpdateFile(self):
        '''
        标准化更新的文件格式，防止需要更新的文件与获取的文件格式不一致，导致文件混乱
        valuation表：column--'id','day'，'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
       'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
       'circulating_market_cap', 'pe_ratio_lyr'
        '''
        valuation_file_path = self.finance_dir + "valuation\\"
        for code in self.stock_code:
            update_file = valuation_file_path + code + ".csv"
            if os.path.exists(update_file):
                valuation_data = pd.read_csv(update_file)
                try:
                    valuation_data = valuation_data[['id',
                                                     'day',
                                                     'code',
                                                     'pe_ratio',
                                                     'turnover_ratio',
                                                     'pb_ratio',
                                                     'ps_ratio',
                                                     'pcf_ratio',
                                                     'capitalization',
                                                     'market_cap',
                                                     'circulating_cap',
                                                     'circulating_market_cap',
                                                     'pe_ratio_lyr']]

                    valuation_data.to_csv(update_file)
                    print("stander valuation code:%r" % code)
                except KeyError:
                    print("keyerror code:%r" % code)
                    valuation_data = valuation_data[['id',
                                                     'day.1',
                                                     'code.1',
                                                     'pe_ratio',
                                                     'turnover_ratio',
                                                     'pb_ratio',
                                                     'ps_ratio',
                                                     'pcf_ratio',
                                                     'capitalization',
                                                     'market_cap',
                                                     'circulating_cap',
                                                     'circulating_market_cap',
                                                     'pe_ratio_lyr']]
                    valuation_data.columns = [
                        'id',
                        'day',
                        'code',
                        'pe_ratio',
                        'turnover_ratio',
                        'pb_ratio',
                        'ps_ratio',
                        'pcf_ratio',
                        'capitalization',
                        'market_cap',
                        'circulating_cap',
                        'circulating_market_cap',
                        'pe_ratio_lyr']
                    valuation_data.to_csv(update_file)
                    print("keyerror code:%r update end" % code)
    # ==============================================================================

    def standerSingleUpdateFile(self, code):
        '''
        标准化单个的文件，防止需要更新的文件与获取的文件格式不一致，导致文件混乱
        valuation表：column--'id','day'，'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
       'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
       'circulating_market_cap', 'pe_ratio_lyr'
        :param code: joinquant代码
        :return:
        '''
        valuation_file_path = self.finance_dir + "valuation\\"

        update_file = valuation_file_path + str(code) + ".csv"
        if (os.path.exists(update_file)):
            valuation_data = pd.read_csv(update_file)
            try:
                valuation_data = valuation_data[['id',
                                                 'day',
                                                 'code',
                                                 'pe_ratio',
                                                 'turnover_ratio',
                                                 'pb_ratio',
                                                 'ps_ratio',
                                                 'pcf_ratio',
                                                 'capitalization',
                                                 'market_cap',
                                                 'circulating_cap',
                                                 'circulating_market_cap',
                                                 'pe_ratio_lyr']]

                valuation_data.to_csv(update_file)
                print("stander valuation code:%r" % code)
            except KeyError:
                print("keyerror code:%r" % code)
                valuation_data = valuation_data[['id',
                                                 'day.1',
                                                 'code.1',
                                                 'pe_ratio',
                                                 'turnover_ratio',
                                                 'pb_ratio',
                                                 'ps_ratio',
                                                 'pcf_ratio',
                                                 'capitalization',
                                                 'market_cap',
                                                 'circulating_cap',
                                                 'circulating_market_cap',
                                                 'pe_ratio_lyr']]
                valuation_data.columns = [
                    'id',
                    'day',
                    'code',
                    'pe_ratio',
                    'turnover_ratio',
                    'pb_ratio',
                    'ps_ratio',
                    'pcf_ratio',
                    'capitalization',
                    'market_cap',
                    'circulating_cap',
                    'circulating_market_cap',
                    'pe_ratio_lyr']
                valuation_data.to_csv(update_file)
                print("keyerror code:%r update end" % code)

    # ==============================================================================
    def drop_duplicate(self):
        '''
        不用买次都运行，可以每月使用一次该函数
        功能：去除冗余的行，joinquant更新数据，可能有冗余的数据行
        :return:
        '''
        valuation_file_path = self.finance_dir + "valuation\\"
        for code in self.stock_code:
            update_file = valuation_file_path + code + ".csv"
            if (os.path.exists(update_file)):
                valuation_data = pd.read_csv(update_file)
                print("%r 去重前，数据行数：%r" % (code, len(valuation_data.index)))
                valuation_data = valuation_data.drop_duplicates('day')
                print("%r 去重后，数据行数：%r" % (code, len(valuation_data.index)))
                print("==============================")
                valuation_data.to_csv(update_file, index=False)
    # ==============================================================================

    def update(self):
        '''
        更新数据
        一次更新valuation，balance，income，case_flow，indicator表
        '''
        self.update_valuation()
        print("all stock valuation table update end ")
        self.regularlog.debug("all stock valuation table update end ")

    # ==============================================================================
    def update_valuation(self):
        '''
        更新valuation表
        目标文件夹valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
        增量更新，更新到当前日期的前一天
        如果有新加入的stock，则创建文件
        '''
        valuation_file_path = self.finance_dir + "valuation\\"
        for code in self.stock_code:
            update_file = valuation_file_path + code + ".csv"
            # print(update_file)
            if os.path.exists(update_file):
                valuation_data = pd.read_csv(update_file)
                print("读取的文件行数：%r" % len(valuation_data.index))
                if valuation_data.empty:  # 文件为空，则从基础信息中获取上市日期，从上市日期开始更新
                    print("文件 %code 存在，但没有数据，从上市日期开始获取数据" % code)
                    start_date = self.stock_info.loc[code, ["start_date"]]
                    # start_date返回是series，要根据index把值取出来，值类型是'2007/3/1'
                    start_time = start_date.start_date
                    q = query(valuation).filter(valuation.code == code)
                    dates = pd.date_range(start_time, self.end_time)
                    for date in dates:
                        df_valuation = get_fundamentals(q, date)
                        if df_valuation.empty:
                            continue
                        else:
                            valuation_data = valuation_data[['id',
                                                             'day',
                                                             'code',
                                                             'pe_ratio',
                                                             'turnover_ratio',
                                                             'pb_ratio',
                                                             'ps_ratio',
                                                             'pcf_ratio',
                                                             'capitalization',
                                                             'market_cap',
                                                             'circulating_cap',
                                                             'circulating_market_cap',
                                                             'pe_ratio_lyr']]
                            valuation_data = pd.concat(
                                [valuation_data, df_valuation], sort=False)
                    valuation_data.to_csv(update_file, mode='a', header=None)
                    self.regularlog.info(
                        "valuation code:%r(souce file empty) update successful" %
                        code)
                    print(
                        "valuation code:%r(souce file empty) update successful" %
                        code)
                else:
                    print("文件存在，增量更新开始，code：%r" % code)
                    start_date = valuation_data.day[len(valuation_data.day) - 1]
                    print("增量更新 start_date:%r" % start_date)
                    if '/' in start_date:
                        tmp_start_date = datetime.strptime(start_date, "%Y/%m/%d")
                        yesterday = datetime.today().date() - timedelta(days=1)
                        # print("tmp_start_date:%r,yesterday:%r "%(tmp_start_date, yesterday))
                        if tmp_start_date.date() == yesterday:
                            print(" code: %r already update to %r" % (code, yesterday.strftime("%Y-%m-%d")))
                            continue

                    if '-' in start_date:
                        tmp_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                        yesterday = datetime.today().date() - timedelta(days=1)
                        if tmp_start_date == yesterday:
                            print("already update to %r" % yesterday.strftime("%Y-%m-%d"))
                            continue

                    q = query(valuation).filter(valuation.code == code)
                    try:
                        dates = pd.date_range(start_date, self.end_time)
                        print("增量更新日期：%r-->%r" % (start_date, self.end_time))
                    except BaseException:
                        self.errorCodeList.append(code)
                        continue

                    df_valuation = get_fundamentals(q, dates[0])  # 节假日返回的是前一个交易日的数据
                    for date in dates:
                        tmp = get_fundamentals(q, date)
                        if tmp.empty:
                            continue
                        else:
                            df_valuation = pd.concat([df_valuation, tmp], sort=False)
                            print("获取一次数据，df_valuation 行数为：%r" % len(df_valuation.index))
                    df_valuation = df_valuation[['id',
                                                 'day',
                                                 'code',
                                                 'pe_ratio',
                                                 'turnover_ratio',
                                                 'pb_ratio',
                                                 'ps_ratio',
                                                 'pcf_ratio',
                                                 'capitalization',
                                                 'market_cap',
                                                 'circulating_cap',
                                                 'circulating_market_cap',
                                                 'pe_ratio_lyr']]
                    df_valuation.to_csv(update_file, mode='a', header=None)
                    print("获取数据结束，写入csv的，df_valuation 行数为：%r" % len(df_valuation.index))
                    self.regularlog.info("增量更新 valuation code:%r update successful" % code)
            else:
                print("需要更新的文件code：% 不存在，创建并获取" % code)
                start_date = self.stock_info.loc[code, ["start_date"]]
                # start_date返回是series，要根据index把值取出来，值类型是'2007/3/1'
                start_time = start_date.start_date
                tmp_ST = datetime.strptime(start_time, "%Y/%m/%d")
                # 如果上市时间早于2014-01-01，则从2014年开始获取数据，前面数据不用了
                if tmp_ST < datetime.strptime("2004-01-01", "%Y-%m-%d"):
                    start_time = "2006-01-03"
                else:
                    # 获取数据按照上市后半年在用
                    tmp_ST = tmp_ST + timedelta(days=7)
                    start_time = tmp_ST.strftime("%Y-%m-%d")
                q = query(valuation).filter(valuation.code == code)
                try:
                    dates = pd.date_range(start_time, self.end_time)
                except BaseException:
                    self.errorCodeList.append(code)
                    continue
                valuation_data = get_fundamentals(q, start_time)
                error_counter = 0
                for date in dates:
                    try:
                        df_valuation = get_fundamentals(q, date)
                        # print(df_valuation)
                        if df_valuation.empty:
                            continue
                        else:
                            valuation_data = valuation_data[['id',
                                                             'day',
                                                             'code',
                                                             'pe_ratio',
                                                             'turnover_ratio',
                                                             'pb_ratio',
                                                             'ps_ratio',
                                                             'pcf_ratio',
                                                             'capitalization',
                                                             'market_cap',
                                                             'circulating_cap',
                                                             'circulating_market_cap',
                                                             'pe_ratio_lyr']]
                            valuation_data = pd.concat([valuation_data, df_valuation], sort=False)
                    except BaseException:
                        error_counter = error_counter + 1
                        continue
                if error_counter > 0:
                    self.errorCodeList.append(code)
                valuation_data.to_csv(update_file)
                # 按格式标准化
                self.standerSingleUpdateFile(code)
                self.regularlog.info("valuation code:%r create&update successful" % code)
                print("valuation code:%r create&update successful" % code)
            if len(self.errorCodeList) > 0:
                print("error code list:%r" % self.errorCodeList)
            print("update code:%r, end" % code)

    # ==============================================================================

    def update_balance_by_code(self, code_list, strDate):
        '''
        更新balance，cash_flow，income，indicator数据
        季度数据，不判断最后更新时间，通过传入strDate值，更新季度或年度数据
        code_list:输入需要更新的code，list结构
        strDate:'2019q1'季度值，’2019‘年度值
        '''
        # update数据的文件夹路径
        balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"

        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        if len(code_list) != 0:
            for code in code_list:
                file_path = balance_file_path + code + ".csv"
                try:
                    q = query(balance).filter(balance.code == code)
                    df_balance = get_fundamentals(q, statDate=strDate)
                    if df_balance.empty:
                        print(
                            "get balance code:%r ,but return empty pd" %
                            (code))
                        continue
                    if os.path.exists(file_path):
                        df_balance.to_csv(file_path, mode='a', header=None)
                    else:
                        df_balance.to_csv(file_path)
                except BaseException:
                    error_code_list.append(code)
                    continue
                print("get balance code:%r successful" % (code))
            print("error code list:%r" % error_code_list)
            return

    # ==============================================================================
    def batch_update_balance(self, strDate):
        '''
        批量更新balance表
        strDate：'2019q1'季度值，’2019‘年度值
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        '''
        balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        # 获取balance表
        for code in self.stock_code:
            file_path = balance_file_path + code + ".csv"
            try:
                q = query(balance).filter(balance.code == code)
                df_balance = get_fundamentals(q, statDate=strDate)
                if df_balance.empty:
                    print("get balance code:%r ,but return empty pd" % (code))
                    continue
                if os.path.exists(file_path):
                    df_balance.to_csv(file_path, mode='a', header=None)
                else:
                    df_balance.to_csv(file_path)
                print("get balance code:%r successful" % (code))
            except BaseException:
                error_code_list.append(code)
                continue
        print("error code list:%r" % error_code_list)
    # ==============================================================================

    def update_case_flow(self, code_list, strDate):
        '''
        更新balance，cash_flow，income，indicator数据
        季度数据，不判断最后更新时间，通过传入strDate值，更新季度或年度数据
        code_list:输入需要更新的code，list结构
        strDate:'2019q1'季度值，’2019‘年度值
        '''
        cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        if len(code_list) != 0:
            for code in code_list:
                file_path = cash_flow_file_path + code + ".csv"
                try:
                    q = query(cash_flow).filter(cash_flow.code == code)
                    df_cash = get_fundamentals(q, statDate=strDate)
                    if df_cash.empty:
                        print(
                            "get cash flow code:%r ,but return empty pd" %
                            (code))
                        continue
                    if os.path.exists(file_path):
                        df_cash.to_csv(file_path, mode='a', header=None)
                    else:
                        df_cash.to_csv(file_path)
                except BaseException:
                    error_code_list.append(code)
                    continue
                print("get cash flow code:%r successful" % (code))
            print("error code list:%r" % error_code_list)
            return
    # ==============================================================================

    def batch_update_case_flow(self, strDate):
        '''
        批量更新case_flow
        strDate：'2019q1'季度值，’2019‘年度值
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        '''
        cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
        error_code_list = []
        # 获取balance表
        for code in self.stock_code:
            file_path = cash_flow_file_path + code + ".csv"
            try:
                q = query(cash_flow).filter(cash_flow.code == code)
                df_cash = get_fundamentals(q, statDate=strDate)
                if df_cash.empty:
                    print(
                        "get cash flow code:%r ,but return empty pd" %
                        (code))
                    continue
                if os.path.exists(file_path):
                    df_cash.to_csv(file_path, mode='a', header=None)
                else:
                    df_cash.to_csv(file_path)
            except BaseException:
                error_code_list.append(code)
                continue
            print("get cash flow code:%r successful" % (code))
        print("error code list:%r" % error_code_list)

    # ==============================================================================
    def update_income(self, code_list, strDate):
        '''
        更新balance，cash_flow，income，indicator数据
        季度数据，不判断最后更新时间，通过传入strDate值，更新季度或年度数据
        code_list:输入需要更新的code，list结构
        strDate:'2019q1'季度值，’2019‘年度值
        '''
        income_file_path = "C:\\quanttime\\data\\finance\\income\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        if len(code_list) != 0:
            for code in code_list:
                file_path = income_file_path + code + ".csv"
                try:
                    q = query(income).filter(income.code == code)
                    df_income = get_fundamentals(q, statDate=strDate)
                    if df_income.empty:
                        print(
                            "get income code:%r ,but return empty pd" %
                            (code))
                        continue
                    if os.path.exists(file_path):
                        df_income.to_csv(file_path, mode='a', header=None)
                    else:
                        df_income.to_csv(file_path)
                except BaseException:
                    error_code_list.append(code)
                    continue
                print("get income code:%r successful" % (code))
            print("error code list:%r" % error_code_list)

    # ==============================================================================
    def batch_update_income(self, strDate):
        '''
        批量更新income
        strDate：'2019q1'季度值，’2019‘年度值
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        '''
        income_file_path = "C:\\quanttime\\data\\finance\\income\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []

        for code in self.stock_code:
            file_path = income_file_path + code + ".csv"
            try:
                q = query(income).filter(income.code == code)
                df_income = get_fundamentals(q, statDate=strDate)
                if df_income.empty:
                    print("get income code:%r ,but return empty pd" % (code))
                    continue
                if os.path.exists(file_path):
                    df_income.to_csv(file_path, mode='a', header=None)
                else:
                    df_income.to_csv(file_path)
            except BaseException:
                error_code_list.append(code)
                continue
            print("get income code:%r successful" % (code))
        print("error code list:%r" % error_code_list)

    # ==============================================================================

    def update_indicator(self, code_list, strDate):
        '''
        更新balance，cash_flow，income，indicator数据
        季度数据，不判断最后更新时间，通过传入strDate值，更新季度或年度数据
        code_list:输入需要更新的code，list结构
        strDate:'2019q1'季度值，’2019‘年度值
        '''
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        if len(code_list) != 0:
            for code in code_list:
                file_path = indicator_file_path + code + ".csv"
                try:
                    q = query(indicator).filter(indicator.code == code)
                    df_indicator = get_fundamentals(q, statDate=strDate)
                    if df_indicator.empty:
                        print(
                            "get indicator code:%r ,but return empty pd" %
                            (code))
                        continue
                    if os.path.exists(file_path):
                        df_indicator.to_csv(file_path, mode='a', header=None)
                    else:
                        df_indicator.to_csv(file_path)
                except BaseException:
                    error_code_list.append(code)
                    continue
                print("get indicator code:%r successful" % (code))
            print("error code list:%r" % error_code_list)
    # ==============================================================================

    def batch_update_indicator(self, strDate):
        '''
        批量更新indicator
        strDate：'2019q1'季度值，’2019‘年度值
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        '''
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        for code in self.stock_code:
            file_path = indicator_file_path + code + ".csv"
            try:
                q = query(indicator).filter(indicator.code == code)
                df_indicator = get_fundamentals(q, statDate=strDate)
                if df_indicator.empty:
                    print(
                        "get indicator code:%r ,but return empty pd" %
                        (code))
                    continue
                if os.path.exists(file_path):
                    df_indicator.to_csv(file_path, mode='a', header=None)
                else:
                    df_indicator.to_csv(file_path)
            except BaseException:
                error_code_list.append(code)
                continue
            print("get indicator code:%r successful" % (code))
        print("error code list:%r" % error_code_list)

    # ==============================================================================


if __name__ == "__main__":
    regular = financeMaintenance()
    # regular.standerSingleUpdateFile("600432.XSHG")
    # regular.drop_duplicate()
    regular.update()
    # regular.update_balance(["000001.XSHE"],'2018q2')
    # regular.batch_update_balance('2018q3')
    # regular.update_case_flow(["000001.XSHE"],'2018q2')
    # regular.batch_update_case_flow('2018q3')
    # regular.update_indicator(["000001.XSHE"],'2018q2')
    # regular.batch_update_indicator('2018q3')
