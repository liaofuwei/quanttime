# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta

from futuquant import *
import logging
import pymongo
import tushare as ts
sys.path.append(('C:\\quanttime\\src\\comm'))
import trade_date_util

"""
日常维护使用：
1、数据来源：joinquant, tushare

2、更新finance数据，finance数据按照valuation，income，cash_flow，balance，indicator5个文件夹分别存储，
   其中20180730之前的数据已经批量获取存储在本地，之后只需要更新维护即可
   valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
   balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
   cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
   income_file_path = "C:\\quanttime\\data\\finance\\income\\"
   indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"

3、valuation表数据是按日获取

4、income，cash_flow,balance,indicator表分为批量更新与指定code更新

5、20190529增加tushare的每日指标更新，相当于joinquant的valuation
   存储目录为"C:\\quanttime\\data\\finance\\ts\\valuation\\"
   
6、20190604增加tushare每日指标更新方式，按日获取所有的股票的valuation，然后分别更新到对应的文件夹
   加快更新的效率

"""


class financeMaintenance:
    def __init__(self):
        # 初始化日志
        self.regularlog = logging.getLogger("finance_maintenance")
        self.cmdconsole = logging.StreamHandler()
        self.cmdconsole.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.cmdconsole.setFormatter(self.formatter)
        self.regularlog.addHandler(self.cmdconsole)

        self.finance_dir = "C:\\quanttime\\data\\finance\\"

        # 从C:\quanttime\data\basic_info文件夹获取所有的code，删除退市的代码
        self.stock_info = pd.read_csv(
            "C:\\quanttime\\data\\basic_info\\all_stock_info.csv",
            index_col=["code"],
            encoding="gbk",
            parse_dates=["start_date", "end_date"])
        # 退市代码，end_time!=2200/1/1
        stock_code = self.stock_info[self.stock_info["end_date"] == datetime.strptime("2200-01-01", "%Y-%m-%d")]
        self.stock_code = stock_code.index
        if len(self.stock_code) == 0:
            self.regularlog.warning("stock code is empty,please check!!!")
            sys.exit(0)

        yesterday = datetime.today().date() - timedelta(days=1)
        self.end_time = yesterday.strftime("%Y-%m-%d")

        self.errorCodeList = []

        # jqdata context
        auth('13811866763', "sam155")  # jqdata 授权

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        # ts 授权
        self.pro = ts.pro_api()

        # mongodb client
        self.mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
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
        tmp_code = code.split(".")[0]
        finance_db = self.mongo_client["finance_db"]
        table_data = finance_db[tmp_code]
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
                table_data.insert_many(valuation_data.to_dict(orient="record"))
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
                table_data.insert_many(valuation_data.to_dict(orient="record"))
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
            if os.path.exists(update_file):
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
        更新valuation表 joinquant
        目标文件夹valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
        增量更新，更新到当前日期的前一天
        如果有新加入的stock，则创建文件
        '''
        valuation_file_path = self.finance_dir + "valuation\\"
        finance_db = self.mongo_client["finance_db"]
        for code in self.stock_code:
            update_file = valuation_file_path + code + ".csv"
            tmp_code = code.split(".")[0]
            table_data = finance_db[tmp_code]
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
                    table_data.insert_many(valuation_data.to_dict(orient="record"))
                    self.regularlog.info(
                        "valuation code:%r(source file empty) update successful" %
                        code)
                    print(
                        "valuation code:%r(source file empty) update successful" %
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
                    table_data.insert_many(valuation_data.to_dict(orient="record"))
                    print("获取数据结束，写入csv的，df_valuation 行数为：%r" % len(df_valuation.index))
                    self.regularlog.info("增量更新 valuation code:%r update successful" % code)
            else:
                print("需要更新的文件code：%s 不存在，创建并获取" % code)
                # start_date返回是series，要根据index把值取出来，值类型是datetime类型
                start_date = self.stock_info.loc[code, ["start_date"]].start_date

                # 如果上市时间早于2014-01-01，则从2014年开始获取数据，前面数据不用了
                if start_date < datetime.strptime("2014-01-01", "%Y-%m-%d"):
                    start_time = datetime.strptime("2014-01-04", "%Y-%m-%d")
                else:
                    # 获取数据按照上市后一个月在用
                    start_time = start_time + timedelta(days=30)
                end_time = datetime.today().date() - timedelta(days=1)
                if start_time.date() <= end_time:
                    # 取数据的时间比当日的前一天晚
                    continue
                q = query(valuation).filter(valuation.code == code)
                try:
                    dates = pd.date_range(start_time.date(), end_time)
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

                self.regularlog.info("valuation code:%s create&update successful" % code)
                print("valuation code:%s create&update successful" % code)
            if len(self.errorCodeList) > 0:
                print("error code list:%r" % self.errorCodeList)
            print("update code:%r, end" % code)

    # ==============================================================================

    def update_valuation_by_jq_day(self):
        '''
        更新joinquant的valuation表。采取按天获取所有的stock，然后按股票更新到不同的文件夹
        改善更新的效率
        :return:
        '''
        last_update = pd.read_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv',
                                  index_col=["module"], parse_dates=["date"])
        print(last_update)
        module_name = 'update_valuation_by_jq_day'
        last_update_date = 0

        # 将空值填0
        last_update = last_update.fillna(0)
        if module_name in last_update.index:
            last_update_date = last_update.loc[module_name, ["date"]].date

        if last_update_date != 0:
            try:
                if last_update_date.date() >= datetime.today().date() - timedelta(days=1):
                    print("记录已更新，今日不用再更新")
                    return
            except ValueError:
                print("记录的最后更新日期格式或记录有误")
        df_stocks_info = get_all_securities(types=['stock'])
        # 提取还在上市的股票
        df_stocks_info = df_stocks_info[df_stocks_info["end_date"] == "2200-01-01"]
        list_stocks_code = df_stocks_info.index.tolist()
        list_date_stock = []
        # valuation文件夹中没有，说明是上市没多久的新股，该新股放入新股list
        list_new_stocks = []
        for stock_code in list_stocks_code:
            valuation_file = self.finance_dir + "valuation\\" + stock_code + '.csv'
            if not os.path.exists(valuation_file):
                print("valution table:%s 不存在" % stock_code)
                list_new_stocks.append(stock_code)
                continue
            df_valuation = pd.read_csv(valuation_file, parse_dates=["day"])
            if df_valuation.empty:
                print("读取的valuation：%s为空" % stock_code)
                continue
            tmp_last_date = df_valuation.iloc[-1, df_valuation.columns.get_loc('day')].date()
            tmp_stock_code = df_valuation.iloc[-1, df_valuation.columns.get_loc('code')]
            list_date_stock.append([tmp_last_date, tmp_stock_code])
        df_date_stock = pd.DataFrame(data=list_date_stock, columns=["date", "code"])
        df_date_stock = df_date_stock.set_index("date")
        list_different_date = df_date_stock.index.unique()
        dic_date_stocks = {}
        for last_date in list_different_date:
            # same_last_update_stock是具有相同最后更新日期的stock集合
            same_last_update_stock = df_date_stock.loc[last_date, ["code"]]["code"]
            if isinstance(same_last_update_stock, str):
                dic_date_stocks[str(last_date)] = df_date_stock.loc[last_date, ["code"]]["code"]
            else:
                dic_date_stocks[str(last_date)] = df_date_stock.loc[last_date, ["code"]]["code"].tolist()

        finance_db = self.mongo_client["finance_db"]
        # 对于某个日期，数量少(小于500只股票)的stock，采取按code，一只一只更新，不采取批量更新的办法
        list_single_update_by_code = []
        cloumns_name = ['Unnamed: 0', 'id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
                        'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap', 'circulating_market_cap',
                        'pe_ratio_lyr']
        for tmp_last_date, stock_codes in dic_date_stocks.items():
            if len(stock_codes) < 500:
                list_single_update_by_code.append(stock_codes)
                continue
            tmp_start = datetime.strptime(tmp_last_date, "%Y-%m-%d") + timedelta(days=1)
            tmp_end = datetime.today().date() - timedelta(days=1)
            list_trade_date = get_trade_days(start_date=tmp_start, end_date=tmp_end)
            if len(list_trade_date) == 0:
                print("不用更新，start：%s，end：%s" % (str(tmp_start), str(tmp_end)))
                continue
            for tmp_update_date in list_trade_date:
                df_data = get_fundamentals(query(valuation).filter(
                        # 这里不能使用 in 操作, 要使用in_()函数
                        valuation.code.in_(list_stocks_code)
                    ), date=tmp_update_date)
                if df_data.empty:
                    print("获取的valuation数据为空")
                    continue
                file_path = self.finance_dir + "\\valuation_day\\" + str(tmp_update_date) + '.csv'
                df_data.to_csv(file_path)
                for i in range(len(df_data)):
                    df_stock = pd.DataFrame(data=[df_data.iloc[i].values],
                                            index=pd.Index([df_data.index[i]]),
                                            columns=df_data.columns)
                    # 重新排列列名，与已存在数据匹配
                    record_columns = ['id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio',
                                      'ps_ratio', 'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                                      'circulating_market_cap','pe_ratio_lyr']
                    df_stock = df_stock[record_columns]
                    tmp_name = df_data.iloc[i, df_data.columns.get_loc('code')]
                    if tmp_name in list_new_stocks:
                        print("%s 是新股，不增量更新" % tmp_name)
                        continue
                    print("正在处理批量更新的 %s。。。。" % tmp_name)
                    file_path = self.finance_dir + "\\valuation\\" + tmp_name + '.csv'
                    df_stock.to_csv(file_path, mode='a', header=None)
                    # 存数据库
                    table = finance_db[tmp_name[0:6]]
                    table.insert_many(df_stock.to_dict(orient="record"))

        print("需要单只更新的code list:%r" % list_single_update_by_code)
        tmp_list = []
        for i in list_single_update_by_code:
            if isinstance(i, list):
                for j in i:
                    tmp_list.append(j)
            else:
                tmp_list.append(i)
        list_single_update_by_code = tmp_list

        for i_code in list_single_update_by_code:
            self.update_valuation_jq_by_code(i_code)

        # 处理新股的更新
        print("本次需要更新的新股：%r" % list_new_stocks)
        for i_code in list_new_stocks:
            self.update_valuation_jq_by_code(i_code)

        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = str(datetime.today().date() - timedelta(days=1))
        last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')
        print("本次更新结束")

    # ==============================================================================

    def update_valuation_jq_by_code(self, code):
        '''
        更新单只股票的聚宽valuation
        :param code: 股票代码
        :return:
        '''
        valuation_file = self.finance_dir + "\\valuation\\" + code + '.csv'
        finance_db = self.mongo_client["finance_db"]
        end_date = datetime.today().date() - timedelta(days=1)
        print(code)
        if os.path.exists(valuation_file):
            df_valuation = pd.read_csv(valuation_file, index_col=["day"], parse_dates=True)
            last_date = df_valuation.index[-1].date().strftime("%Y-%m-%d")
            trade_range = trade_date_util.get_trade_date_range(last_date, end_date.strftime("%Y-%m-%d"))
            if len(trade_range) == 1:
                print("%s不用更新,start=%s, end=%s" % (code, last_date, end_date))
                return
            q = query(valuation).filter(valuation.code == code)
            df_data = get_fundamentals_continuously(q, end_date=end_date, count=len(trade_range)-1).to_frame()
            if df_data.empty:
                print("获取的valuation为空，stock：%s" % code)
                return
            # 重新排列列名，与已存在数据匹配
            record_columns = ['Unname:1', 'id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio',
                              'ps_ratio', 'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                              'circulating_market_cap', 'pe_ratio_lyr']
            # 无用列，纯粹为了匹配之前的格式
            df_data["Unname:1"] = 0
            df_data = df_data.rename(columns={'day.1': 'day', 'code.1': 'code'})
            df_data[record_columns].to_csv(valuation_file, index=False, mode='a', header=None)
            # 存数据库
            table = finance_db[code[0:6]]
            table.insert_many(df_data.to_dict(orient="record"))
            print("更新完stock：%s" % code)
        else:
            q = query(valuation).filter(valuation.code == code)
            df_data = get_fundamentals_continuously(q, end_date=end_date, count=100).to_frame()
            if df_data.empty:
                print("获取的valuation为空，stock：%s" % code)
                return
            # 重新排列列名，与已存在数据匹配
            record_columns = ['Unname:1', 'id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio',
                              'ps_ratio', 'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                              'circulating_market_cap', 'pe_ratio_lyr']

            df_data["Unname:1"] = 0
            df_data = df_data.rename(columns={'day.1': 'day', 'code.1': 'code'})
            df_data[record_columns].to_csv(valuation_file, index=False)
            # 存数据库
            table = finance_db[code[0:6]]
            table.insert_many(df_data.to_dict(orient="record"))
            print("更新完stock：%s" % code)

    # ==========================================

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
                            code)
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
        """
        批量更新indicator
        strDate：'2019q1'季度值，’2019‘年度值
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        """
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        for code in self.stock_code:
            file_path = indicator_file_path + code + ".csv"
            try:
                q = query(indicator).filter(indicator.code == code)
                df_indicator = get_fundamentals(q, statDate=strDate)
                if df_indicator.empty:
                    print("get indicator code:%r ,but return empty pd" % code)
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
        print("处理记录冗余。。。。")
        self.del_duplicate_indicator_record(False)

    # =====================================================
    def batch_update_indicator_year(self, strDate):
        """
        批量更新indicator年度指标
        strDate：'2019'年度值，
        不需要传入code ，需要更新的code通过读取所有的stock code获得
        """
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        for code in self.stock_code:
            file_path = indicator_file_path + code[0:6] + '_year' + code[6:11] + ".csv"
            try:
                q = query(indicator).filter(indicator.code == code)
                df_indicator = get_fundamentals(q, statDate=strDate)
                if df_indicator.empty:
                    print("get indicator code:%r ,but return empty pd" % code)
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
        print("处理记录冗余。。。。")
        self.del_duplicate_indicator_record(True)

    # =====================================================
    def del_duplicate_indicator_record(self, bYear):
        """
        删除冗余的indicator记录
        :param bYear: true:处理年indicator文件，false处理按季度更新的indicator文件
        :return:
        """
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        error_code_list = []
        # for test ####self.stock_code = ['000001.XSHE','000002.XSHE','000004.XSHE']
        for code in self.stock_code:
            if bYear:
                file_path = indicator_file_path + code[0:6] + '_year' + code[6:11] + ".csv"
            else:
                file_path = indicator_file_path + code + ".csv"
            if os.path.exists(file_path):
                df_indicator = pd.read_csv(file_path)
                df_indicator = df_indicator.drop_duplicates('statDate')
                if not df_indicator.empty:
                    df_indicator.to_csv(file_path, index=False)
            else:
                error_code_list.append(code)
        print("处理有问题code：%r" % error_code_list)


    # =====================================================
    def update_valuation_by_ts(self):
        """
        通过tushare接口更新valuation表
        存储目录为"C:\\quanttime\\data\\finance\\ts\\valuation\\"
        1、首先通过tushare获取所有上市股票code--pro.stock_basic
        2、根据上述获取的所有上市股票code，轮询所有code，获取valuation
        3、运行目录下csv记录模块最后更新的日期及最后更新的code
        module	                date	        last_update_code
        update_valuation_by_ts	2019/5/13	    000001

        'ts_code'：ts代码 str
        'trade_date'：交易日期
        'close'：当日收盘价 float
        'turnover_rate'：换手率%
        'turnover_rate_f'：换手率（自由流通股）%
       'volume_ratio'：量比
       'pe'：
       'pe_ttm'
       'pb'
       'ps'：市销率
       'ps_ttm'
       'total_share'：总股本（万股）
       'float_share'：流通股本
       'free_share'：自由流通股本
       'total_mv'：总市值（万）
       'circ_mv'：流通市值

        :return:
        """

        last_update = pd.read_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv',
                                  index_col=["module"], parse_dates=["date"])
        print(last_update)
        module_name = 'update_valuation_by_ts'
        last_update_date = ""
        last_update_code = ""

        # 将空值填0
        last_update = last_update.fillna(0)
        if module_name in last_update.index:
            last_update_date = last_update.loc[module_name, ["date"]].date
            last_update_code = last_update.loc[module_name, ["last_update_code"]].last_update_code

        if last_update_date != 0:
            try:
                if last_update_date.date() >= datetime.today().date() - timedelta(days=1):
                    print("记录已更新，今日不用再更新")
                    return
            except ValueError:
                print("记录的最后更新日期格式或记录有误")

        # 股票基本信息只获取ts_code与list_date
        stock_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
        if stock_basic.empty:
            print("获取ts股票基本信息失败，return")
            return
        stock_basic = stock_basic.set_index("ts_code")
        stock_basic['list_date'] = stock_basic['list_date'].apply(pd.to_datetime)

        update_code_list = []
        if last_update_code == 0:
            update_code_list = stock_basic.index
        else:
            get_index_loc = stock_basic.index.get_loc(last_update_code)
            if isinstance(get_index_loc, int):
                update_code_list = stock_basic.index[get_index_loc:]
            else:
                print("由last_update_code在所有股票信息表中获取对应的code_list失败")
                return
        # ==for test
        # print("本次需要更新数量：%d" % len(update_code_list))
        # update_code_list = update_code_list[0:5]
        # print("测试更新的code：%r" % update_code_list)
        basic_dir = "C:\\quanttime\\data\\finance\\ts\\valuation\\"
        finance_db = self.mongo_client["ts_finance_db"]
        end_date = str(datetime.today().date() - timedelta(days=1))
        tmp_list = end_date.split('-')
        end_date = tmp_list[0] + tmp_list[1] + tmp_list[2]
        for stock_code in update_code_list:
            valuation_file = basic_dir + stock_code + '.csv'
            if os.path.exists(valuation_file):
                valuation_data = pd.read_csv(valuation_file, index_col=["trade_date"], parse_dates=True)
                if valuation_data.index[-1].date() >= datetime.today().date() - timedelta(days=1):
                    print("code：%s 已经是最新了，更次不更新" % stock_code)
                    continue
                start_date = valuation_data.index[-1].date().strftime("%Y-%m-%d")
                start_date = trade_date_util.get_close_trade_date(start_date, 1)
                tmp_list = start_date.split("-")
                if len(tmp_list) != 3:
                    print("日期解析的格式有误，code：%s" % stock_code)
                    continue
                start_date = tmp_list[0] + tmp_list[1] + tmp_list[2]
                df_data = self.pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date)
                if df_data.empty:
                    print("code:%r 本次更新获取的数据为空" % stock_code)
                    time.sleep(0.3)
                    continue
                df_data = df_data.set_index("trade_date")
                df_data = df_data.sort_index(ascending=True)
                df_data.to_csv(valuation_file, mode='a', header=None)
                # 存数据库
                table = finance_db[stock_code[0:6]]
                table.insert_many(df_data.to_dict(orient="record"))
                print("tushare 更新valuation数据 完成，code：%s" % stock_code)
                time.sleep(0.3)
            else:
                start_date = "20050103"
                df_data = self.pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date)
                if df_data.empty:
                    print("code:%r 本次首次获取的数据为空" % stock_code)
                    continue
                df_data = df_data.set_index("trade_date")
                df_data = df_data.sort_index(ascending=True)
                df_data.to_csv(valuation_file)
                # 存数据库
                table = finance_db[stock_code[0:6]]
                table.insert_many(df_data.to_dict(orient="record"))
                print("code:%s 首次获取数据更新完成" % stock_code)
        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = str(datetime.today().date() - timedelta(days=1))
        last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')

    # ============================================
    def update_valuation_by_ts_day(self):
        """
        tushare可以按天获取所有stock的valuation数据，然后分别更新到对应不同code文件内
        对于相同日期小于500只股票的日期，后面统一采用单只按日获取更新
        当天一次性获取的所有股票的valuation，增加一个文件夹单独存储，按日期命名
        平时更新使用该方法
        存储目录为"C:\\quanttime\\data\\finance\\ts\\valuation\\"
        因为不用输入ts_code，即不需要首先获取所有股票的code信息
        :return:
        """
        last_update = pd.read_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv',
                                  index_col=["module"], parse_dates=["date"])
        print(last_update)
        module_name = 'update_valuation_by_ts_day'
        last_update_date = 0

        # 将空值填0
        last_update = last_update.fillna(0)
        if module_name in last_update.index:
            last_update_date = last_update.loc[module_name, ["date"]].date

        if last_update_date != 0:
            try:
                if last_update_date.date() >= datetime.today().date() - timedelta(days=1):
                    print("记录已更新，今日不用再更新")
                    return
            except ValueError:
                print("记录的最后更新日期格式或记录有误")

        # 股票基本信息只获取ts_code与list_date
        stock_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
        if stock_basic.empty:
            print("获取ts股票基本信息失败，return")
            return
        stock_basic = stock_basic.set_index("ts_code")
        basic_dir = "C:\\quanttime\\data\\finance\\ts\\valuation\\"
        columns_name = ['ts_code', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb',
                        'ps', 'ps_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
        valuation_data_last_record = pd.DataFrame(columns=columns_name)
        valuation_data_last_record.index.name = "trade_date"
        list_empty_code = []
        for stock_code in stock_basic.index:
            # print("process:%s...." % stock_code)
            valuation_file = basic_dir + stock_code + '.csv'
            if os.path.exists(valuation_file):
                valuation_data = pd.read_csv(valuation_file, index_col=["trade_date"], parse_dates=True)
                if valuation_data.empty:
                    list_empty_code.append(stock_code)
                    continue
                # 提取记录中的最后一条记录
                valuation_data_last_record = valuation_data_last_record.append(valuation_data.iloc[-1, :])
                # print("valution len:%d" % len(valuation_data_last_record))
            else:
                list_empty_code.append(stock_code)
        # 更新的截止日期
        et = datetime.today().date() - timedelta(days=1)
        et = et.strftime("%Y-%m-%d")

        # 提取最后更新记录时间集
        list_record_last_date = valuation_data_last_record.index.unique()
        dic_date_stocks = {}
        for last_date in list_record_last_date:
            # same_last_update_stock是具有相同最后更新日期的stock集合
            same_last_update_stock = valuation_data_last_record.loc[last_date, ["ts_code"]]["ts_code"]
            if isinstance(same_last_update_stock, str):
                dic_date_stocks[str(last_date.date())] = valuation_data_last_record.loc[last_date, ["ts_code"]][
                    "ts_code"]
            else:
                dic_date_stocks[str(last_date.date())] = valuation_data_last_record.loc[last_date, ["ts_code"]][
                    "ts_code"].tolist()

        finance_db = self.mongo_client["ts_finance_db"]
        # 对于某个日期，数量少(小于500只股票)的stock，采取按code，一只一只更新，不采取批量更新的办法
        list_single_update_by_code = []
        for last_update_date, stock_codes in dic_date_stocks.items():
            if len(stock_codes) < 500:
                list_single_update_by_code.append(stock_codes)
                continue
            list_trade_date = trade_date_util.get_trade_date_range(last_update_date, et)
            if list_trade_date:
                # get_trade_date_range返回的是闭区间，所有把记录的最后一天的下一天，作为开始更新更新的时间
                list_trade_date.pop(0)
                for day in list_trade_date:
                    tmp = day.split('-')
                    day = tmp[0] + tmp[1] + tmp[2]
                    df_update_data = self.pro.daily_basic(ts_code='', trade_date=day)
                    if df_update_data.empty:
                        print("%s, 获取当天valuation，返回为空" % day)
                        continue
                    # 新增一个按天存储所有股票valuation的文件夹，按照日期命名
                    tmp_file = "C:\\quanttime\\data\\finance\\ts\\valuation_day\\" + day + ".csv"
                    df_update_data.to_csv(tmp_file)
                    df_update_data = df_update_data.set_index("trade_date")
                    for i in range(len(df_update_data)):
                        df_stock = pd.DataFrame(data=[df_update_data.iloc[i].values],
                                                index=pd.Index([df_update_data.index[i]]),
                                                columns=columns_name)
                        update_stock_code = df_update_data.iloc[i, df_update_data.columns.get_loc('ts_code')]
                        if update_stock_code in stock_codes:
                            print("存储code:%s ...." % update_stock_code)
                            valuation_file = basic_dir + update_stock_code + '.csv'
                            df_stock.to_csv(valuation_file, mode='a', header=None)
                            # 存数据库
                            table = finance_db[update_stock_code[0:6]]
                            table.insert_many(df_stock.to_dict(orient="record"))

            print("批量更新结束，本次共更新：%d" % len(stock_codes))
        tmp_list = []
        print("需要单只更新的code list:%r" % list_single_update_by_code)
        for i in list_single_update_by_code:
            if isinstance(i, list):
                for j in i:
                    tmp_list.append(j)
            else:
                tmp_list.append(i)
        list_single_update_by_code = tmp_list

        for i_code in list_single_update_by_code:
            self.update_valuation_by_ts_by_code(i_code)

        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = str(datetime.today().date() - timedelta(days=1))
        last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')
        print("本次更新结束")

    # =========================================
    def update_valuation_by_ts_by_code(self, ts_code):
        """
        使用tushare接口，单只股票更新valuation
        :param ts_code: ts code
        :return:
        """
        basic_dir = "C:\\quanttime\\data\\finance\\ts\\valuation\\"
        finance_db = self.mongo_client["ts_finance_db"]
        end_date = str(datetime.today().date() - timedelta(days=1))
        tmp_list = end_date.split('-')
        end_date = tmp_list[0] + tmp_list[1] + tmp_list[2]
        valuation_file = basic_dir + ts_code + '.csv'
        if os.path.exists(valuation_file):
            valuation_data = pd.read_csv(valuation_file, index_col=["trade_date"], parse_dates=True)
            if valuation_data.index[-1].date() >= datetime.today().date() - timedelta(days=1):
                print("code：%s 已经是最新了，更次不更新" % ts_code)
                return
            start_date = valuation_data.index[-1].date().strftime("%Y-%m-%d")
            start_date = trade_date_util.get_close_trade_date(start_date, 1)
            tmp_list = start_date.split("-")
            if len(tmp_list) != 3:
                print("日期解析的格式有误，code：%s" % ts_code)
                return
            start_date = tmp_list[0] + tmp_list[1] + tmp_list[2]
            df_data = self.pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df_data.empty:
                print("code:%r 本次更新获取的数据为空" % ts_code)
                return
            df_data = df_data.set_index("trade_date")
            df_data = df_data.sort_index(ascending=True)
            df_data.to_csv(valuation_file, mode='a', header=None)
            # 存数据库
            table = finance_db[ts_code[0:6]]
            table.insert_many(df_data.to_dict(orient="record"))
            print("tushare 更新valuation数据 完成，code：%s" % ts_code)

    # =========================================
    def update_indicator_by_ts(self):
        """
        运行时间可以在每年的1-15至4-30 一季报年报
        7-15至8-31 半年报
        10月 三季报
        通过tushare更新每股财务指标，形如report_date=20190331
        1、存储目录：C:\quanttime\data\finance\ts\indicator
        2、通过tushare获取所有上市股票code--pro.stock_basic
        3、根据上述获取的所有上市股票code，轮询所有code，获取indicator
        :param report_date: 输入报告期，在第一次获取所有报告期数据，以后每次更新都要输入报告期
                报告期按照：1231年报，0331一季报 0630半年报 0930三季报
        :return:

        """
        # 股票基本信息只获取ts_code与list_date
        stock_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
        if stock_basic.empty:
            print("获取ts股票基本信息失败，return")
            return
        stock_basic = stock_basic.set_index("ts_code")

        basic_dir = "C:\\quanttime\\data\\finance\\ts\\indicator\\"
        finance_db = self.mongo_client["ts_indicator_db"]
        end_date = str(datetime.today().date() - timedelta(days=1))
        for stock_code in stock_basic.index:
            indicator_file = basic_dir + stock_code + '.csv'
            if os.path.exists(indicator_file):
                indicator_data = pd.read_csv(indicator_file, index_col=['end_date'], parse_dates=True)
                if not indicator_data.empty:
                    diff = datetime.today().date() - indicator_data.index[-1].date()
                    if diff.days < 90:
                        print("本次更新时间距离上个季报期间隔小于90天，不用更新")
                        continue
                    start_date = str(indicator_data.index[-1].date() + timedelta(days=1))
                    tmp = start_date.split("-")
                    start_date = tmp[0] + tmp[1] +tmp[2]
                    end_date = str(datetime.today().date())
                    tmp = end_date.split("-")
                    end_date = tmp[0] + tmp[1] + tmp[2]
                    df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)

                else:
                    start_date = "20050101"
                    end_date = str(datetime.today().date())
                    tmp = end_date.split("-")
                    end_date = tmp[0] + tmp[1] + tmp[2]
                    df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)

                if df_indicator.empty:
                    print("code:%s,本日：%s没有获取到当季indicator" % (stock_code, str(datetime.today().date())))
                    time.sleep(1)
                    continue
                df_indicator = df_indicator.set_index("end_date")
                df_indicator = df_indicator.sort_index(ascending=True)
                df_indicator.to_csv(indicator_file, mode='a', header=None)
                # 存数据库
                table = finance_db[stock_code[0:6]]
                table.insert_many(df_indicator.to_dict(orient="record"))
                print("code:%s indicator数据更新完成" % stock_code)
            else:
                start_date = "20050101"
                end_date = str(datetime.today().date())
                tmp = end_date.split("-")
                end_date = tmp[0] + tmp[1] + tmp[2]
                df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)
                if df_indicator.empty:
                    print("code:%s, 第一次取全部indicator为空" % stock_code)
                    continue
                df_indicator = df_indicator.set_index("end_date")
                df_indicator = df_indicator.sort_index(ascending=True)
                df_indicator.to_csv(indicator_file)
                # 存数据库
                table = finance_db[stock_code[0:6]]
                table.insert_many(df_indicator.to_dict(orient="record"))
                print("code:%s 首次获取indicator数据更新完成" % stock_code)
            time.sleep(1)

    # ====================
    def get_all_jq_indicator_together(self):
        """
        将所有的jq的indicator表，增加roe3，roe5后，聚合成一张大表indicator_all_year.csv，再查询是就不用每张表读取，计算
        步骤是读取finance/indicator下每一个year的文件，然后计算roe的三年平均与五年平均
        如果要计算其他指标的统计数据，可以根据需求添加
        :return:
        """
        indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"
        # 如果有输入的code list则按照输入的code更新数据，作用主要是用于更新失败的数据，或者指定更新的code
        error_code_list = []
        df_indicator = pd.DataFrame()
        for code in self.stock_code:
            file_path = indicator_file_path + code[0:6] + '_year' + code[6:11] + ".csv"
            if os.path.exists(file_path):
                df_tmp = pd.read_csv(file_path)
                if len(df_tmp) >= 5:
                    df_tmp["roe5"] = df_tmp.iloc[-5:, df_tmp.columns.get_loc('inc_return')].mean()
                    df_tmp["roe3"] = df_tmp.iloc[-3:, df_tmp.columns.get_loc('inc_return')].mean()
                elif len(df_tmp) >= 3:
                    df_tmp["roe3"] = df_tmp.iloc[-3:, df_tmp.columns.get_loc('inc_return')].mean()
                    df_tmp["roe5"] = -1
                else:
                    df_tmp["roe5"] = -1
                    df_tmp["roe3"] = -1
                df_indicator = pd.concat([df_indicator, df_tmp])
            else:
                error_code_list.append(code)
        print("不存在的stock indicator file：%r" % error_code_list)
        df_indicator.to_csv(indicator_file_path + 'indicator_all_year.csv', index=False)


if __name__ == "__main__":
    regular = financeMaintenance()
    # regular.standerSingleUpdateFile("600432.XSHG")
    # regular.drop_duplicate()
    # regular.update()
    # regular.update_balance(["000001.XSHE"],'2018q2')
    # regular.batch_update_balance('2018q3')
    # regular.update_case_flow(["000001.XSHE"],'2018q2')
    # regular.batch_update_case_flow('2018q3')
    # regular.update_indicator(["000001.XSHE"],'2018q2')
    # regular.batch_update_indicator('2018q3')
    # regular.update_valuation_by_ts()
    # regular.update_indicator_by_ts()
    # regular.update_valuation_by_jq_day()
    # regular.update_valuation_by_jq_day()
    # regular.update_valuation_jq_by_code("000776.XSHE")

    #update_quar = [str(i) for i in range(2006, 2020)]
    #for date in update_quar:
    #   regular.batch_update_indicator_year(date)
    # regular.batch_update_indicator("2019q2")
    # regular.get_all_jq_indicator_together()
    regular.del_duplicate_indicator_record(False)

