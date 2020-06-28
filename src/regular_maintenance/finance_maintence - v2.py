# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta
import time

import logging
import tushare as ts

"""
日常维护使用：
1、数据来源：joinquant, tushare

2、自动获取运行盘符，存储数据

3、更新finance数据，finance数据按照valuation，income，cash_flow，balance，indicator5个文件夹分别存储，
   其中20180730之前的数据已经批量获取存储在本地，之后只需要更新维护即可

4、valuation表数据是按日获取

4、income，cash_flow,balance,indicator表分为批量更新与指定code更新

5、20190529增加tushare的每日指标更新，相当于joinquant的valuation
   存储目录为":\\quanttime\\data\\finance\\ts\\valuation\\"

6、20190604增加tushare每日指标更新方式，按日获取所有的股票的valuation，然后分别更新到对应的文件夹
   加快更新的效率

"""


class FinanceMaintenance:
    def __init__(self):
        # 初始化日志
        self.regularlog = logging.getLogger("finance_maintenance")
        self.cmdconsole = logging.StreamHandler()
        self.cmdconsole.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.cmdconsole.setFormatter(self.formatter)
        self.regularlog.addHandler(self.cmdconsole)

        # 存储数据目录
        self.finance_dir = os.getcwd()[0] + ":\\quanttime\\data\\finance\\"

        # 错误代码
        self.errorCodeList = []

        # jqdata context
        auth('13811866763', "sam155")  # jqdata 授权

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        # ts 授权
        self.pro = ts.pro_api()

        # 获取所有joinquant stock信息,并去掉已退市的股票
        self.all_security = get_all_securities()
        self.all_security = self.all_security[self.all_security["end_date"] == "2200-01-01"]
        self.all_security.index.name = "code"
        self.stock_code = self.all_security.index
        # 获取运行当天的前一个交易日的日期
        self.end_date = get_trade_days(end_date=datetime.today().date(), count=2)[0]

        # tushare接口获取所有的股票信息
        self.stock_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')

    # ==============================================================================

    # ==============================================================================
    def update_valuation(self, debug=False):
        """
        更新valuation表 joinquant,该方法为按股票code一只只更新
        目标文件夹valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
        增量更新，更新到当前日期的前一天
        如果有新加入的stock，则创建文件
        代码更新日期：2020-6-23
        :param debug: bool，测试时使用参数(默认为false）
        :return:
        """
        if debug:
            self.stock_code = ["601318.XSHG"]

        valuation_file_path = self.finance_dir + "jq\\valuation\\"
        valuation_columns_name = ['id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
                                  'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                                  'circulating_market_cap', 'pe_ratio_lyr']
        for code in self.stock_code:
            update_file = valuation_file_path + code + ".csv"
            # print(update_file)
            if os.path.exists(update_file):
                valuation_data = pd.read_csv(update_file, parse_dates=["day"])
                start_date = valuation_data.iloc[-1, valuation_data.columns.get_loc('day')]
                dates = get_trade_days(start_date=start_date, end_date=self.end_date)
                if len(dates) <= 1:
                    continue
                dates = dates[1:]
                print("增量更新日期：%r-->%r" % (dates[0], dates[-1]))
                q = query(valuation).filter(valuation.code == code)
                df_valuation = pd.DataFrame(columns=valuation_columns_name)
                for date in dates:
                    tmp = get_fundamentals(q, date)
                    if tmp.empty:
                        continue
                    else:
                        df_valuation = pd.concat([df_valuation, tmp], sort=False)
                if df_valuation.empty:
                    self.regularlog.warning("本次获取的code：%s,valuation表为空" % code)
                    continue
                df_valuation = df_valuation[valuation_columns_name]
                df_valuation.to_csv(update_file, mode='a', header=None)

                print("获取数据结束，写入csv的，df_valuation 行数为：%r" % len(df_valuation.index))
                self.regularlog.info("增量更新 valuation code:%r update successful" % code)
            else:
                print("需要更新的文件code：%s 不存在，创建并获取" % code)
                # start_date返回是series，要根据index把值取出来，值类型是datetime类型
                start_date = self.all_security.loc[code, ["start_date"]]["start_date"]

                # 如果上市时间早于2014-01-01，则从2014年开始获取数据，前面数据不用了
                if start_date < datetime.strptime("2014-01-01", "%Y-%m-%d"):
                    start_time = datetime.strptime("2014-01-04", "%Y-%m-%d")

                q = query(valuation).filter(valuation.code == code)
                dates = get_trade_days(start_time.date(), self.end_date)
                valuation_data = pd.DataFrame(columns=valuation_columns_name)

                for date in dates:
                    df_valuation = get_fundamentals(q, date)
                    # print(df_valuation)
                    if df_valuation.empty:
                        continue
                    else:
                        valuation_data = valuation_data[valuation_columns_name]
                        valuation_data = pd.concat([valuation_data, df_valuation], sort=False)
                valuation_data.to_csv(update_file)

                self.regularlog.info("valuation code:%s create&update successful" % code)
                print("valuation code:%s create&update successful" % code)
            print("update code:%r, end" % code)

    # ==============================================================================

    def update_valuation_by_jq_day(self, debug=False):
        """
        更新joinquant的valuation表。采取按天获取所有的stock，然后按股票更新到不同的文件夹
        改善更新的效率
        按日获取所有股票的valuation，然后更新到对应的个股的valuation表
        代码更新日期：2020-6-23
        :param debug: bool，测试时使用参数(默认为false）
        :return:
        """
        stock_code_list = self.stock_code
        if debug:
            stock_code_list = ["601318.XSHG"]

        last_update = pd.read_csv(r'valuation_last_update.csv', index_col=["module"], parse_dates=["date"])
        # print(last_update)
        module_name = 'update_valuation_by_jq_day'
        last_update_date = 0

        # 将空值填0
        last_update = last_update.fillna(0)
        if module_name in last_update.index:
            last_update_date = last_update.loc[module_name, ["date"]].date

        if last_update_date != 0:
            try:
                if last_update_date.date() >= self.end_date:
                    self.regularlog.info("记录已更新，今日不用再更新")
                    print("记录已更新，今日不用再更新")
                    return
            except ValueError:
                self.regularlog.info("记录的最后更新日期格式或记录有误")
                print("记录的最后更新日期格式或记录有误")
                return

        list_trade_date = get_trade_days(start_date=last_update_date, end_date=self.end_date)
        if len(list_trade_date) <= 1:
            return

        valuation_columns_name = ['id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
                                  'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                                  'circulating_market_cap', 'pe_ratio_lyr']
        valuation_file_path = self.finance_dir + "jq\\valuation\\"

        for trade_date in list_trade_date:
            df_data = get_fundamentals(query(valuation).filter(
                # 这里不能使用 in 操作, 要使用in_()函数
                valuation.code.in_(stock_code_list)
            ), date=trade_date)
            if df_data.empty:
                print("获取的valuation数据为空")
                continue
            file_path = self.finance_dir + "jq\\valuation_day\\" + str(trade_date) + '.csv'
            df_data.to_csv(file_path)
            df_data = df_data[valuation_columns_name]
            for i in range(len(df_data)):
                df_stock = pd.DataFrame(data=[df_data.iloc[i].values],
                                        index=pd.Index([df_data.index[i]]),
                                        columns=df_data.columns)
                tmp_name = df_data.iloc[i, df_data.columns.get_loc('code')]
                print("正在处理批量更新的 %s。。。。" % tmp_name)
                file_path = valuation_file_path + tmp_name + '.csv'
                df_stock.to_csv(file_path, mode='a', header=None)

        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = str(datetime.today().date() - timedelta(days=1))
        last_update.to_csv(r'valuation_last_update.csv')
        print("本次更新结束")

    # ==============================================================================

    def update_valuation_jq_by_code(self, code):
        """
        更新单只股票的聚宽valuation
        代码更新日期：2020-6-23
        :param code: 股票代码
        :return:
        """
        valuation_file = self.finance_dir + "jq\\valuation\\" + code + '.csv'
        valuation_columns_name = ['id', 'day', 'code', 'pe_ratio', 'turnover_ratio', 'pb_ratio', 'ps_ratio',
                                  'pcf_ratio', 'capitalization', 'market_cap', 'circulating_cap',
                                  'circulating_market_cap', 'pe_ratio_lyr']
        if os.path.exists(valuation_file):
            df_valuation = pd.read_csv(valuation_file, index_col=["day"], parse_dates=True)
            trade_range = get_trade_days(df_valuation.index[-1].date(), self.end_date)
            if len(trade_range) <= 1:
                print("%s不用更新" % code)
                return
            q = query(valuation).filter(valuation.code == code)
            df_data = get_fundamentals_continuously(q, end_date=self.end_date, count=len(trade_range) - 1, panel=False)
            if df_data.empty:
                print("获取的valuation为空，stock：%s" % code)
                return
            df_data = df_data.loc[:, valuation_columns_name]
            df_data.to_csv(valuation_file, mode='a', header=None)
            print("更新完stock：%s" % code)
        else:
            q = query(valuation).filter(valuation.code == code)
            df_data = get_fundamentals_continuously(q, end_date=self.end_date, count=100, panel=False)
            if df_data.empty:
                print("获取的valuation为空，stock：%s" % code)
                return
            df_data = df_data.loc[:, valuation_columns_name]
            df_data.to_csv(valuation_file)
            print("更新完stock：%s" % code)

    # ==========================================

    def update_valuation_by_ts(self):
        """
        通过tushare接口更新valuation表
        存储目录为":\\quanttime\\data\\finance\\ts\\valuation\\"
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
       dv_ratio:股息率
       dv_ttm
       'total_share'：总股本（万股）
       'float_share'：流通股本
       'free_share'：自由流通股本
       'total_mv'：总市值（万）
       'circ_mv'：流通市值

        :return:
        """

        # 股票基本信息只获取ts_code与list_date
        if self.stock_basic.empty:
            print("获取ts股票基本信息失败，return")
            return
        stock_basic = self.stock_basic.set_index("ts_code")
        for code in stock_basic.index:
            self.update_valuation_by_ts_by_code(code)

        print("集中更新图share valuation表结束。")

    # ============================================
    def update_valuation_by_ts_day(self, debug=False):
        """
        代码更新：20200624
        tushare可以按天获取所有stock的valuation数据，然后分别更新到对应不同code文件内
        对于相同日期小于500只股票的日期，后面统一采用单只按日获取更新
        当天一次性获取的所有股票的valuation，增加一个文件夹单独存储，按日期命名
        平时更新使用该方法
        存储目录为"X:\\quanttime\\data\\finance\\ts\\valuation\\"
        因为不用输入ts_code，即不需要首先获取所有股票的code信息
        :param debug: bool，测试时使用参数(默认为false）
        :return:
        """
        last_update = pd.read_csv(r'valuation_last_update.csv', index_col=["module"], parse_dates=["date"])
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
                return

        list_trade_date = get_trade_days(start_date=last_update_date, end_date=self.end_date)
        if len(list_trade_date) <= 1:
            return

        basic_dir = self.finance_dir + "ts\\valuation\\"
        columns_name = ['trade_date', 'ts_code', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe',
                        'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share', 'float_share',
                        'free_share', 'total_mv', 'circ_mv']

        for trade_date in list_trade_date:
            df_tmp = self.pro.daily_basic(ts_code='', trade_date=trade_date.strftime("%Y%m%d"))
            if df_tmp.empty:
                continue
            df_tmp = df_tmp[columns_name]
            df_tmp.to_csv(self.finance_dir + "ts\\valuation_day\\" + trade_date.strftime("%Y%m%d") + ".csv")
            for i in range(len(df_tmp)):
                df_stock = pd.DataFrame(data=[df_tmp.iloc[i].values],
                                        index=pd.Index([df_tmp.index[i]]),
                                        columns=df_tmp.columns)
                tmp_name = df_tmp.iloc[i, df_tmp.columns.get_loc('ts_code')]

                if debug and (tmp_name != "601318.SH"):
                    continue
                print("正在处理批量更新的 %s。。。。" % tmp_name)
                file_path = basic_dir + tmp_name + '.csv'
                print(file_path)
                df_stock.to_csv(file_path, mode='a', header=None, index=False)

        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = self.end_date
        last_update.to_csv(r'valuation_last_update.csv')
        print("本次更新结束")

    # =========================================
    def update_valuation_by_ts_by_code(self, ts_code):
        """
        代码更新：20200624
        使用tushare接口，单只股票更新valuation
        :param ts_code: ts code
        :return:
        """
        basic_dir = self.finance_dir + "ts\\valuation\\"

        columns_name = ['trade_date', 'ts_code', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe',
                        'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share', 'float_share',
                        'free_share', 'total_mv', 'circ_mv']

        end_date = datetime.today().date() - timedelta(days=1)
        end_date = end_date.strftime("%Y%m%d")

        stock_basic = self.stock_basic.set_index("ts_code")

        valuation_file = basic_dir + ts_code + '.csv'
        if os.path.exists(valuation_file):
            valuation_data = pd.read_csv(valuation_file, index_col=["trade_date"], parse_dates=True)
            if valuation_data.index[-1].date() >= datetime.today().date() - timedelta(days=1):
                print("code：%s 已经是最新了，更次不更新" % ts_code)
                return
            start_date = valuation_data.index[-1].date() + timedelta(days=1)
            start_date = start_date.strftime("%Y%m%d")

            df_data = self.pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df_data.empty:
                print("code:%r 本次更新获取的数据为空" % ts_code)
                return
            df_data = df_data.loc[:, columns_name]
            df_data = df_data.set_index("trade_date")
            df_data = df_data.sort_index(ascending=True)
            df_data.to_csv(valuation_file, mode='a', header=None)
        else:
            start_date = stock_basic.loc[ts_code, ["list_date"]]["list_date"]
            if datetime.strptime(start_date, "%Y%m%d") < datetime(2007, 1, 1):
                start_date = "20070103"
            df_data = self.pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df_data.empty:
                print("code:%r 本次更新获取的数据为空" % ts_code)
                return
            df_data = df_data.loc[:, columns_name]
            df_data = df_data.set_index("trade_date")
            df_data = df_data.sort_index(ascending=True)
            df_data.to_csv(valuation_file)
            print("tushare 更新valuation数据 完成，code：%s" % ts_code)

    # =========================================
    def update_indicator_by_ts(self, debug=False):
        """
        运行时间可以在每年的1-15至4-30 一季报年报
        7-15至8-31 半年报
        10月 三季报
        通过tushare更新每股财务指标，形如report_date=20190331
        1、存储目录：C:\quanttime\data\finance\ts\indicator
        2、通过tushare获取所有上市股票code--pro.stock_basic
        3、根据上述获取的所有上市股票code，轮询所有code，获取indicator
        :param debug: bool，测试时使用参数(默认为false）
        :return:

        """
        # 股票基本信息只获取ts_code与list_date
        stock_basic = self.stock_basic.set_index("ts_code")
        if debug:
            stock_code_list = ["601318.SH"]
        else:
            stock_code_list = stock_basic.index

        basic_dir = self.finance_dir + "\\ts\\indicator\\"
        end_date = self.end_date.strftime("%Y%m%d")
        for stock_code in stock_code_list:
            indicator_file = basic_dir + stock_code + '.csv'
            if os.path.exists(indicator_file):
                indicator_data = pd.read_csv(indicator_file, index_col=['end_date'], parse_dates=True)
                if not indicator_data.empty:
                    diff = datetime.today().date() - indicator_data.index[-1].date()
                    if diff.days < 90:
                        print("本次更新时间距离上个季报期间隔小于90天，不用更新")
                        continue
                    start_date = indicator_data.index[-1].date() + timedelta(days=1)
                    start_date = start_date.strftime("%Y%m%d")
                    df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)

                else:
                    start_date = "20050101"
                    df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)

                if df_indicator.empty:
                    print("code:%s,本日：%s没有获取到当季indicator" % (stock_code, str(datetime.today().date())))
                    time.sleep(1)
                    continue
                df_indicator = df_indicator.set_index("end_date")
                df_indicator = df_indicator.sort_index(ascending=True)
                df_indicator.to_csv(indicator_file, mode='a', header=None)
                print("code:%s indicator数据更新完成" % stock_code)
            else:
                start_date = "20050101"
                df_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=end_date)
                if df_indicator.empty:
                    print("code:%s, 第一次取全部indicator为空" % stock_code)
                    continue
                df_indicator = df_indicator.set_index("end_date")
                df_indicator = df_indicator.sort_index(ascending=True)
                df_indicator.to_csv(indicator_file)
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
    regular = FinanceMaintenance()
    # regular.update_valuation(True)
    # regular.update_valuation_by_jq_day(True)
    # regular.update_valuation_jq_by_code("601318.XSHG")
    # regular.update_valuation_by_ts_by_code("000002.SZ")
    # regular.update_valuation_by_ts_day(True)
    regular.update_indicator_by_ts(True)
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

    # update_quar = [str(i) for i in range(2006, 2020)]
    # for date in update_quar:
    #   regular.batch_update_indicator_year(date)
    # regular.batch_update_indicator("2019q2")
    # regular.get_all_jq_indicator_together()
    # regular.del_duplicate_indicator_record(False)

