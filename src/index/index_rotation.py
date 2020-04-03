# -*-coding:utf-8 -*-
# 所有需要的库文件添加该处
import pandas as pd
import os
import sys
from datetime import datetime, timedelta, date
from jqdatasdk import *
import logging
import time

sys.path.append('C:\\quanttime\\src\\comm\\')
import trade_date_util

pd.set_option('precision', 3)
auth('13811866763', "sam155")  # jqdata 授权
pd.set_option('display.max_columns', None)


class IndexRotationTest(object):
    def __init__(self, index_codes=None, front=None, end=None, st_start=None, st_end=None):
        '''

        :param index_codes: list, 回测的指数code list
        :param front: 回测时的日期往前推front天，买入比较基准
        :param end: 回测时的日期往前推end天，卖出的比较基准
        :param st_start: 回测起始日期
        :param st_end: 回测结束日期
        '''
        # 设置比较基准,front_buy:与前front_buy进行比较，如果高则买入，与end_sell比较，如果低则卖出
        if index_codes is None:
            self.index_codes = None
        else:
            self.index_codes = index_codes
        if front is None:
            self.front_buy = 20
        else:
            self.front_buy = front
        if end is None:
            self.end_sell = 20
        else:
            self.end_sell = end
        if st_start is None:
            # 默认从该指数起始的日期开始回测
            self.back_test_start = None
            log_st = " "
        else:
            self.back_test_start = datetime.strptime(st_start, "%Y-%m-%d")
            log_st = str(st_start)
        if st_end is None:
            # 默认回测到当前日期的昨天
            self.back_test_end = datetime.today() - timedelta(days=1)
            log_end = str(datetime.today().date())
        else:
            self.back_test_end = datetime.strptime(st_end, "%Y-%m-%d")
            log_end = str(st_end)

        log_name = "IndexRotationTest_" + \
                   str(self.front_buy) + "_" + str(self.end_sell) + "_" + log_st + "_" + log_end + ".log"
        log_path = "C:\\quanttime\\log\\"
        log_file = log_path + log_name
        # 初始化日志
        logging.basicConfig(level=logging.DEBUG,
                            filename=log_file,
                            datefmt='%Y-%m-%d %H:%M:%S',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger("IndexRotationTest")
        # 指数k线所在的目录
        self.index_dir = "C:\\quanttime\\data\\index\\jq\\"
        # position:0 空仓，1 持仓
        self.position = 0

    # =========================================
    def get_trade_signal(self, code, trade_date):
        '''
        获取交易信号，输入指数的code，起始日期
        例如获取date=2019-6-1 的交易信号
        '''
        file = self.index_dir + code + '.csv'
        df = pd.read_csv(file, index_col=["date"], parse_dates=True, usecols=["date", "close"], skip_blank_lines=True)
        df = df[~df.reset_index().duplicated().values]
        list_date_buy = trade_date_util.get_trade_date_by_count(trade_date, self.front_buy)
        list_date_sell = trade_date_util.get_trade_date_by_count(trade_date, self.end_sell)
        if len(list_date_buy) < (self.front_buy + 1):
            print("交易日往前推：%d，获取的交易日:%s,list长度小于前推日数，不正确:%r" %
                  (self.front_buy, str(trade_date), list_date_buy))
        if len(list_date_buy) < 2:
            return "unkown"
        pre_front_buy = list_date_buy[0]
        pre_end_sell = list_date_sell[0]
        pre_1 = list_date_buy[-2]

        if (pre_1 not in df.index) or (pre_front_buy not in df.index) or (pre_end_sell not in df.index):
            return "unkown"

        try:
            close_1 = df.loc[pre_1, ["close"]]["close"]
            close_front_buy = df.loc[pre_front_buy, ["close"]]["close"]
            close_end_sell = df.loc[pre_end_sell, ["close"]]["close"]
        except KeyError:
            return "unkown"
        self.log.debug("index:%s, 交易日：%s前一天收盘价：%.2f, 前20天close：%.2f, 前end天close：%.2f," %
                       (code, str(trade_date), close_1, close_front_buy, close_end_sell))

        if (close_1 > close_front_buy) and (close_1 > close_end_sell):
            self.log.debug("buy")
            return "buy"
        elif (close_1 < close_end_sell) and (close_1 < close_front_buy):
            self.log.debug("sell")
            return "sell"
        else:
            self.log.debug("hold")
            return "hold"
    # ============================================

    def process_trade_result(self, list_record):
        '''
        处理交易结果
        '''
        print(list_record)
        list_trade_point = []
        # 单次最大涨幅
        max_increase = 0
        # 单次最大损失
        max_loss = 0
        # 正收益次数
        positive = 0
        # 总交易次数
        trade_count = 0
        for pos, dic in enumerate(list_record):
            tmp_dic_key = dic.keys()
            tmp_dic_value = dic.values()
            for i in tmp_dic_key:
                trade_direct = i.split('_')[0]
                trade_date = i.split('_')[1]
            for i in tmp_dic_value:
                dic_value = i

            if pos == 0:
                if trade_direct != "buy":
                    print("本次交易记录有误，record：%r" % list_record)
                    return
            if pos % 2 == 0:
                if trade_direct != "buy":
                    print("本次交易记录有误，record：%r" % list_record)
                    return
            else:
                if trade_direct != "sell":
                    print("本次交易记录有误，record：%r" % list_record)
                    return
            list_trade_point.append(dic_value)
        list_diff = []
        print("list_trade_point:%r" % list_trade_point)
        trade_count = int(len(list_trade_point) / 2)
        for i in range(int(len(list_trade_point) / 2)):
            front = list_trade_point.pop(0)
            end = list_trade_point.pop(0)
            diff = (end - front) / front
            list_diff.append(diff)
            if diff > max_increase:
                max_increase = diff
            if diff > 0:
                positive = positive + 1
            if diff < max_loss:
                max_loss = diff
        result = 1
        print("list_diff:%r" % list_diff)
        for i in list_diff:
            result = result * (1 + i)
        # 返回结果[收益，交易次数，胜率，单次最大收益，单次最大损失]
        return [result, trade_count, positive, max_increase, max_loss]

    # ===================================

    def calc_index_rotation(self):
        '''
        计算轮动结果
        :param st_start: 回测开始的时间
        :param st_end: 回测结束的时间
        :return:
        '''

        # 获取所有的指数标的
        index_security = get_all_securities(types=['index'], date=None)
        # 滤除退市的，或者当前不存在的指数
        index_security = index_security[index_security["end_date"] == "2200-01-01"]
        if self.index_codes is None:
            self.index_codes = index_security.index

        # 交易后经过计算后的结果集，用于构成一个pd.DataFrame
        list_result = []
        columns_name = ["code", "name", "总收益", "交易次数", "胜率", "单次最大收益",
                        "单次最大损失", "回测起始时间", "回测结束时间"]
        # position:0 空仓，1 持仓
        # counter计数器，每计算10只指数，记录一次结果，防止长时间计算出现意外
        counter = 0
        for code in self.index_codes:
            # 交易结果list，内部元素时一个字典数据，按照交易方向_日期：交易点位，如'buy_2019-02-11': 604.26
            # 形如[{'buy_2019-01-07': 596.43},{'sell_2019-02-01': 597.81},
            # {'buy_2019-02-11': 604.26},{'sell_2019-03-25': 791.65}]
            self.position = 0
            list_trade_result_record = []
            if self.back_test_start is None:
                start_date = index_security.loc[code, ["start_date"]]["start_date"]
                start = start_date + timedelta(days=180)
                self.back_test_start = start
            file = self.index_dir + code + '.csv'
            df = pd.read_csv(file, index_col=["date"], parse_dates=True, usecols=["date", "open"], skip_blank_lines=True)
            df = df[~df.reset_index().duplicated().values]
            if self.back_test_start >= (datetime.today() - timedelta(days=90)):
                print("指数开始的时间过于接近当前日期，暂时不统计，start_date:%s" % str(self.back_test_start))
                continue

            list_trade_date = trade_date_util.get_trade_date_range2(self.back_test_start, self.back_test_end)
            for st in list_trade_date:
                signal = self.get_trade_signal(code, st)
                if (signal == "buy") and (self.position == 0):
                    try:
                        buy_price = df.loc[st, ["open"]]["open"]
                        list_trade_result_record.append({'buy_' + str(st): buy_price})
                        print("%s 买入index：%s,价格：%.2f" % (str(st), code, buy_price))
                        self.log.info("%s 买入index：%s,价格：%.2f" %(str(st), code, buy_price))
                    except KeyError:
                        print("date:%s 没有对应的指数值" % str(st))
                    self.position = 1
                elif (signal == "sell") and (self.position == 1):
                    try:
                        sell_price = df.loc[st, ["open"]]["open"]
                        list_trade_result_record.append({'sell_' + str(st): sell_price})
                        print("%s 卖出index：%s,价格：%.2f" % (str(st), code, sell_price))
                        self.log.info("%s 卖出index：%s,价格：%.2f" % (str(st), code, sell_price))
                    except KeyError:
                        self.log.debug("date:%s 没有对应的指数值" % str(st))
                    self.position = 0
                else:
                    self.log.debug("date:%s 指数相对20天前没有变化或获取的状态未知" % str(st))
            self.log.info("index:%s,trade_list:%r" % (code, list_trade_result_record))
            # 返回结果[收益，交易次数，胜率，单次最大收益，单次最大损失]
            result = self.process_trade_result(list_trade_result_record)
            list_trade_result_record.clear()
            if not isinstance(result, list):
                print("index:%s, 计算收益返回值值错误" % code)
                continue

            result.insert(0, index_security.loc[code, ["display_name"]]["display_name"])
            result.insert(0, code)
            result.append(str(self.back_test_start.date()))
            result.append(str(self.back_test_end.date()))
            list_result.append(result)
            print(result)
            counter = counter + 1
            if counter >= 10:
                df_result = pd.DataFrame(data=list_result, columns=columns_name)
                save_file = "C:\\quanttime\\data\\tmp\\" + "index_rotation_test_" + str(self.front_buy) + "_" + \
                    str(self.end_sell) + ".csv"
                try:
                    df_result.to_csv(save_file, mode='a', encoding="gbk", index=False)
                except PermissionError:
                    time.sleep(30)
                    df_result.to_csv(save_file, mode='a', encoding="gbk", index=False)
                counter = 0
                list_result.clear()
            elif len(self.index_codes) <= 10:
                df_result = pd.DataFrame(data=list_result, columns=columns_name)
                save_file = "C:\\quanttime\\data\\tmp\\" + "index_rotation_test_" + str(self.front_buy) + "_" + \
                            str(self.end_sell) + ".csv"

                df_result.to_csv(save_file, mode='a', encoding="gbk", index=False)
                counter = 0
                list_result.clear()
        # print(df_result)
# =========================


if __name__ == "__main__":
    # test = IndexRotationTest(index_codes=["399975.XSHE"], st_start="2019-01-03")
    # code_list = ["3999" + str(i) + ".XSHE" for i in range(78, 99) if i != 88 and i != 84]
    # print(code_list)
    test = IndexRotationTest(index_codes=["399975.XSHE"], st_start="2016-01-01", st_end="2019-06-21")
    test.calc_index_rotation()
