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
		#仓位信息
        self.position = {"ag": {"amount": 0, "price": 0, "totals": 0},
                         "au": {"amount": 0, "price": 0, "totals": 0},
                         "ratio": 0,
                         "status": "empty",
                         "equity":1000000,
						 "timestamp":"2000-01-01 00:00:00"}

        self.start_back_test = "2019-01-01"
        self.end_back_test = "2019-06-21"
        self.loss_limit = 0.01  #设置止损比例
        self.b_loss_limit = False #true进行止损操作，false不进行止损操作
        self.back_day_stat = -20  #设置当前日期往前推几天的统计信息
        self.long_buy_value = 0.10  #做多金银比，统计买入线，如0.10即10%分位线
        self.long_sell_value = 0.20 #做多金银比，统计卖出线，如0.15即15%分位线
        self.short_buy_value = 0.90 #做空金银比，统计的买入线，如0.85即85%分位线
        self.short_sell_value = 0.80 #做空金银比，统计的卖出线
        self.ag_charge_per = 0.00005 #ag交易费率
        self.au_charge_per = 0.00003 #au交易费率
        log_name = "AUAGTest_min_" + self.start_back_test + "_" + self.end_back_test + "_" + str(self.back_day_stat)+ \
                   "_" + str(int(self.long_buy_value * 100)) + "_" + str(int(self.long_sell_value * 100)) + "_" + \
                   str(int(self.short_buy_value * 100)) + "_" + str(int(self.short_sell_value * 100)) + "_"
        if self.b_loss_limit:
            log_name = log_name + 't' + ".log"
        else:
            log_name = log_name + 'f' + ".log"

        # 初始化日志
        logging.basicConfig(level=logging.INFO,
                            filename=log_name,
                            datefmt='%Y-%m-%d %H:%M:%S',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger("AUAGRadioTest")

        self.trade_trace_list = [] #交易轨迹记录，元素为self.position
        self.profit_list = []

        self.gold_future = "C:\\quanttime\\data\\gold\\sh_future\\gold.csv"
        self.silver_future = "C:\\quanttime\\data\\gold\\sh_future\\silver.csv"

        stander_dtype = {'open': float, "close": float, "high": float, "low": float, "volume":float, "money":float  }
        self.gold_future_data = pd.read_csv(self.gold_future, parse_dates=["date"], index_col=["date"], dtype=stander_dtype)
        self.gold_future_data = self.gold_future_data[~self.gold_future_data.reset_index().duplicated().values]

        self.silver_future_data = pd.read_csv(self.silver_future, parse_dates=["date"], index_col=["date"], dtype=stander_dtype)
        self.silver_future_data = self.silver_future_data[~self.silver_future_data.reset_index().duplicated().values]

        self.future_data = pd.merge(self.gold_future_data, self.silver_future_data, left_index=True, right_index=True,
                               suffixes=('_gold', '_silver'))
        self.future_data["compare"] = self.future_data["close_gold"] / self.future_data["close_silver"] * 1000
        # 去重
        self.future_data = self.future_data.dropna()
        #金银合并以后的交易日序列
        self.future_data_trade_date = self.future_data.index
        #print(self.future_data_trade_date[-1])
        #print(type(self.future_data_trade_date[-1]))

        self.gold_future_mins = "C:\\quanttime\\data\\gold\\sh_future\\gold_mins.csv"
        self.silver_future_mins = "C:\\quanttime\\data\\gold\\sh_future\\silver_mins.csv"
        self.gold_future_mins_data = pd.read_csv(self.gold_future_mins, parse_dates=["datetime"], index_col=["datetime"], dtype=stander_dtype)
        #print("gold future mins data before duplicated:%d" % len(self.gold_future_mins_data))
        self.gold_future_mins_data = self.gold_future_mins_data[~self.gold_future_mins_data.reset_index().duplicated().values]
        #print("gold future mins data after duplicated:%d" % len(self.gold_future_mins_data))
        self.silver_future_mins_data = pd.read_csv(self.silver_future_mins, parse_dates=["datetime"], index_col=["datetime"], dtype=stander_dtype)
        self.silver_future_mins_data = self.silver_future_mins_data[~self.silver_future_mins_data.reset_index().duplicated().values]

        self.future_data_mins = pd.merge(self.gold_future_mins_data, self.silver_future_mins_data, left_index=True, right_index=True,
                                    suffixes=('_gold', '_silver'))
        self.future_data_mins["compare"] = self.future_data_mins["close_gold"] / self.future_data_mins["close_silver"] * 1000
        self.future_data_mins = self.future_data_mins.dropna()
        #print(self.future_data_mins.index)
#-------------------------------------------------------------------------------------------------------------------
    def get_appoint_date_stat_info(self, strDate, nDays):
        '''
        获取指定日期前推或者往后推的几天的统计信息，统计信息来源为合并后的self.future_data
        :param strDate:input 日期，如2010-10-10
        :param nDays:往前或者往后的n天,<0 往前推，>0往后推
        :return:统计信息的pd.dataframe

        eg, describe:
                       compare
             count	20.000000
             mean	78.479471
              std	0.625898
              min	77.456493
             25%	77.980835
             50%	78.401895
             75%	79.030748
             max	79.437169
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

# -------------------------------------------------------------------------------------------------------------------
    def set_buy_sell_value(self, strDate, nDays, long_buyValue, long_sellValue, short_buyValue, short_sellValue):
        '''
        获取买入，卖出的基准
        该基准可调整，如做空金银比时，按照做空金银比最大值的5%分位设置操作起始线，回落到10%分位线卖出，
        做多金银比按照最小值的5%分位线作为操作起始线，涨到10%分位线卖出
        :param strDate:input 日期，如2010-10-10
        :param nDays:往前或者往后的n天,<0 往前推，>0往后推
        :param long_buyValue:做多买入线
        :param long_sellValue:做多卖出线
        :param short_buyValue:做空买入线
        :param short_sellValue:做空卖出线
        :return:list 返回一个按照做空买入线，卖出线，做多买入线，卖出线的list
        '''
        inputDate = pd.to_datetime(strDate)
        if nDays < 0:
            if self.future_data.index.tolist().index(inputDate) + nDays >= 0:
                pre_date = self.future_data.index[self.future_data.index.tolist().index(inputDate) + nDays]
                pre_date_1 = self.future_data.index[self.future_data.index.tolist().index(inputDate) -1]
                #print("input date: %s, stat begin:%s, end:%s" % (strDate, pre_date, pre_date_1))
                df_stat = self.future_data.loc[pre_date:pre_date_1, ["compare"]]
                v_5 = df_stat.quantile(long_buyValue).compare #5%分位
                v_10 = df_stat.quantile(long_sellValue).compare #10%分位
                v_90 = df_stat.quantile(short_sellValue).compare  # 90%分位
                v_95 = df_stat.quantile(short_buyValue).compare  # 95%分位
                return [v_95, v_90, v_5, v_10]
            else:
                return []
        else:
            return [] #后统计，是不是未来函数，屏蔽不用？？？？？
            if self.future_data.index.tolist().index(inputDate) + nDays <= len(self.future_data.index) - 1:
                after_date = self.future_data.index[self.future_data.index.tolist().index(inputDate) + nDays]
                after_date_1 = self.future_data.index[self.future_data.index.tolist().index(inputDate) + 1]
                #print("input date: %s, stat begin:%s, end:%s" % (strDate, after_date_1, after_date))
                return self.future_data.loc[after_date_1:after_date,["compare"]].describe()
            else:
                return -1

# -------------------------------------------------------------------------------------------------------------------
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
        return "error"
#-------------------------------------------------------------------------
    def trade_charge(self, au_totals, ag_totals):
        '''
        计算交易费用
        :param au_totals: 黄金总金额
        :param ag_totals: 白银总金额
        :return: 总费用。float
        '''
        sum = self.ag_charge_per * ag_totals + self.au_charge_per * au_totals
        return round(sum, 2)

# ---------------------------------------------------------------------------
    def calc_profit(self, strDatetime):
        '''
        计算指定日期的浮动利润，用于止损
        :param strDatetime: datetime，日期
        :return: float
        '''
        au_long = self.position["au"]["amount"] > 0  # true au:long --false au:short
        ag_long = self.position["ag"]["amount"] > 0  # true ag:long --false ag:short
        if au_long and (not ag_long):  # 卖空白银，做多黄金，说明是做多金银比
            # 相当于买入白银，平空仓
            pre_ag_price = self.position["ag"]["price"]
            # gold，卖出黄金平多仓
            pre_au_price = self.position["au"]["price"]

            ag_profit = (pre_ag_price - self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver) * 5 * 15  # <0
            au_profit = (self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold - pre_au_price) * 1000  # >0
            total_profit = ag_profit + au_profit
            return total_profit
        elif not au_long and ag_long:  # 当前为卖空黄金，做多白银，即做空金银比
            #相当于卖出白银，平多仓
            pre_ag_price = self.position["ag"]["price"]
            #相当于买入黄金，平空仓
            pre_au_price = self.position["au"]["price"]

            ag_profit = (self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver - pre_ag_price) * 5 * 15
            au_profit = (pre_au_price - self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold) * 1000
            total_profit = ag_profit + au_profit
            return total_profit
        else:
            print("error")
            return -1

# -------------------------------------------------------------------------------------------------------------------
    def close_position(self,strDatetime, direct):
        '''
        平仓操作
        :param strDatetime: 平仓的日期
        :param direct: 做多金银比平仓direct=1，做空金银比平仓direct=-1
        :return:
        '''
        if direct == 1:#做多金银比平仓
            ag_buy_amount = abs(self.position["ag"]["amount"])
            pre_ag_price = self.position["ag"]["price"]
            self.position["ag"]["amount"] = 0
            self.position["ag"]["price"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver
            self.position["ag"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver * 5 * 15
            # gold，卖出黄金平多仓
            au_sell_amount = self.position["au"]["amount"]
            pre_au_price = self.position["au"]["price"]
            self.position["au"]["amount"] = 0
            self.position["au"]["price"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold
            self.position["au"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold * 1000

            ag_profit = (pre_ag_price - self.position["ag"]["price"]) * 5 * 15  # <0
            au_profit = (self.position["au"]["price"] - pre_au_price) * 1000  # >0
            total_profit = ag_profit + au_profit
            self.profit_list.append(total_profit)
            self.position["status"] = "empty"
            charge = self.trade_charge(self.position["au"]["totals"], self.position["ag"]["totals"])
            self.position["equity"] = self.position["equity"] - charge + total_profit
            self.position["timestamp"] = strDatetime
            self.trade_trace_list.append(self.position)

            self.log.info("                           ")
            self.log.info("============================")
            self.log.info("平仓交易（做多金银比平仓），trade date:%s" % strDatetime)
            self.log.info("买入白银平空：%d 手" % abs(self.position["ag"]["amount"]))
            self.log.info("买入价格：%f, ****前价：%f" % (self.position["ag"]["price"], pre_ag_price))
            self.log.info("买入白银总金额：%f" % self.position["ag"]["totals"])
            self.log.info("################################")
            self.log.info("卖出黄金平仓：%d 手" % abs(self.position["au"]["amount"]))
            self.log.info("卖出价格：%f, ****前价：%f" % (self.position["au"]["price"], pre_au_price))
            self.log.info("卖出黄金总金额：%f" % self.position["au"]["totals"])
            self.log.info("本次操作净利：%f" % total_profit)
            self.log.info("============================")
            self.log.info("                           ")
        elif direct == -1:#做空金银比平仓
            # 卖出白银，平多仓
            ag_sell_amount = self.position["ag"]["amount"]
            pre_ag_price = self.position["ag"]["price"]
            self.position["ag"]["amount"] = 0
            self.position["ag"]["price"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver
            self.position["ag"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver * 5 * 15
            # gold 买入黄金，平空仓
            au_buy_amount = self.position["au"]["amount"]
            pre_au_price = self.position["au"]["price"]
            self.position["au"]["amount"] = 0
            self.position["au"]["price"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold
            self.position["au"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold * 1000
            ag_profit = (self.position["ag"]["price"] - pre_ag_price) * 5 * 15
            au_profit = (pre_au_price - self.position["au"]["price"]) * 1000
            total_profit = au_profit + ag_profit
            self.profit_list.append(total_profit)
            self.position["status"] = "empty"
            charge = self.trade_charge(self.position["au"]["totals"], self.position["ag"]["totals"])
            self.position["equity"] = self.position["equity"] - charge + total_profit
            self.position["timestamp"] = strDatetime
            self.trade_trace_list.append(self.position)

            self.log.info("                           ")
            self.log.info("============================")
            self.log.info("平仓交易（做空金银比平仓），trade date:%s" % strDatetime)
            self.log.info("卖出白银平多：%d 手" % ag_sell_amount)
            self.log.info("卖出价格：%f, 前价：%f" % (self.position["ag"]["price"], pre_ag_price))
            self.log.info("卖出白银总金额：%f" % self.position["ag"]["totals"])
            self.log.info("################################")
            self.log.info("买入黄金平空仓：%d 手" % abs(au_buy_amount))
            self.log.info("买入价格：%f, 前价：%f" % (self.position["au"]["price"], pre_au_price))
            self.log.info("买入黄金总金额：%f" % self.position["au"]["totals"])
            self.log.info("本次操作净利：%f" % total_profit)
            self.log.info("============================")
            self.log.info("                           ")
        else:
            print("买卖方向填写错误，direct=1或者-1，不能为其他值")
            self.log.info("买卖方向填写错误，direct=1或者-1，不能为其他值")

# -------------------------------------------------------
    def open_position(self, strDatetime, direct):
        '''
        开仓操作
        :param strDate: 交易日期
        :param direct:
        :return: 做多金银比开仓direct=1，做空金银比开仓direct=-1
        '''
        if direct == 1:
            # 做多金银比，由低比值向均值回归，此时应该卖空silver，买入gold
            self.position["ag"]["amount"] = -5
            self.position["ag"]["price"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver
            self.position["ag"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver * 5 * 15
            # gold
            self.position["au"]["amount"] = 1
            self.position["au"]["price"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold
            self.position["au"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold * 1000
            self.position["status"] = "full"
            charge = self.trade_charge(self.position["au"]["totals"], self.position["ag"]["totals"])
            self.position["equity"] = self.position["equity"] - charge
            self.position["timestamp"] = strDatetime
            self.trade_trace_list.append(self.position)

            self.log.info("                            ")
            self.log.info("============================")
            self.log.info("开仓交易（做多金银比），trade date:%s" % strDatetime)
            self.log.info("卖出白银：%d 手" % abs(self.position["ag"]["amount"]))
            self.log.info("卖出价格：%f" % self.position["ag"]["price"])
            self.log.info("卖出白银总金额：%f" % self.position["ag"]["totals"])
            self.log.info("################################")
            self.log.info("买入黄金：%d 手" % abs(self.position["au"]["amount"]))
            self.log.info("买入价格：%f" % self.position["au"]["price"])
            self.log.info("买入黄金总金额：%f" % self.position["au"]["totals"])
            self.log.info("============================")
            self.log.info("                            ")
        elif direct == -1:
            # 做空金银比，由高比值向均值回归，此时应该卖空gold，买入silver
            self.position["ag"]["amount"] = 5
            self.position["ag"]["price"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver
            self.position["ag"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_silver"]].close_silver * 5 * 15
            # gold
            self.position["au"]["amount"] = -1
            self.position["au"]["price"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold
            self.position["au"]["totals"] = self.future_data_mins.loc[strDatetime, ["close_gold"]].close_gold * 1000
            # position status置位,交易费用
            self.position["status"] = "full"
            charge = self.trade_charge(self.position["au"]["totals"], self.position["ag"]["totals"])
            self.position["equity"] = self.position["equity"] - charge
            self.position["timestamp"] = strDatetime
            self.trade_trace_list.append(self.position)

            self.log.info("                            ")
            self.log.info("============================")
            self.log.info("开仓交易（做空金银比），trade date:%s" % strDatetime)
            self.log.info("买入白银：%d 手" % abs(self.position["ag"]["amount"]))
            self.log.info("买入价格：%f" % self.position["ag"]["price"])
            self.log.info("买入白银总金额：%f" % self.position["ag"]["totals"])
            self.log.info("################################")
            self.log.info("卖出黄金：%d 手" % abs(self.position["au"]["amount"]))
            self.log.info("卖出价格：%f" % self.position["au"]["price"])
            self.log.info("卖出黄金总金额：%f" % self.position["au"]["totals"])
            self.log.info("============================")
            self.log.info("                            ")
        else:
            print("传递的参数有误，direct:%d" % direct)
            self.log.error("传递的参数有误，direct:%d" % direct)

#---------------------------------------------------------
    def get_future_mins_trade_data(self, strDate):
        '''
        通过日期获得分钟级交易数据
        :param strDate: 交易日
        :return: 分钟数据交易数据，df
        '''
        #交易日开始于9:01，当前23:59，早期是没有夜盘，但可以按23:59结束来取值
        strDate = strDate.date().strftime("%Y-%m-%d")
        st = strDate + " 09:01:00"
        et = strDate + " 23:59:00"
        df_future_data_min = self.future_data_mins[pd.to_datetime(st):pd.to_datetime(et)]
        return df_future_data_min
#--------------------------------------------------------------
    def run_back_test_by_mins(self, strDate=None, strEnd=None):
        '''
        按分钟回测
        :param strDate: 回测起始日期
        :param strEnd: 回测结束日期，如果未填写，则回测到数据的最后的一个交易日
        :return:
        '''
        #回测结束时间
        if strEnd == None:
            end = self.future_data_trade_date[-1]
            print("test end date: %s" % end)
            self.log.info("test end date: %s" % end)
        else:
            end = self.end_back_test
            print("test end date: %s" % end)
            self.log.info("test end date: %s" % end)

        #回测开始时间
        if strDate == None:
            start = self.get_close_trade_date(self.start_back_test)
            print("back test start:%s" % start)
            self.log.info("back test start:%s" % start)
        else:
            start = self.get_close_trade_date(strDate)
            print("back test start:%s" % start)
            self.log.info("back test start:%s" % start)

        if start == "error":
            print("back test failed,check para")
            self.log.info("back test failed,check para")
            return

        trade_days = self.future_data.loc[start:end].index
        for trade_date in trade_days:
            #df_stat: mean,std,min,25%, 50%,75%,max
            print("正在回测：%s" % trade_date)
            df_stat = self.get_appoint_date_stat_info(trade_date, self.back_day_stat)
            '''
            self.back_day_stat = -20
            self.long_buy_value = 0.10  #做多金银比，统计买入线，如0.10即10%分位线
            self.long_sell_value = 0.15 #做多金银比，统计卖出线，如0.15即15%分位线
            self.short_buy_value = 0.90 #做空金银比，统计的买入线，如0.85即85%分位线
            self.short_sell_value = 0.85 #做空金银比，统计的卖出线
            '''

            compare_line = self.set_buy_sell_value(trade_date, self.back_day_stat, \
                                                   self.long_buy_value, \
                                                   self.long_sell_value, \
                                                   self.short_buy_value, \
                                                   self.short_sell_value)
            #print(compare_line)
            if len(compare_line) != 4:
                self.log.info("%s, 获取比较线失败，return" % trade_date)
                continue
            '''
            #按照25%，mean，75%设置的买入卖出线
            long_buy_value = df_stat.loc['25%', ["compare"]].compare #做多金银比时，低于该线买入
            long_sell_value = df_stat.loc['mean', ["compare"]].compare #做多金银比时，高于该线卖出
            short_buy_value = df_stat.loc['75%', ["compare"]].compare #做空金银比时，高于该线买入
            short_sell_value = df_stat.loc['mean', ["compare"]].compare #做空金银比时，低于该线卖出
            '''
            # [v_95, v_90, v_5, v_10]排列，注意顺序不要错
            long_buy_value = compare_line[2]
            long_sell_value = compare_line[3]
            short_buy_value = compare_line[0]
            short_sell_value = compare_line[1]

            self.log.debug("统计20日前，做多金银比，买入线：%f" % long_buy_value)
            self.log.debug("统计20日前，做多金银比，卖出线：%f" % long_sell_value)
            self.log.debug("统计20日前，做空金银比，买入线：%f" % short_buy_value)
            self.log.debug("统计20日前，做空金银比，卖出线：%f" % short_sell_value)

            df_future_min = self.get_future_mins_trade_data(trade_date)
            if df_future_min.empty:
                self.log.info("trade date:%s,获取的min数据df为空" % trade_date)
                continue
            for trade_min in df_future_min.index:
                ratio = df_future_min.loc[trade_min, ["compare"]].compare

                if self.position["status"] == "empty":
                    if ratio > short_buy_value:
                        self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                        self.open_position(trade_min, -1)
                    elif ratio < long_buy_value:
                        self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                        self.open_position(trade_min, 1)
                    else:
                        self.log.debug("trade date:%s do nothing " % trade_min)
                        continue
                elif self.position["status"] == "full":
                    #增加止损操作，当亏损超过一定比例时，进行止损，止损比例1%，2%等设置，可调，self.loss_limit
                    au_long = self.position["au"]["amount"] > 0  # true au:long --false au:short
                    ag_long = self.position["ag"]["amount"] > 0  # true ag:long --false ag:short
                    if self.b_loss_limit:
                        total_value = abs(self.position["ag"]["amount"]) * 15 * self.position["ag"]["price"] + \
                                    abs(self.position["au"]["amount"]) * self.position["au"]["price"] * 1000
                        max_loss = total_value * self.loss_limit
                        current_profit = self.calc_profit(trade_min)
                        if current_profit == -1:
                            self.log.error("%s,计算利润发生错误" % trade_min)
                        if (current_profit < 0) and abs(current_profit) > max_loss:
                            print("超过设置的最大亏损数额，进行止损操作")
                            self.log.info("超过设置的最大亏损数额，进行止损操作")
                            if au_long and (not ag_long):  # 卖空白银，做多黄金，说明是做多金银比
                                self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                                self.close_position(trade_min, 1)
                            elif not au_long and ag_long:  # 当前为卖空黄金，做多白银，即做空金银比
                                self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                                self.close_position(trade_min, -1)
                            else:
                                print("error,判断做空做多方向有误")
                                self.log.error("error,判断做空做多方向有误")
                            continue

                    if au_long and (not ag_long):#卖空白银，做多黄金，说明是做多金银比
                        if ratio > long_sell_value: #平仓
                            #买入白银，平空仓
                            self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                            self.close_position(trade_min, 1)
                        else:
                            self.log.debug("trade date:%s,没有达到交易门限值，维持仓位不变" % trade_min)
                    elif not au_long and ag_long:#当前为卖空黄金，做多白银，即做空金银比
                        if ratio < short_sell_value: #金银比回落到平仓线,平白银多仓，黄金空仓
                            self.log.info("%s,当前金银比：%f" % (trade_min, ratio))
                            self.close_position(trade_min, -1)
                        else:
                            self.log.debug("trade date:%s,没有达到交易门限值，维持仓位不变" % trade_min)
                    else:
                        self.log.info("error,判断做空做多方向有误")
                else:
                    print("error")
                    continue

        #print(self.trade_trace_list)
        big_than_zero = 0
        less_than_zero = 0
        less_than_zero_list = []
        for i in self.profit_list:
            if i < 0:
                less_than_zero = less_than_zero + 1
                less_than_zero_list.append(round(i, 2))
            elif i > 0:
                big_than_zero = big_than_zero + 1
            else:
                continue

        self.profit_list = [round(x, 2) for x in self.profit_list]
        self.log.info("最大亏损：%f" % min(self.profit_list))
        self.log.info("亏损金额list：%r" % less_than_zero_list)
        self.log.info("最大盈利：%f" % max(self.profit_list))
        self.log.info("盈利次数：%d" % big_than_zero)
        self.log.info("亏损次数：%d" % less_than_zero)
        self.log.info("总盈利：%f" % round(sum(self.profit_list), 2))
        print(round(sum(self.profit_list),2))




if __name__ == "__main__":
    test = AUAGRadioTest()
    #result = test.get_appoint_date_stat_info("2019-03-11",-20)
    #result2 = test.set_buy_sell_value("2019-03-11",-20, 0.05, 0.10, 0.95, 0.90)
    #result = test.get_close_trade_date("2019-03-10")
    #print(result)
    #print(result2)
    #print(type(result))
    test.run_back_test_by_mins()