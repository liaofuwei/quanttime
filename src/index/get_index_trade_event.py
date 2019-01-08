#-*-coding:utf-8 -*-
__author__ = 'Administrator'
import sys

import tushare as ts
from datetime import datetime, timedelta
import pandas as pd
import logging
from jqdatasdk import *
from prettytable import PrettyTable


def indexTrade():
    '''
    功能：通过比较10,20日等的收盘价，判断是否买入或者卖出
    注意，需要在交易日运行，否则判断日期不准确！！！！！！！
    当前比较基准有：
    1、当前价格与20日前收盘价进行比较，如果高则买入，如果低则卖出
    2、当前价格与20日前收盘价进行比较，如果高则买入，当前价格与10日前收盘价比较，如果低则卖出
    3、当前价格与76日前收盘价进行比较，如果高则买入，当前价格与33日前收盘价比较，如果低则卖出
    :return:
    '''
    auth('13811866763', "sam155")  # jqdata 授权
    # 所用文件目录如下：
    index_basic_info_dir = "C:\\quanttime\\data\\basic_info\\index_trade.csv"
    index_k_dir = "C:\\quanttime\\data\\index\\jq\\"
    #index 基本信息等
    df_index_info = pd.read_csv(index_basic_info_dir, index_col=["code"], encoding="gbk")

    all_trade_day = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv", index_col=["trade_date"])

    result = []
    for index1 in df_index_info.index:
        row_record = []
        index_file = index_k_dir + str(index1) + ".csv"
        index_name = df_index_info.loc[index1, ["display_name"]].display_name
        current_date = datetime.today().date().strftime("%Y-%m-%d")
        #current_date = "2018-12-28" #for test
        # 获取当时的价格
        current_df = get_price(index1, start_date=current_date, end_date=current_date, frequency='daily', fields=None)
        # print(index_name)
        row_record.append(index_name)
        try:
            index_k_data = pd.read_csv(index_file)
            index_k_data = index_k_data.drop_duplicates("date") #去重
            index_k_data = index_k_data.set_index("date")
            row_record.append(current_date)
            # 1、与前20天的情况比较，当前持有则低于前20日收盘价则卖出，当前空仓则高于前20日收盘价则买入
            back_trace = 20
            if all_trade_day.index.tolist().index(current_date) - back_trace >= 0:
                pre_date = all_trade_day.index[all_trade_day.index.tolist().index(current_date) - back_trace]
                pre_value = index_k_data.loc[pre_date, ['close']].close
                current = current_df.loc[current_date,['close']].close #当前的close价格
                if float(pre_value) > float(current):
                    # compare = u'当前close低于20日前close'
                    compare = 'sell c:%r vs p:%r '%(current,pre_value)
                    row_record.append(compare)
                    # print("%r-%r 当前价格低于20日前"%(index1,index_name))
                else:
                    # compare = u'当前close高于（或等于）20日前close'
                    compare = 'buy c:%r vs p:%r '%(current,pre_value)
                    row_record.append(compare)
                    # print("%r-%r 当前价格高于20日前"%(index1,index_name))
                # 2、前20日收盘价，前10日收盘价情况比较，当前持有则低于前10日收盘价则卖出，当前空仓则高于前20日收盘价则买入
                back_trace = 10
                pre_date = all_trade_day.index[all_trade_day.index.tolist().index(current_date) - back_trace]
                #pre_value = index_k_data.loc[pre_date, ['close']].close
                if float(current) >= float(pre_value):
                    # compare = u'当前close高于等于20日前close'
                    compare = 'buy c:%r vs p:%r '%(current, pre_value)
                    row_record.append(compare)
                elif float(current) < float(index_k_data.loc[pre_date, ['close']].close):
                    # compare = u'当前close低于10日前close'
                    compare = 'sell c:%r vs p:%r '%(current,index_k_data.loc[pre_date, ['close']].close)
                    row_record.append(compare)
                else:
                    # compare = u'当前close介于20日前close与10日前close之间'
                    compare = 'hold c:%r vs p:%r '%(current,pre_value)
                    row_record.append(compare)
            # 3、买入与前76日收盘价比较，卖出与33日收盘价比较，即当前close高于前76日close则买入或继续持有，当前close低于前33日close则卖出
            back_trace = 76
            if all_trade_day.index.tolist().index(current_date) - back_trace >= 0:
                pre_date = all_trade_day.index[all_trade_day.index.tolist().index(current_date) - back_trace]
                pre_date_33 = all_trade_day.index[all_trade_day.index.tolist().index(current_date) - 33]
                pre_value = index_k_data.loc[pre_date, ['close']].close
                #current = index_k_data.loc[current_date, ['close']].close
                # print(pre_value)
                # print(current)
                if float(current) >= float(pre_value):
                    # compare = u'当前close高于等于76日前close'
                    compare = 'buy c:%r vs p:%r '%(current,pre_value)
                    row_record.append(compare)
                elif float(current) < float(index_k_data.loc[pre_date_33, ['close']].close):
                    # compare = u'当前close低于33日前close'
                    compare = 'sell c:%r vs p:%r '%(current,index_k_data.loc[pre_date_33, ['close']].close)
                    row_record.append(compare)
                else:
                    # compare = u'当前close介于76日前close与33日前close之间'
                    compare = 'hold c:%r vs 76-p:%r---33-p:%r '%(current,pre_value,index_k_data.loc[pre_date_33, ['close']].close)
                    row_record.append(compare)
        except:
            print("%r process error" % index1)
            continue
        # print(row_record)
        result.append(row_record)
    df_columns = ['index_name', 'date', '20buy VS 20sell', u'20buy VS 10sell', u'76buy vs 33sell']
    df = pd.DataFrame(data=result, index=df_index_info.index, columns=df_columns)
    buy_sell_table = PrettyTable(df_columns)
    buy_sell_table.align['index_name'] = "l"
    buy_sell_table.padding_width = 1
    for row_value in result:
        buy_sell_table.add_row(row_value)
    #print(df.head(5))
    print(buy_sell_table)

#============================================================================================
def index_rotation():
    '''
    功能：指数轮动，即常见的如二八轮动，二八轮动plus
    1、二八轮动。即采用沪深300与中证500指数作为轮动标的，当前价格与20日前close价格对比，哪个涨幅大持有哪个，都下跌则不持有
    2、二八轮动plus，则在上诉基础上加入一个均值判断，即在判断哪个涨幅大的基础上，在与19,20,21日三天的均值进行比较，大于/小于均值才转换
    :return:
    '''

    #比较的指数对,当前沪深300VS中证500，沪深300VS创业板50
    compare_indexs_list = [["000300.XSHG", "399905.XSHE"],["000300.XSHG", "399673.XSHE"]]
    auth('13811866763', "sam155")  # jqdata 授权
    # 所用文件目录如下：
    index_basic_info_dir = "C:\\quanttime\\data\\basic_info\\index_trade.csv"
    index_k_dir = "C:\\quanttime\\data\\index\\jq\\"
    # index 基本信息等
    df_index_info = pd.read_csv(index_basic_info_dir, index_col=["code"], encoding="gbk")

    all_trade_day = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv", index_col=["trade_date"])


    row_record = []
    for index_pair in compare_indexs_list:
        if len(index_pair) != 2:
            continue

        #===============
        pre_value_list = [] #存放tmp_pre_list，即两个指数的21,20,19的close值，其中分别是两个list，放在pre_value_list中
        current_list = []
        index_name_list = []
        current_date = datetime.today().date().strftime("%Y-%m-%d")
        #current_date = "2018-12-28" #for test
        for index in index_pair:
            tmp_pre_list = []  # 用于暂时存储21,20,19的close值
            index_file = index_k_dir + str(index) + ".csv"
            index_name = df_index_info.loc[index, ["display_name"]].display_name
            index_name_list.append(index_name)
            # 获取当时的价格
            current_df = get_price(index, start_date=current_date, end_date=current_date, frequency='daily',
                                   fields=None)
            if current_df.empty:
                print("index: %r  get price empty check date!!!")
                continue
            current_list.append(current_df.loc[current_date, ['close']].close)   # 当前的close价格
            index_k_data = pd.read_csv(index_file, index_col=["date"])

            # 21,20,19
            for back_trace in range(21, 18, -1):
                if all_trade_day.index.tolist().index(current_date) - back_trace >= 0:
                    pre_date = all_trade_day.index[all_trade_day.index.tolist().index(current_date) - back_trace]
                    pre_value = index_k_data.loc[pre_date, ['close']].close
                    tmp_pre_list.append(float(pre_value))

            pre_value_list.append(tmp_pre_list)

        print(pre_value_list)
        #1、二八轮动判断
        index0 = float(current_list[0]) > float(pre_value_list[0][1])
        print("index:%r(%r),current close:%r,before 20day close:%r" % \
              (index_pair[0],index_name_list[0],current_list[0],pre_value_list[0][1]))
        index1 = float(current_list[1]) > float(pre_value_list[1][1])
        print("index:%r(%r),current close:%r,before 20day close:%r" % \
              (index_pair[1], index_name_list[1], current_list[1], pre_value_list[1][1]))

        if index0 and index1: #两个指数都比各自的20日前的close价高，两个指数都进入买入门槛
            index0_diff = (float(current_list[0]) - float(pre_value_list[0][1]))/ float(pre_value_list[0][1])
            index1_diff = (float(current_list[1]) - float(pre_value_list[1][1]))/ float(pre_value_list[1][1])
            if index0_diff > index1_diff:
                result = "buy %r(%r)"%(index_pair[0],index_name_list[0])
            elif index0_diff < index1_diff:
                result = "buy %r(%r)"%(index_pair[1],index_name_list[1])
            else:
                result = "%r(%r),%r(%r) amount of increase equality"% \
                         (index_pair[0],index_name_list[0],index_pair[1],index_name_list[1])
        elif index0 and (not index1):# index0>0.index1<0,取index0
            result = "buy %r(%r)" % (index_pair[0], index_name_list[0])
        elif (not index0) and index1:
            result = "buy %r(%r)" % (index_pair[1], index_name_list[1])
        else:
            result = "sell %r(%r),%r(%r) all"%(index_pair[0],index_name_list[0],index_pair[1],index_name_list[1])

        tmp_row_result = ["28stander", current_date, result]
        row_record.append(tmp_row_result)

        #2、二八轮动plus
        #19,20,21,三天收盘价的均值
        pre_index0_mean = sum(pre_value_list[0]) / len(pre_value_list[0])
        pre_index1_mean = sum(pre_value_list[1]) / len(pre_value_list[1])
        index0 = float(current_list[0]) > float(pre_index0_mean)
        print("index:%r(%r),current close:%r,before 20day !!mean!! close:%r" % \
              (index_pair[0], index_name_list[0], current_list[0], pre_index0_mean))
        index1 = float(current_list[1]) > float(pre_index1_mean)
        print("index:%r(%r),current close:%r,before 20day !!mean!! close:%r" % \
              (index_pair[1], index_name_list[1], current_list[1], pre_index1_mean))

        if index0 and index1: #两个指数都比各自的20日前平均的close价高，两个指数都进入买入门槛
            index0_diff = (float(current_list[0]) - pre_index0_mean)/ pre_index0_mean
            index1_diff = (float(current_list[1]) - pre_index1_mean)/ pre_index1_mean
            if index0_diff > index1_diff:
                result = "buy %r(%r)"%(index_pair[0],index_name_list[0])
            elif index0_diff < index1_diff:
                result = "buy %r(%r)"%(index_pair[1],index_name_list[1])
            else:
                result = "%r(%r),%r(%r) amount of increase equality"% \
                         (index_pair[0],index_name_list[0],index_pair[1],index_name_list[1])
        elif index0 and (not index1):# index0>0.index1<0,取index0
            result = "buy %r(%r)" % (index_pair[0], index_name_list[0])
        elif (not index0) and index1:
            result = "buy %r(%r)" % (index_pair[1], index_name_list[1])
        else:
            result = "sell %r(%r),%r(%r) all"%(index_pair[0],index_name_list[0],index_pair[1],index_name_list[1])

        tmp_row_result = ["28 plus", current_date, result]
        row_record.append(tmp_row_result)

    table_columns = ['strategy_name', 'date', "result"]
    buy_sell_table = PrettyTable(table_columns)
    buy_sell_table.align['strategy_name'] = "l"
    buy_sell_table.padding_width = 1
    for row_value in row_record:
        buy_sell_table.add_row(row_value)

    print(buy_sell_table)







if __name__ == "__main__":
    indexTrade()
    index_rotation()


