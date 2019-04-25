#-*-coding:utf-8 -*-
__author__ = 'Administrator'
import sys
from datetime import datetime, timedelta
import pandas as pd
import calendar
from dateutil.parser import parse


'''
本文件主要实现一些经常使用的，自己定义的函数
'''

# ================================


def get_max_date(datelist):
    '''
    功能：从一个str的list中获取最大的日期，形如：['2015/10/30', '2015/11/30', '2015/12/31', '2015/5/29', '2015/6/30']
    :param datelist:list
    :return:str 最大的日期
    '''
    if len(datelist) == 0:
        return ""
    dt1 = []
    for i in datelist:
        if '/' in i:
            dt1.append(datetime.strptime(i, "%Y/%m/%d"))
        if '-' in i:
            dt1.append(datetime.strptime(i, "%Y-%m-%d"))

    tmp = dt1.pop(0)
    for j in dt1:
        delta = j - tmp
        if delta.days > 0:
            tmp = j
    return tmp.strftime("%Y-%m-%d")
# ==============================================


def get_close_trade_date(strDate, direct):
    '''
        功能：获取节假日最近的一个交易日，比如2018-10-1日，的前一个交易日是9-28日
        这里是生成了一个已strDate为end的，往前推10个日期的时间序列
        :param strDate：str类型日期，如"2018-10-01"
               1:日期之后的交易时间，如2018-10-01日后的交易日期
               -1：日期之前的交易时间，如2018-10-01日前的交易日期
        :return:str 类型，如果没有找到则返回strDate的值
    '''
    trade_date = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
                             index_col=["trade_date"],
                             parse_dates=["trade_date"])
    if direct == -1:
        dates = pd.date_range(end=strDate, periods=10)
        dates = list(reversed(dates.tolist()))
        for tmpDate in dates:
            try:
                return trade_date.index[trade_date.index.tolist().index(tmpDate)].date().strftime("%Y-%m-%d")
            except ValueError:
                continue
    elif direct == 1:
        dates = pd.date_range(start=strDate, periods=10)
        for tmpDate in dates.tolist():
            try:
                return trade_date.index[trade_date.index.tolist().index(tmpDate)].date().strftime("%Y-%m-%d")
            except ValueError:
                continue
    else:
        return strDate
    return strDate

# =========================================


def get_trade_list(start_date, end_date):
    '''
    功能：获取startDate, endDate交易日时间段，该时间查表于all_trade_day.csv
    :param start_date: str，该值需要在all_trade_day.csv内
    :param end_date: str 该值需要在all_trade_day.csv
    :return: str list
    '''
    #print("input startDate:%r,endDate: %r" % (startDate, endDate))
    start = parse(start_date)
    end = parse(end_date)

    trade_date = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
                             index_col=["trade_date"],
                             parse_dates=["trade_date"])
    try:
        start = trade_date.loc[start][0]
        #print("start:%r"%start)
        end = trade_date.loc[end][0]
        #print("end:%r" % end)
    except KeyError:
        print("传入的参数日期不在trade day中")
        return []
    trade_days = trade_date.iloc[start:end+1, 0]
    # return trade_days.index.tolist()
    tmp_list = []
    for i in trade_days.index.tolist():
        tmp_list.append(i.date().strftime("%Y-%m-%d"))
    return tmp_list
#===================================================


def get_appoint_trade_date(strInputDate, nDays):
    '''
    功能：获得指定日期的前，或者后n个交易日
    :param strInputDate: str类型的日期类型，形如“2018-12-28”，
    :param nDays: 前推/后退nDays个交易日, nDays<0 前推，nDays>0后退
    :return: str 日期 形如“2018-12-28”
    '''

    input_date = strInputDate

    if nDays == 0:
        return input_date

    df_trade_date = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
                                index_col=["trade_date"],
                                parse_dates=["trade_date"])

    if nDays < 0:
        trade_date = get_close_trade_date(input_date, -1)
        print("trade_date:%r" % trade_date)
        trade_date = parse(trade_date)
        if df_trade_date.index.tolist().index(trade_date) + nDays >= 0:
            pre_date = df_trade_date.index[df_trade_date.index.tolist().index(trade_date) + nDays]
            return pre_date.date().strftime("%Y-%m-%d")
        else:
            return ""

    else :
        trade_date = get_close_trade_date(input_date, 1)
        print("trade_date:%r" % trade_date)
        trade_date = parse(trade_date)
        if df_trade_date.index.tolist().index(trade_date) + nDays <= len(df_trade_date.index)-1:
            after_date = df_trade_date.index[df_trade_date.index.tolist().index(trade_date) + nDays]
            return after_date.date().strftime("%Y-%m-%d")
        else:
            return ""


#===================================================================================================================
def get_trade_date_by_year_month(nYear, nMonth):
    '''
    功能：按年份月份，取出当月所有的交易日
    :param nYear: 年
    :param nMonth: 月
    :return: list，交易日的所有日期的list，形如["2010-01-03",……,"2010-01-31"]
    '''
    df_trade_date = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
                                index_col=["trade_date"],
                                parse_dates=["trade_date"])
    big_month = [1, 3, 5, 7, 8, 10, 12]
    small_month = [4, 6, 9, 11]


    if nMonth in big_month:
        if nMonth < 10:
            strStart = str(nYear) + "-0" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-0" + str(nMonth) + "-" + "31"
        else:
            strStart = str(nYear) + "-" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-" + str(nMonth) + "-" + "31"
    elif nMonth in small_month:
        if nMonth < 10:
            strStart = str(nYear) + "-0" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-0" + str(nMonth) + "-" + "30"
        else:
            strStart = str(nYear) + "-" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-" + str(nMonth) + "-" + "30"
    else:
        if calendar.isleap(nYear):
            strStart = str(nYear) + "-0" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-0" + str(nMonth) + "-" + "29"
        else:
            strStart = str(nYear) + "-0" + str(nMonth) + "-" + "01"
            strEnd = str(nYear) + "-0" + str(nMonth) + "-" + "28"

    #print("strStart:%r" % strStart)
    #print("strEnd:%r" % strEnd)
    trade_date_st = get_close_trade_date(strStart, 1)
    trade_date_end = get_close_trade_date(strEnd, -1)
    #print("trade_date_st:%r"%trade_date_st)
    #print("trade_date_end:%r"%trade_date_end)
    #print(df_trade_date.index.tolist)
    index_st = df_trade_date.index.tolist().index(parse(trade_date_st))
    index_end = df_trade_date.index.tolist().index(parse(trade_date_end))
    trade_list = df_trade_date.index.tolist()[index_st:index_end+1]
    list = []
    for i in trade_list:
        list.append(i.date().strftime("%Y-%m-%d"))
    return list

#===================================================================================================================



if __name__ == "__main__":
    #print(get_appoint_trade_date("2019-12-25",-3))
    print(get_trade_date_by_year_month(2018, 2))
    # print(get_trade_list("20190415","2019-04-19"))
    # ret = get_close_trade_date("2018-10-01", 1)
    #print(ret)

