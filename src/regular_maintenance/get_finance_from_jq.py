#-*-coding:utf-8 -*-
__author__ = 'Administrator'


from jqdatasdk import *
import pandas as pd
from jqdatasdk import *
import os
from datetime import datetime,timedelta

#授权
auth('13811866763',"sam155")

'''
一次性获取聚宽的财务数据，包括balance，case_flow,income,indicator表
目录在finance文件下，各子文件为'valuation', 'balance', 'cash_flow', 'income', 'indicator'
完全跑受到聚宽数据当天只能查询200w条记录的限制
'''
table_name = ['valuation', 'balance', 'cash_flow', 'income', 'indicator']
#update数据的文件夹路径
valuation_file_path = "C:\\quanttime\\data\\finance\\valuation\\"
balance_file_path = "C:\\quanttime\\data\\finance\\balance\\"
cash_flow_file_path = "C:\\quanttime\\data\\finance\\cash_flow\\"
income_file_path = "C:\\quanttime\\data\\finance\\income\\"
indicator_file_path = "C:\\quanttime\\data\\finance\\indicator\\"

#获取所有stock信息
stock_info = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_stock_info.csv",encoding='gbk')

statdate=["2006q1","2006q2","2006q3","2006q4","2006", \
          "2007q1","2007q2","2007q3","2007q4","2007", \
          "2008q1","2008q2","2008q3","2008q4","2008", \
          "2009q1","2009q2","2009q3","2009q4","2009", \
          "2010q1","2010q2","2010q3","2010q4","2010", \
          "2011q1","2011q2","2011q3","2011q4","2011", \
          "2012q1","2012q2","2012q3","2012q4","2012", \
          "2013q1","2013q2","2013q3","2013q4","2013", \
          "2014q1","2014q2","2014q3","2014q4","2014", \
          "2015q1","2015q2","2015q3","2015q4","2015", \
          "2016q1","2016q2","2016q3","2016q4","2016", \
          "2017q1","2017q2","2017q3","2017q4","2017", \
          "2018q1","2018q2","2018q3","2018q4","2018"]

#获取balance表
for i in range(0,len(stock_info)):
    filepath = balance_file_path + str(stock_info.iloc[i,0]) + ".csv"
    q = query(balance).filter(balance.code == stock_info.iloc[i,0])
    df_balance = get_fundamentals(q,statDate = "2006q1")
    for date in statdate:
        tmp = get_fundamentals(q,statDate=date)
        df_balance = pd.concat([df_balance,tmp])
    df_balance.to_csv(filepath)
    print("get balance code:%r successful"%(stock_info.iloc[i,0]))

#获取cash_flow表
for i in range(0,len(stock_info)):
    filepath = cash_flow_file_path + str(stock_info.iloc[i,0]) + ".csv"
    q = query(cash_flow).filter(cash_flow.code==stock_info.iloc[i,0])
    df_cash = get_fundamentals(q, statDate = "2006q1")
    for date in statdate:
        tmp = get_fundamentals(q,statDate=date)
        df_cash = pd.concat([df_cash,tmp])
    df_cash.to_csv(filepath)
    print("get cash flow code:%r successful"%(stock_info.iloc[i,0]))

#获取income表
for i in range(0,len(stock_info)):
    filepath = income_file_path + str(stock_info.iloc[i,0]) + ".csv"
    q = query(income).filter(income.code==stock_info.iloc[i,0])
    df_income = get_fundamentals(q, statDate = "2006q1")
    for date in statdate:
        tmp = get_fundamentals(q,statDate=date)
        df_income = pd.concat([df_income,tmp])
    df_income.to_csv(filepath)
    print("get income code:%r successful"%(stock_info.iloc[i,0]))

#获取indicator表
for i in range(0,len(stock_info)):
    filepath = indicator_file_path + str(stock_info.iloc[i,0]) + ".csv"
    q = query(indicator).filter(indicator.code==stock_info.iloc[i,0])
    df_indicator = get_fundamentals(q, statDate = "2006q1")
    for date in statdate:
        tmp = get_fundamentals(q,statDate=date)
        df_indicator = pd.concat([df_indicator,tmp])
    df_indicator.to_csv(filepath)
    print("get indicator code:%r successful"%(stock_info.iloc[i,0]))