#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd
import logging

import os

from datetime import timedelta, date, datetime
import tushare as ts
import calendar

'''
本程序主要维护基本信息，如stock 基本信息，bond基本信息等
主要包括：
1、standerConvertBondBasicInfo,从集思录的网页拷贝信息，后，与joinquant的基本stock信息合成一张及转债与对应正股的信息表
2、
3、
4、
5、
'''


auth('13811866763',"sam155") #jqdata 授权

token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
ts.set_token(token)
pro = ts.pro_api()#ts 授权

#维护的文件目录
stock_basic_info_dir = "C:\\quanttime\\data\\basic_info\\all_stock_info.csv"
stock_basic_info_ts_dir = "C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv"
convert_bond_basic_info_dir = "C:\\quanttime\\data\\basic_info\\convert_bond_basic_info.csv"

def standerConvertBondBasicInfo():
    '''
    功能：从集思录copy的转债信息表csv，当中很多信息无用，使用该函数将有用信息标准化后存储
    :return:
    '''
    convert_bond_raw = "C:\\quanttime\\data\\basic_info\\convert_bond_basic_info_raw.csv"
    stock_info_basic = pd.read_csv(stock_basic_info_dir, index_col=["display_name"],encoding="gbk")
    #集思录的原始colums信息命名，其中名称中带——x表示改行信息完全无用，只是用来标记不同的名称
    jisilu_column_name = ["bond_code", "bond_name", "bond_price", "bond_x1", "stock_name", "stock_price", "stock_x1",\
                   "pb","convert_price", "convert_value", "premium", "bond_value", "band", "option_value", \
                   "shuishou_price", "qiangshu_price", "bond_x2", "bond_x3", "expire", "year_to_end", "return", \
                   "shuishou_ret", "amount", "operation"]
    convert_bond_basic_info_raw = pd.read_csv(convert_bond_raw, encoding="gbk", names=jisilu_column_name, index_col=False)
    convert_bond_basic_info_raw = convert_bond_basic_info_raw.set_index("stock_name")
    convert_bond_basic_info = pd.merge(convert_bond_basic_info_raw, stock_info_basic, left_index=True,right_index=True,\
                                       suffixes=['_bond', '_stock'])

    save_columns = ["bond_code","code","bond_name","convert_price","shuishou_price","qiangshu_price","expire","year_to_end"]
    convert_bond_basic_info.to_csv(convert_bond_basic_info_dir,columns=save_columns,encoding="gbk",index_label="stock_name")
    print("standerConvertBondBasicInfo end!")


if __name__ == "__main__":
    standerConvertBondBasicInfo()