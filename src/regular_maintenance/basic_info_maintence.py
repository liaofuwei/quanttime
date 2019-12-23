# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os

from datetime import timedelta, date, datetime
import tushare as ts
import pymongo

'''
程序已检视20191223 ，有效
当前交易日信息也更新至2020-12-31

本程序主要维护基本信息，如stock 基本信息，bond基本信息等
主要包括：
1、def：get_basic_info_table 更新股票基本信息表，该表从tushare和joinquant分别获取，不定期更新
2、def：get_all_trade_day() 获取所有交易日信息，该表也是从tushare与joinquant分别获取，当前更新至2020-12-31


'''

# jqdata 授权
auth('13811866763', "sam155")

token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
ts.set_token(token)
# ts 授权
pro = ts.pro_api()

# 维护的文件目录
stock_basic_info_dir = "C:\\quanttime\\data\\basic_info\\all_stock_info.csv"
stock_basic_info_ts_dir = "C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv"
convert_bond_basic_info_dir = "C:\\quanttime\\data\\basic_info\\convert_bond_basic_info.csv"

mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
all_db_names = mongo_client.list_database_names()


def get_basic_info_table():
    """
    获取股票的基本信息表
    1、是joinquant的基本信息表，存储的文件名及路径为：C:\\quanttime\\data\\basic_info\\all_stock_info.csv
    2、tushare的基本信息表，存储的文件名及路径为：C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv
    同时存入mongodb利于查询使用
    :return:
    """
    if os.path.exists(stock_basic_info_dir):
        stock_info_basic = pd.read_csv(
            stock_basic_info_dir,
            index_col=["code"],
            encoding="gbk")

        get_from_jq = get_all_securities(
            types=["stock"],
            date=None)

        update_stock_index = []
        if len(stock_info_basic) > len(get_from_jq):
            print("更新joinquant的stock 基本信息表竟然比现存的表的小，检查原因！先返回不更新")
        else:
            stock_differ = set(get_from_jq.index) - set(stock_info_basic.index)
            if len(stock_differ) == 0:
                print("本次更新joinquant不需要更新")
            else:
                update_stock_index = list(stock_differ)
        print("%s 更新joinquant stock基本信息表，增加code数量：%r" %
            (datetime.today().date().strftime("%Y-%m-%d"),
             len(update_stock_index)))

        get_from_jq.index.name = "code"
        # 覆盖之前的文件
        get_from_jq.to_csv(stock_basic_info_dir, encoding="gbk")
        if "basic_info_db" in all_db_names:
            basic_info_db = mongo_client["basic_info_db"]
            all_stock_info = basic_info_db["all_stock_info"]
            all_stock_info.drop()
            all_stock_info.insert_many(get_from_jq.to_dict(orient="record"))
            print("完成joinquant 基本stock信息表更新。")
        else:
            print("数据库不存在，先创建数据库在操作！")
    else:
        get_from_jq = get_all_securities(
            types=["stock"],
            date=None)
        get_from_jq.index.name = "code"
        get_from_jq.to_csv(stock_basic_info_dir, encoding="gbk")
        if "basic_info_db" in all_db_names:
            basic_info_db = mongo_client["basic_info_db"]
            all_stock_info = basic_info_db["all_stock_info"]
            all_stock_info.drop()
            all_stock_info.insert_many(get_from_jq.to_dict(orient="record"))
            print("完成joinquant 基本stock信息表更新。")
        else:
            print("数据库不存在，先创建数据库在操作！")
    # ================= tushare 基本信息表更新========================
    if os.path.exists(stock_basic_info_ts_dir):
        ts_stock_info = pd.read_csv(
            stock_basic_info_ts_dir,
            encoding="gbk",
            index_col=["ts_code"])
        get_stock_info_from_ts = pro.query(
            'stock_basic',
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,list_date')
        get_stock_info_from_ts = get_stock_info_from_ts.set_index("ts_code")
        update_stock_index = []
        if len(ts_stock_info) > len(get_stock_info_from_ts):
            print("更新tushare的stock 基本信息表竟然比现存的表的小，检查原因！先返回不更新")
        else:
            stock_differ = set(get_stock_info_from_ts.index) - set(ts_stock_info.index)
            if len(stock_differ) == 0:
                print("本次更新tushare不需要更新")
            else:
                update_stock_index = list(stock_differ)
        print("%s 更新tushare stock基本信息表，增加code数量：%r" %
              (datetime.today().date().strftime("%Y-%m-%d"),
               len(update_stock_index)))
        # 覆盖之前的文件
        get_stock_info_from_ts.to_csv(stock_basic_info_ts_dir, encoding="gbk")
        if "basic_info_db" in all_db_names:
            basic_info_db = mongo_client["basic_info_db"]
            all_stock_info = basic_info_db["all_stock_info_ts"]
            all_stock_info.drop()
            all_stock_info.insert_many(get_stock_info_from_ts.to_dict(orient="record"))
            print("tushare 基本stock信息表更新。")
        else:
            print("数据库不存在，先创建数据库在操作！")
    else:
        get_stock_info_from_ts = pro.query(
            'stock_basic',
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,list_date')
        get_stock_info_from_ts = get_stock_info_from_ts.set_index("ts_code")
        get_stock_info_from_ts.to_csv(stock_basic_info_ts_dir, encoding="gbk")
        if "basic_info_db" in all_db_names:
            basic_info_db = mongo_client["basic_info_db"]
            all_stock_info = basic_info_db["all_stock_info_ts"]
            all_stock_info.drop()
            all_stock_info.insert_many(get_stock_info_from_ts.to_dict(orient="record"))
            print("tushare 基本stock信息表更新。")
        else:
            print("数据库不存在，先创建数据库在操作！")

# ===============================================================
# =============获取交易日信息===================


def get_all_trade_day():
    """
    功能：获取所有的交易日信息，写入到C:\quanttime\data\basic_info文件夹内,数据量很小，不做增量更新，每次获取全部，覆盖重写
    文件名称：allTradeDay.csv
    :return:
    """
    files_path = "C:\\quanttime\\data\\basic_info\\all_trade_day.csv"
    all_trade_day = get_all_trade_days()
    data = pd.DataFrame(data=all_trade_day, columns=["trade_date"])
    data.to_csv(files_path)
    # basic_info_db = mongo_client["basic_info_db"]
    # trade_date = basic_info_db["all_trade_day"]
    # trade_date.drop()
    # trade_date.insert_many(data.to_dict(orient="record"))
    print("update joinquant all_trade_day file end!")
    # ================
    # 从tushare获取
    files_path = "C:\\quanttime\\data\\basic_info\\all_trade_day_ts.csv"
    ts_date = pro.query('trade_cal', start_date='20050104', end_date='20201231')
    '''
    返回字段：
    exchange：交易所，默认上交所
    cal_date：交易日
    is_open: 是否开市
    '''
    basic_info_db = mongo_client["basic_info_db"]
    trade_date = basic_info_db["all_trade_day_ts"]
    trade_date.drop()
    trade_date.insert_many(ts_date.to_dict(orient="record"))
    ts_date = ts_date.set_index("cal_date")
    ts_date.to_csv(files_path)
    print("update tushare all_trade_day file end!")


if __name__ == "__main__":
    # standerConvertBondBasicInfo()
    get_all_trade_day()
    # get_basic_info_table()
