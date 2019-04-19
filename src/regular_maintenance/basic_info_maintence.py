# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd
import logging

import os

from datetime import timedelta, date, datetime
import tushare as ts
import pymongo

'''
本程序主要维护基本信息，如stock 基本信息，bond基本信息等
主要包括：
1、standerConvertBondBasicInfo,从集思录的网页拷贝信息，后，与joinquant的基本stock信息合成一张及转债与对应正股的信息表
2、
3、
4、
5、
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
    '''
    获取股票的基本信息表
    1、是joinquant的基本信息表，存储的文件名及路径为：C:\\quanttime\\data\\basic_info\\all_stock_info.csv
    2、tushare的基本信息表，存储的文件名及路径为：C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv
    同时存入mongodb利于查询使用
    :return:
    '''
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
def getAllTradeDay():
    '''
    功能：获取所有的交易日信息，写入到C:\quanttime\data\basic_info文件夹内,数据量很小，不做增量更新，每次获取全部，覆盖重写
    文件名称：allTradeDay.csv
    :return:
    '''
    files_path = "C:\\quanttime\\data\\basic_info\\all_trade_day.csv"
    all_trade_day = get_all_trade_days()
    data = pd.DataFrame(data=all_trade_day, columns=["trade_date"])
    data.to_csv(files_path)
    basic_info_db = mongo_client["basic_info_db"]
    trade_date = basic_info_db["all_trade_day"]
    trade_date.drop()
    trade_date.insert_many(data.to_dict(orient="record"))
    print("update joinquant all_trade_day file end!")
    # ================
    # 从tushare获取
    files_path = "C:\\quanttime\\data\\basic_info\\all_trade_day_ts.csv"
    ts_date = pro.query('trade_cal', start_date='20050104', end_date='20191231')
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


def delConvertPriceStar(star):
    '''
    功能：从集思录copy的转债信息表csv，转换价格在出现下调或者下调公告后会标记*，该函数主要作用是去掉*
    :return:

    '''
    convert_price = str(star)
    nums = convert_price.split("*")
    return nums[0]


def standerConvertBondBasicInfo():
    '''
    功能：从集思录copy的转债信息表csv，当中很多信息无用，使用该函数将有用信息标准化后存储
    :return:
    '''
    convert_bond_raw = "C:\\quanttime\\data\\basic_info\\convert_bond_basic_info_raw.csv"
    stock_info_basic = pd.read_csv(
        stock_basic_info_dir,
        index_col=["display_name"],
        encoding="gbk")
    # 集思录的原始colums信息命名，其中名称中带——x表示改行信息完全无用，只是用来标记不同的名称
    jisilu_column_name = [
        "bond_code",
        "bond_name",
        "bond_price",
        "bond_x1",
        "stock_name",
        "stock_price",
        "stock_x1",
        "pb",
        "convert_price",
        "convert_value",
        "premium",
        "bond_value",
        "band",
        "option_value",
        "shuishou_price",
        "qiangshu_price",
        "bond_x2",
        "bond_x3",
        "expire",
        "year_to_end",
        "return",
        "shuishou_ret",
        "amount",
        "operation"]
    convert_bond_basic_info_raw = pd.read_csv(
        convert_bond_raw,
        encoding="gbk",
        names=jisilu_column_name,
        index_col=False)
    convert_bond_basic_info_raw = convert_bond_basic_info_raw.set_index(
        "stock_name")
    convert_bond_basic_info_raw["convert_price"] = convert_bond_basic_info_raw["convert_price"].map(
        delConvertPriceStar)
    convert_bond_basic_info = pd.merge(
        convert_bond_basic_info_raw,
        stock_info_basic,
        left_index=True,
        right_index=True,
        suffixes=[
            '_bond',
            '_stock'])
    convert_bond_basic_info = convert_bond_basic_info.rename(
        columns={"code": "stock_code"})
    save_columns = [
        "bond_code",
        "stock_code",
        "bond_name",
        "convert_price",
        "pb",
        "shuishou_price",
        "qiangshu_price",
        "expire",
        "year_to_end"]
    convert_bond_basic_info.to_csv(
        convert_bond_basic_info_dir,
        columns=save_columns,
        encoding="gbk",
        index_label="stock_name")
    print("standerConvertBondBasicInfo end!")





if __name__ == "__main__":
    # standerConvertBondBasicInfo()
    # getAllTradeDay()
    get_basic_info_table()
