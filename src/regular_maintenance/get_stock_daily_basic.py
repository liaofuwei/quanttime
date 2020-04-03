# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import pandas as pd
import logging
import os
import sys
from datetime import timedelta, date, datetime
import tushare as ts

from dateutil.parser import parse
import pymongo

# 配置log
log_format = "%(asctime)s - %(levelname)s - %(message)s"
date_format = "%Y-%m-%d %H:%M:%S %p"
log_file_name = "C:\\quanttime\\log\\stock_daily_basic_maintence.log"

logging.basicConfig(filename=log_file_name, level=logging.DEBUG, format=log_format, datefmt=date_format)

sys.path.append('C:\\quanttime\\src\\mydefinelib\\')
import mydefinelib as mylib

token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
# ts.set_token(token)
pro = ts.pro_api(token)  # ts 授权

mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

def get_stock_daily_basic_by_tushare():
    '''
    从tushare获取股票每日信息，包括：收盘价，换手率，市盈率，市净率等等
    获取的数据存放目录：C:\quanttime\data\stock_daily_indicator
    数据使用code作为文件名，例如：000001.sz.csv
    获取股票每日指标信息输入的code获取方式：
    1、先从tushare的stock_basic方法获取，
    2、如未获取成功则从C:\quanttime\data\basic_info读取all_stock_info_ts.csv
    创建一个单独的数据库stock_daily_indicator_db, 表名按照code存储
    :return:
    '''
    logging.debug("===========================")
    logging.debug("开始tushare的每股股票指标数据维护")
    db = mongo_client["stock_daily_indicator_db"]
    tushare_stock_basic_info_path = "C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv"

    stock_list = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,list_date')
    stock_list = stock_list.set_index("ts_code")
    if stock_list.empty:
        stock_list = pd.read_csv(tushare_stock_basic_info_path, index_col=["ts_code"], parse_dates=["list_date"])
    else:
        stock_list["list_date"] = stock_list["list_date"].map(lambda x: parse(x))

    test_list_code = ['000001.SZ', '601318.SH']
    for code in test_list_code:
    #for code in stock_list["ts_code"]:
        file_path = "C:\\quanttime\\data\\stock_daily_indicator\\" + str(code) + ".csv"
        if os.path.exists(file_path):
            stock_data = pd.read_csv(file_path, index_col=["trade_date"], parse_dates=["trade_date"])
            # 记录日期往后的第一个交易日
            update_start = mylib.get_close_trade_date(stock_data.index[-1].date().strftime("%Y-%m-%d"), 1)
            st = update_start.split("-")
            if len(st) == 3:
                update_start = st[0] + st[1] + st[2]
            else:
                logging.error("获取记录中往后一天的日期格式错误")
                continue
            df = pro.daily_basic(ts_code=code, trade_date=update_start)
        else:
            stock_list_date = stock_list.loc[code, ["list_date"]].list_date
            if stock_list_date < date(2006, 1, 1):
                update_start = "20060101"
            else:
                update_start = mylib.get_appoint_trade_date(stock_list_date.date().strftime("%Y-%m-%d"), 30)
                if update_start == "":
                    logging.warning("")





if __name__ == "__main__":
    get_stock_daily_basic_by_tushare()