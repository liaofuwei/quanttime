# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import pandas as pd

import time
from datetime import datetime, timedelta

import pymongo
import os
import tushare as ts

'''
从tushare获取财报审计机构，即审计意见
财报审计意见按年度获取
年报季后集体更新一次
注意：更新不做增量更新，所有在运行前，先备份文件夹在更新
除特殊情况，如中报需要送转，或者交易所要求的情况，中报不审计，所以中报不更新
'''


def get_audit_from_ts():
    # tushare connect context
    token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
    ts.set_token(token)
    # ts 授权
    pro = ts.pro_api()

    save_path = "C:\\quanttime\\data\\finance\\audit\\"

    # 股票基本信息只获取ts_code与list_date
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
    if stock_basic.empty:
        print("获取ts股票基本信息失败，return")
        return
    stock_basic = stock_basic.set_index("ts_code")
    stock_basic['list_date'] = stock_basic['list_date'].apply(pd.to_datetime)

    audit_file_path = "C:\\quanttime\\data\\finance\\audit\\"
    list_audit_file = os.listdir(audit_file_path)
    list_audit_file = [s[0:9] for s in list_audit_file]
    list_empty_audit = []

    # stock_basic = stock_basic.iloc[0:2] 调试时使用
    for stock_code, row in stock_basic.iterrows():
        # print(stock_code)
        # print(row['list_date'].date().year)
        print("正在处理%s....." % stock_code)
        if stock_code in list_audit_file:
            print("%s 已存在，不获取" % stock_code)
            continue
        start = str(row['list_date'].date().year) + '1230'
        end = str(datetime.today().date().year) + str(datetime.today().date().month) + str(datetime.today().date().day)
        df = pro.fina_audit(ts_code=stock_code, start_date=start, end_date=end)
        if df.empty:
            print("获取%s的财务审计信息为空" % stock_code)
            list_empty_audit.append(stock_code)
            time.sleep(1)
            continue
        df.to_csv(save_path+stock_code+'.csv', encoding="gbk", index=False)
        time.sleep(1.1)
    print("本次更新完成，共获取：%d只股票的审计机构" % len(stock_basic))
    print("本次更新，获取信息为空的stock list：%r" % list_empty_audit)


if __name__ == "__main__":
    get_audit_from_ts()