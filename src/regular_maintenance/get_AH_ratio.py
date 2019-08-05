# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta
import time

'''
功能：
1、获取AH的比价信息（目前直接采用joinquant的数据）
2、存储文件目录：C:\quanttime\data\AH_ratio
3、在存储文件目录存放AH的code，包含A、H的code信息，该表来至于集思录
'''

#授权
auth('13811866763', "sam155")


def get_AH_ratio():
    '''
    从joinquant获取AH历史比价信息
    :return:
    '''
    last_update = pd.read_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv',
                              index_col=["module"], parse_dates=["date"])
    print(last_update)
    module_name = 'get_AH_ratio'
    last_update_date = 0

    # 将空值填0
    last_update = last_update.fillna(0)
    if module_name in last_update.index:
        last_update_date = last_update.loc[module_name, ["date"]].date

    if last_update_date != 0:
        try:
            if last_update_date.date() >= datetime.today().date() - timedelta(days=1):
                print("记录已更新，今日不用再更新")
                return
        except ValueError:
            print("记录的最后更新日期格式或记录有误")

    ah_info = r'C:\quanttime\data\AH_ratio\AH_code.csv'
    file_basic_path = 'C:\\quanttime\\data\\AH_ratio\\'
    columns_name = ["name", "a_code", "h_code", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
    df_ah_code = pd.read_csv(ah_info, encoding="gbk", header=None, names=columns_name)
    df_ah_code["a_code"] = df_ah_code["a_code"].map(stander_code)
    print(df_ah_code)
    a_code_list = df_ah_code["a_code"].tolist()
    a_code_list = normalize_code(a_code_list)
    print(a_code_list)
    for code in a_code_list:
        file_path = file_basic_path + code + '.csv'
        if os.path.exists(file_path):
            df_data = pd.read_csv(file_path, index_col=["day"], parse_dates=True, encoding="gbk")
            data_last_update = df_data.index[-1].date().strftime("%Y-%m-%d")
            # 最多取100条，更新周期不大于1个月
            q = query(finance.STK_AH_PRICE_COMP).filter(finance.STK_AH_PRICE_COMP.a_code == code,
                                                        finance.STK_AH_PRICE_COMP.day >= data_last_update).order_by(
                finance.STK_AH_PRICE_COMP.day).limit(30)
            df = finance.run_query(q)
            if df.empty:
                print("code:%r 本次更新没有获取到AH radio数据" % code)
                continue
            df.to_csv(file_path, mode='a', header=None, encoding="gbk")
        else:
            # 非增量更新
            data_last_update = "2011-01-01"
            q = query(finance.STK_AH_PRICE_COMP).filter(finance.STK_AH_PRICE_COMP.a_code == code,
                                                        finance.STK_AH_PRICE_COMP.day >= data_last_update).order_by(
                finance.STK_AH_PRICE_COMP.day).limit(2999)
            df = finance.run_query(q)
            if df.empty:
                print("code:%r 本次更新没有获取到AH radio数据" % code)
                continue
            df.to_csv(file_path, encoding="gbk")
        print("更新code：%r结束......" % code)

    # 更新最后更新的时间
    last_update.loc[module_name, ["date"]] = str(datetime.today().date() - timedelta(days=1))
    last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')
    print("本次更新结束")


def stander_code(code):
    '''
    标准化code
    该方法仅限于深市0开头标准化
    :return:
    '''
    code = str(code)
    if len(code) != 6:
        return code.zfill(6)
    else:
        return code


if __name__ == "__main__":
    get_AH_ratio()