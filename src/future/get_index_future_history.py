# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta
import pymongo

# jqdata context jqdata 授权
auth('13811866763', "sam155")
# mongodb client
mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

def get_index_future_history():
    '''
    获取期指所有合约的历史k线数据
    添加结算价和持仓量数据
    :return:
    '''
    save_path = "C:\\quanttime\\data\\future\\index\\"
    df_future_all = get_all_securities(['futures'])
    if df_future_all.empty:
        print("获取指数合约信息失败，return")
        return

    all_index_future_file = os.listdir(save_path)
    all_index_future_file = [s[0:11] for s in all_index_future_file]

    index_future_db = mongo_client["future_index"]
    list_ccfx = []
    for future_index in df_future_all.index:
        if 'IC' in future_index:
            list_ccfx.append(future_index)
        elif 'IF' in future_index:
            list_ccfx.append(future_index)
        elif 'IH' in future_index:
            list_ccfx.append(future_index)
    df_index_future = df_future_all.loc[list_ccfx]
    # 筛选出期指中已经结束的合约
    df_index_future = df_index_future[df_index_future["end_date"] < pd.Timestamp.today()]
    for future_code in df_index_future.index:
        if future_code in all_index_future_file:
            print("合约：%s已存。。。" % future_code)
            continue
        start = df_index_future.loc[future_code, ["start_date"]]["start_date"]
        end = df_index_future.loc[future_code, ["end_date"]]["end_date"]
        # 获取合约的日行情数据
        df_data = get_price(future_code, start_date=start, end_date=end)
        # 获取结算价与持仓
        df_sett_price = get_extras('futures_sett_price', [future_code], start_date=start, end_date=end)
        df_positions = get_extras('futures_positions', [future_code], start_date=start, end_date=end)
        df_data = pd.merge(df_data, df_sett_price, left_index=True, right_index=True)
        df_data = pd.merge(df_data, df_positions, left_index=True, right_index=True,
                           suffixes=('_sett_price', '_positions'))
        df_data = df_data.rename(columns={str(future_code) + '_sett_price': "sett_price",
                                          str(future_code) + '_positions': "positions"})

        file_name = save_path + str(future_code) + '.csv'
        df_data.to_csv(file_name)
        # save to mogodb
        table = index_future_db[future_code[0:6]]
        table.drop()
        table.insert_many(df_data.to_dict(orient="record"))
        print("存储合约%s, end" % future_code)


if __name__ == "__main__":
    get_index_future_history()