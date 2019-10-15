# -*-coding:utf-8 -*-
__author__ = 'liaowei'

import pandas as pd
import os
import numpy as np
import re

# futu的交易记录文件hk-order.csv
futu_account = r'C:\quanttime\data\account\trade_record\hk-order.csv'
"""
所有华泰的交易记录文件在trade_record文件夹内，通过人工转成了csv保存
华泰的交易记录通过ht48、ht49区分两个不同的账户
按照htxx-201910区分不同的月份如2019年10月
出现htxx-20190731这样的命名主要是华泰在交易时间保存的交易记录只能下载30日
所有出现了脑残的31日单独的一天
"""
account_dir = "C:\\quanttime\\data\\account\\trade_record\\"
write_dir = "C:\\quanttime\\data\\account\\"


def process_ht_trade_record():
    """
    处理华泰的交易记录
    :return:
    """
    date_ranges = [201710, 201711, 201712,
                   201801, 201802, 201803, 201804, 201805, 201806, 201807, 201808, 201809, 201810, 201811, 201812,
                   201901, 201902, 201903, 201904, 201905, 201906, 201907, 201908, 201909, 201910, 201911, 201912,
                   20180131, 20190131, 20190531, 20190731]
    # 后续需要添加月份，添加到add_date_range中，添加的格式为年月，为int格式，由str转成字符串
    add_date_ranges = []

    date_ranges = date_ranges + add_date_ranges

    df_ht = pd.DataFrame()
    # 对原始的交易记录只分析关键列
    ht_columns_name = ["trade_date", "code", "name", "operation", "amount", "price", "total_money"]
    # 排除一些逆回购，理财之类的持仓
    exclude_code_list = ['204001', '131810', '204007', '940247', '940037', '003474', '940018', '204004', '000662',
                         '131811', '204003', '132073', 'SHC827']
    for account in ["ht48-", "ht49-"]:
        select_stock_path = write_dir + account + '.csv'
        # 先清除csv里的内容
        df_tmp = pd.DataFrame()
        df_tmp.to_csv(select_stock_path)

        # ht48_all_record.csv,按股票code记录交易记录，为界面调取单只股票交易轨迹储备，减少文件处理的复杂度
        all_record_path = write_dir + account + 'all_record.csv'
        df_all_record = pd.DataFrame(columns=ht_columns_name)

        for year_month in date_ranges:
            file_path = account_dir + account + str(year_month) + ".csv"
            if os.path.exists(file_path):
                ht_cici = pd.read_csv(file_path, encoding="gbk", usecols=[0, 2, 3, 4, 5, 6, 7], header=0,
                                      names=ht_columns_name, parse_dates=['trade_date'])
                df_ht = pd.concat([df_ht, ht_cici])
        df_ht["code"] = df_ht["code"].map(str)
        df_ht["code"] = df_ht["code"].map(lambda x: x.zfill(6))
        position_stock = pd.unique(df_ht["code"])
        for stock_code in position_stock:
            if stock_code in exclude_code_list:
                continue
            df_select = df_ht[df_ht["code"] == stock_code]
            if df_select.empty:
                continue
            if (len(df_select) == 1) and (df_select.iloc[0, 3] == '申购配号'):
                continue

            df_select.to_csv(select_stock_path, mode='a', index=False, encoding="gbk")
            df_all_record = pd.concat([df_all_record, df_select])
            df_buy_vol = df_select[df_select["operation"] == '证券买入']
            df_sell_vol = df_select[df_select["operation"] == '证券卖出']
            if sum(df_buy_vol["amount"]) == -sum(df_sell_vol["amount"]):
                # 说明已经清仓,计算盈亏数额
                result = sum(df_select['price'] * df_select["amount"])
                data = [["盈亏总额：", -result]]
                df_result = pd.DataFrame(data=data, columns=['A', 'B'])
                df_result.to_csv(select_stock_path, mode='a', encoding="gbk", index=False, header=None)
            else:
                position_amount = sum(df_buy_vol["amount"]) + sum(df_sell_vol["amount"])
                data = [["当前持仓总量：", position_amount]]
                df_result = pd.DataFrame(data=data, columns=['A', 'B'])
                df_result.to_csv(select_stock_path, mode='a', encoding="gbk", index=False, header=None)
            df_tmp.to_csv(select_stock_path, mode='a')
        df_all_record.to_csv(all_record_path, encoding="gbk", index=False)

# ========================


def process_futu_trade_record():
    """
    # 以下处理futu的交易记录
    1、futu的交易记录写入 write_dir = "C:\\quanttime\\data\\account\\"
    2、csv命名为：futu_trade_record.csv
    """
    futu_columns_name = ['direct', 'code', 'name', 'order_price', 'amount', 'status', 'time']
    futu_acc = pd.read_csv(futu_account, header=0, names=futu_columns_name, parse_dates=['time'], encoding='gbk')
    futu_acc["code"] = futu_acc["code"].map(lambda x: str(x).zfill(5))
    # futu_acc["amount"] = futu_acc["amount"].map(process_dot_split)
    futu_acc["amount"] = futu_acc["amount"].map(lambda x: ''.join(s for s in x if s.isalnum()))
    futu_acc["amount"] = pd.to_numeric(futu_acc["amount"])
    futu_acc["order_price"] = pd.to_numeric(futu_acc["order_price"], downcast='float')
    for i in range(len(futu_acc)):
        if futu_acc.iloc[i, 0] == '卖出':
            futu_acc.iloc[i, 4] = -futu_acc.iloc[i, 4]

    futu_code = pd.unique(futu_acc["code"])
    futu_trade_record_path = write_dir + "futu_trade_record.csv"
    # 清空记录文件
    pd.DataFrame().to_csv(futu_trade_record_path)
    # 记录已实现的盈亏总额
    total_return = []
    for code in futu_code:
        df_select = futu_acc[futu_acc["code"] == code]
        df_select.to_csv(futu_trade_record_path, index=False, mode='a', encoding="gbk")
        if sum(df_select["amount"]) == 0:
            ret = sum(df_select["order_price"] * df_select["amount"])
            df_ret = pd.DataFrame(data=[["盈亏总额：", -ret], ["", ""]], columns=['a', 'b'])
            df_ret.to_csv(futu_trade_record_path, mode='a', encoding="gbk", index=False, header=None)
            total_return.append(-ret)
        else:
            df_ret = pd.DataFrame(data=[["当前持仓：", sum(df_select["amount"])], ["", ""]], columns=['a', 'b'])
            df_ret.to_csv(futu_trade_record_path, mode='a', encoding="gbk", index=False, header=None)
    total_money = sum(total_return)
    pd.DataFrame(data=[["", ""], ["总实现盈亏：", total_money]], columns=['a', 'b']).to_csv(
        futu_trade_record_path, mode='a', encoding="gbk", index=False, header=None)
# ============================


if __name__ == "__main__":
    process_futu_trade_record()