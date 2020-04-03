# -*-coding:utf-8 -*-
__author__ = 'liao'

import pymongo
import pandas as pd
import os

'''
功能：将csv格式的数据转存到mongodb中

'''


def csv2mongodb():
    '''
    将制定目录下所有的csv文件转存到MongoDB中
    可通过配置目录切换，实现导入不同目录下的csv
    :return:
    '''
    # 1、配置目录文件夹，该文件夹内即为需要转存的csv文件，配置该处
    basic_path = "C:\\quanttime\\data\\"
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

    # 批量插入数量
    batch_insert_nums = 1000

    # 1 basic info 文件夹内容转存
    path = basic_path + "basic_info\\"
    basic_db = mongo_client["basic_info_db"]

    # 在basic_info文件夹内需要转存的csv文件可以放到这个list中,没有写文件名后缀主要是为方便后面作为table name 使用
    csv_files = ["all_stock_info", "all_trade_day"]
    for file_name in csv_files:
        tmp_name = path + file_name + ".csv"
        # print(tmp_name)
        table_data = basic_db[file_name]
        df_basic_info = pd.read_csv(tmp_name, encoding="gbk")
        # print(df_basic_info.head(5))
        df_len = len(df_basic_info)
        if df_len > batch_insert_nums:
            fetch_times = int(df_len / batch_insert_nums)
            for i in range(fetch_times):
                df_tmp = fetch_df_data_by_index(df_basic_info, i*batch_insert_nums, (i+1)*batch_insert_nums)
                dic_data = df_tmp.to_dict(orient="record")
                table_data.insert_many(dic_data)
            table_data.insert_many(df_basic_info.iloc[fetch_times*batch_insert_nums:, :].to_dict(orient="record"))
        else:
            table_data.insert_many(df_basic_info.to_dict(orient="record"))
    print("basic info db insert end")
    # =============================================================


def dump_invaluation():
    '''

    :return:
    '''
    # 2 finance 文件夹
    basic_path = "C:\\quanttime\\data\\"
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

    # 批量插入数量
    batch_insert_nums = 1000

    path = basic_path + "finance\\"
    finance_db = mongo_client["finance_db"]
    # invaluation table
    invaluation_path = path + "valuation\\"
    file_list = os.listdir(invaluation_path)
    for file_name in file_list:
        code = file_name.split(".")[0]
        table_data = finance_db[code]
        tmp_name = invaluation_path + file_name
        df_invaluation = pd.read_csv(tmp_name, encoding="gbk")
        # print(tmp_name)
        print(df_invaluation.head(5))
        if df_invaluation.empty:
            continue
        df_len = len(df_invaluation)
        if df_len > batch_insert_nums:
            fetch_times = int(df_len / batch_insert_nums)
            for i in range(fetch_times):
                df_tmp = fetch_df_data_by_index(df_invaluation, i*batch_insert_nums, (i+1)*batch_insert_nums)
                dic_data = df_tmp.to_dict(orient="record")
                table_data.insert_many(dic_data)
            table_data.insert_many(df_invaluation.iloc[fetch_times*batch_insert_nums:, :].to_dict(orient="record"))
        else:
            table_data.insert_many(df_invaluation.to_dict(orient="record"))
    print("dump invaluation end!")

def dump_tushare_index_valuation():
    '''
    转存tushare的指数信息到mongodb
    :return:
    '''




def fetch_df_data_by_index(df, st_index, et_index):
    '''
    根据起始index与结束index，取df数据
    :param df:
    :param st_index: int
    :param et_index: int
    :return: df
    '''
    if et_index > (len(df) - 1):
        return df.iloc[0:0]

    return df.iloc[st_index:et_index, :]

if __name__ == "__main__":
    # csv2mongodb()
    dump_invaluation()