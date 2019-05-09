# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime
import time

import logging
import pymongo
import tushare as ts

'''
功能：获取所有股票的历史分红信息，存储方式为csv及mogondb
     命名按照code命名，MongoDB新建数据库，名称为dividend，表名为code
     csv的存储路径为：C:\quanttime\data\dividend,考虑到后续增加其他数据来源，该目录下增加子目录tushare
     获取数据的接口：tushare
一个季度或者到了半年报，年报后运行

'''


class dividend:
    def __init__(self):
        # 初始化日志
        self.regularlog = logging.getLogger("dividend_maintenance")
        self.cmdconsole = logging.StreamHandler()
        self.cmdconsole.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.cmdconsole.setFormatter(self.formatter)
        self.regularlog.addHandler(self.cmdconsole)

        self.tushare_dir = "C:\\quanttime\\data\\dividend\\tushare\\"

        # 从C:\quanttime\data\basic_info文件夹获取所有的code，删除退市的代码
        self.stock_info = pd.read_csv(
            "C:\\quanttime\\data\\basic_info\\all_stock_info.csv",
            index_col=["code"],
            encoding="gbk",
            parse_dates=["start_date", "end_date"])
        # 退市代码，end_time!=2200/1/1
        stock_code = self.stock_info[self.stock_info["end_date"] == datetime.strptime(
            "2200-01-01", "%Y-%m-%d")]
        self.stock_code = stock_code.index
        if len(self.stock_code) == 0:
            self.regularlog.warning("stock code is empty,please check!!!")
            sys.exit(0)

        self.errorCodeList = []

        # jqdata context 暂时不用先注释
        # auth('13811866763', "sam155")

        # mongodb client
        self.mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        self.cons = ts.get_apis()
        # ts 授权
        self.pro = ts.pro_api()
    # ===================================================

    def get_dividend_by_ts(self):
        '''
        使用tushare接口获取分红信息
        ts_code:ts代码
        end_date：分红年度
        ann_date：预案公告日
        div_proc：实施进度
        stk_div：每股送转
        stk_bo_rate：每股送转比例
        stk_co_rate：每股转增比例
        case_div：每股分红（税后）
        cash_div_tax：每股分红（税前）
        record_date：股权登记日
        ex_date：除权除息日
        pay_date：派息日
        div_listdate：红股上市日
        imp_ann_date：实施公告日
        base_date：基准日
        base_share：基准股本（万）

        table_data = dividend_db[tmp_code]
        :return:
        '''
        print("开始获取ts分红信息")
        dividend_db = self.mongo_client["dividend_db"]
        tables = dividend_db.list_collection_names()
        feilds = 'ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,' \
                 'record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share'
        self.stock_code = self.stock_code.map(self.jqcode2tscode)
        # test_code_list = ["601318.SH", "000001.SZ"]
        # for ts_code in test_code_list:
        for ts_code in self.stock_code:
            if ts_code == '000000':
                continue
            print("正在获取%s的分红信息。。。。" % ts_code)
            df = self.pro.dividend(ts_code=ts_code, fields=feilds)
            if df.empty:
                continue
            df = df.set_index("ts_code")
            path = self.tushare_dir + ts_code.split('.')[0] + '.csv'
            df.to_csv(path, encoding="gbk")
            table_data = dividend_db[ts_code.split('.')[0]]
            if ts_code.split('.')[0] in tables:
                table_data.drop()
            table_data.insert_many(df.to_dict(orient="record"))
            time.sleep(1)
        print("获取tushare分红信息结束")

    # ===================================================

    @staticmethod
    def jqcode2tscode(x):
        '''
        joinquant code --> tushare code
        即601318.XSHG ---> 601318.SH
        :param x:
        :return:
        '''
        jq_code = str(x)
        ret = jq_code.split('.')

        if ret[0].isnumeric():
            if ret[0][0] == '6':
                return ret[0] + '.SH'
            else:
                return ret[0] + '.SZ'

        else:
            print("code is not standard,code=%r ", jq_code)
            return "000000"

# ===================================================


if __name__ == "__main__":
    regular = dividend()
    regular.get_dividend_by_ts()