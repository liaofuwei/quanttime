# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta
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
tushare获取分红信息为非增量更新，更新时将全部覆盖后重写

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

    def get_dividend_by_ts(self, input_code_list=None):
        """
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

        说明：
        从tushare获取分红信息，从运行程序当天算，3天以内会读取valuation_last_update.csv作为判断是全部更新还是更新某一部分股票
        该判断主要是为了防止在更新出现网络等问题，导致的更新效率低下（记录最后更新的股票，下次运行从该股票开始更新，提供更新效率）
        在程序获取完所有股票的分红信息后，会调用一个获取所有股票最新分红的函数，用于生成一个大表，包含所有股票最新分红信息的表格
        使用该大表查询时，可以获得更好的查询效率

        :param: input_code_list:list，需要获取的code
        :return:
        """
        last_update = pd.read_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv',
                                  index_col=["module"], parse_dates=["date"])
        print(last_update)
        module_name = 'get_dividend_by_ts'
        last_update_date = 0

        need_update_all = True

        # 将空值填0
        last_update = last_update.fillna(0)
        if module_name in last_update.index:
            last_update_date = last_update.loc[module_name, ["date"]].date

        if last_update_date != 0:
            try:
                # 三天内做的更新都算是新数据，不做全部更新
                if last_update_date.date() >= datetime.today().date() - timedelta(days=3):
                    need_update_all = False
                    last_update_code = last_update.loc[module_name, ["last_update_code"]]["last_update_code"]
                    print("最近更新过，需要读取last_update_code判断是否更新完毕")
            except ValueError:
                print("全部更新")

        print("开始获取ts分红信息")
        list_code_empty_info = []
        dividend_db = self.mongo_client["dividend_db"]
        tables = dividend_db.list_collection_names()
        feilds = 'ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,' \
                 'record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share'

        # 每获取50条记录，写一次记录，用于记录最后更新的code，这样即使出错也记录到了最接近错误记录的code
        counter = 50
        i_counter = 0

        if input_code_list is None:
            list_code = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
            list_code = list_code.set_index("ts_code")
            if (not need_update_all) and (last_update_code != 0):
                start_code_pos = list_code.index.get_loc(last_update_code)
                list_code = list_code.index[start_code_pos:]
            else:
                list_code = list_code.index
        else:
            list_code = input_code_list
        for ts_code in list_code:
            print("正在获取%s的分红信息。。。。" % ts_code)
            path = self.tushare_dir + ts_code.split('.')[0] + '.csv'
            df = self.pro.dividend(ts_code=ts_code, fields=feilds)
            if df.empty:
                list_code_empty_info.append(ts_code)
                continue

            i_counter = i_counter + 1

            df.to_csv(path, encoding="gbk", index=False)
            table_data = dividend_db[ts_code.split('.')[0]]
            if ts_code.split('.')[0] in tables:
                table_data.drop()
            table_data.insert_many(df.to_dict(orient="record"))
            time.sleep(1)
            if i_counter >= counter:
                # 更新最后更新的时间
                last_update.loc[module_name, ["date"]] = str(datetime.today().date())
                last_update.loc[module_name, ["last_update_code"]] = ts_code
                last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')
                i_counter = 0
        print("本次获取分红信息，返回为空的code list：%r" % list_code_empty_info)
        # 更新最后更新的时间
        last_update.loc[module_name, ["date"]] = str(datetime.today().date())
        last_update.loc[module_name, ["last_update_code"]] = ts_code
        last_update.to_csv(r'C:\quanttime\src\regular_maintenance\valuation_last_update.csv')
        #self.get_newest_ts_dividend()
        print("获取tushare分红信息结束")

    # ===================================================
    def get_newest_ts_dividend(self):
        """
        将ts获取的分红信息进行提取
        提取最新的分红信息，如果最新的（比如半年报)分红信息为空，则提取年报的数据
        :return:
        """
        codes = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
        codes = codes.set_index("ts_code")
        code_list = codes.index
        # code_list = ["000001.SZ"]
        list_last_div_record = []
        list_index = []
        columns_name = []
        i = 0
        for ts_code in code_list:
            path = self.tushare_dir + ts_code.split('.')[0] + '.csv'
            if os.path.exists(path):
                df_dividend = pd.read_csv(path, encoding="gbk", parse_dates=[1])
                if df_dividend.empty:
                    continue
                last_record = df_dividend.iloc[0, :]
                i = i + 1
                if last_record["cash_div"] == 0.0:
                    # 半年报中没分红等信息，则取去年年报数据
                    get_date = datetime(datetime.today().year - 1, 12, 31)
                    last_record = df_dividend[df_dividend['end_date'] == get_date]
                    if not last_record.empty:
                        list_last_div_record.append(last_record.iloc[0, :].tolist())
                        list_index.append(i)
                        columns_name = df_dividend.columns
                    else:
                        continue
                else:
                    list_last_div_record.append(last_record.tolist())
                    list_index.append(i)
                    columns_name = df_dividend.columns
        df_last_div_record = pd.DataFrame(data=list_last_div_record, columns=columns_name, index=list_index)
        df_last_div_record.to_csv(self.tushare_dir + 'all_dividend.csv', encoding="gbk")
        print("完成最后分红信息集成。。。。")

    # ==================================================

    @staticmethod
    def jqcode2tscode(x):
        """
        joinquant code --> tushare code
        即601318.XSHG ---> 601318.SH
        :param x:
        :return:
        """
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
    # regular.get_newest_ts_divident()