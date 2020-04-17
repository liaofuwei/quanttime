# -*-coding:utf-8 -*-
__author__ = 'liao'

from jqdatasdk import *
import pandas as pd
import numpy as np

import os
import sys
from datetime import datetime, timedelta

"""
进行可转债正股的波动性分析
波动性计算函数来至于joinquant，各函数无所谓好坏，用于定性分析
计算时：
1、在程序运行目录需要有raw_data.csv（该数据人工拷贝至集思录）
2、calc_volatility为计算后将结果写入到程序运行目录，文件名为：bond_volatility_result_2.csv
3、需要的行情数据取至joinquant
4、行情数据采用的joinquant数据
最后更新日期：2020-01-03
"""

# jqdata context
auth('13811866763', "sam155")  # jqdata 授权


class CBCalc:
    """"
    可转债波动率计算
    """
    @staticmethod
    def get_convert_bond_code():
        """
            获取可转债的code及对应正股的code
            方法：读取程序运行目录下的csv文件，获取可转债的基本信息
            该csv文件通过人工复制集思录中可转债信息

            :return:df columns:'bond_code', 'bond_name', 'stock_code', 'stock_name'
                    bond_code   bond_name     stock_code          display_name
            0       113527      维格转债        603518.XSHG         锦泓集团
            1       128062      亚药转债        002370.XSHE         亚太药业
            2       127004      模塑转债        000700.XSHE         模塑科技
        """
        convert_basic_file = r'raw_data.csv'
        columns_name = ['bond_code', 'bond_name', 'stock_name']
        df_convert = pd.read_csv(convert_basic_file, usecols=[0, 1, 4], encoding="gbk", names=columns_name,
                                 header=0)
        df_convert["stock_name"] = df_convert["stock_name"].map(lambda x: str(x).replace("?R", ""))
        df_stock = get_all_securities(types=['stock'], date=None)
        df_stock.index.name = "stock_code"
        df_stock = df_stock.reset_index()
        # print(df_stock)
        df = pd.merge(df_convert, df_stock, left_on='stock_name', right_on='display_name')
        df = df[["bond_code", "bond_name", "stock_code", "display_name"]]
        return df
    # =============================
    """
    根据joinquant的社区分享的计算波动率的公式
    进行再次封装
    input参数只有data，即获得行情数据
    返回ret为计算的波动率
    """
    @staticmethod
    def parkinson_vol(data):
        if len(data) <= 20:
            return 0
        vol = np.sqrt(sum(np.log(data['high'] / data['low']) ** 2) / (4 * len(data) * np.log(2))) * np.sqrt(250)
        return vol

    @staticmethod
    def garmanklass_vol(data):
        if len(data) <= 20:
            return 0
        a = 0.5 * np.log(data['high'] / data['low']) ** 2
        b = (2 * np.log(2) - 1) * (np.log(data['close'] / data['open']) ** 2)
        vol = np.sqrt(sum(a - b) / len(data)) * np.sqrt(250)
        return vol

    @staticmethod
    def rogerssatchell_vol(data):
        if len(data) <= 20:
            return 0
        a = np.log(data['high'] / data['low']) * np.log(data['high'] / data['open'])
        b = np.log(data['low'] / data['close']) * np.log(data['low'] / data['open'])
        vol = np.sqrt(sum(a + b) / len(data)) * np.sqrt(250)
        return vol

    @staticmethod
    def garmanklassyang_vol(data):
        if len(data) <= 20:
            return 0
        a = 0.5 * np.log(data['high'][1:] / data['low'][1:]) ** 2
        b = (2 * np.log(2) - 1) * (np.log(data['close'][1:] / data['open'][1:]) ** 2)
        c_array = np.log(data['open'][1:].values / data['close'][:-1].values) ** 2
        c = pd.Series(c_array, index=list(a.index))
        vol = np.sqrt(sum(a - b + c) / (len(data) - 1)) * np.sqrt(250)
        return vol

    @staticmethod
    def yangzhang_vol(data):
        if len(data) <= 20:
            return 0
        a1 = np.log(data['open'][1:].values / data['close'][:-1].values)
        a = sum((a1 - a1.mean()) ** 2) / (len(a1) - 1)
        b1 = np.log(data['close'][1:].values / data['open'][1:].values)
        b = sum((b1 - b1.mean()) ** 2) / (len(b1) - 1)
        c1 = np.log(data['high'][1:] / data['low'][1:])
        c2 = np.log(data['high'][1:] / data['open'][1:])
        c3 = np.log(data['low'][1:] / data['close'][1:])
        c4 = np.log(data['low'][1:] / data['open'][1:])
        c = sum(c1 * c2 + c3 * c4) / len(c1)
        N = len(c1)
        k = 0.34 / (1 + (N + 1) / (N - 1))
        vol = np.sqrt(a + k * b + (1 - k) * c) * np.sqrt(250)
        return vol

    @staticmethod
    def normal_vol(data):
        if len(data) <= 20:
            return 0
        rets = np.diff(np.log(data), axis=0)
        std = rets.std() * np.sqrt(250)
        return std
# ==============波动率计算公式 end ======================

    def calc_volatility(self):
        """
            计算股票的波动率
            :return:
        """
        # 获取所有股票的基本信息
        df_stock = get_all_securities(types=['stock'], date=None)
        df_stock.index.name = "stock_code"
        df_stock = df_stock.reset_index()

        # 在程序的运行目录下有一个从集思录人工拷贝的转债基本信息表，raw_data.csv
        convert_basic_file = r'raw_data.csv'
        columns_name = ['bond_code', 'bond_name', 'stock_name']
        df_convert = pd.read_csv(convert_basic_file, usecols=[0, 1, 4], encoding="gbk", names=columns_name, header=0)
        df_convert["stock_name"] = df_convert["stock_name"].map(lambda x: str(x).replace("?R", ""))
        df = pd.merge(df_convert, df_stock, left_on="stock_name", right_on="display_name")

        yesterday = datetime.today().date() - timedelta(days=1)
        result_list = []
        for index, row in df.iterrows():
            start = row["start_date"]
            # 计算波动性的k线，从上市后一个月开始算，开始连续的涨停数据无意义
            if start + timedelta(days=30) < pd.Timestamp("2016-01-03"):
                start = datetime(2014, 1, 3).date()
            df_stock_price = get_price(row["stock_code"], start_date=start, end_date=yesterday, frequency='daily',
                                       fields=['open', 'close', 'high', 'low'], skip_paused=True, fq='pre')
            parkinson = self.parkinson_vol(df_stock_price)
            garmanKlass = self.garmanklass_vol(df_stock_price)
            rogersSatchell = self.rogerssatchell_vol(df_stock_price)
            garmanKlassYang = self.garmanklassyang_vol(df_stock_price)
            yangZhang = self.yangzhang_vol(df_stock_price)
            normal = self.normal_vol(df_stock_price)
            tmp_list = [row["bond_code"], row["bond_name"], row["stock_name"], parkinson, garmanKlass, rogersSatchell,
                        garmanKlassYang, yangZhang, normal]
            result_list.append(tmp_list)
            # print("calc ing...")
        result_columns_name = ["bond_code", "bond_name", "stock_name", "parkinson", "garmanKlass", "rogersSatchell",
                               "garmanKlassYang", "yangZhang", "normal"]
        df_result = pd.DataFrame(data=result_list, columns=result_columns_name)
        df_ret = df_result
        df_result = df_result.sort_values(by=["normal"])
        result_file_path = r'bond_volatility_result_2.csv'
        df_result.to_csv(result_file_path, encoding="gbk", index=False)
        print("calc end.")
        return df_ret


if __name__ == "__main__":
    cb = CBCalc()
    cb.calc_volatility()
