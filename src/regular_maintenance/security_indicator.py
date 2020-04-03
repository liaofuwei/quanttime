# -*-coding:utf-8 -*-
__author__ = 'liao'

from jqdatasdk import *
import pandas as pd

import os
import sys
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

"""
joinquant 券商专有指标
指标频度：年度指标
查询joinquant security_indicator表
按年更新
"""


class SecurityIndicatorMaintenance:
    def __init__(self):
        # 初始化日志
        self.log = logging.getLogger("security_indicator")
        # 日志文件最大1M，最大备份3个
        self.disk_name = os.getcwd()[0]
        # 日志文件都存在quanttime\log文件夹内
        log_path = self.disk_name + ":\\quanttime\\log\\security_indicator.txt"
        file_handler = RotatingFileHandler(log_path, maxBytes=1 * 1024 * 1024, backupCount=3)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        # 控制台
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(formatter)

        self.log.addHandler(file_handler)
        self.log.addHandler(console)

        self.basic_dir = self.disk_name + ":\\quanttime\\data\\security_indicator\\"

        # jqdata context
        auth('13811866763', "sam155")  # jqdata 授权
    # ======

    def get_security_indicator_from_jq(self):
        """
        从jq获取券商专用指标
        :return:
        """
        # J67券商行业码
        stocks_code = get_industry_stocks('J67')
        if len(stocks_code) == 0:
            print("从joinquant获取证券行业所有股票代码，返回空")
            return
        curr_year = datetime.today().year
        date_year = [str(curr_year-1)]
        # for test stocks_code = ["600030.XSHG"]
        for code in stocks_code:
            q = query(security_indicator).filter(security_indicator.code == code)
            df = pd.DataFrame()
            path = self.basic_dir + "jq\\" + str(code) + ".csv"
            print(path)
            for stat_date in date_year:
                tmp = get_fundamentals(q, statDate=stat_date)
                # print(tmp)
                if tmp.empty:
                    continue
                df = pd.concat([df, tmp])
            if df.empty:
                continue
            if os.path.exists(path):
                df.to_csv(path, mode='a', encoding="gbk", header=None)
            else:
                df.to_csv(path, encoding="gbk")
        print("update end....")


if __name__ == "__main__":
    indicator = SecurityIndicatorMaintenance()
    indicator.get_security_indicator_from_jq()

