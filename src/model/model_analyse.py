#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
from futuquant import *


#授权
#auth('13811866763',"sam155")

class model_Analyse:
    #是否需要下载数据
    __bNeed_download_data = False

    def __init__(self, code):
        self.code = code
        stock_info = pd.read_csv("C:\\quanttime\\data\\basic_info\\all_stock_info.csv", index_col="code", encoding="gbk")
        self.name = stock_info.loc[code,["display_name"]]
        #print(self.name)

    #设置所需数据的路径
    def set_data_file_path(self , path):
        self.dataFilePath = path

    def download_data_ctx(self, dataSource=""):
        if dataSource == "futu":
            self.quote_ctx = OpenQuoteContext(host='127.0.0.1',port=11111)
        elif dataSource == "jq":
            #授权
            auth('13811866763',"sam155")
        elif dataSource=="":
            self.quote_ctx = OpenQuoteContext(host='127.0.0.1',port=11111)
            auth('13811866763',"sam155")

    #从futu下载历史行情数据
    def download_data_from_futu(self, start ,end):
        (self.ret1, self.df) = self.quote_ctx.get_history_kline(self.code,start=start,end=end)
        if self.ret1 != RET_OK:
            print("get price hisdata from futu failed,the reason:%r"%(self.df))

    #从finance目录下读取财政信息
    def load_finance_data(self):
        finance_path = "C:\\quanttime\\data\\finance\\"
        file_name = finance_path + self.code + "_" + self.name + '.csv'
        if os.path.exists(file_name):
            print("file exist")
        else:
            pass



if __name__ == "__main__":
    theModel = model_Analyse("000001.XSHE")