#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
from datetime import datetime, timedelta



'''
1、上期所金，银主力合约的历史K线数据
2、存储目录：C:\quanttime\data\gold\sh_future
3、更新会自动通过读取已存在的数据判断日期，当前存储数据的日期加1日，开始更新
'''

def sh_future_gold_silver_K_data():
    auth('13811866763', "sam155")  # jqdata 授权
    gold_dir = "C:\\quanttime\\data\\gold\\sh_future\\gold.csv"
    silver_dir = "C:\\quanttime\\data\\gold\\sh_future\\silver.csv"

    b_gold_dir_exist = False
    b_silver_dir_exist = False

    # 判断文件目录是否存在
    if os.path.exists(gold_dir):
        b_gold_dir_exist = True
        print("gold dir is existed!!!")

    if os.path.exists(silver_dir):
        b_silver_dir_exist = True
        print("silver dir is existed!!!")

    start_time = "2008-01-01"  # gold:2008  silver:2012
    # end_time = datetime.today().date().strftime("%Y-%m-%d")
    # end_time = "2008-05-01"
    today = datetime.today().date()
    oneday = timedelta(days=1)
    yesterday = today - oneday
    end_time = yesterday.strftime("%Y-%m-%d")

    if b_gold_dir_exist == True:
        gold_hisdata = pd.read_csv(gold_dir, index_col=["date"])
        try:
            d1 = datetime.strptime(gold_hisdata.index[len(gold_hisdata.index) - 1], '%Y-%m-%d')

        except ValueError:
            d1 = datetime.strptime(gold_hisdata.index[len(gold_hisdata.index) - 1], '%Y/%m/%d')
        try:
            d2 = datetime.strptime(gold_hisdata.index[0], '%Y-%m-%d')
        except ValueError:
            d2 = datetime.strptime(gold_hisdata.index[0], '%Y/%m/%d')
        delta = d1 - d2
        delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
        # 判断存储在csv中的日期是顺序还是逆序
        if delta.days > 0:
            d1 = d1 + delta2
            start_time = d1.strftime('%Y-%m-%d')
        else:
            d2 = d2 + delta2
            start_time = d2.strftime('%Y-%m-%d')

        gold_hisdata = get_price('AU9999.XSGE', start_date=start_time, end_date=end_time)
        gold_hisdata.index.name = "date"
        gold_hisdata.to_csv(gold_dir, mode='a', header=None)
        print("%s update sh future gold hisdata succussful" % datetime.now().strftime('%Y-%m-%d'))

    else:
        gold_hisdata = get_price('AU9999.XSGE', start_date=start_time, end_date=end_time)
        gold_hisdata.index.name = "date"
        gold_hisdata.to_csv(gold_dir)
        print("get sh future gold hisdata successful!")

    start_time = "2012-01-01"  # gold:2008  silver:2012
    # end_time = datetime.today().date().strftime("%Y-%m-%d")

    if b_silver_dir_exist:
        silver_hisdata = pd.read_csv(silver_dir, index_col=["date"])
        #print(silver_hisdata.index[len(silver_hisdata.index) - 1])
        try:
            d1 = datetime.strptime(silver_hisdata.index[len(silver_hisdata.index) - 1], '%Y-%m-%d')
        except ValueError:
            d1 = datetime.strptime(silver_hisdata.index[len(silver_hisdata.index) - 1], '%Y/%m/%d')
        try:
            d2 = datetime.strptime(silver_hisdata.index[0], '%Y-%m-%d')
        except ValueError:
            d2 = datetime.strptime(silver_hisdata.index[0], '%Y/%m/%d')

        delta = d1 - d2
        delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
        # 判断存储在csv中的日期是顺序还是逆序
        if delta.days > 0:
            d1 = d1 + delta2
            start_time = d1.strftime('%Y-%m-%d')
        else:
            d2 = d2 + delta2
            start_time = d2.strftime('%Y-%m-%d')

        silver_hisdata = get_price('AG9999.XSGE', start_date=start_time, end_date=end_time)
        silver_hisdata.index.name = "date"
        silver_hisdata.to_csv(silver_dir, mode='a', header=None)
        print("%s update sh future silver hisdata succussful" % datetime.now().strftime('%Y-%m-%d'))

    else:
        silver_hisdata = get_price('AG9999.XSGE', start_date=start_time, end_date=end_time)
        silver_hisdata.index.name = "date"
        silver_hisdata.to_csv(silver_dir)
        print("get sh future silver hisdata successful!")

def sh_future_gold_silver_mins_data():
    '''
    获取分钟级数据
    :return:
    '''
    auth('13811866763', "sam155")  # jqdata 授权
    gold_dir = "C:\\quanttime\\data\\gold\\sh_future\\gold_mins.csv"
    silver_dir = "C:\\quanttime\\data\\gold\\sh_future\\silver_mins.csv"

    b_gold_dir_exist = False
    b_silver_dir_exist = False

    # 判断文件目录是否存在
    if os.path.exists(gold_dir):
        b_gold_dir_exist = True
        print("gold_mins dir is existed!!!")

    if os.path.exists(silver_dir):
        b_silver_dir_exist = True
        print("silver_mins dir is existed!!!")

    start_time = "2012-05-10" + " 09:00:00" # gold:2008  silver:2012
    # end_time = datetime.today().date().strftime("%Y-%m-%d")
    # end_time = "2008-05-01"
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    end_time = yesterday.strftime("%Y-%m-%d") + " 23:59:00"

    if b_gold_dir_exist == True:
        gold_hisdata = pd.read_csv(gold_dir, index_col=["datetime"])
        try:
            d1 = datetime.strptime(gold_hisdata.index[len(gold_hisdata.index) - 1], '%Y-%m-%d %H:%M:%S')

        except ValueError:
            d1 = datetime.strptime(gold_hisdata.index[len(gold_hisdata.index) - 1], '%Y/%m/%d %H:%M:%S')
        try:
            d2 = datetime.strptime(gold_hisdata.index[0], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            d2 = datetime.strptime(gold_hisdata.index[0], '%Y/%m/%d %H:%M:%S')
        delta = d1 - d2
        delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
        # 判断存储在csv中的日期是顺序还是逆序
        if delta.days > 0:
            d1 = d1 + delta2
            start_time = d1.strftime('%Y-%m-%d') + " 09:00:00"
        else:
            d2 = d2 + delta2
            start_time = d2.strftime('%Y-%m-%d') + " 09:00:00"

        gold_hisdata = get_price('AU9999.XSGE', start_date=start_time, end_date=end_time, frequency='1m')
        gold_hisdata.index.name = "datetime"
        gold_hisdata.to_csv(gold_dir, mode='a', header=None)
        print("%s update sh future gold mins hisdata succussful" % datetime.now().strftime('%Y-%m-%d'))

    else:
        gold_hisdata = get_price('AU9999.XSGE', start_date=start_time, end_date=end_time, frequency='1m')
        gold_hisdata.index.name = "datetime"
        gold_hisdata.to_csv(gold_dir)
        print("get sh future gold mins hisdata successful!")

    if b_silver_dir_exist:
        silver_hisdata = pd.read_csv(silver_dir, index_col=["datetime"])
        silver_hisdata.index = pd.to_datetime(silver_hisdata.index)

        d1 = silver_hisdata.index[-1]
        d2 = silver_hisdata.index[0]

        delta = d1 - d2
        delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
        # 判断存储在csv中的日期是顺序还是逆序
        if delta.days > 0:
            d1 = d1 + delta2
            start_time = d1.date().strftime('%Y-%m-%d') + " 09:00:00"
        else:
            d2 = d2 + delta2
            start_time = d2.date().strftime('%Y-%m-%d') + " 09:00:00"

        silver_hisdata = get_price('AG9999.XSGE', start_date=start_time, end_date=end_time, frequency='1m')
        silver_hisdata.index.name = "datetime"
        silver_hisdata.to_csv(silver_dir, mode='a', header=None)
        print("%s update sh future silver mins hisdata succussful" % datetime.now().strftime('%Y-%m-%d'))

    else:
        silver_hisdata = get_price('AG9999.XSGE', start_date=start_time, end_date=end_time, frequency='1m')
        silver_hisdata.index.name = "datetime"
        silver_hisdata.to_csv(silver_dir)
        print("get sh future silver hisdata mins successful!")




if __name__ == "__main__":
    #sh_future_gold_silver_K_data()
    sh_future_gold_silver_mins_data()
