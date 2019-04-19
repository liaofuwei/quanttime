# -*-coding:utf-8 -*-
__author__ = 'Administrator'


from jqdatasdk import *
import pandas as pd
import logging
from opendatatools import swindex
import os
import sys
from datetime import timedelta, date, datetime
import tushare as ts

from dateutil.parser import parse
import pymongo

sys.path.append('C:\\quanttime\\src\\mydefinelib\\')
import mydefinelib as mylib

pd.set_option('precision', 3)
auth('13811866763', "sam155")  # jqdata 授权

token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
# ts.set_token(token)
pro = ts.pro_api(token)  # ts 授权

mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')

'''
1、各种指数历史数据的维护，目前包括申万指数，以及joinquant中含括的600多个指数
2、存储目录：申万--C:\quanttime\data\index\sw  聚宽--C:\quanttime\data\index\jq tushare--C:\quanttime\data\index\\tushare
3、命名按照指数的code命名
4、程序能够自动识别最后更新的日期，按照最后的日期自动更新,更新的日期为yesterday
5、申万指数采用的是opendatatools库
6、20181207 增加指数的估值数据，数据存储于C:\quanttime\data\index\index_valuation，按照指数命名如：399975.XSHE。csv
7、20181214 运行正常，增加日志功能，日志存放在C:\\quanttime\\log\\index_maintenance.log
update_jq_index
update_sw_index
get_tushare_index
maintenance_index_valuation
get_index_weights_from_jq
'''


# 配置log
log_format = "%(asctime)s - %(levelname)s - %(message)s"
date_format = "%Y-%m-%d %H:%M:%S %p"
log_file_name = "C:\\quanttime\\log\\index_maintenance.log"

logging.basicConfig(
    filename=log_file_name,
    level=logging.DEBUG,
    format=log_format,
    datefmt=date_format)


def update_jq_index():
    '''
    本函数功能：通过joinquant更新聚宽包括的600多个指数，如果是新添加指数，则添加，
    ！！！！该数据其实是k线数据，不包含估值！！！！
    存储目录：C:\quanttime\data\index\jq
    文件名：code.csv的格式
    :return:
    '''
    auth('13811866763', "sam155")  # jqdata 授权
    index_dir = "C:\\quanttime\\data\\index\\jq\\"
    # 获取所有的指数标的
    index_security = get_all_securities(types=['index'], date=None)
    '''
                 display_name	name	start_date	    end_date	    type
    000001.XSHG	 上证指数	        SZZS	1991-07-15	    2200-01-01	    index
    000002.XSHG	 A股指数	        AGZS	1992-02-21	    2200-01-01	     index
    '''

    today = datetime.today().date()
    oneday = timedelta(days=1)
    yesterday = today - oneday
    end_time = yesterday.strftime("%Y-%m-%d")
    for index_name in index_security.index:
        start_time = index_security.loc[index_name]["start_date"].date()
        update_file_name = index_dir + str(index_name) + ".csv"
        if os.path.exists(update_file_name):
            data = pd.read_csv(update_file_name, index_col=["date"])
            try:
                d1 = datetime.strptime(
                    data.index[len(data.index) - 1], '%Y-%m-%d')
                d2 = datetime.strptime(data.index[0], '%Y-%m-%d')
            except BaseException:
                d1 = datetime.strptime(
                    data.index[len(data.index) - 1], '%Y/%m/%d')
                d2 = datetime.strptime(data.index[0], '%Y/%m/%d')
            delta = d1 - d2
            delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
            # 判断存储在csv中的日期是顺序还是逆序
            if delta.days > 0:
                d1 = d1 + delta2
                start_time = d1.strftime('%Y-%m-%d')
            else:
                d2 = d2 + delta2
                start_time = d2.strftime('%Y-%m-%d')
            tmp = get_price(
                index_name,
                start_date=start_time,
                end_date=end_time,
                frequency='daily',
                fields=None)
            tmp.to_csv(update_file_name, mode='a', header=None)
            print("update index:%r successful" % str(index_name))
        else:
            tmp = get_price(
                index_name,
                start_date=start_time,
                end_date=end_time,
                frequency='daily',
                fields=None)
            tmp.index.name = "date"
            tmp.to_csv(update_file_name)
            print("first time get index:%r successful" % str(index_name))
# =====================================================================================================================


def update_sw_index():
    '''
    功能：更新申万指数，使用opendatatools库
    申万指数可以查表
    目标文件夹：C:\quanttime\data\index\sw
    文件名：code。csv
    :return:
    '''
    index_dir = "C:\\quanttime\\data\\index\\sw\\"
    sw_index_code_dir = index_dir + "sw_index_code_info.csv"
    sw_index_info = pd.read_csv(
        sw_index_code_dir,
        index_col=["code"],
        encoding="gbk")
    start_time = "2004-01-01"  # 默认起始时间
    today = datetime.today().date()
    one_day = datetime.timedelta(days=1)
    yesterday = today - one_day
    end_time = yesterday.strftime("%Y-%m-%d")
    update_failed_code_list = []
    for sw_index in sw_index_info.index:
        print("begin update code:%r" % str(sw_index))
        update_file = index_dir + str(sw_index) + ".csv"
        if os.path.exists(update_file):
            data = pd.read_csv(update_file, index_col=["date"], encoding="gbk")
            d1 = datetime.strptime(data.index[-1], '%Y-%m-%d')
            d2 = datetime.strptime(data.index[0], '%Y-%m-%d')
            delta = d1 - d2
            delta2 = timedelta(days=1)  # 读取的最后日期往后推一天，作为更新的起始日期
            # 判断存储在csv中的日期是顺序还是逆序
            if delta.days > 0:
                d1 = d1 + delta2
                start_time = d1.strftime('%Y-%m-%d')
            else:
                d2 = d2 + delta2
                start_time = d2.strftime('%Y-%m-%d')
            df, msg = swindex.get_index_dailyindicator(
                sw_index, start_time, end_time, 'D')
            if df.empty:
                print(
                    "update sw code:%r ,but return empty pd!!!!" %
                    str(sw_index))
                update_failed_code_list.append(str(sw_index))
                continue
            else:
                df = df.set_index("date")
                df = df.sort_index(ascending=True)
                df.to_csv(update_file, mode='a', header=None, encoding="gbk")
                print("update sw code:%r successful!" % str(sw_index))
        else:
            print("first get sw code:%r " % str(sw_index))
            df, msg = swindex.get_index_dailyindicator(
                sw_index, start_time, end_time, 'D')
            if df.empty:
                print(
                    "first get sw code:%r ,but return empty pd!!!!" %
                    str(sw_index))
                continue
            else:
                df = df.set_index("date")
                df = df.sort_index(ascending=True)
                df.to_csv(update_file, encoding="gbk")
                print("first get sw code:%r successful!" % str(sw_index))

    print("update sw index end!")
    print("this time update failed code:%r" % update_failed_code_list)
# ==========================================================================


def get_tushare_index_basic_info():
    '''
    获取tushare的指数基本信息
    市场包括：MSCI，中证指数，上交所指数，深交所指数，中金所，申万，其他等
    :return:
    '''
    index_db = mongo_client["index_db"]
    table_list = index_db.list_collection_names()
    #market_list = ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"]
    market_list = ["CSI", "SSE", "SZSE", "SW"]
    basic_index_info_path = "C:\\quanttime\\data\\index\\basic_index_info\\"
    for market in market_list:
        get_index_info = pro.index_basic(market=market)
        if get_index_info.empty:
            print("获取index market:%s 为空" % market)
            continue
        market = market.lower()
        file_name = basic_index_info_path + market + ".csv"
        index_table = index_db[market]
        if os.path.exists(file_name):
            df_index_info = pd.read_csv(
                file_name, index_col=["ts_code"], encoding="gbk")
            diff = set(get_index_info["ts_code"].values) - set(df_index_info.index)
            if diff == 0:
                print("%s 新获取的index信息与已存在的index信息一致，本次%s 不更新" %
                      (datetime.today().date().strftime("%Y-%m-%d"),
                       market))
                continue
            else:
                print("本次新增的code：%r" % diff)
                if market in table_list:
                    index_table.drop()
                    index_table.insert_many(get_index_info.to_dict(orient="record"))
                else:
                    index_table.insert_many(get_index_info.to_dict(orient="record"))
                get_index_info = get_index_info.set_index("ts_code")
                get_index_info.to_csv(file_name, encoding="gbk")
                print("完成更新%s" % market)
        else:
            if market in table_list:
                index_table.drop()
                index_table.insert_many(get_index_info.to_dict(orient="record"))
            else:
                index_table.insert_many(get_index_info.to_dict(orient="record"))
            get_index_info = get_index_info.set_index("ts_code")
            get_index_info.to_csv(file_name, encoding="gbk")
            print("完成更新%s" % market)






# =============================================

def get_tushare_index():
    '''
    功能：获取tushare的大盘指数信息，目前包括上证指数，深证指数，上证50，中证500，中小板指，创业板的每日指标数据

    :return:
    '''
    ts_code_list = [
        "000001.SH",
        "000300.SH",
        "000905.SH",
        "399001.SZ",
        "399005.SZ",
        "399006.SZ",
        "399016.SZ",
        "399300.SZ"]
    file_basic_path = "C:\\quanttime\\data\\index\\tushare\\"
    for index in ts_code_list:
        file_path = file_basic_path + index + '.csv'
        if os.path.exists(file_path):
            print("%r begin to update %r, 是增量更新" %
                  (datetime.today().date().strftime("%Y-%m-%d"), index))
            logging.debug("%r begin to update %r, 是增量更新" %
                          (datetime.today().date().strftime("%Y-%m-%d"), index))
            df_index = pd.read_csv(file_path, index_col=["trade_date"])
            yesterday = datetime.today().date() - timedelta(days=1)
            end_date = get_close_trade_date(yesterday.strftime("%Y-%m-%d"), -1)

            if '/' in df_index.index[len(df_index.index) - 1]:
                file_end_date = datetime.strptime(
                    df_index.index[len(df_index.index) - 1], "%Y/%m/%d")
            if '-' in df_index.index[len(df_index.index) - 1]:
                file_end_date = datetime.strptime(
                    df_index.index[len(df_index.index) - 1], "%Y-%m-%d")
            if file_end_date.date() >= yesterday:
                logging.debug(
                    "index code:%r valuation already update new,this time need't update" %
                    index)
                print(
                    "index code:%r valuation already update new,this time need't update" %
                    index)
                continue

            start = get_close_trade_date(
                file_end_date.date().strftime("%Y-%m-%d"), 1)
            tmp_start = start.split('-')
            if len(tmp_start) != 3:
                print("get_tushare_index start date(%r) format error " % start)
                logging.debug(
                    "get_tushare_index start date(%r) format error " %
                    start)
                continue
            start = tmp_start[0] + tmp_start[1] + tmp_start[2]

            tmp_end = end_date.split('-')
            if len(tmp_end) != 3:
                print(
                    "get_tushare_index end_date date(%r) format error " %
                    end_date)
                logging.debug(
                    "get_tushare_index end_date date(%r) format error " %
                    end_date)
                continue
            end_date = tmp_end[0] + tmp_end[1] + tmp_end[2]

            df_index = pro.index_dailybasic(
                ts_code=index, start_date=start, end_date=end_date)
            df_index["trade_date"] = df_index["trade_date"].map(
                lambda x: parse(x).date().strftime('%Y-%m-%d'))
            df_index = df_index.sort_index(ascending=False)
            df_index = df_index.set_index("trade_date")
            df_index.to_csv(file_path, mode='a', header=None)
            print("%r update end" % index)
            logging.debug("%r update end" % index)
        else:
            print("%r begin to update %r, create file" %
                  (datetime.today().date().strftime("%Y-%m-%d"), index))
            logging.debug(
                "%r begin to update %r, create file" %
                (datetime.today().date().strftime("%Y-%m-%d"), index))
            df_index1 = pro.index_dailybasic(
                ts_code=index, start_date="20040104", end_date="20141231")
            df_index2 = pro.index_dailybasic(
                ts_code=index, start_date="20150104", end_date="20181231")
            # tushare 按照日期降序排列，即最新的日期在最前面
            df_index = pd.concat([df_index2, df_index1])
            df_index["trade_date"] = df_index["trade_date"].map(
                lambda x: parse(x).date().strftime('%Y-%m-%d'))
            df_index = df_index.sort_index(ascending=False)
            df_index = df_index.set_index("trade_date")
            df_index.to_csv(file_path)
            print("%r create and get data end" % index)
            logging.debug("%r create and get data end" % index)


# =====================================================================================================================
def tushare_index_PEPB_info():
    '''
    功能：tushare只提供了目前只提供了大盘指数，该指数由本程序中get_tushare_index函数维护，本函数主要是将tushare的格式转换后按照date，pe，pb
    的格式存储
    tushare数据存放地址：C:\\quanttime\\data\\index\\tushare\\
    目标存储地址：C:\quanttime\data\index\index_valuation\\
    每次完成get_tushare_index，即调用该函数

    :return:无
    '''
    ts_code_list = [
        "000001.SH",
        "000300.SH",
        "000905.SH",
        "399001.SZ",
        "399005.SZ",
        "399006.SZ",
        "399016.SZ",
        "399300.SZ"]
    file_basic_path = "C:\\quanttime\\data\\index\\tushare\\"
    dest_basic_path = "C:\\quanttime\\data\\index\\index_valuation\\"
    for ts_code in ts_code_list:
        file_path = file_basic_path + ts_code + ".csv"
        ts_df = pd.read_csv(
            file_path,
            index_col=['trade_date'],
            usecols=[
                "trade_date",
                "pe",
                "pb"])
        ts_df.index.name = "date"
        ts_df = ts_df.rename(columns={"pe": "PE", "pb": "PB"})
        if "SH" in ts_code:
            ts_code = ts_code[0:7] + "XSHG"
        if "SZ" in ts_code:
            ts_code = ts_code[0:7] + "XSHE"
        dest_file_path = dest_basic_path + ts_code + ".csv"
        ts_df.to_csv(dest_file_path)
        print("提取index:%r pepb end" % ts_code)


# =====================================================================================================================
# 所有指数信息，可通过get_all_securities(types=['index'], date=None)实时获取，同时该信息也存在本地，路径如下
'''
                display_name	name	  start_date	end_date	type
000001.XSHG	    上证指数	        SZZS	  1991-07-15	2200-01-01	index
000002.XSHG	    A股指数	        AGZS	  1992-02-21	2200-01-01	index
'''


def maintenance_index_valuation():
    '''
    维护所有指数的估值表
    所有指数信息，存放于C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv
    index_all_valuation_info.csv可增加需要计算的指数
    '''
    index_all_info_dir = "C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv"
    index_save_dir = "C:\\quanttime\\data\\index\\index_valuation\\"
    index_all_info = pd.read_csv(
        index_all_info_dir,
        index_col=["code"],
        encoding="gbk")

    yesterday = datetime.today().date() - timedelta(days=1)
    end_date = get_close_trade_date(yesterday.strftime("%Y-%m-%d"), -1)

    for code in index_all_info.index:
        update_file = index_save_dir + str(code) + '.csv'
        if os.path.exists(update_file):
            pe_pb_data = pd.read_csv(update_file, index_col=["date"])
            if '/' in pe_pb_data.index[len(pe_pb_data.index) - 1]:
                file_end_date = datetime.strptime(
                    pe_pb_data.index[len(pe_pb_data.index) - 1], "%Y/%m/%d")
            if '-' in pe_pb_data.index[len(pe_pb_data.index) - 1]:
                file_end_date = datetime.strptime(
                    pe_pb_data.index[len(pe_pb_data.index) - 1], "%Y-%m-%d")
            if file_end_date.date() >= yesterday:
                logging.debug(
                    "index code:%r valuation already update new,this time need't update" %
                    code)
                print(
                    "index code:%r valuation already update new,this time need't update" %
                    code)
                continue

            start = get_close_trade_date(
                file_end_date.date().strftime("%Y-%m-%d"), 1)
            get_index_pe_pb(code, start, end_date)
            logging.debug("index code:%r process end" % code)
        else:
            start = index_all_info.loc[code, ["start_date"]].start_date
            get_index_pe_pb(code, start, end_date)
            logging.debug("index code:%r process end" % code)

# =====================================================================================================================


def get_index_pe_pb(code, start_date=None, end_date=None):
    '''
    计算指定指数的历史PE_PB
    code：输入指数代码 str
    start_date: str
    end_date:str
    存入C:\\quanttime\\data\\index\\index_valuation\\文件夹内
    '''
    index_save_dir = "C:\\quanttime\\data\\index\\index_valuation\\"
    index_info = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv",
        index_col=["code"],
        encoding="gbk")
    print("start_date:%r" % start_date)

    if start_date is None:
        start_date = index_info.loc[code, ["start_date"]].start_date
        if '/' in start_date:
            start_date = datetime.strptime(start_date, "%Y/%m/%d").date()
        elif '-' in start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            print("start_date:%r" % start_date)

        if start_date < datetime.date(2006, 1, 4):  # 只计算2006年以来的数据
            start_date = "2006-01-04"
        else:
            start_date = start_date.strftime("Y-%m-%d")

    if end_date is None:
        end_date = datetime.today().date() - timedelta(days=1)
        end_date = end_date.strftime("%Y-%m-%d")

    if '/' in start_date:
        if datetime.strptime(start_date,
                             "%Y/%m/%d").date() < datetime.strptime("2006-01-04",
                                                                    "%Y-%m-%d").date():
            start_date = "2006-01-04"
    if '-' in start_date:
        if datetime.strptime(start_date,
                             "%Y-%m-%d").date() < datetime.strptime("2006-01-04",
                                                                    "%Y-%m-%d").date():
            start_date = "2006-01-04"

    if not isinstance(end_date, str):
        print("get_index_pe_pb,para end_date(%r) is not str" % end_date)
        return

    dates = get_trade_list(start_date, end_date)
    if len(dates) == 0:
        print("index: %r,get_trade_list ==0 check! " % code)
        logging.warning("index: %r,get_trade_list ==0 check! " % code)

    pe_list = []
    pb_list = []
    for date in dates:
        pe_pb = calc_PE_PB_date(code, date)
        if len(pe_pb) != 2:
            logging.warning(
                "calc_PE_PB_date return error,code:%r,date:%r" %
                (code, date))
            print(
                "calc_PE_PB_date return error,code:%r,date:%r" %
                (code, date))
            continue
        pe_list.append(pe_pb[0])
        pb_list.append(pe_pb[1])
    df_pe_pb = pd.DataFrame({'PE': pd.Series(pe_list, index=dates), 'PB': pd.Series(
        pb_list, index=dates)}, columns=['PB', 'PE'])
    df_pe_pb.index.name = "date"
    save_file_dir = index_save_dir + str(code) + '.csv'
    if(os.path.exists(save_file_dir)):
        df_pe_pb.to_csv(save_file_dir, mode='a', header=None)
    else:
        df_pe_pb.to_csv(save_file_dir)
    print("save code:%r pe_pb valuation successful" % code)

# =====================================================================================================================


def calc_PE_PB_date(code, date):
    '''
    计算指定日期的指数PB PE，当前（2018-12-16）按照等权重计算，后续加入权重
    code：joinquant code stander
    date：日期
    return：list，按照PE，PB的顺序排列
    '''

    # 所有stock的pe，pb每日数据存放在valuation表中，按照code文件名的方式存放
    data_dir = "C:\\quanttime\\data\\finance\\valuation\\"
    stocks = get_index_stocks(code, date)  # 指定日期指数各成分股，返回一个list
    # print("code:%r,stocks:%r"%(code,stocks))
    if len(stocks) == 0:
        logging.warning("index code: % in date:%r 获取成分股返回空" % (code, date))
    pe_list = []
    pb_list = []
    pe_counter = 0
    pb_counter = 0
    for stock in stocks:
        stock_path = data_dir + str(stock) + ".csv"
        try:
            valuation_data = pd.read_csv(
                stock_path, index_col=['day'], usecols=[
                    "day", "pe_ratio", "pb_ratio"])
        except BaseException:
            logging.debug("stock code:%r --file does not exist" % stock)
            continue
        valuation_data = valuation_data[~valuation_data.reset_index(
        ).duplicated().values]
        try:
            pe_value = valuation_data.loc[date, ['pe_ratio']].pe_ratio
            pb_value = valuation_data.loc[date, ['pb_ratio']].pb_ratio
            # 剔除了pb，pe的负值
            if pb_value <= 0:
                logging.debug(
                    "%r code: %r, pb<=0:( %r) " %
                    (date, stock, pb_value))
                #print("%r code: %r, pb<=0:( %r) " % (date, stock, pb_value))
                pb_counter = pb_counter + 1
                continue
            pb_list.append(pb_value)
            if pe_value <= 0:
                logging.debug(
                    "%r code: %r, pe<=0:( %r) " %
                    (date, stock, pe_value))
                #print("%r code: %r, pe<=0:( %r) " % (date, stock, pe_value))
                pe_counter = pe_counter + 1
                continue
            pe_list.append(pe_value)

        except BaseException:
            logging.warning("code: %r, in %r 缺少对应的pe，pb数据" % (stock, date))
            #print("%r code: %r, in %r 缺少对应的pe，pb数据"%(date, stock, date))
            # return [float('NaN'), float('NaN')]
    if len(pe_list) > 0:
        pe_mean = round(sum(pe_list) / len(pe_list), 2)
    else:
        pe_mean = float('NaN')

    if len(pb_list) > 0:
        pb_mean = round(sum(pb_list) / len(pb_list), 2)
    else:
        pb_mean = float('NaN')

    if pb_counter > int(len(pb_list) / 2):
        print("index: %r in %r, PB数据缺失/剔除大于计算总数的一半" % (code, date))

    if pe_counter > int(len(pe_list) / 2):
        print("index: %r in %r, PE数据缺失/剔除大于计算总数的一半" % (code, date))

    return [pe_mean, pb_mean]

# =====================================================================================================================


def get_index_stock_and_weight():
    '''
    获取指数的成分和权重
    '''
    dic_stocks = {}
# =====================================================================================================================


def calc_index_valuation(index_list):
    '''
    功能：计算指数的valuation，目前只包括PB，PE
    para: index_list：指数,如果输入为[]，则读取index_all_valuation_info.csv文件中获取
    计算的时间段为index_all_valuation_info.csv文件中的起始时间（如果早于2010-01-03则从2010-01-03开始计算）到2018-12-31

    '''

    index_all_info_dir = "C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv"
    index_all_info = pd.read_csv(
        index_all_info_dir,
        index_col=["code"],
        encoding="gbk")
    stocks_all_info = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\all_stock_info.csv",
        index_col=["code"],
        encoding="gbk")
    data_dir = "C:\\quanttime\\data\\finance\\valuation\\"

    process_index_list = []
    if len(index_list) != 0:
        process_index_list = index_list
    else:
        process_index_list = index_all_info.index

    for index_code in process_index_list:
        start_date = index_all_info.loc[index_code, ["start_date"]].start_date
        index_name = index_all_info.loc[index_code, [
            "display_name"]].display_name
        print("start_date:%r" % start_date)
        if '/' in start_date:
            start_date = datetime.strptime(start_date, "%Y/%m/%d").date()
        elif '-' in start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            print("start_date:%r 格式不符合" % start_date)

        if start_date < date(2010, 1, 4):  # 只计算2010年以来的数据,因为成分股获取的数据可能早了没数据
            start_date = date(2010, 1, 4)

        date_range = pd.date_range(start_date, "2018-12-31", freq="BM")
        print("date_range:%r" % date_range[0].year)

        for comm_date in date_range:
            month_trade_date = mylib.get_trade_date_by_year_month(
                comm_date.year, comm_date.month)
            stocks = []
            if len(month_trade_date) != 0:
                stocks = get_index_stocks(
                    index_code, month_trade_date[-1])  # 指定日期指数各成分股，返回一个list
                print("%r-%r index:%r 成分股：%r" %
                      (comm_date.year, comm_date.month, index_code, stocks))
            else:
                print(
                    "%r-%r,get_trade_date_by_year_month return empty" %
                    (comm_date.year, comm_date.month))

        # 将成分股的pe，pb信息线组合到一个dataframe中
        # =================================pb====================================
            dic_miss_stock = {}
            df_valuation = pd.read_csv(
                "C:\\quanttime\\data\\finance\\valuation\\000000.XSHG.csv",
                index_col=['day'],
                usecols=[
                    "day",
                    "pb_ratio"])
            for stock in stocks:
                valuation_file = data_dir + str(stock) + '.csv'
                if os.path.exists(valuation_file):
                    df_tmp = pd.read_csv(
                        valuation_file, index_col=['day'], usecols=[
                            "day", "pb_ratio"])
                    df_tmp = df_tmp[~df_tmp.reset_index(
                    ).duplicated().values]  # 去重
                    if not df_tmp.empty:
                        df_valuation = pd.merge(
                            df_valuation, df_tmp, how="outer", left_index=True, right_index=True)
                else:
                    dic_miss_stock[stock] = stocks_all_info.loc[stock, [
                        "display_name"]].display_name
            print(
                "index:%r, %r-%r,miss stock:%r" %
                (index_code,
                 comm_date.year,
                 comm_date.month,
                 dic_miss_stock))

            pb_mean_list = []
            for trade_date in month_trade_date:
                try:
                    pb_value = df_valuation.loc[trade_date]
                except BaseException:
                    continue
                record_counter = len(pb_value)
                pb_list = pb_value.dropna().tolist()
                if len(pb_list) < (record_counter - 1) / 2:
                    print("%r calc pb value ,the nums of data miss half")

                if len(pb_list) > 0:
                    pb_mean = round(sum(pb_list) / len(pb_list), 2)
                    pb_mean_list.append(pb_mean)
                else:
                    pb_mean = float('NaN')
                    pb_mean_list.append(pb_mean)

        # =================================pb====================================

        # =================================pe====================================
            df_valuation = pd.read_csv(
                "C:\\quanttime\\data\\finance\\valuation\\000000.XSHG.csv",
                index_col=['day'],
                usecols=[
                    "day",
                    "pe_ratio"])
            for stock in stocks:
                valuation_file = data_dir + str(stock) + '.csv'
                if os.path.exists(valuation_file):
                    df_tmp = pd.read_csv(
                        valuation_file, index_col=['day'], usecols=[
                            "day", "pe_ratio"])
                    df_tmp = df_tmp[~df_tmp.reset_index(
                    ).duplicated().values]  # 去重
                    if not df_tmp.empty:
                        df_valuation = pd.merge(
                            df_valuation, df_tmp, how="outer", left_index=True, right_index=True)

            pe_mean_list = []
            for trade_date in month_trade_date:
                try:
                    pe_value = df_valuation.loc[trade_date]
                except BaseException:
                    continue
                record_counter = len(pe_value)
                pe_list = pe_value.dropna().tolist()
                if len(pe_list) < (record_counter - 1) / 2:
                    print("%r calc pe value ,the nums of data miss half")

                for tmp in pe_list:
                    if tmp < 0:
                        pe_list.remove(tmp)

                if len(pe_list) > 0:
                    pe_mean = round(sum(pe_list) / len(pe_list), 2)
                    pe_mean_list.append(pe_mean)
                else:
                    pe_mean = float('NaN')
                    pe_mean_list.append(pe_mean)
        # =================================pe====================================

            df_pe_pb = pd.DataFrame(
                {
                    'PE': pd.Series(
                        pe_mean_list,
                        index=month_trade_date),
                    'PB': pd.Series(
                        pb_mean_list,
                        index=month_trade_date)},
                columns=[
                    'PB',
                    'PE'])
            df_pe_pb.index.name = "date"

            save_file_dir = "C:\\quanttime\\data\\index\\index_valuation\\init\\" + \
                str(index_code) + '.csv'
            if (os.path.exists(save_file_dir)):
                df_pe_pb.to_csv(save_file_dir, mode='a', header=None)
            else:
                df_pe_pb.to_csv(save_file_dir)
        print("save code:%r pe_pb valuation successful" % index_code)


# =====================================================================================================================

def update_index_valuation(update_code_list):
    '''
    功能：按日更新index valuation
    para: list, update_code_list 需要更新的list，joinquant格式的code
    :return:
    '''
    index_all_info_dir = "C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv"
    index_all_info = pd.read_csv(
        index_all_info_dir,
        index_col=["code"],
        encoding="gbk")
    stocks_all_info = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\all_stock_info.csv",
        index_col=["code"],
        encoding="gbk")
    data_dir = "C:\\quanttime\\data\\finance\\valuation\\"
    trade_all_date = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
        index_col=["trade_date"])

    process_index_list = []
    if len(update_code_list) != 0:
        process_index_list = update_code_list
    else:
        process_index_list = index_all_info.index

    yesterday = datetime.today().date() - timedelta(days=1)
    yesterday = mylib.get_close_trade_date(yesterday.strftime("%Y-%m-%d"), -1)
    for index_code in process_index_list:
        index_valuation_file = "C:\\quanttime\\data\\index\\index_valuation\\init\\" + \
            str(index_code) + ".csv"
        if os.path.exists(index_valuation_file):
            df_index_valuation = pd.read_csv(
                index_valuation_file, index_col=["date"])
            end_record_date = df_index_valuation.index[len(
                df_index_valuation.index) - 1]
            print("当前文件最后记录日期：%r" % end_record_date)
            end_record_date = mylib.get_close_trade_date(end_record_date, 1)
            trade_date_list = mylib.get_trade_list(end_record_date, yesterday)
            print("本次更新的date：%r" % trade_date_list)

            pb_mean_list = []
            pe_mean_list = []
            for trade_date in trade_date_list:
                stocks = get_index_stocks(index_code, trade_date)
                if len(stocks) == 0:
                    print(
                        "%r index: %r ,get_index_stocks return empty" %
                        (trade_date, index_code))
                    continue
                pe_list = []
                pb_list = []
                pe_counter = 0
                pb_counter = 0
                for stock_code in stocks:
                    stock_path = data_dir + str(stock_code) + ".csv"
                    try:
                        valuation_data = pd.read_csv(
                            stock_path, index_col=['day'], usecols=[
                                "day", "pe_ratio", "pb_ratio"])
                    except BaseException:
                        logging.debug(
                            "stock code:%r --valuation file does not exist" %
                            stock_code)
                        continue
                    valuation_data = valuation_data[~valuation_data.reset_index(
                    ).duplicated().values]
                    try:
                        pe_value = valuation_data.loc[trade_date, [
                            'pe_ratio']].pe_ratio
                        pb_value = valuation_data.loc[trade_date, [
                            'pb_ratio']].pb_ratio
                        # 剔除了pb，pe的负值
                        if pb_value <= 0:
                            logging.debug(
                                "%r code: %r, pb<=0:( %r) " %
                                (trade_date, stock_code, pb_value))
                            # print("%r code: %r, pb<=0:( %r) " % (trade_date, stock, pb_value))
                            pb_counter = pb_counter + 1
                            continue
                        pb_list.append(pb_value)
                        if pe_value <= 0:
                            logging.debug(
                                "%r code: %r, pe<=0:( %r) " %
                                (trade_date, stock_code, pe_value))
                            # print("%r code: %r, pe<=0:( %r) " % (date, stock, pe_value))
                            pe_counter = pe_counter + 1
                            continue
                        pe_list.append(pe_value)

                    except BaseException:
                        logging.warning(
                            "code: %r, in %r 缺少对应的pe，pb数据" %
                            (stock_code, trade_date))
                        # print("%r code: %r, in %r 缺少对应的pe，pb数据"%(date, stock, trade_date))
                        # return [float('NaN'), float('NaN')]
                if len(pe_list) > 0:
                    pe_mean = round(sum(pe_list) / len(pe_list), 2)
                    pe_mean_list.append(pe_mean)
                else:
                    pe_mean = float('NaN')
                    pe_mean_list.append(pe_mean)

                if len(pb_list) > 0:
                    pb_mean = round(sum(pb_list) / len(pb_list), 2)
                    pb_mean_list.append(pb_mean)
                else:
                    pb_mean = float('NaN')
                    pb_mean_list.append(pb_mean)

                if pb_counter > int(len(pb_list) / 2):
                    print(
                        "index: %r in %r, PB数据缺失/剔除大于计算总数的一半" %
                        (stock_code, trade_date))

                if pe_counter > int(len(pe_list) / 2):
                    print(
                        "index: %r in %r, PE数据缺失/剔除大于计算总数的一半" %
                        (stock_code, trade_date))

            df_pe_pb = pd.DataFrame(
                {
                    'PE': pd.Series(
                        pe_mean_list,
                        index=trade_date_list),
                    'PB': pd.Series(
                        pb_mean_list,
                        index=trade_date_list)},
                columns=[
                    'PB',
                    'PE'])
            df_pe_pb.index.name = "date"
            df_pe_pb.to_csv(index_valuation_file, mode='a', header=None)
            print("update code:%r pe_pb valuation successful" % index_code)
        else:
            print("该指数：%r没有计算过，先使用calc_index_valuation计算" % index_code)


# =====================================================================================================================

def get_index_weights_from_jq():
    '''
    功能：从joinquant获取权重数据
    存放目录：C:\quanttime\data\index\weights，文件名用指数code+csv
    jq interface：get_index_weights(index_id, date=None) 月度数据
    :return:无
    '''
    index_all_info_dir = "C:\\quanttime\\data\\basic_info\\index_all_valuation_info.csv"
    index_all_info = pd.read_csv(
        index_all_info_dir,
        index_col=["code"],
        encoding="gbk")
    weight_save_dir = "C:\\quanttime\\data\\index\\weights\\"
    for index_code in index_all_info.index:
        update_weight_file = weight_save_dir + str(index_code) + ".csv"
        start_date = ""
        if os.path.exists(update_weight_file):
            update_index = pd.read_csv(
                update_weight_file, index_col=[
                    "date", "code"], encoding="gbk")
            if len(update_index.index.levels[0]) != 0:
                start_date = get_max_date(update_index.index.levels[0])
                print("1start_date:%r" % start_date)
            else:
                start_date = index_all_info.loc[index_code, [
                    "start_date"]].start_date

            if '-' in start_date:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            elif '/' in start_date:
                start_date = datetime.strptime(start_date, "%Y/%m/%d")
            if start_date == "":
                continue
            today = datetime.today().date()
            dt_start = "%r-%r" % (start_date.year, start_date.month)
            dt_end = "%r-%r" % (today.year, today.month - 1)
            # print("========================")
            #print("dt_start:%r" % dt_start)
            #print("2start_date:%r" % start_date)
            #print("dt_end:%r" % dt_end)
            # print("========================")
            if (start_date.year == today.year) and (
                    start_date.month == (today.month - 1)):
                print(" code: %r is newest data,need't update!!" % index_code)
                continue

            dates = pd.date_range(dt_start, dt_end, freq="BM")

            df_weights = pd.DataFrame(
                columns=[
                    "display_name",
                    "date",
                    "weight"],
                index=['code'])

            for date in dates:
                df_weights_tmp = get_index_weights(index_code, date)
                if df_weights_tmp.empty:
                    continue
                df_weights = pd.concat([df_weights, df_weights_tmp])
            df_weights = df_weights.dropna()
            df_weights.to_csv(
                update_weight_file,
                mode='a',
                header=None,
                encoding="gbk")
            print("update index:%r weights end" % index_code)
            logging.debug("update index:%r weights end" % index_code)
        else:
            start_date = index_all_info.loc[index_code, [
                "start_date"]].start_date
            if '-' in start_date:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            if '/' in start_date:
                start_date = datetime.strptime(start_date, "%Y/%m/%d")
            today = datetime.today().date()
            dt_start = "%r-%r" % (start_date.year, start_date.month)
            dt_end = "%r-%r" % (today.year, today.month)
            dates = pd.date_range(dt_start, dt_end, freq="BM")
            df_weights = pd.DataFrame(
                columns=[
                    "display_name",
                    "date",
                    "weight"],
                index=['code'])

            for date in dates:
                df_weights_tmp = get_index_weights(index_code, date)
                if df_weights_tmp.empty:
                    continue
                df_weights = pd.concat([df_weights, df_weights_tmp])
            df_weights = df_weights.dropna()
            df_weights.to_csv(update_weight_file, encoding="gbk")
            print("create and update index:%r weights end" % index_code)
            logging.debug(
                "create and update index:%r weights end" %
                index_code)
# ======================================================================================================


def get_max_date(datelist):
    '''
    功能：从一个str的list中获取最大的日期，形如：['2015/10/30', '2015/11/30', '2015/12/31', '2015/5/29', '2015/6/30']
    :param datelist:list
    :return:str 最大的日期
    '''
    if len(datelist) == 0:
        return ""
    dt1 = []
    for i in datelist:
        if '/' in i:
            dt1.append(datetime.strptime(i, "%Y/%m/%d"))
        if '-' in i:
            dt1.append(datetime.strptime(i, "%Y-%m-%d"))

    tmp = dt1.pop(0)
    for j in dt1:
        delta = j - tmp
        if delta.days > 0:
            tmp = j
    return tmp.strftime("%Y-%m-%d")
# ======================================================================================================


def get_close_trade_date(strDate, direct):
    '''
        功能：获取节假日最近的一个交易日，比如2018-10-1日，的前一个交易日是9-28日
        这里是生成了一个已strDate为end的，往前推10个日期的时间序列
        :param strDate：str类型日期，如"2018-10-01"
               1:日期之后的交易时间，如2018-10-01日后的交易日期
               -1：日期之前的交易时间，如2018-10-01日前的交易日期
        :return:str 类型，如果没有找到则返回strDate的值
    '''
    trade_date = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
        index_col=["trade_date"])
    if direct == -1:
        dates = pd.date_range(end=strDate, periods=10)
        dates = list(reversed(dates.tolist()))
        for tmpDate in dates:
            try:
                return trade_date.index[trade_date.index.tolist().index(
                    tmpDate.date().strftime("%Y-%m-%d"))]
            except ValueError:
                continue
    elif direct == 1:
        dates = pd.date_range(start=strDate, periods=10)
        for tmpDate in dates.tolist():
            try:
                return trade_date.index[trade_date.index.tolist().index(
                    tmpDate.date().strftime("%Y-%m-%d"))]
            except ValueError:
                continue
    else:
        return strDate
    return strDate
# ======================================================================================================


def get_trade_list(startDate, endDate):
    '''
    功能：获取startDate, endDate交易日时间段，该时间查表于all_trade_day.csv
    :param startDate: str，该值需要在all_trade_day.csv内
    :param endDate: str 该值需要在all_trade_day.csv
    :return: list
    '''
    print("input startDate:%r,endDate: %r" % (startDate, endDate))
    if "/" in startDate:
        startDate = datetime.strptime(
            startDate, "%Y/%m/%d").strftime("%Y-%m-%d")
    if "/" in endDate:
        endDate = datetime.strptime(endDate, "%Y/%m/%d").strftime("%Y-%m-%d")

    trade_date = pd.read_csv(
        "C:\\quanttime\\data\\basic_info\\all_trade_day.csv",
        index_col=["trade_date"])
    try:
        start = trade_date.loc[startDate][0]
        print("start:%r" % start)
        end = trade_date.loc[endDate][0]
        print("end:%r" % end)
    except KeyError:
        print("传入的参数日期不在trade day中")
        return []
    trade_days = trade_date.iloc[start:end + 1, 0]
    return trade_days.index.tolist()


if __name__ == "__main__":
    get_tushare_index_basic_info()
    # update_sw_index()
    # maintenance_index_valuation()
    # get_index_weights_from_jq()
    # get_tushare_index()
    # tushare_index_PEPB_info()
    #tmp = get_close_trade_date("2019-01-03",1)
    #tmp = get_trade_list("2019-01-03","2019-12-27")
    # print(tmp)
    # calc_index_valuation(["399673.XSHE"])
    # update_index_valuation(["399673.XSHE"])
