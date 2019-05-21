# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import tushare as ts
from pytdx.hq import TdxHq_API
from jqdatasdk import *
import configparser
import pandas as pd
from pytdx.config.hosts import hq_hosts

'''
功能：屏蔽实时行情获取接口的选择
接入的行情接口有：通达信，tushare，jq
'''

config_path = 'conf.ini'
'''
[quote]
level = tdx,ts,jq

[ts]
token = 1xxxxxxxxxxxxxxxxxxxxx

[jq]
useid = 13811866763
pw = samxxxx
'''

# 定义stock实时行情返回的数据结构
stock_columns = ["market", "code",
                 "price", "last_close",
                 "open", "high", "low", "vol", "cur_vol", "amount", "s_vol", "b_vol",
                 "bid1", "ask1", "bid_vol1", "ask_vol1",
                 "bid2", "bid_vol2", "ask2", "ask_vol2",
                 "bid3", "bid_vol3", "ask3", "ask_vol3",
                 "bid4", "bid_vol4", "ask4", "ask_vol4",
                 "bid5", "bid_vol5", "ask5", "ask_vol5"]
df_stock_quote_ret = pd.DataFrame(columns=stock_columns)


# =====================================================
def load_config(file=None):
    '''
    配置文件读取，可根据配置按优先级选取实时行情源
    :param file: 配置文件目录，默认为当前目录，可通过指定file的方式指定config文件，文件名为config.json
    :return:
    '''
    conf = configparser.ConfigParser()
    if file is None:
        conf.read(config_path)
    else:
        conf.read(file)

    dic_conf = {
        "level": conf.get("quote", "level"),
        "ts": conf.get("ts", "token"),
        "jq_id": conf.get("jq", "useid"),
        "jq_pw": conf.get("jq", "pw")
    }

    return dic_conf

# =====================================


def get_stock_quote(code_list):
    '''
    获取股票实时行情
    :param code_list: 输入的code，list
    :return:
    '''
    if len(code_list) == 0:
        return pd.DataFrame()

    conf = load_config()
    level = conf["level"]
    level_list = level.split(',')

    for source in level_list:
        if source == "tdx":
            df_ret = get_quote_by_tdx(code_list)
            if not df_ret.empty:
                return df_ret
        if source == "ts":
            pass
        if source == "jq":
            pass

# ====================================


def get_quote_by_tdx(code_list):
    '''
    使用通达信接口获取股票实时行情
    :param code_list: 股票代码list
    :return: df
    '''
    tdx_code = stander_stock_code(code_list, 'tdx')
    if not tdx_code:
        return pd.DataFrame()

    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        data = api.to_df(api.get_security_quotes(tdx_code))
        data = data[stock_columns]
        return data
# ====================================


def get_quote_by_ts(code_list):
    '''
    使用tushare接口获取股票实时行情
    :param code_list:  股票代码list
    :return: df
    '''
    ts_code = stander_stock_code(code_list, "ts")
    if not ts_code:
        return pd.DataFrame()

    # tushare connect context
    conf = load_config()
    token = conf["ts"]

    ts.set_token(token)
    # ts 授权
    pro = ts.pro_api()

    """
    Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
    'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
    'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
    'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
    dtype='object')
    """
    data = ts.get_realtime_quotes(ts_code)



# =====================================


def stander_stock_code(code_list, quote_mark):
    '''
    标准化code，输入的code可能是多种形式，标准化成对应行情接口能接受的格式
    :param code_list: 输入的code list
    :param quote_mark: 市场行情源，tdx：通达信，ts：tushare，jq：joinquant
    :return:tdx: list,格式：[(0,"code"),(1,"code")] 0-深圳,1-上海,代表不同的市场，其他为标准的list
    '''
    if len(code_list) == 0:
        return []

    # 当前接受的code格式有：000001， 000001.SZ，000001.XSHG
    format = code_list[0].split('.')[0]
    ret_code_list = []
    if quote_mark == "tdx":
        for code in code_list:
            if code[0] == "6":
                ret_code_list.append((1, code.split('.')[0]))
            else:
                ret_code_list.append((0, code.split('.')[0]))
        return ret_code_list
    elif quote_mark == "ts":
        # 老的tushare获取实时股票行情的code是纯6位数字，现在新的获取其他信息的接口加上了市场代码如000001.SZ
        return [str(x).split('.')[0] for x in code_list]
    elif quote_mark == "jq":
        for code in code_list:
            if code[0] == '6':
                ret_code_list.append(code.split('.')[0] + '.XSHE')
            else:
                ret_code_list.append(code.split('.')[0] + '.XSHG')
        return ret_code_list
    else:
        print("市场代码未知，Mark：%s" % quote_mark)
        return []





if __name__ == "__main__":
    print(load_config())
    get_stock_quote(["0000"])
    # print(stander_stock_code(['6001318.XSHE', '000001.XSHG'], 'ts'))
    print(get_quote_by_tdx(['601318.XSHE', '000001.XSHG']))