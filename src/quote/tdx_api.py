# -*-coding:utf-8 -*-
__author__ = 'Administrator'


from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API

import pandas as pd
from pytdx.config.hosts import hq_hosts

'''
通达信行情接口
对pytdx接口进行2次开发
1、获取A股行情数据：get_a_quote_by_tdx(code_list) 包括股票，ETF，lof，国债，逆回购，转债
2、获取港股的主板行情数据：get_hk_quote_by_tdx(code_list)
'''


# 定义stock实时行情返回的数据结构,通达信标准结构节选
stock_tdx_columns = ["market", "code",
                     "price", "last_close",
                     "open", "high", "low", "vol", "cur_vol", "amount", "s_vol", "b_vol",
                     "bid1", "ask1", "bid_vol1", "ask_vol1",
                     "bid2", "bid_vol2", "ask2", "ask_vol2",
                     "bid3", "bid_vol3", "ask3", "ask_vol3",
                     "bid4", "bid_vol4", "ask4", "ask_vol4",
                     "bid5", "bid_vol5", "ask5", "ask_vol5"]
df_stock_quote_ret = pd.DataFrame(columns=stock_tdx_columns)

# ======================================


def get_a_quote_by_tdx(code_list):
    """
    获取A股市场行情，该接口只获取A股市场行情
    其他的行情使用拓展行情接口
    code_list：数字或者字符串格式的list，调用def standard_tdx_code(code_list)来格式化市场信息及code
    """

    if len(code_list) == 0:
        return pd.DataFrame(columns=stock_tdx_columns)
    # 一次最多获取50只股票的实时行情，如果code_list 多于50只，则50只股票取一次行情，然后进行拼接
    tdx_api = TdxHq_API()
    if len(code_list) >= 50:
        tmp = div_list(code_list, 50)
        df_tmp = pd.DataFrame(columns=stock_tdx_columns)
        if tdx_api.connect('119.147.212.81', 7709):
            for tmp_list in tmp:
                tmp_list = standard_tdx_code(tmp_list)
                tdx_data = tdx_api.to_df(tdx_api.get_security_quotes(tmp_list))
                df_tmp = pd.concat([df_tmp, tdx_data])
            df_tmp = df_tmp[stock_tdx_columns]
    else:
        tmp = standard_tdx_code(code_list)
        if tdx_api.connect('119.147.212.81', 7709):
            tdx_data = tdx_api.to_df(tdx_api.get_security_quotes(tmp))
            df_tmp = tdx_data[stock_tdx_columns]

    tdx_api.disconnect()
    return process_tdx_price(df_tmp)

# =========================================


def div_list(list_temp, n):
    """
    将输入的列表listTemp分成n等分
    :param list_temp:
    :param n:
    :return:
    用法：
    tmp = div_list(list_temp, n)
    for i in tmp:
        print(i)

    listTemp = [1,2,3,4,5,6,7,8,9]
    # func(listTemp, 3)

    # 返回的temp为评分后的每份可迭代对象
    temp = func(listTemp, 4)

    for i in temp:
        print(i)

    '''
    [1, 2, 3, 4]
    [5, 6, 7, 8]
    [9]
    '''

    """
    for i in range(0, len(list_temp), n):
        yield list_temp[i:i+n]


# ===================

def get_hk_quote_by_tdx(code_list):
    """
    获取香港主板市场行情数据
    需要使用拓展行情接口
    通达信接口的港股行情数据比较简陋，只有当前价格，缺乏bid，ask以及对应的量能
    :param code_list:
    :return:
    """
    api_ex = TdxExHq_API()
    result = []
    if api_ex.connect('140.143.179.226', 7727):
        for code in code_list:
            data = api_ex.get_instrument_quote(31, code)
            result.append([data[0]["code"], data[0]["price"], data[0]["xianliang"]])
        api_ex.disconnect()
    df = pd.DataFrame(data=result, columns=["code", "price", "xianliang"])
    return df


# ==============================================


def standard_tdx_code(code_list):
    """
    标准化通达信code，转换成带市场信息的符合通达信quote api的元组
    深圳市场：0   普通股票：0和3，ETF：15，lof及封闭式基金：16，国债及国开债等：10，债券逆回购：13，转债：12
    上海市场：1   普通股票：6，ETF：51，lof及封闭式基金：50，国债及国开债等：01，债券逆回购：20，转债：11
    测试code：
    ["601318", "1", "300002", "159931", "160105", "018003", "18003", "108602", "131810", "204001", "515050", "501057",
    "123002", "110059"]
    """
    if len(code_list) == 0:
        return []
    ret_list = []
    for code in code_list:
        tmp = str(code)
        if len(tmp) < 6:
            tmp = tmp.zfill(6)
        if (tmp[0:2] == "15") or (tmp[0:2] == "16") or (tmp[0:2] == "10") or (tmp[0:2] == "13") or (tmp[0:2] == "12"):
            ret_list.append((0, tmp))
        elif (tmp[0:2] == "51") or (tmp[0:2] == "50") or (tmp[0:2] == "01") or (tmp[0:2] == "20") or (tmp[0:2] == "11"):
            ret_list.append((1, tmp))
        elif (tmp[0] == "0") or (tmp[0] == "3"):
            ret_list.append((0, tmp))
        elif tmp[0] == "6":
            ret_list.append((1, tmp))
        else:
            print("code error %s" % tmp)
    return ret_list

# ================================


def process_tdx_price(df):
    """
    处理通达信返回的价格，债券，逆回购等返回的价格是每股价格*10
    需要进行价格处理的code开头有：
    ETF：15,51；
    lof及封闭式基金：16，50
    国债及国开债等：10，01
    债券逆回购：13，20
    转债：12， 11
    :param df: 通达信返回的原始行情数据
    :return:
    """
    need_process_columns = ["price", "last_close",
                            "open", "high", "low",
                            "bid1", "ask1",
                            "bid2", "ask2",
                            "bid3", "ask3",
                            "bid4", "ask4",
                            "bid5", "ask5"]

    if df.empty:
        return df
    result_list = []
    for idx, row in df.iterrows():
        if (row["code"][0] != "6") and (row["code"][0:2] != "00") and (row["code"][0] != "3"):
            tmp_list = []
            for column in stock_tdx_columns:
                if column in need_process_columns:
                    tmp_list.append(round(row[column] / 10, 3))
                else:
                    tmp_list.append(row[column])
            result_list.append(tmp_list)
        else:
            result_list.append(row.tolist())
    tmp_df = pd.DataFrame(data=result_list,columns=stock_tdx_columns)
    return tmp_df


# ===========================


if __name__ == "__main__":
    test_code = ["601318", "1", "300002", "159931", "160105", "018003", "18003", "108602", "131810", "204001",
                 "515050", "501057", "123002", "110059"]
    df = get_a_quote_by_tdx(test_code)

    hk = ["00005", "09988"]
    #df = get_hk_quote_by_tdx(hk)
    print(df)


    """
    tmp = div_list(test_code, 5)
    for i in tmp:
        print(i)
    """

    """
    api = TdxHq_API()
    if api.connect('119.147.212.81', 7709):
        data = api.get_security_bars(9, 0, '000001', 4, 3)
        print(data)
        stocks = api.get_security_list(2, 0)
        print(stocks)
        api.disconnect()
    """

