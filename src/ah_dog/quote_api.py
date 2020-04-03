# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import tushare as ts
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from jqdatasdk import *
import configparser
import pandas as pd
from pytdx.config.hosts import hq_hosts
from bs4 import BeautifulSoup
import urllib
import re
from futu import *
from requests import get
from opendatatools import fx
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

# tushare stock columns--> tdx columns(stock_tdx_columns)
'''
Index(['name', 'open', 'pre_close', 'price', 'high', 'low', 'bid', 'ask',
    'volume', 'amount', 'b1_v', 'b1_p', 'b2_v', 'b2_p', 'b3_v', 'b3_p',
    'b4_v', 'b4_p', 'b5_v', 'b5_p', 'a1_v', 'a1_p', 'a2_v', 'a2_p', 'a3_v',
    'a3_p', 'a4_v', 'a4_p', 'a5_v', 'a5_p', 'date', 'time', 'code'],
    dtype='object')
'''
ts_2_tdx_columns = {'pre_close': "last_close",
                    'volume': 'vol',
                    'b1_p': 'bid1',
                    'b1_v': 'bid_vol1',
                    'b2_p': 'bid2',
                    'b2_v': 'bid_vol2',
                    'b3_p': 'bid3',
                    'b3_v': 'bid_vol3',
                    'b4_p': 'bid4',
                    'b4_v': 'bid_vol4',
                    'b5_p': 'bid5',
                    'b5_v': 'bid_vol5',
                    'a1_p': 'ask1',
                    'a1_v': 'ask_vol1',
                    'a2_p': 'ask2',
                    'a2_v': 'ask_vol2',
                    'a3_p': 'ask3',
                    'a3_v': 'ask_vol3',
                    'a4_p': 'ask4',
                    'a4_v': 'ask_vol4',
                    'a5_p': 'ask5',
                    'a5_v': 'ask_vol5'}

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
        data = data[stock_tdx_columns]
        return data
# ====================================


def get_quote_by_tdx2(code_list, isbTDX):
    '''
    使用通达信接口获取股票实时行情
    该方法带有市场信息，主要是对于一些ETF或者lof行情，不能用0开头还是6开头来判断是上海市场还是深圳市场
    :param code_list: 股票代码list,带市场信息
    :param isbTDX:true,带有通达信标准获取信息行情的内容
    :return: df
    '''
    if not isbTDX:
        return pd.DataFrame()

    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        data = api.to_df(api.get_security_quotes(code_list))
        if data.empty:
            return pd.DataFrame()
        data = data[stock_tdx_columns]
        return data
# =============================================


def get_finance_by_tdx(market, code):
    '''
    通过通达信获取股票的finance信息
    参数：市场代码， 股票代码， 如： 0,000001 或 1,600300
    :param market:市场码
    :param code: tuple（市场代码，股票代码的元组）（因为可能涉及非标准的股票代码如lof等，所有在进行封装
    :return:
    有序字典
    ['market', 'code', 'liutongguben', 'province', 'industry', 'updated_date', 'ipo_date', 'zongguben', 'guojiagu',
    'faqirenfarengu', 'farengu', 'bgu', 'hgu', 'zhigonggu', 'zongzichan', 'liudongzichan', 'gudingzichan',
    'wuxingzichan', 'gudongrenshu', 'liudongfuzhai', 'changqifuzhai', 'zibengongjijin', 'jingzichan', 'zhuyingshouru',
     'zhuyinglirun', 'yingshouzhangkuan', 'yingyelirun', 'touzishouyu', 'jingyingxianjinliu', 'zongxianjinliu',
     'cunhuo', 'lirunzonghe', 'shuihoulirun', 'jinglirun', 'weifenpeilirun', 'meigujingzichan', 'baoliu2']

     返回是一个有序字典，可以dic["key"]来获取
     如return为data
     data['meigujingzichan']
    '''
    api = TdxHq_API()
    with api.connect('119.147.212.81', 7709):
        data = api.get_finance_info(market, code)
        return data
# =================================================


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
    data = data.rename(columns=ts_2_tdx_columns)
    data['market'] = data['code'].apply(process_market)
    data['s_vol'] = '--'
    data['b_vol'] = '--'
    data['cur_vol'] = '--'
    data = data.drop(columns=['name', 'date', 'time', 'bid', 'ask'])
    data['vol'] = data['vol'].apply(pd.to_numeric)
    data['vol'] = data['vol'].map(lambda x: int(x/100))
    return data

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
    # format = code_list[0].split('.')[0]
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

# =========================================


def process_market(x):
    x = str(x)
    if x[0] == '6':
        return 1
    elif x[0] == '0':
        return 0
    elif x[0] == '3':
        return 0
    else:
        return -1

# ============================================


def get_auag_quote_by_sina(code):
    '''
        获取期货实时行情数据，返回字典数据
        :param code: str 类型，如“AU0”,多个标的用“，”分割，不区分大小写，有程序处理大小写问题
        :return: {}
    '''
    # http://hq.sinajs.cn/list=AU0 黄金连续
    basicUrl = "http://hq.sinajs.cn/list="
    code = code.upper()
    url = basicUrl + str(code)
    future_dict = dict()  # 获取到行情后拼成一个字典数据
    code_list = code.split(",")

    web_data = urllib.request.urlopen(url)
    soup = BeautifulSoup(web_data, "lxml")
    data = soup.find("p")
    data_text = data.get_text()
    # print(data_text)

    pattern = re.compile('"(.*)"')
    str_tmp_list = pattern.findall(data_text)

    if len(str_tmp_list) != len(code_list):
        print("行情获取的数据与输入code数量不符")
        return {}

    for i in range(len(code_list)):
        key_data = str_tmp_list[i].split(",")

        # print(str_tmp_list[i])
        # print(key_data)
        future_dict[code_list[i]] = dict(
            name=key_data[0],
            open=float(key_data[2]),
            high=float(key_data[3]),
            low=float(key_data[4]),
            yesterday_close=float(key_data[5]),
            bid=float(key_data[6]),
            ask=float(key_data[7]),
            new_price=float(key_data[8]),
            settle_price=float(key_data[9]),
            yesterday_settle_price=float(key_data[10]),
            bid_amount=int(key_data[11]),
            ask_amount=int(key_data[12]),
            position=int(key_data[13]),
            total_amount=int(key_data[14]),
            date=key_data[17],
        )
    # print (future_dict)
    return future_dict
# =========================================


def get_quote_by_futu(code_list):
    '''
    通过futu api获取实时行情
    :param code_list:
    :return:df
    futu 返回    ret == RET_OK 返回pd dataframe数据，data.DataFrame数据, 数据列格式如下

                ret != RET_OK 返回错误字符串

    =======================   =============   =================================================================
    参数                       类型                        说明
    =======================   =============   =================================================================
    code                       str            股票代码
    update_time                str            更新时间(yyyy-MM-dd HH:mm:ss)，（美股默认是美东时间，港股A股默认是北京时间）
    last_price                 float          最新价格
    open_price                 float          今日开盘价
    high_price                 float          最高价格
    low_price                  float          最低价格
    prev_close_price           float          昨收盘价格
    volume                     int            成交数量
    turnover                   float          成交金额
    turnover_rate              float          换手率
    suspension                 bool           是否停牌(True表示停牌)
    listing_date               str            上市日期 (yyyy-MM-dd)
    equity_valid               bool           是否正股（为true时以下正股相关字段才有合法数值）
    issued_shares              int            发行股本
    total_market_val           float          总市值
    net_asset                  int            资产净值
    net_profit                 int            净利润
    earning_per_share          float          每股盈利
    outstanding_shares         int            流通股本
    net_asset_per_share        float          每股净资产
    circular_market_val        float          流通市值
    ey_ratio                   float          收益率（该字段为比例字段，默认不展示%）
    pe_ratio                   float          市盈率（该字段为比例字段，默认不展示%）
    pb_ratio                   float          市净率（该字段为比例字段，默认不展示%）
    pe_ttm_ratio               float          市盈率TTM（该字段为比例字段，默认不展示%）
    price_spread               float          当前摆盘价差亦即摆盘数据的买档或卖档的相邻档位的报价差
    （期权，涡轮相关字段省去）

    '''
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    get_colunms_name = ['code', 'last_price']
    ret, df_futu = quote_ctx.get_market_snapshot(code_list)
    if ret == RET_OK:
        df_futu = df_futu.loc[:, get_colunms_name]
    else:
        df_futu = pd.DataFrame()
    quote_ctx.close()
    return df_futu

# =========================================
def get_hk_price_by_tdx():
    pass


# ===========================================


def get_option_price_by_sina(code):
    '''
    从新浪获取50ETF期权的实时数据
    返回的字段含义：
    var hq_str_CON_OP_代码="买量(0)，买价，最新价，卖价，卖量，持仓量，涨幅，行权价，昨收价，开盘价，涨停价，跌停价(11),
    申卖 价五，申卖量五，申卖价四，申卖量四，申卖价三，申卖量三，申卖价二，申卖量二，申卖价一，申卖量一，申买价一，
    申买量一 ，申买价二，申买量二，申买价三，申买量三，申买价四，申买量四，申买价五，申买量五，行情时间，主力合约标识，状态码，
    标的证券类型，标的股票，期权合约简称，振幅(38)，最高价，最低价，成交量，成交额"

    :param code: 期权代码
    :return:
    '''
    dic_fields = {'buy_amount': '买量',
                  'buy_price': '买价',
                  'new_price': '最新价',
                  'sell_price': '卖价',
                  'sell_amount': '卖量',
                  'position': '持仓量',
                  'rose_increase': '涨幅',
                  'exercise_price': '行权价',
                  'pre_close': '昨收价',
                  'open': '开盘价',
                  'high_limit_price': '涨停价',
                  'drop_stop_price': '跌停价',
                  'ask5': '卖五',
                  'ask_v5': '卖量五',
                  'ask4':'卖四',
                  'ask_v4': '卖量四',
                  'ask3':'卖三',
                  'ask_v3':'卖量三',
                  'ask2': '卖二',
                  'ask_v2': '申卖量二',
                  'ask1':'卖一',
                  'ask_v1': '卖量一',
                  'bid1': '申买价一',
                  'bid_v1': '申买量一 ',
                  'bid2': '申买价二',
                  'bid_v2': '申买量二',
                  'bid3': '申买价三',
                  'bid_v3': '申买量三',
                  'bid4': '申买价四',
                  'bid_v4': '申买量四',
                  'bid5': '申买价五',
                  'bid_v5': '申买量五',
                  'time': '行情时间',
                  'dominant_contract': '主力合约标识',
                  'state': '状态码',
                  'type': '标的证券类型',
                  'underlying_stock': '标的股票',
                  'name': '期权合约简称',
                  'zhenfu': '振幅',
                  'high': '最高价',
                  'low': '最低价',
                  'vol': '成交量',
                  'amount': '成交额',
                  'unkown1': 'unkown',
                  'unkown2': 'unkown'

    }
    url = "http://hq.sinajs.cn/list=CON_OP_{code}".format(code=code)
    data = get(url).content.decode('gbk')
    data = data[data.find('"') + 1: data.rfind('"')].split(',')
    fields = ['买量', '买价', '最新价', '卖价', '卖量', '持仓量', '涨幅', '行权价', '昨收价', '开盘价', '涨停价',
              '跌停价', '卖五', '卖量五', '卖四', '卖量四', '卖三', '申卖量三', '申卖价二',
              '申卖量二', '申卖价一', '申卖量一', '申买价一', '申买量一 ', '申买价二', '申买量二', '申买价三',
              '申买量三', '申买价四', '申买量四', '申买价五', '申买量五', '行情时间', '主力合约标识', '状态码',
              '标的证券类型', '标的股票', '期权合约简称', '振幅', '最高价', '最低价', '成交量', '成交额']

    df = pd.DataFrame(data=[data], columns=list(dic_fields.keys()))
    return df
# ===========================================


def get_option_price_by_tdx(code):
    '''
    通过通达信获取option的实时价格
    上海期权的市场代码是：8
    :param code:
    :return:df
    df columns:
    ['market', 'code', 'pre_close', 'open', 'high', 'low', 'price',
       'kaicang', 'zongliang', 'xianliang', 'neipan', 'waipan', 'chicang',
       'bid1', 'bid2', 'bid3', 'bid4', 'bid5', 'bid_vol1', 'bid_vol2',
       'bid_vol3', 'bid_vol4', 'bid_vol5', 'ask1', 'ask2', 'ask3', 'ask4',
       'ask5', 'ask_vol1', 'ask_vol2', 'ask_vol3', 'ask_vol4', 'ask_vol5']
    '''
    api_ex = TdxExHq_API()
    data = pd.DataFrame()
    if api_ex.connect('61.49.50.181', 7727):
        data = api_ex.to_df(api_ex.get_instrument_quote(8, code))
    return data
# ===========================================


def get_option_greek_alphabet(code):
    '''
    获取期权的指标数据
    :param code:
    :return:
    '''
    url = "http://hq.sinajs.cn/list=CON_SO_{code}".format(code=code)
    data = get(url).content.decode('gbk')
    data = data[data.find('"') + 1: data.rfind('"')].split(',')
    fields = ['期权合约简称', '成交量', 'Delta', 'Gamma', 'Theta', 'Vega', '隐含波动率', '最高价', '最低价', '交易代码',
              '行权价', '最新价', '理论价值']
    return list(zip(fields, [data[0]] + data[4:]))

# ===========================================


def get_cny_spot_from_opendatatool(cny_pair):
    """
    从opendatatools获取人民币实时汇率数据
    :param cny_pair: 人民币对xxx，如HKD/CNY，当前支持的查询汇率如下：
    'USD/CNY', 'EUR/CNY', '100JPY/CNY', 'HKD/CNY', 'GBP/CNY', 'AUD/CNY',
    'NZD/CNY', 'SGD/CNY', 'CHF/CNY', 'CAD/CNY', 'CNY/MYR', 'CNY/RUB',
    'CNY/ZAR', 'CNY/KRW', 'CNY/AED', 'CNY/SAR', 'CNY/HUF', 'CNY/PLN',
    'CNY/DKK', 'CNY/SEK', 'CNY/NOK', 'CNY/TRY', 'CNY/MXN', 'CNY/THB'
    :return:float
    """
    df_cny = fx.get_cny_spot_price()
    if not isinstance(df_cny, pd.DataFrame):
        return -1
    if df_cny.empty:
        return -1

    df_cny = df_cny.set_index("ccyPair")
    ret = df_cny.loc[cny_pair, ["askPrc"]]["askPrc"]
    if isinstance(ret, str):
        return float(ret)
    elif isinstance(ret, float):
        return ret
    else:
        return -1
# ===========================================


if __name__ == "__main__":
    # print(load_config())
    # get_stock_quote(["0000"])
    # print(stander_stock_code(['6001318.XSHE', '000001.XSHG'], 'ts'))
    pd.set_option('display.max_columns', None)
    # print(get_quote_by_tdx(['601318.XSHE', '000001.XSHG']))
    # print(get_quote_by_ts(['601318.XSHE', '000001.XSHG']))
    # price = get_auag_quote_by_sina("au0,ag0")
    # print(price)
    # au = price['AU0']['new_price']
    # ag = price['AG0']['new_price']
    # time = price['AU0']['date']
    # print([au, ag, time])
    # print(get_auag_quote_by_sina("ag0"))
    print(get_quote_by_futu(['SH.601998', 'HK.00998']))
    df = get_option_price_by_tdx("10001874")
    print(df)
    print(df.columns)
