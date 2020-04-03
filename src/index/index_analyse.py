#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time,requests, os, xlrd, sys
from datetime import timedelta, date, datetime
from jqdatasdk import *
pd.set_option('precision', 3)

auth('13811866763', "sam155") #jqdata 授权

#所有stock的pe，pb每日数据存放在valuation表中，按照code文件名的方式存放
data_dir = "C:\\quanttime\\data\\finance\\valuation\\"
#所有指数信息，可通过get_all_securities(types=['index'], date=None)实时获取，同时该信息也存在本地，路径如下
'''
                display_name	name	  start_date	end_date	type
000001.XSHG	    上证指数	        SZZS	  1991-07-15	2200-01-01	index
000002.XSHG	    A股指数	        AGZS	  1992-02-21	2200-01-01	index
'''



def maintenance_index_valuation():
    '''
    维护所有指数的估值表
    所有指数信息，存放于C:\\quanttime\\data\\basic_info\\index_all_info.csv
    '''
    index_all_info_dir = "C:\\quanttime\\data\\basic_info\\index_all_info.csv"
    index_save_dir = "C:\\quanttime\\data\\index\\index_valuation\\"
    index_all_info = pd.read_csv(index_all_info_dir, index_col=["code"], encoding="gbk")
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    for code in index_all_info.index:
        update_file = index_save_dir + str(code) + '.csv'
        if os.path.exists(update_file):
            pe_pb_data = pd.read_csv(update_file, index_col=["date"])
            file_end_date = datetime.strptime(pe_pb_data.index[len(pe_pb_data.index)-1], "%Y-%m-%d")
            if file_end_date >= yesterday:
                print("index code:%r valuation already update new,this time need't update"%code)
                continue
            start = file_end_date + timedelta(days=1)
            start = start.date().strftime("%Y-%m-%d")
            get_index_pe_pb(code, start, yesterday.date().strftime("%Y-%m-%d"))
        else:
            start = index_all_info.loc[code,["start_date"]].start_date
            get_index_pe_pb(code, start, yesterday.strftime("%Y-%m-%d"))



def get_index_pe_pb(code, start_date=None, end_date=None):
    '''
    计算指定指数的历史PE_PB
    code：输入指数代码 str
    start_date: str
    end_date:str
    存入C:\\quanttime\\data\\index\\index_valuation\\文件夹内
    '''
    index_save_dir = "C:\\quanttime\\data\\index\\index_valuation\\"
    if start_date is None:
        start_date = index_info.loc[code, ["start_date"]].start_date.date()
        if start_date < date(2006,1,4): #只计算2005年以来的数据
            start_date = date(2006,1,4)
    if end_date is None:
        end_date = pd.datetime.today().date() - timedelta(1)

    dates = pd.date_range(start_date, end_date)
    pe_list = []
    pb_list = []
    for date in dates:
        tmp_date = date.date().strftime("%Y-%m-%d")
        pe_pb = calc_PE_PB_date(code, tmp_date)
        if len(pe_pb) !=2 :
            print("calc_PE_PB_date return error,code:%r,date:%r"%(code, date))
            continue
        pe_list.append(pe_pb[0])
        pb_list.append(pe_pb[1])
    df_pe_pb = pd.DataFrame({'PE':pd.Series(pe_list,index=dates),
                             'PB':pd.Series(pb_list,index=dates)})
    df_pe_pb.index.name = "date"
    save_file_dir = index_save_dir + str(code) + '.csv'
    if(os.path.exists(save_file_dir)):
        df_pe_pb.to_csv(save_file_dir, mode='a', header='None')
    else:
        df_pe_pb.to_csv(save_file_dir)
    print("save code:%r pe_pb valuation successful"%code)



def calc_PE_PB_date(code, date):
    '''
    计算指定日期的指数PB PE，当前（2018-12-16）按照等权重计算，后续加入权重
    code：joinquant code stander
    date：日期
    return：list，按照PE，PB的顺序排列
    '''
    stocks = get_index_stocks(code, date)#指定日期指数各成分股，返回一个list
    pe_list = []
    pb_list = []
    for stock in stocks:
        stock_path = data_dir + str(stock) + ".csv"
        valuation_data = pd.read_csv(stock_path,index_col=['day'], usecols=["day","pe_ratio","pb_ratio"])
        valuation_data = valuation_data.drop_duplicates()
        try:
            pe_value = valuation_data.loc[date,['pe_ratio']].pe_ratio
            pb_value = valuation_data.loc[date,['pb_ratio']].pb_ratio
            #剔除了pb，pe的负值
            if pb_value <=0 :
                print("code: %r, pb<=0:( %r) " % (stock, pb_value))
                continue
            pb_list.append(pb_value)
            if pe_value <=0 :
                print("code: %r, pe<=0:( %r) " % (stock, pe_value))
                continue
            pe_list.append(pe_value)

        except:
            print("code: %r, in %r 缺少对应的pe，pb数据"%(stock,date))
            #return [float('NaN'), float('NaN')]
    if len(pe_list) > 0:
        pe_mean = round(sum(pe_list)/len(pe_list), 2)
    else:
        pe_mean = float('NaN')

    if len(pb_list) > 0:
        pb_mean = round(sum(pb_list)/len(pb_list), 2)
    else:
        pb_mean = float('NaN')
    return [pe_mean, pb_mean]




def calc_state(data):
    if data < 10.0:
        return u'极度低估'
    elif 10 <= data  and data < 20:
        return u'低估'
    elif 20 <= data  and data < 40:
        return u'正常偏低'
    elif 40 <= data  and data < 60:
        return u'正常'
    elif 60 <= data  and data < 80:
        return u'正常偏高'
    elif 80 <= data  and data < 90:
        return u'高估'
    elif 90 <= data:
        return u'极度高估'

def convert_code(code):
    if code.endswith('XSHG'):
        return 'sh' + code[0:6]
    elif code.endswith('XSHE'):
        return 'sz' + code[0:6]



def pe_pb_analysis(index_list=['000300.XSHG','000905.XSHG']):
    '''PE_PB分析'''

    pe_results = []
    pe_code_list = []
    pb_results = []
    pb_code_list = []
    #沪深
    for code in index_list:
        data_path = '%s%s_pe_pb.csv'%(data_root,convert_code(code))
        index_name = all_index.ix[code].display_name
        df_pe_pb = pd.DataFrame.from_csv(data_path)
        df_pe_pb = df_pe_pb[df_pe_pb.iloc[-1].name.date() - timedelta(365*10):] #最长十年的数据
        if len(df_pe_pb)<250*3: #每年250个交易日,小于3年不具有参考价值
#                 print code, 'samples:', len(df_pe_pb), index_name
            continue
        pe_ratio = len(df_pe_pb.PE[df_pe_pb.PE<df_pe_pb.iloc[-1].PE])/float(len(df_pe_pb.PE))*100
        pb_ratio = len(df_pe_pb.PB[df_pe_pb.PB<df_pe_pb.iloc[-1].PB])/float(len(df_pe_pb.PB))*100
        pe_results.append([index_name, df_pe_pb.iloc[-1].PE, '%.2f'%pe_ratio, calc_state(pe_ratio),
                           min(df_pe_pb.PE), max(df_pe_pb.PE), '%.2f'%median(df_pe_pb.PE), '%.2f'%std(df_pe_pb.PE),
                           df_pe_pb.iloc[0].name.date()])
        pb_results.append([index_name, df_pe_pb.iloc[-1].PB, '%.2f'%pb_ratio, calc_state(pb_ratio),
                           min(df_pe_pb.PB), max(df_pe_pb.PB), '%.2f'%median(df_pe_pb.PB),'%.2f'%std(df_pe_pb.PB),
                           df_pe_pb.iloc[0].name.date()])
        pe_code_list.append(code)
        pb_code_list.append(code)
    #港股
    for code in ['hsi', 'hscei']:
        df_pe = read_hk_data(code)
        pe_ratio = len(df_pe.PE[df_pe.PE<df_pe.iloc[-1].PE])/float(len(df_pe.PE))*100
        pe_results.append([hk_idx_name[code], df_pe.iloc[-1].PE, '%.2f'%pe_ratio, calc_state(pe_ratio),
                           min(df_pe.PE), max(df_pe.PE),'%.2f'%median(df_pe.PE),'%.2f'%std(df_pe.PE),
                           df_pe.iloc[0].name.date()])
        pe_code_list.append(code.upper())

#     print '估值日期: ', df_pe_pb.iloc[-1].name.date()
    date_str = df_pe_pb.iloc[-1].name.date().strftime('%Y%m%d')
    pe_columns=[u'名称', u'当前PE', u'百分位(%)', u'估值状态', u'最小', u'最大', u'中位数', u'标准差', u'起始日期']
    pe_df = pd.DataFrame(data=pe_results,index=pe_code_list,columns=pe_columns)
    pe_df.index = pe_df[u'名称']
    del pe_df[u'名称']
    pe_df.index.name = date_str

    pb_columns=[u'名称', u'当前PB', u'百分位(%)', u'估值状态', u'最小', u'最大', u'中位数', u'标准差', u'起始日期']
    pb_df = pd.DataFrame(data=pb_results,index=pb_code_list,columns=pb_columns)
    pb_df.index = pb_df[u'名称']
    del pb_df[u'名称']
    pb_df.index.name = date_str

    return (pe_df.sort([u'百分位(%)'],ascending=True), pb_df.sort([u'百分位(%)'],ascending=True))


maintenance_index_valuation()