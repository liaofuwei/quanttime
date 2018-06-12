# -*- coding: utf-8 -*-
__author__ = 'Administrator'

from bs4 import BeautifulSoup
import requests
import time
import urllib

def getQuotation(code,market):
    basicUrl="http://hq.sinajs.cn/list=" #http://hq.sinajs.cn/list=sh113009
    url=basicUrl+str(market)+str(code)
    web_data = urllib.urlopen(url)
    soup = BeautifulSoup(web_data,"lxml")
    data=soup.find("p")
    data_text = data.get_text()
    print data_text
    datalist=data_text.split(",")

    #通过datalist长度判断是否成功获得数据
    if len(datalist)<3:
        print "get data failed"
        return

    datadic={
        "code":code,
        "open":datalist[1],
        "yesterdayclose":datalist[2],
        "close":datalist[3],
        "high":datalist[4],
        "low":datalist[5],
        "bid1":datalist[6],
        "current_price":datalist[7],
        "totoal_amount":datalist[8],
        "total_money":datalist[9],
        "bid1_amount":datalist[10],
        "bid1":datalist[11],
        "bid2_amount":datalist[12],
        "bid2":datalist[13],
        "bid3_amount":datalist[14],
        "bid3":datalist[15],
        "bid4_amount":datalist[16],
        "bid4":datalist[17],
        "bid5_amount":datalist[18],
        "bid5":datalist[19],
        "sell1_amount":datalist[20],
        "sell1":datalist[21],
        "sell2_amount":datalist[22],
        "sell2":datalist[23],
        "sell3_amount":datalist[24],
        "sell3":datalist[25],
        "sell4_amount":datalist[26],
        "sell4":datalist[27],
        "sell5_aount":datalist[28],
        "sell5":datalist[29],
        "datetime":datalist[30]+" "+datalist[31]

    }
    return datadic
