# -*- coding: utf-8 -*-
__author__ = 'Administrator'

from bs4 import BeautifulSoup
import requests
import time
import urllib
import pandas as pd

def getQuotation(code):
    basicUrl="http://hq.sinajs.cn/list=" #http://hq.sinajs.cn/list=sh113009
    codelen=len(code)
    url=""
    if codelen<=0:
        print "input code list is null,return"
        return
    else:
        for singlecode in code:
            url=url+singlecode+","
    url=basicUrl+url
    #print "url:%r"%(url)
    web_data = urllib.urlopen(url)
    soup = BeautifulSoup(web_data,"lxml")
    data=soup.find("p")
    data_text = data.get_text()

    datalist=data_text.split(";")
    #print datalist[0]
    #print datalist[1]
    #print datalist[2]

    codelist=[]
    openlist=[]
    precloselist=[]
    closelist=[]
    highlist=[]
    lowlist=[]
    bidlist=[]
    currentPricelist=[]
    totalAmountlist=[]
    totalMoneylist=[]
    b1_vlist=[]
    b1list=[]
    b2_vlist=[]
    b2list=[]
    b3_vlist=[]
    b3list=[]
    b4_vlist=[]
    b4list=[]
    b5_vlist=[]
    b5list=[]
    a1_vlist=[]
    a1list=[]
    a2_vlist=[]
    a2list=[]
    a3_vlist=[]
    a3list=[]
    a4_vlist=[]
    a4list=[]
    a5_vlist=[]
    a5list=[]
    datetimelist=[]

    datadic={}
    j=1
    for stockdatalist in datalist:
        stockdata=stockdatalist.split(",")
        if len(stockdata)<3:
            continue
        if(j==0):
            codelist.append(stockdata[0][13:19])
            j=j+1
        else:
            codelist.append(stockdata[0][14:20])#第二行多了一个换行符，code后移一位
        bidlist.append(stockdata[6])
        currentPricelist.append(stockdata[7])
        totalAmountlist.append(stockdata[8])
        totalMoneylist.append(stockdata[9])
        openlist.append(stockdata[1])
        precloselist.append(stockdata[2])
        closelist.append(stockdata[3])
        highlist.append(stockdata[4])
        lowlist.append(stockdata[5])
        b1_vlist.append(stockdata[10])
        b1list.append(stockdata[11])
        b2_vlist.append(stockdata[12])
        b2list.append(stockdata[13])
        b3_vlist.append(stockdata[14])
        b3list.append(stockdata[15])
        b4_vlist.append(stockdata[16])
        b4list.append(stockdata[17])
        b5_vlist.append(stockdata[18])
        b5list.append(stockdata[19])
        a1_vlist.append(stockdata[20])
        a1list.append(stockdata[21])
        a2_vlist.append(stockdata[22])
        a2list.append(stockdata[23])
        a3_vlist.append(stockdata[24])
        a3list.append(stockdata[25])
        a4_vlist.append(stockdata[26])
        a4list.append(stockdata[27])
        a5_vlist.append(stockdata[28])
        a5list.append(stockdata[29])
        datetimelist.append(stockdata[30]+" "+stockdata[31])

    datadic={
            "code":code,

            "bid":bidlist,
            "ask":currentPricelist,
            "volumn":totalAmountlist,
            "amount":totalMoneylist,
            "open":openlist,
            "preclose":precloselist,
            "close":closelist,
            "high":highlist,
            "low":lowlist,

            "b1_v":b1_vlist,
            "b1":b1list,
            "b2_v":b2_vlist,
            "b2":b2list,
            "b3_v":b3_vlist,
            "b3":b3list,
            "b4_v":b4_vlist,
            "b4":b4list,
            "b5_v":b5_vlist,
            "b5":b5list,
            "a1_v":a1_vlist,
            "a1":a1list,
            "a2_v":a2_vlist,
            "a2":a2list,
            "a3_v":a3_vlist,
            "a3":a3list,
            "a4_v":a4_vlist,
            "a4":a4list,
            "a5_v":a5_vlist,
            "a5":a5list,
            "datetime":datetimelist
        }
    #print datadic
    df=pd.DataFrame(datadic,columns=["code","bid","ask","volumn","amount","open","preclose","close",\
                                     "high","low","b1_v","b1","b2_v","b2","b3_v","b3","b4_v","b4","b5_v","b5",\
                                     "a1_v","a1","a2_v","a2","a3_v","a3","a4_v","a4","a5_v","a5","datetime"])

    return df
#print getQuotation(["sz128014","sz128016"])
#print dfdata