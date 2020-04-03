#-*-coding:utf-8 -*-
__author__ = 'Administrator'

from futuquant import *
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

'''
获取券商股的A+H历史K线
主要用于分析AH折溢价情况
'''
class AHPremium:
    #Acode--A股代码 沪市 sh.600000
    #Hcode--H股代码 hk.00700
    filePath=""

    def __init__(self, Acode, Hcode):
        self.Acode=Acode
        self.Hcode=Hcode

    def addDataFilePath(self,filepath="C:\\quanttime\\data\\security_hisdata\\"):
        self.filePath = filepath
        print("filepath:%r"%(self.filePath))

    def getHistoryAHpremium(self, start_time, end_time):

        try:
            #"C:\\quanttime\\data\\tmp\\hk_06837.csv"
            dfH_file_path = self.filePath+"hk_"+self.Hcode[3:8]+".csv"
            print("H stock path and code:%r"%(dfH_file_path))
            dfH = pd.read_csv(dfH_file_path, index_col="time_key")

            #"C:\\quanttime\\data\\tmp\\SH_600837.csv"
            dfA_file_path = self.filePath+self.Acode[0:2]+"_"+self.Acode[3:9]+".csv"
            print("A stock path and code:%r"%dfA_file_path)
            dfA = pd.read_csv(dfA_file_path,index_col="time_key")

        except FileNotFoundError:
            print("read csv file failed,check path or file!!!")
            return

        dfA = dfA[["code","close","pe_ratio"]]
        dfA = dfA.rename(columns={"close":"Aclose","pe_ratio":"Ape_ratio"})
        dfAH = pd.merge(dfH, dfA, left_index=True,right_index=True)

        st = datetime.strptime(dfAH.index[0][0:9],"%Y-%m-%d")
        last = datetime.strptime(dfAH.index[-1][0:9],"%Y-%m-%d")

        start_time1 = datetime.strptime(start_time,"%Y-%m-%d")
        end_time1 = datetime.strptime(end_time,"%Y-%m-%d")
        if ((start_time1 < st) and (end_time1 > last)):
            print("输入的时间范围大于现存数据日期的最大范围")

        dfAH["premium"] = (dfAH["Aclose"] - dfAH["close"])/ dfAH["Aclose"]

        #dfAH.loc[:,["close","Aclose","premium"]].plot(secondary_y=["premium"])
        #plt.show()
        return dfAH

if __name__ == "__main__":
    premium = AHPremium("sh.600837","hk.06837")
    premium.addDataFilePath()
    df = premium.getHistoryAHpremium("2010-01-01","2018-06-30")
    df.loc[:,["close","Aclose","premium"]].plot(secondary_y=["premium"])
    print (df["premium"].describe())
    plt.show()




