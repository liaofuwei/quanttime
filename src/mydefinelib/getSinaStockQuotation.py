# -*- coding: utf-8 -*-
__author__ = 'Administrator'

from bs4 import BeautifulSoup
import requests
import time
import urllib
import re
import pandas as pd

class sinaStockData(object):
    def __init__(self):
        # http://hq.sinajs.cn/list=sh601318
        self.basicUrl = "http://hq.sinajs.cn/list="

    #-------------------------------------------------------------------------------------
    def get_stock_price(self, code):
        '''
        获取股票实时行情数据，返回pandas
        :param code: str 类型，如“sh601318”,多个标的用“，”分割，不区分大小写，有程序处理大小写问题
        :return: {}
        '''
        code = code.upper()
        url = self.basicUrl + str(code)
        future_dict = dict() #获取到行情后拼成一个字典数据
        code_list = code.split(",")

        web_data = urllib.request.urlopen(url)
        soup = BeautifulSoup(web_data, "lxml")
        data = soup.find("p")
        data_text = data.get_text()
        #print(data_text)

        pattern = re.compile('"(.*)"')
        str_tmp_list = pattern.findall(data_text)