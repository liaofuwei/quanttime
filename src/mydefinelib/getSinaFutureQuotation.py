# -*- coding: utf-8 -*-
__author__ = 'Administrator'

from bs4 import BeautifulSoup
import requests
import time
import urllib
import re

class sinaFutureData(object):
    def __init__(self):
        # http://hq.sinajs.cn/list=AU0 黄金连续
        self.basicUrl = "http://hq.sinajs.cn/list="

    def get_future_price(self, code):
        '''
        获取期货实时行情数据，返回字典数据
        :param code: str 类型，如“AU0”,多个标的用“，”分割，不区分大小写，有程序处理大小写问题
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

        if len(str_tmp_list) != len(code_list):
            print("行情获取的数据与输入code数量不符")
            return -1

        for i in range(len(code_list)):
            key_data = str_tmp_list[i].split(",")

            #print(str_tmp_list[i])
            #print(key_data)
            future_dict[code_list[i]] = dict(
                name = key_data[0],
                open = float(key_data[2]),
                high = float(key_data[3]),
                low = float(key_data[4]),
                yesterday_close = float(key_data[5]),
                bid = float(key_data[6]),
                ask = float(key_data[7]),
                new_price = float(key_data[8]),
                settle_price = float(key_data[9]),
                yesterday_settle_price = float(key_data[10]),
                bid_amount = int(key_data[11]),
                ask_amount = int(key_data[12]),
                position = int(key_data[13]),
                total_amount = int(key_data[14]),
                date = key_data[17],
            )
        #print (future_dict)
        return future_dict


if __name__ == "__main__":
    future = sinaFutureData()
    dic = future.get_future_price("Au0,AG0")
    print(dic["AG0"]["open"])