# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import pandas as pd
from datetime import datetime, timedelta

yesterday = datetime.today().date() - timedelta(days=1)


def is_last_update_ok(module_name, date=yesterday):
    '''
    判断是否已经更新过
    程序获取运行目录内的last_update.csv文件，读取模块名和时间
    module	date
    A	    2019/5/13

    与输入的日期做比较判断是否当前已经更新
    :param module_name: str 模块名
    :param date: datetime 日期
    :return: true--已经更新，false--无更新或者当天更新失败
    '''
    last_update = pd.read_csv("last_update.csv", index_col=["module"], parse_dates=["date"])
    if module_name not in last_update.index:
        print("没有module：%s的更新记录信息" % module_name)
        return False, ""
    else:
        last_update_date = last_update.loc[module_name, ["date"]].date
        if date > last_update_date.date():
            return False, last_update_date.date().strftime("%Y-%m-%d")
        else:
            return True, last_update_date.date().strftime("%Y-%m-%d")


def update_txt_record(module_name, date=yesterday):
    '''
    更新运行目录下的last_update.csv文档记录的最后更新时间
    默认最后更新日期为昨天
    :param module_name:模块名
    :param date: 日期
    :return: 无
    '''
    last_update = pd.read_csv("last_update.csv", index_col=["module"])
    if module_name not in last_update.index:
        last_update.loc[module_name] = [date]
    else:
        last_update.loc[module_name, ["date"]] = date

    print(last_update)
    last_update.to_csv("last_update.csv")


if __name__ == "__main__":
    bret, strDate = is_last_update_ok('A')
    print(bret)
    print(strDate)
    bret, strDate = is_last_update_ok('B')
    print(bret)
    print(strDate)
    update_txt_record("B")

