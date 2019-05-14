# -*-coding:utf-8 -*-
__author__ = 'Administrator'

import pandas as pd
from datetime import datetime, timedelta

yesterday = datetime.today().date() - timedelta(days=1)


def is_last_update_ok(date=yesterday):
    '''
    判断是否已经更新过
    程序获取更新目录价内的last_update.txt文件，读取时间
    与输入的日期做比较判断是否当前已经更新
    :param date: 日期
    :return: true--已经更新，false--无更新或者当天更新失败
    '''
    last_update = pd.read_csv("last_update.txt", delim_whitespace=True,
                              names=["date"], index_col=["date"], parse_dates=True)

    if not last_update.index.empty:
        if date > last_update.index[0].date():
            return False
        else:
            return True
    else:
        print("检查last_update.txt文档")
        return False


def update_txt_record(date=yesterday):
    '''
    更新运行目录下的last_update.txt文档记录的最后更新时间
    默认最后更新日期为昨天
    :param date: 日期
    :return: 无
    '''
    index = [date]
    record = pd.DataFrame(index=index)
    print(record)
    record.to_csv("last_update.txt", header=False)


if __name__ == "__main__":
    print(is_last_update_ok())
    update_txt_record()

