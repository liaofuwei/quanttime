#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import sys
from datetime import datetime

sys.path.append('C:\\quanttime\\src\\regular_maintenance\\')
from finance_maintence import financeMaintenance

'''
当前日常维护的数据：
jq的按日获取全部valuation，后更新方式

'''

if __name__ == "__main__":
    finance_update = financeMaintenance()
    finance_update.update_valuation_by_jq_day()

