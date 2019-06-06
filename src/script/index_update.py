#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import sys
from datetime import datetime

sys.path.append('C:\\quanttime\\src\\index\\')#index维护src目录
import index_maintenance as index_update

'''
当前日常维护的数据：
2、指数数据
'''

if __name__ == "__main__":
    index_update.update_jq_index()
    index_update.get_tushare_index()
    index_update.tushare_index_PEPB_info()
    index_update.maintenance_index_valuation()
    index_update.get_tushare_index_basic_info()
    index_update.update_sw_index_valuation_by_opendatatool()
    index_update.get_sw_index_daily_by_opendatatool()

