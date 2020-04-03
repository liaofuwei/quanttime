#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import sys
from datetime import datetime

sys.path.append('C:\\quanttime\\src\\index\\')#index维护src目录
sys.path.append('C:\\quanttime\\src\\regular_maintenance\\')
sys.path.append('C:\\quanttime\\src\\gold\\')# gold，silver的K线数据维护
sys.path.append(('C:\\quanttime\\src\\comm'))

from finance_maintence import financeMaintenance
import index_maintenance as index_update
import basic_info_maintence as bm
import get_SH_future_gold_hisdata as shgold
import last_update
'''
当前日常维护的数据：
1、valuation数据
2、指数数据
'''

if __name__ == "__main__":
    #交易日期更新,已更新到2019年，暂时都不用更新
    #bm.getAllTradeDay()

    # valuation update
    finance_update = financeMaintenance()
    finance_update.update()

    #==================================
    index_update.update_jq_index()
    index_update.get_tushare_index()
    index_update.tushare_index_PEPB_info()
    index_update.maintenance_index_valuation()
    index_update.get_tushare_index_basic_info()
    index_update.update_sw_index_valuation_by_opendatatool()
    index_update.get_sw_index_daily_by_opendatatool()

    today = datetime.today()
    if today.date().day == 32: #权重在每月的28日在更新，属于月度数据
        index_update.get_index_weights_from_jq()

    #SH gold silver k hisdata
    shgold.sh_future_gold_silver_K_data()
    shgold.sh_future_gold_silver_mins_data()

    # 更新tushare的valuation
    # finance_update.update_valuation_by_ts()
