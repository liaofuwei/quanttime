#-*-coding:utf-8 -*-
__author__ = 'Administrator'

import sys
from datetime import datetime

# gold，silver的K线数据维护
sys.path.append('C:\\quanttime\\src\\gold\\')

import get_SH_future_gold_hisdata as shgold

'''
当前日常维护的数据：
黄金
'''

if __name__ == "__main__":
    # SH gold silver k hisdata
    shgold.sh_future_gold_silver_K_data()
    shgold.sh_future_gold_silver_mins_data()

