__author__ = 'Administrator'

from jqdatasdk import *
import pandas as pd

import os
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
from futuquant import *
#授权
#auth('13811866763',"sam155")



quote_ctx = OpenQuoteContext(host='127.0.0.1',port=11111)
(ret2, dfsh) = quote_ctx.get_history_kline("SH.601088",start='2006-01-01',end="2018-07-12")
(ret1, dfhn) = quote_ctx.get_history_kline("SH.600011",start='2006-01-01',end="2018-07-12")
path = "C:\\quanttime\\src\\coal_electric\\data\\"
if ret1 == RET_OK:
    pathname =  path + "SH.600011" + ".csv"
    print(pathname)
    dfhn.to_csv(pathname)
else:
    print("get hisdata failed error reason:%r"%(dfhn))

if ret2 == RET_OK:
    pathname =  path + "SH.601088" + ".csv"
    print(pathname)
    dfsh.to_csv(pathname)
else:
    print("get hisdata failed error reason:%r"%(dfsh))

readpath =  path + "SH.601088" + ".csv"
zgsh = pd.read_csv(readpath,index_col = "time_key")
zgsh = zgsh[["code","close"]]
zgsh = zgsh.rename(columns={"close":"zhsh_close","code":"zgsh_code"})
readpath = path + "SH.600011" + ".csv"
hngj = pd.read_csv(readpath,index_col = "time_key")
hngj = hngj[["code","close"]]
hngj = hngj.rename(columns={"close":"hngj_close","code":"hngj_code"})


df = pd.merge(zgsh, hngj, left_index=True, right_index=True)
df.plot(secondary_y=["hngj_close"])
plt.show()