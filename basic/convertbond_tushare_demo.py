__author__ = 'Administrator'
import tushare as ts
import sys
import time
sys.path.append("C:\\quanttime\\src\\auto_hunter")
import getSinaquotation1 as getsina

data=getsina.getQuotation(["sz128014","sz128014"])
cons = ts.get_apis()

df=ts.new_cbonds()
#df_head = df.head(5)
#print df_head

start = time.time()
real_price=ts.quotes('128016',conn=cons)
elapsed_per=(time.time()-start)/10
print "ts.quotes comsume: %r"%(elapsed_per)
'''
([u'code', u'price', u'last_close', u'open', u'high', u'low', u'vol',
       u'cur_vol', u'amount', u's_vol', u'b_vol', u'bid1', u'ask1',
       u'bid_vol1', u'ask_vol1', u'bid2', u'ask2', u'bid_vol2', u'ask_vol2',
       u'bid3', u'ask3', u'bid_vol3', u'ask_vol3', u'bid4', u'ask4',
       u'bid_vol4', u'ask_vol4', u'bid5', u'ask5', u'bid_vol5', u'ask_vol5'],
      dtype='object')
'''
real_price=real_price.loc[:,['code','price','bid1','bid_vol1','ask1','ask_vol1']]
data=getsina.getQuotation(["sz128016"])
data=data.loc[:,['code','bid','b1','b1_v','a1','a1_v']]
stock=ts.get_realtime_quotes("601318")

print real_price
print "======================"
print data
print "==================="
print stock