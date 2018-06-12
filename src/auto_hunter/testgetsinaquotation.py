__author__ = 'Administrator'

import sys
sys.path.append("C:\\quanttime\\src\\auto_hunter")
import getSinaquotation1 as getsina

data=getsina.getQuotation(["sz128014","sz128014"])
print data

