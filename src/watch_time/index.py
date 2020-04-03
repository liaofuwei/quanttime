# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore, QtGui, QtWidgets
from quote_api import *

'''
指数轮动分析，包括标准轮动模型及plus版本
'''


class IndexThread(QtCore.QThread):
    signal_df_out = QtCore.pyqtSignal(pd.DataFrame)

    def __init__(self, parent=None):
        super(IndexThread, self).__init__(parent)
        self.is_running = True