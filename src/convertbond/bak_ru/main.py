#-*-coding:utf-8 -*-
__author__ = 'Administrator'



import sys
import getRealTimeTradeEvent
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidget ,QTableWidgetItem


if __name__ == "__main__":
    app = QApplication(sys.argv)
    MainWidow = QMainWindow()
    ui = RealTimeTradeEvent()
    ui.setupUi(MainWidow)

    newItem = QTableWidgetItem('1234')
    ui.setItemContext(1,1,newItem)

    MainWidow.show()
    sys.exit(app.exec_())


