# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'monitorwindows.ui'
#
# Created by: PyQt5 UI code generator 5.6
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_monitorMainWindow(object):
    def setupUi(self, monitorMainWindow):
        monitorMainWindow.setObjectName("monitorMainWindow")
        monitorMainWindow.resize(800, 600)
        self.centralwidget = QtWidgets.QWidget(monitorMainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.bondcodelabel = QtWidgets.QLabel(self.centralwidget)
        self.bondcodelabel.setGeometry(QtCore.QRect(80, 100, 81, 21))
        self.bondcodelabel.setObjectName("bondcodelabel")
        self.bondcodelineEdit = QtWidgets.QLineEdit(self.centralwidget)
        self.bondcodelineEdit.setGeometry(QtCore.QRect(130, 100, 113, 20))
        self.bondcodelineEdit.setObjectName("bondcodelineEdit")
        self.stockcodelabel = QtWidgets.QLabel(self.centralwidget)
        self.stockcodelabel.setGeometry(QtCore.QRect(270, 100, 81, 21))
        self.stockcodelabel.setObjectName("stockcodelabel")
        self.stockcodelineEdit = QtWidgets.QLineEdit(self.centralwidget)
        self.stockcodelineEdit.setGeometry(QtCore.QRect(340, 100, 113, 20))
        self.stockcodelineEdit.setObjectName("stockcodelineEdit")
        self.tableWidget = QtWidgets.QTableWidget(self.centralwidget)
        self.tableWidget.setGeometry(QtCore.QRect(80, 170, 521, 192))
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(5)
        self.tableWidget.setRowCount(5)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setVerticalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setVerticalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setVerticalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setVerticalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setVerticalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setItem(0, 0, item)
        monitorMainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(monitorMainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 800, 23))
        self.menubar.setObjectName("menubar")
        monitorMainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(monitorMainWindow)
        self.statusbar.setObjectName("statusbar")
        monitorMainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(monitorMainWindow)
        QtCore.QMetaObject.connectSlotsByName(monitorMainWindow)

    def retranslateUi(self, monitorMainWindow):
        _translate = QtCore.QCoreApplication.translate
        monitorMainWindow.setWindowTitle(_translate("monitorMainWindow", "MainWindow"))
        self.bondcodelabel.setText(_translate("monitorMainWindow", "bondcode:"))
        self.stockcodelabel.setText(_translate("monitorMainWindow", "stockcode"))
        item = self.tableWidget.verticalHeaderItem(0)
        item.setText(_translate("monitorMainWindow", "1"))
        item = self.tableWidget.verticalHeaderItem(1)
        item.setText(_translate("monitorMainWindow", "2"))
        item = self.tableWidget.verticalHeaderItem(2)
        item.setText(_translate("monitorMainWindow", "3"))
        item = self.tableWidget.verticalHeaderItem(3)
        item.setText(_translate("monitorMainWindow", "4"))
        item = self.tableWidget.verticalHeaderItem(4)
        item.setText(_translate("monitorMainWindow", "6"))
        item = self.tableWidget.horizontalHeaderItem(0)
        item.setText(_translate("monitorMainWindow", "bondcode"))
        item = self.tableWidget.horizontalHeaderItem(1)
        item.setText(_translate("monitorMainWindow", "bondname"))
        item = self.tableWidget.horizontalHeaderItem(2)
        item.setText(_translate("monitorMainWindow", "stockcode"))
        item = self.tableWidget.horizontalHeaderItem(3)
        item.setText(_translate("monitorMainWindow", "stockname"))
        item = self.tableWidget.horizontalHeaderItem(4)
        item.setText(_translate("monitorMainWindow", "discount(%)"))
        __sortingEnabled = self.tableWidget.isSortingEnabled()
        self.tableWidget.setSortingEnabled(False)
        self.tableWidget.setSortingEnabled(__sortingEnabled)

    def setTableCellContext(self,i,j,context):
        _translate = QtCore.QCoreApplication.translate
        item = QtWidgets.QTableWidgetItem(context)
        self.tableWidget.setItem(i,j,item)

    def showTable(self):
        self.tableWidget.show()