# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'ahdogMainWindows.ui'
#
# Created by: PyQt5 UI code generator 5.11.3
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_ahdog(object):
    def setupUi(self, ahdog):
        ahdog.setObjectName("ahdog")
        ahdog.resize(1662, 894)
        self.centralwidget = QtWidgets.QWidget(ahdog)
        self.centralwidget.setObjectName("centralwidget")
        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setGeometry(QtCore.QRect(20, 30, 1581, 841))
        self.tabWidget.setObjectName("tabWidget")
        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")
        self.tableWidget = QtWidgets.QTableWidget(self.tab)
        self.tableWidget.setGeometry(QtCore.QRect(40, 50, 1431, 631))
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(0)
        self.tableWidget.setRowCount(0)
        self.tabWidget.addTab(self.tab, "")
        self.tab_2 = QtWidgets.QWidget()
        self.tab_2.setObjectName("tab_2")
        self.tableWidget_2 = QtWidgets.QTableWidget(self.tab_2)
        self.tableWidget_2.setGeometry(QtCore.QRect(40, 70, 551, 192))
        self.tableWidget_2.setObjectName("tableWidget_2")
        self.tableWidget_2.setColumnCount(5)
        self.tableWidget_2.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_2.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_2.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_2.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_2.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_2.setHorizontalHeaderItem(4, item)
        self.tableWidget_3 = QtWidgets.QTableWidget(self.tab_2)
        self.tableWidget_3.setGeometry(QtCore.QRect(680, 70, 861, 192))
        self.tableWidget_3.setObjectName("tableWidget_3")
        self.tableWidget_3.setColumnCount(8)
        self.tableWidget_3.setRowCount(0)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(5, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(6, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget_3.setHorizontalHeaderItem(7, item)
        self.comboBox = QtWidgets.QComboBox(self.tab_2)
        self.comboBox.setGeometry(QtCore.QRect(380, 20, 211, 41))
        self.comboBox.setObjectName("comboBox")
        self.tabWidget.addTab(self.tab_2, "")
        ahdog.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(ahdog)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1662, 23))
        self.menubar.setObjectName("menubar")
        ahdog.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(ahdog)
        self.statusbar.setObjectName("statusbar")
        ahdog.setStatusBar(self.statusbar)

        self.retranslateUi(ahdog)
        self.tabWidget.setCurrentIndex(1)
        QtCore.QMetaObject.connectSlotsByName(ahdog)

    def retranslateUi(self, ahdog):
        _translate = QtCore.QCoreApplication.translate
        ahdog.setWindowTitle(_translate("ahdog", "MainWindow"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("ahdog", "概览"))
        item = self.tableWidget_2.horizontalHeaderItem(0)
        item.setText(_translate("ahdog", "name"))
        item = self.tableWidget_2.horizontalHeaderItem(1)
        item.setText(_translate("ahdog", "Acode"))
        item = self.tableWidget_2.horizontalHeaderItem(2)
        item.setText(_translate("ahdog", "当前比价"))
        item = self.tableWidget_2.horizontalHeaderItem(3)
        item.setText(_translate("ahdog", "Aprice"))
        item = self.tableWidget_2.horizontalHeaderItem(4)
        item.setText(_translate("ahdog", "Hprice"))
        item = self.tableWidget_3.horizontalHeaderItem(0)
        item.setText(_translate("ahdog", "均值"))
        item = self.tableWidget_3.horizontalHeaderItem(1)
        item.setText(_translate("ahdog", "5%分位"))
        item = self.tableWidget_3.horizontalHeaderItem(2)
        item.setText(_translate("ahdog", "10%分位"))
        item = self.tableWidget_3.horizontalHeaderItem(3)
        item.setText(_translate("ahdog", "75%分位"))
        item = self.tableWidget_3.horizontalHeaderItem(4)
        item.setText(_translate("ahdog", "90%分位"))
        item = self.tableWidget_3.horizontalHeaderItem(5)
        item.setText(_translate("ahdog", "max"))
        item = self.tableWidget_3.horizontalHeaderItem(6)
        item.setText(_translate("ahdog", "min"))
        item = self.tableWidget_3.horizontalHeaderItem(7)
        item.setText(_translate("ahdog", "std"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("ahdog", "细节"))


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ahdog = QtWidgets.QMainWindow()
    ui = Ui_ahdog()
    ui.setupUi(ahdog)
    ahdog.show()
    sys.exit(app.exec_())
