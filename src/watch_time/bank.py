# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore, QtGui, QtWidgets
from quote_api import *

'''
银行股息率计算
接收主页面的更新股息率的信号
从quote_api接口获取实时行情数据
emit获取的实时行情数据到主页面，主页面更新银行信息表
'''


class BankThread(QtCore.QThread):
    df_bank_out = QtCore.pyqtSignal(pd.DataFrame)
    signal_df_ah_premium = QtCore.pyqtSignal(pd.DataFrame)

    def __init__(self, parent=None):
        super(BankThread, self).__init__(parent)
        self.is_running = True

        '''
        银行基本信息目录及文件名
        ts_code	    name
        000001.SZ	平安银行
        002142.SZ	宁波银行
        002807.SZ	江阴银行
        '''
        self.bank_basic_info_dir = r"C:\quanttime\src\watch_time\bank_info.csv"

        '''
        a_code	    name	hk_code
        002936.SZ	郑州银行	HK.6196
        600016.SH	民生银行	HK.1988
        600036.SH	招商银行	HK.3968
        '''
        self.bank_AH_dir = r'C:\quanttime\src\watch_time\bank_A_H.csv'

        # tushare connect context
        token = "17e7755e254f02cc312b8b7e22ded9a308924147f8546fdfbe653ba1"
        ts.set_token(token)
        # ts 授权
        self.pro = ts.pro_api()

# ================================================

    def update_bank_industry_table(self):
        '''
        slot:更新银行业信息表
        由主界面的银行信息更新按钮pushbutton的clicked signal触发，该slot执行
        需要读取tushare获取的所有股票的基本信息，从内获取industry==银行的股票，all_stock_info_ts.csv由维护程序定期维护
        本程序只是读取all_stock_info_ts.csv，不对该表文件进行维护
        1、读取stock基本信息目录（C:\quanttime\data\basic_info）下的all_stock_info_ts.csv文件，获取行业为银行的所有内容
        2、将该信息转存到程序运行目录（C:\quanttime\src\watch_time）下，命名为bank_info.csv
        :return:
        '''
        select_columns = ["ts_code", "name", "industry"]
        bank = pd.read_csv(r"C:\quanttime\data\basic_info\all_stock_info_ts.csv",
                           usecols=select_columns, encoding="gbk", index_col=["ts_code"])
        bank = bank[bank["industry"] == "银行"]
        bank[["name"]].to_csv(self.bank_basic_info_dir, encoding="gbk")

# ==============================================================

    def process_bank_dividend(self):
        '''
        处理银行的分红信息，该方法为slot，由主页面线程处理银行分红信息的pushbutton发射信号，触发该slot函数
        :return:
        '''
        bank = pd.read_csv(self.bank_basic_info_dir, encoding="gbk", index_col=["ts_code"])
        # 获取分红信息
        df_bank_dividend = self.get_dividend_by_tushare(bank.index.tolist())
        df_bank_dividend = df_bank_dividend.set_index("ts_code")
        df_bank_dividend = pd.merge(df_bank_dividend, bank, left_index=True, right_index=True)

        # 获取实时股价，先从通达信获取
        df_bank_price = get_quote_by_tdx(bank.index.tolist())
        if df_bank_price.empty:
            print("tdx获取实时行情失败，从tushare获取")
            df_bank_price = get_quote_by_ts(bank.index.tolist())
        # 如果从tushare也没有获取到实时行情则return
        if df_bank_price.empty:
            print("从tushare获取也失败")
            return
        df_bank_price['code'] = df_bank_price['code'].apply(self.code_add_market)
        df_bank_price = df_bank_price.set_index("code")
        df_bank = pd.merge(df_bank_dividend, df_bank_price, left_index=True, right_index=True)
        columns_need_2_float = ["cash_div_tax", "price"]
        df_bank[columns_need_2_float] = df_bank[columns_need_2_float].apply(pd.to_numeric)
        df_bank["div_rate"] = df_bank["cash_div_tax"] / df_bank["price"]
        df_bank = df_bank.sort_values(by=["div_rate"], ascending=False)
        df_bank["div_rate"] = df_bank["div_rate"].map(self.display_percent_format)
        print(df_bank)
        self.df_bank_out.emit(df_bank)

# ==============================================================
    def get_dividend_by_tushare(self, ts_code_list):
        '''
        tushare接口获取分红信息
        :param ts_code_list: code list，需要判断是否满足tushare的code格式要求
        :return: 包含分红信息的df
        '''
        # end_date:分红年度 cash_div_tax：每股分红税前 record_date：股权登记日 pay_date：派息日
        columns_name = ["ts_code", "end_date", "cash_div_tax", "record_date", "pay_date"]
        get_feilds = 'ts_code,end_date,cash_div_tax,record_date,pay_date'
        df_bank_dividend = pd.DataFrame(columns=columns_name)
        curr_year = datetime.today().year
        end_dividend_date = datetime(curr_year-1, 12, 31).date().strftime("%Y%m%d")

        # df_bank_dividend = self.pro.dividend(ts_code=ts_code_list.pop(0), fields=get_feilds)
        for ts_code in ts_code_list:
            df_tmp = self.pro.dividend(ts_code=ts_code, fields=get_feilds)
            if df_tmp.empty:
                continue
            # 只取第一行最新的记录，旧的分红记录不需要
            # df_tmp = df_tmp.loc[df_tmp.index[0], columns_name]
            df_tmp = df_tmp[df_tmp['end_date'] == end_dividend_date]
            df_tmp = df_tmp.iloc[0, :]
            df_bank_dividend = df_bank_dividend.append(df_tmp, ignore_index=True)
        return df_bank_dividend

# ===================================================
    def get_AH_premium(self):
        '''
        获取AH股折溢价情况
        A股与H股code从运行文件夹内的bank_A_H.csv获取
        :return:
        '''
        ah = pd.read_csv(self.bank_AH_dir, encoding="gbk")

        df_a_price = get_quote_by_futu(ah['a_code'].tolist())
        df_hk_price = get_quote_by_futu(ah['hk_code'].tolist())
        if not df_a_price.empty and not df_hk_price.empty:
            ah = pd.merge(ah, df_a_price, left_on='a_code', right_on='code')
            ah = ah.drop(columns=['code'])
            ah = pd.merge(ah, df_hk_price, left_on='hk_code', right_on='code', suffixes=['_a', '_h'])
            ah = ah.drop(columns=['code'])
            self.signal_df_ah_premium.emit(ah)
        else:
            self.signal_df_ah_premium.emit(pd.DataFrame())

# ========================================================
    @staticmethod
    def code_add_market(x):
        '''
        6位纯数字code添加代表市场信息的后缀，6--SH，0,3--SZ
        :param x:
        :return:
        '''
        x = str(x)
        if x[0] == '6':
            return x + '.SH'
        elif x[0] == '3':
            return x + '.SZ'
        elif x[0] == '0':
            return x + '.SZ'
        else:
            return '000000'

# =========================================
    @staticmethod
    def display_percent_format(x):
        '''
        功能：小数按照百分数%显示，保留两位小数
        '''
        try:
            data = float(x)
        except ValueError:
            print("input is not numberic")
            return 0

        return "%.2f%%" % (data * 100)

# ==============
if __name__ == "__main__":
    theBank = BankThread()
    df = theBank.get_dividend_by_tushare(["000001.SZ"])
    print(df)
