# -*-coding:utf-8 -*-
__author__ = 'Administrator'

from PyQt5 import QtCore
import pandas as pd
from datetime import datetime
import numpy as np
import os

"""
ts_code:ts代码
ann_date:公告日期
end_date:报告期
eps:每股收益
dt_eps:稀释每股收益
total_revenue_ps:每股营业总收入
revenue_ps:每股营业收入
capital_rese_ps:每股资本公积
surplus_rese_ps:每股盈余公积
undist_profit_ps:每股未分配利润
extra_item:非经常损益
profit_dedt:扣非净利润
gross_margin:毛利
current_ratio:流动比率
quick_ratio:速动比率
cash_ratio:保守速动比率
ar_turn:应收账款周转率
ca_turn:流动资产周转率
fa_turn:固定资产周转率
assets_turn:总资产周转率
op_income:营业活动净收益
ebit:息税前利润
ebitda:息税折旧摊销前利润
fcff:企业自由现金流
fcfe:股权自由现金流
current_exint:无息流动负债
noncurrent_exint:无息非流动负债
interestdebt:带息负债
netdebt:净债务
tangible_asset:有形资产
working_capital:营运资金
networking_capital:营运流动资本
invest_capital:投资资本
retained_earnings:留存收益
diluted2_eps:期末摊薄每股收益
bps:每股净资产
ocfps:每股经营活动产生的现金流净额
retainedps:每股留存收益
cfps:每股现金流量净额
ebit_ps:每股息税前利润
fcff_ps:每股企业自由现金流量
fcfe_ps:每股股东自由现金流量
netprofit_margin:销售净利润率
grossprofit_margin:销售毛利率
cogs_of_sales:销售成本率
expense_of_sales:销售期间费用率
profit_to_gr:净利润/营业总收入
saleexp_to_gr:销售费用/营业总收入
adminexp_of_gr:管理费用/营业总收入
finaexp_of_gr:财务费用/营业总收入
impai_ttm:资产减值损失/营业总收入
gc_of_gr:营业总成本/营业总收入
op_of_gr:营业利润/营业总收入
ebit_of_gr:息税前利润/营业总收入
roe:净资产收益率
roe_waa:加权平均净资产收益率
roe_dt:扣非后roe
roa:总资产报酬率
npta:总资产净利润
roic:投入资本回报率
roe_yearly:年化roe
roa2_yearly:年化roa
debt_to_assets:资产负债率
assets_to_eqt:权益乘数
dp_assets_to_eqt:权益乘数（杜邦分析）
ca_to_assets:流动资产/总资产
nca_to_assets:非流动资产/总资产
tbassets_to_totalassets:有形资产/总资产
int_to_talcap:带息负债/全部投入资本
eqt_to_talcapital:归属于母公司股东权益/全部投入资本
currentdebt_to_debt:流动负债/负债总计
longdeb_to_debt:非流动负债/负债总计
ocf_to_shortdebt:经营活动产生的现金流量净额/流动负债
debt_to_eqt:产权比率
eqt_to_debt:归属于母公司股东权益/负债总计
eqt_to_interestdebt:归属于母公司股东权益/带息负债
tangibleasset_to_debt:有形资产/债务合计
tangasset_to_intdebt:有形资产/带息债务
tangibleasset_to_netdebt:有形资产/净债务
ocf_to_debt:经营活动产生的现金流动净额/负债总计
turn_days:营业周期
roa_yearly:年化总资产净利率
roa_dp:总资产净利率（杜邦分析）
fixed_assets:固定资产合计
profit_to_op:利润总额/营收
q_saleexp_to_gr:销售费用/营收（单季度）
q_gc_to_gr:营业总成本/营收（单季度）
q_roe:单季度roe
q_dt_roe:扣非单季度roe
q_npta:单季度总资产净利
q_ocf_to_sales:经营活动产生的现金流量净额/营收（单季度）
basic_eps_yoy:基本每股收益同比增长%
dt_eps_yoy:稀释eps同比增长%
cfps_yoy:每股经营活动产生的现金流量净额同比增长%
op_yoy:营业利润同比增长%
ebt_yoy:利润总额同比增长%
netprofit_yoy:归属于母公司股东的净利润同比增长%
dt_netprofit_yoy:归属于母公司股东的净利润同比增长%（扣非）
ocf_yoy:经营活动产生的现金流量净额同比增长%
roe_yoy:roe同比增长%
bps_yoy:每股净资产同比增长%
assets_yoy:资产总计同比增长%
eqt_yoy:归属于母公司的股东权益相对年初增长率%
tr_yoy:营业总收入同比增长%
or_yoy:营业收入同比增长%
q_sales_yoy:营业收入同比增长%（单季度）
q_op_qoq:营业利润环比增长%（单季度）
equity_yoy:净资产同比增长
"""


class FinanceAnalyseThread(QtCore.QThread):
    df_select_indicator_out = QtCore.pyqtSignal(pd.DataFrame)

    def __init__(self, parent=None):
        super(FinanceAnalyseThread, self).__init__(parent)
        self.is_running = False
        self.ts_indicator = "C:\\quanttime\\data\\finance\\ts\\indicator\\"
        self.ts_stock_name = "C:\\quanttime\\data\\basic_info\\all_stock_info_ts.csv"
        self.ts_valuation = "C:\\quanttime\\data\\finance\\ts\\valuation\\"

        self.jq_indicator_dir = "C:\\quanttime\\data\\finance\\indicator\\"
        self.jq_valuation_dir = "C:\\quanttime\\data\\finance\\valuation\\"
        self.jq_stock_info = r"C:\quanttime\data\basic_info\all_stock_info.csv"

        self.dic_stock2name = {}
        self.get_stock_name()

# ===================================================
    def get_stock_name(self):
        """
        获取所有stock name
        :return:
        """
        ts_stock_basic = r'C:\quanttime\data\basic_info\all_stock_info_ts.csv'
        df = pd.read_csv(ts_stock_basic, index_col=["ts_code"], encoding='gbk')
        for row in df.itertuples():
            self.dic_stock2name[row.Index] = row.name
        # print(self.dic_stock2name)

# ====================================================
    def get_chinese_name_from_shortcut(self, shortcut_name):
        """
        通过缩写名称获取中文名称，如roe，净资产收益率
        在C:\\quanttime\\data\\finance\\ts\\indicator\\目录内有一个field_name.txt的文件，有对应的名称说明
        :param shortcut_name:
        :return:
        """
        indicator_name = pd.read_csv(self.ts_indicator + 'field_name.txt', encoding='gbk')
        indicator_name['field_name_en'] = indicator_name['ts_code:ts代码'].apply(lambda x: str(x).split(':')[0])
        indicator_name['field_name_en_cn'] = indicator_name['ts_code:ts代码'].apply(lambda x: str(x).split(':')[1])
        indicator_name = indicator_name.set_index("field_name_en")
        indicator_name = indicator_name[["field_name_en_cn"]]
        if shortcut_name in indicator_name.index:
            return indicator_name.loc[shortcut_name, ["field_name_en_cn"]]["field_name_en_cn"]
        else:
            return " "
# ================================

    def calc_indicator_mean(self, code, nyear, indicator_name):
        """
        计算指标的n年平均值，例如计算roe的5年平均等
        :param code: stock code
        :param nyear: 年数
        :param indicator_name:指标名称
        :return:float
        """

        current_year = datetime.today().year
        analyse_dates = pd.date_range(end=str(current_year), periods=nyear, freq='Y')
        code = str(code)
        if len(code) != 6:
            print("code error,code=%s" % code)
            return -1
        if code[0] == '6':
            code = code + '.SH'
        elif (code[0] == '0') or (code[0] == '3'):
            code = code + '.SZ'
        else:
            print("code error,code=%s" % code)
            return -1

        file_path = self.ts_indicator + str(code) + '.csv'
        if not os.path.exists(file_path):
            print("stock:%s indicator file not exist" % code)
            return -1
        list_calc_list = []
        df_tmp_indicator = pd.read_csv(file_path, index_col=["end_date"], parse_dates=True)
        for anlayse_year in analyse_dates:
            if anlayse_year in df_tmp_indicator.index:
                list_calc_list.append(df_tmp_indicator.loc[anlayse_year, [indicator_name]][indicator_name])
        if len(list_calc_list) < nyear:
            print("实际数据年数少于需要计算的年数")
            return -1

        return round(sum(list_calc_list) / len(list_calc_list), 2)

# ============================================
    def select_ts_target_stock(self, list_indicator):
        """
        根据主页面发射的指标要求，筛选出股票
        :param list_indicator: signal:[pe,pb,roe,净利润增速]的顺序，如果为''说明没有把该项设置为筛选项
        :return:
        """
        print(list_indicator)
        if len(list_indicator) != 4:
            return

        pe = 0
        pb = 0
        roe = 0
        net_profit_increase = 0
        curr_pe_ttm = "-"

        all_indicator_file = os.listdir(self.ts_indicator)
        # 只挑选出股票csv指标文件
        all_csv_file = [d for d in all_indicator_file if len(d) == 13 and 'csv' in d]

        list_result = []
        for stock in all_csv_file:
            file_path = self.ts_indicator + stock
            df_indicator = pd.read_csv(file_path, index_col=["end_date"], parse_dates=True, infer_datetime_format=True)
            df_indicator = df_indicator.fillna(-1)
            # 使用最新年报公布的roe年化数据
            last_year_report_date = pd.Timestamp(datetime.today().year-1, 12, 31)
            if last_year_report_date not in df_indicator.index:
                print("%s indicator表不是最新，需要更新" % stock)
                continue

            curr_roe_yearly = df_indicator.loc[last_year_report_date, ['roe_dt']]['roe_dt']
            curr_net_profit_increase = df_indicator.iloc[-1, [df_indicator.columns.get_loc('dt_netprofit_yoy')]]['dt_netprofit_yoy']
            ts_code = df_indicator.iloc[-1, [df_indicator.columns.get_loc('ts_code')]]['ts_code']
            try:
                stock_name = self.dic_stock2name[ts_code]
            except KeyError:
                stock_name = "-"
            # print(curr_roe_yearly)
            if list_indicator[2] != '':
                roe = float(list_indicator[2])
                if curr_roe_yearly <= roe:
                    # roe不符合条件
                    continue
            if list_indicator[3] != '':
                net_profit_increase = float(list_indicator[3])
                # 扣非净利润增速
                if curr_net_profit_increase <= net_profit_increase:
                    # 净利润增速不符合条件
                    continue
            ts_valuation_file = self.ts_valuation + str(ts_code) + '.csv'
            if os.path.exists(ts_valuation_file):
                df_valuation = pd.read_csv(ts_valuation_file, index_col=["trade_date"], parse_dates=True)
                # 将空值填-1，如果最后的pb，pe数据为空值，暂时认为数据不可靠，排除，留待后续校验数据
                df_valuation = df_valuation.fillna(-1)
                curr_pe = df_valuation.iloc[-1, [df_valuation.columns.get_loc('pe')]]['pe']
                curr_pb = df_valuation.iloc[-1, [df_valuation.columns.get_loc('pb')]]['pb']
                if (curr_pb < 0) or (curr_pe < 0):
                    continue
                curr_pe_ttm = df_valuation.iloc[-1, [df_valuation.columns.get_loc('pe_ttm')]]['pe_ttm']
                if list_indicator[0] != '':
                    pe = float(list_indicator[0])
                    if curr_pe > pe:
                        # pe不符合
                        continue
                if list_indicator[1] != '':
                    pb = float(list_indicator[1])
                    if curr_pb > pb:
                        # pb不符合
                        continue
            roe_3 = self.calc_indicator_mean(ts_code[0:6], 3, "roe_dt")
            roe_5 = self.calc_indicator_mean(ts_code[0:6], 5, "roe_dt")
            dt_netprofit_yoy_5 = self.calc_indicator_mean(ts_code[0:6], 5, "dt_netprofit_yoy")
            dt_netprofit_yoy_3 = self.calc_indicator_mean(ts_code[0:6], 3, "dt_netprofit_yoy")
            debt_asset = df_indicator.iloc[-1, [df_indicator.columns.get_loc('debt_to_assets')]]['debt_to_assets']
            net_cash_flow = df_indicator.iloc[-1, [df_indicator.columns.get_loc('ocfps')]]['ocfps']

            list_result.append([ts_code[0:6], stock_name, curr_pb, curr_pe, curr_pe_ttm, roe_3, roe_5, curr_roe_yearly,
                                curr_net_profit_increase, dt_netprofit_yoy_3, dt_netprofit_yoy_5, debt_asset,
                                net_cash_flow])
        columns_name = ['code', 'name', 'pb', 'pe', 'pe_ttm', 'roe3', 'roe5', 'roe', 'net_profit', 'net_profit3',
                        'net_profit5', 'debt_asset', 'ocfps']
        df_select_result = pd.DataFrame(data=list_result, columns=columns_name)
        print(df_select_result)
        self.df_select_indicator_out.emit(df_select_result)
# ===========================================

    def select_jq_target_stock(self, list_indicator):
        """
        根据主页面发射的指标要求，筛选出股票, 使用joinquant指标筛选
        :param list_indicator: signal:[pe,pb,roe,净利润增速]的顺序，如果为''说明没有把该项设置为筛选项
        :return:
        """
        print(list_indicator)
        if len(list_indicator) != 4:
            return
        pe_comp_value = list_indicator[0]
        pb_comp_value = list_indicator[1]
        roe_comp_value = list_indicator[2]
        net_profit_comp_value = list_indicator[3]

        df_stock_info = pd.read_csv(self.jq_stock_info, encoding='gbk', index_col=["code"])
        df_stock_info = df_stock_info[df_stock_info["end_date"] == "2200-01-01"]
        select_index = df_stock_info.index.tolist()
        print(select_index)
        ret_pbpe = {}
        ret_profit = {}
        ret_roe = {}
        ret_result = {}

        if roe_comp_value != '':
            ret_roe = self.select_jq_roe_stock(select_index, float(roe_comp_value))
            select_index = ret_roe.keys()
        if (pe_comp_value != '') and (pb_comp_value != ''):
            ret_pbpe = self.select_jq_pepb_stock(select_index, [float(pe_comp_value), float(pb_comp_value)])
            select_index = ret_pbpe.keys()
        else:
            if pe_comp_value != '':
                ret_pbpe = self.select_jq_pepb_stock(select_index, [float(pe_comp_value), -1])
                select_index = ret_pbpe.keys()
            if pb_comp_value != '':
                ret_pbpe = self.select_jq_pepb_stock(select_index, [-1, float(pb_comp_value)])
                select_index = ret_pbpe.keys()

        if (len(ret_roe.keys()) != 0) and (len(ret_pbpe.keys()) != 0):
            for tmp_key in select_index:
                if tmp_key in ret_roe.keys():
                    ret_result[tmp_key] = ret_roe[tmp_key] + ret_pbpe[tmp_key]
                else:
                    ret_result[tmp_key] = [-1, -1] + ret_pbpe[tmp_key]
        elif (len(ret_roe.keys()) == 0) and (len(ret_pbpe.keys()) != 0):
            for tmp_key in select_index:
                ret_result[tmp_key] = [-1, -1] + ret_pbpe[tmp_key]
        elif (len(ret_roe.keys()) != 0) and (len(ret_pbpe.keys()) == 0):
            for tmp_key in select_index:
                ret_result[tmp_key] = ret_roe[tmp_key] + [-1, -1]
        else:
            print("?")

        if net_profit_comp_value != '':
            ret_profit = self.select_jq_indicator_stock(select_index, True, "inc_net_profit_year_on_year",
                                                 float(net_profit_comp_value))
            select_index = ret_profit.keys()
        if (len(ret_profit) != 0) and (len(ret_result) != 0):
            for tmp_key in ret_result.keys():
                if tmp_key in ret_profit.keys():
                    ret_result[tmp_key] = ret_result[tmp_key] + [ret_profit[tmp_key]]
        elif (len(ret_profit) == 0) and (len(ret_result) != 0):
            for tmp_key in ret_result.keys():
                ret_result[tmp_key] = ret_result[tmp_key] + [-1]
        elif (len(ret_profit) != 0) and (len(ret_result) == 0):
            for tmp_key in ret_profit.keys():
                ret_result[tmp_key] = [-1, -1, -1, -1] + ret_profit[tmp_key]
        else:
            print("2?")

        return ret_result

# ================================

    def select_jq_roe_stock(self, list_stocks, indicator):
        """
        筛选符合roe指标的stock
        :param list_stocks: 股票list
        :param indicator:指标值
        :return: dic 符合条件的股票,dic:{code:[roe5,roe3],……}
        """
        # inc_return：扣非roe
        analyse_indicator = ['code', 'statDate', 'inc_return']
        dic_result = {}
        for code in list_stocks:
            jq_indicator_file = self.jq_indicator_dir + str(code[0:6]) + "_year" + str(code[6:11]) + '.csv'
            if os.path.exists(jq_indicator_file):
                df_indicator = pd.read_csv(jq_indicator_file, index_col=['statDate'], usecols=analyse_indicator,
                                           parse_dates=True)
                # df_indicator = df_indicator[~df_indicator.reset_index().duplicated().values]#去重
                if len(df_indicator) >= 5:
                    roe_5 = df_indicator.iloc[-5:, df_indicator.columns.get_loc('inc_return')].mean()
                    roe_3 = df_indicator.iloc[-3:, df_indicator.columns.get_loc('inc_return')].mean()
                    if roe_5 >= indicator:
                        dic_result[code] = [roe_5, roe_3]

                elif 3 <= len(df_indicator) < 5:
                    roe_3 = df_indicator.iloc[-3:, df_indicator.columns.get_loc('inc_return')].mean()
                    if roe_3 >= indicator:
                        dic_result[code] = [roe_5, roe_3]
                else:
                    continue
        return dic_result

# ================================

    def select_jq_pepb_stock(self, list_stocks, indicator):
        """
        筛选pb，pe符合条件的股票
        :param list_stocks: 股票list
        :param indicator: 指标值 list：[pe,pb] 如果pe=-1，pb=-1则说明不用刷选，只计算当前pbpe的百分位
        :return:dic 符合条件的股票,dic:{code:[pe,pb],……}
        """
        if len(indicator) != 2:
            return
        dic_result = {}
        valuation_use_col = ["day", "code", "pe_ratio", "pb_ratio"]
        for code in list_stocks:
            jq_valuation_file = self.jq_valuation_dir + str(code) + '.csv'
            if os.path.exists(jq_valuation_file):
                df_valuation = pd.read_csv(jq_valuation_file, usecols=valuation_use_col)
                df_valuation = df_valuation.drop_duplicates("day")
                df_valuation = df_valuation.set_index("day")
                df_valuation = df_valuation.fillna(-1)
                df_pe = df_valuation.sort_values(by="pe_ratio")
                df_pe = df_pe[df_pe["pe_ratio"] > 0]
                df_pb = df_valuation.sort_values(by="pb_ratio")
                df_pb = df_pb[df_pb["pb_ratio"] > 0]
                # 按计算指标的在整个序列的排序位置来标记分位数
                try:
                    pos_pe = round(df_pe.index.get_loc(df_valuation.index[-1]) / len(df_valuation), 2)
                    pos_pb = round(df_pb.index.get_loc(df_valuation.index[-1]) / len(df_valuation), 2)
                except KeyError:
                    # 如果pe<0,会被筛选掉，df_pe.index中就没有
                    continue
                # print(code)
                # print(pos_pe, pos_pb)
                if (indicator[0] == -1) and (indicator[1] == -1):
                    # 没有设置指标，不要刷选
                    dic_result[code] = [pos_pe, pos_pb]
                elif (indicator[0] == -1) and (indicator[1] != -1):
                    # 只筛选符合要求的pb，对pe不进行比较筛选
                    if pos_pb <= indicator[1]:
                        dic_result[code] = [pos_pe, pos_pb]
                elif (indicator[0] != -1) and (indicator[1] == -1):
                    # 只筛选符合要求的pe，对pb不进行比较筛选
                    if pos_pe <= indicator[0]:
                        dic_result[code] = [pos_pe, pos_pb]
                elif (indicator[0] != -1) and (indicator[1] != -1):
                    # 对pb，pe都进行比较筛选
                    if (pos_pe <= indicator[0]) and (pos_pb <= indicator[1]):
                        dic_result[code] = [pos_pe, pos_pb]
                else:
                    print("?")
        return dic_result

# =======================================
    def select_jq_indicator_stock(self, list_stocks, byear_value, indicator_name, indicator_value):
        """
        筛选pb，pe符合条件的股票
        :param list_stocks: 股票list
        :param byear_value: 是否去年报值，true取年报值，false取最近的报告值
        :param indicator_name:指标名称
        :param indicator_value: 指标值 -1代表不刷新，返回全部获取到的指标
        :return:dic 符合条件的股票,dic:{code:indicator}
        """
        analyse_indicator = ['code', 'statDate', indicator_name]
        dic_result = {}
        for code in list_stocks:
            if byear_value:
                jq_indicator_file = self.jq_indicator_dir + str(code[0:6]) + "_year" + str(code[6:11]) + '.csv'
            else:
                jq_indicator_file = self.jq_indicator_dir + str(code) + '.csv'
            if os.path.exists(jq_indicator_file):
                df_indicator = pd.read_csv(jq_indicator_file, index_col=['statDate'], usecols=analyse_indicator,
                                           parse_dates=True)
                if df_indicator.empty:
                    continue
                ind_value = df_indicator.iloc[-1, df_indicator.columns.get_loc(indicator_name)]
                if indicator_value == -1:
                    dic_result[code] = ind_value
                elif ind_value >= indicator_value:
                    dic_result[code] = ind_value
                else:
                    continue
        return dic_result


# =======================================

if __name__ == "__main__":
    theThread = FinanceAnalyseThread()
    #ret = theThread.get_chinese_name_from_shortcut("roe")
    #ret = theThread.calc_indicator_mean("002415", 5, "roe")
    #theThread.select_ts_target_stock(['', 1.2, 20, ''])
    #theThread.get_stock_name()
    #print(ret)

    file_dir = r"C:\quanttime\data\basic_info\all_stock_info.csv"
    df_stock_info = pd.read_csv(file_dir, encoding='gbk', index_col=["code"])
    df_stock_info = df_stock_info[df_stock_info["end_date"] == "2200-01-01"]
    #ret = theThread.select_jq_roe_stock(df_stock_info.index, 20)
    # ret = theThread.select_jq_pepb_stock(df_stock_info.index, [0.1, 0.1])
    # ret = theThread.select_jq_indicator_stock(df_stock_info.index, True, "inc_net_profit_year_on_year", 40)
    ret = theThread.select_jq_target_stock(["", 1.2, 15, ""])
    print(ret)