import time
from datetime import datetime, date, timedelta
from Optimizer_Backtesting.PDF.PDFCreator import PDFCreator
import matplotlib as mpl
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os
import warnings
import time
import chinese_calendar as cal
from scipy.io import loadmat
mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei']
mpl.rcParams['axes.unicode_minus'] = False
warnings.filterwarnings("ignore")
import sys
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import json
global source,config_path
def source_getting():
    """
    获取数据源配置

    Returns:
        str: 数据源模式（'local' 或 'sql'）
    """
    try:
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(current_dir, 'global_setting\\optimizer_path_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        source = config_data['components']['data_source']['mode']
    except Exception as e:
        print(f"获取配置出错: {str(e)}")
        source = 'local'
    return source,config_path
source,config_path= source_getting()
class Back_testing:
    def __init__(self):
        self.df_index_return=self.index_return_withdraw()
        self.df_stock_return=self.stock_return_withdraw()

    def index_return_withdraw(self):
        df=gt.timeSeries_index_return_withdraw()
        return df
    def stock_return_withdraw(self):
        inputpath_stockreturn = glv.get('input_timeseriesstock')
        df=gt.data_getting(inputpath_stockreturn,config_path)
        if source=='sql':
              df=df[['valuation_date','code','pct_chg']]
              df=gt.sql_to_timeseries(df)
        df['valuation_date'] = pd.to_datetime(df['valuation_date'])
        df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df.set_index('valuation_date',inplace=True,drop=True)
        df=df.astype(float)
        df.reset_index(inplace=True)
        return df
    def index_weight_withdraw(self,index_type, available_date):  # 提取指数权重股数据
        df=gt.index_weight_withdraw(index_type, available_date)
        df=df[['code','weight']]
        return df
    def weight_matrix_combination(self, df_input, inputpath_backtesting):
        df_final = pd.DataFrame()
        df_final_weight = pd.DataFrame()
        df_final_code = pd.DataFrame()
        input_list = df_input['name'].tolist()
        for input in input_list:
            inputpath = os.path.join(inputpath_backtesting, input)
            inputpath_top500 = os.path.join(inputpath, 'Stock_code.csv')
            df_top500 = pd.read_csv(inputpath_top500, dtype=str)
            inputpath_quanzhong = os.path.join(inputpath, 'weight.csv')
            df_weight = pd.read_csv(inputpath_quanzhong, header=None)
            df_weight.columns = df_top500.columns.tolist()
            available_date = df_top500.columns.tolist()[0]
            code_list = df_top500[df_top500.columns.tolist()[0]].tolist()
            weight_list = df_weight[df_weight.columns.tolist()[0]].tolist()
            number_missing = 2000 - len(code_list)
            code_list = code_list + [None] * number_missing
            weight_list = weight_list + [None] * number_missing
            df_help = pd.DataFrame()
            df_help[str(available_date)[:10] + '_code'] = code_list
            df_help[str(available_date)[:10] + '_weight'] = weight_list
            df_help.sort_values(by=(str(available_date)[:10] + '_code'), inplace=True)
            df_final_weight[available_date] = weight_list
            df_final_code[available_date] = code_list
            df_help.reset_index(inplace=True, drop=True)
            df_final = pd.concat([df_final, df_help], axis=1)
        return df_final, df_final_code, df_final_weight
    def weight_matrix_update(self,df_weight, df_index):  #权重更新脚本
        weight_list = df_weight[df_weight.columns.tolist()[0]].tolist()
        weight_list.sort()
        weight_list = list(reversed(weight_list))
        num = 0
        number = None
        for i in range(len(weight_list)):
            num += weight_list[i]
            if num >= 0.9999:
                number = i
                break
        if number == None:
            number = len(df_weight)
        df_weight.sort_values(by=df_weight.columns.tolist()[0],inplace=True, ascending=False)
        index1 = df_weight[:number].index
        weight_list = df_weight[:number][df_weight.columns.tolist()[0]].tolist()
        stock_name = df_index.iloc[index1][df_index.columns.tolist()[0]].tolist()
        df_help = pd.DataFrame()
        df_help['code'] = stock_name
        df_help['weight'] = weight_list
        df_help.sort_values(by='code', inplace=True)
        df_help.reset_index(inplace=True, drop=True)
        return df_help
    def index_component_calculate_history(self,df_final):
        df_component = pd.DataFrame()
        ticker_list=df_final.columns.tolist()
        time_list=[]
        weight_list_hs300=[]
        weight_list_zz500 = []
        weight_list_zz1000 = []
        weight_list_zz2000 = []
        weight_list_weipan = []
        weight_list_zzA500=[]
        for i in range(2, len(ticker_list) - 1, 2):
            available_date=ticker_list[i - 2][:10]
            slice_df = df_final[ticker_list[i - 2:i]]  # 切片
            slice_df.rename(columns={ticker_list[i -2]: 'code',ticker_list[i - 1]: 'weight'}, inplace=True)
            slice_df.dropna(inplace=True, axis=0)
            component_list_hs300 = self.index_weight_withdraw(index_type='沪深300', available_date=available_date)['code'].tolist()
            component_list_zz500 = self.index_weight_withdraw(index_type='中证500', available_date=available_date)['code'].tolist()
            component_list_zz1000 = self.index_weight_withdraw(index_type='中证1000', available_date=available_date)['code'].tolist()
            component_list_zz2000 = self.index_weight_withdraw(index_type='中证2000', available_date=available_date)['code'].tolist()
            component_list_zzA500= self.index_weight_withdraw(index_type='中证A500', available_date=available_date)['code'].tolist()
            component_weight_hs300 = slice_df[slice_df['code'].isin(component_list_hs300)]['weight'].sum()
            component_weight_zz500 = slice_df[slice_df['code'].isin(component_list_zz500)]['weight'].sum()
            component_weight_zz1000 = slice_df[slice_df['code'].isin(component_list_zz1000)]['weight'].sum()
            component_weight_zz2000 = slice_df[slice_df['code'].isin(component_list_zz2000)]['weight'].sum()
            component_weight_zzA500 = slice_df[slice_df['code'].isin(component_list_zzA500)]['weight'].sum()
            component_weight_weipan = 1 - component_weight_hs300 - component_weight_zz500 - component_weight_zz1000 - component_weight_zz2000
            time_list.append(available_date)
            weight_list_hs300.append(component_weight_hs300)
            weight_list_zz500.append(component_weight_zz500)
            weight_list_zz1000.append(component_weight_zz1000)
            weight_list_zz2000.append(component_weight_zz2000)
            weight_list_weipan.append(component_weight_weipan)
            weight_list_zzA500.append(component_weight_zzA500)
        df_component['valuation_date'] = time_list
        df_component['沪深300'] = weight_list_hs300
        df_component['中证500'] = weight_list_zz500
        df_component['中证1000'] = weight_list_zz1000
        df_component['中证2000'] = weight_list_zz2000
        df_component['微盘股'] = weight_list_weipan
        df_component['中证A500']=weight_list_zzA500
        return df_component

    def cal_fund_performance2(self, df, year):  # 计算一些技术指标 输入端为 portfolio_return 和 index_return
        df.reset_index(inplace=True, drop=True)
        df['ex_return'] = df['return'] - df['index_return']
        annual_returns2 = (((1 + df['ex_return']).cumprod()).tolist()[-1] - 1) * 252 / len(df)
        annual_returns = round(annual_returns2 * 100, 2)
        vol = round(df['ex_return'].std() * np.sqrt(252), 4)
        # 筛选出基金和基准收益率都为正的数据点
        positive_returns = df[(df['return'] > 0) & (df['index_return'] > 0)]
        # 计算上行捕获收益率
        upside_returns = round(((1 + positive_returns['ex_return']).cumprod().tolist()[-1] - 1) * 252 / len(df) * 100,
                               2)
        # 筛选出基金和基准收益率都为负的数据点
        negative_returns = df[(df['return'] < 0) & (df['index_return'] < 0)]
        # 计算下行捕获收益率
        down_returns = round(((1 + negative_returns['ex_return']).cumprod().tolist()[-1] - 1) * 252 / len(df) * 100, 2)
        # 计算夏普比率
        sharpe = round(annual_returns2 / vol, 2)
        # 计算信息比率
        info_ratio = round((((1 + positive_returns['ex_return']).cumprod().tolist()[-1] - 1) * 252 / len(df)) / vol, 2)
        # IR = round(info_ratio,2)
        df['nav_max'] = (1 + df['ex_return']).cumprod().expanding().max()

        def get_max_drawdown_slow(array):
            drawdowns = []
            for i in range(len(array)):
                max_array = max(array[:i + 1])
                drawdown = (max_array - array[i]) / max_array
                drawdowns.append(drawdown)
            return max(drawdowns)

        max_dd_all = round(get_max_drawdown_slow((1 + df['ex_return']).cumprod().tolist()), 4)
        temp_df = pd.DataFrame({
            year: [annual_returns, sharpe, info_ratio, max_dd_all, vol]
        })
        result_df6 = pd.DataFrame()
        result_df6 = pd.concat([result_df6, temp_df], axis=1)
        result_df6.index = ['年化收益(%)', '夏普比率', '信息比率', '最大回撤', '年化标准差(%)']
        result_df6 = result_df6.T
        result_df6.reset_index(inplace=True)
        result_df6.rename(columns={'index': 'year'}, inplace=True)
        return result_df6
    def turn_over_ratio_calculate(self,df_final, date_type):  # 计算换手率，针对于history的function #输入端为 code和weight的合并
        ticker_list = df_final.columns.tolist()
        time_list = [ticker_list[0][:10]]
        turn_over_list = [1]
        for i in range(2, len(ticker_list) - 1, 2):
            slice_df_last_period = df_final[ticker_list[i - 2:i]]  # 切片
            slice_df_last_period.rename(columns={ticker_list[i - 2]: 'code'}, inplace=True)
            slice_df_this_period = df_final[ticker_list[i:i + 2]]
            slice_df_this_period.rename(columns={ticker_list[i]: 'code'}, inplace=True)
            slice_df_this_period.dropna(inplace=True, axis=0)
            slice_df_last_period.dropna(inplace=True, axis=0)
            last_period_code = slice_df_last_period['code'].tolist()
            this_period_code = slice_df_this_period['code'].tolist()
            code_total = list(set(last_period_code + this_period_code))
            code_total.sort()
            df_total = pd.DataFrame()
            df_total['code'] = code_total
            df_total = df_total.merge(slice_df_last_period, on='code', how='left')
            df_total = df_total.merge(slice_df_this_period, on='code', how='left')
            df_total.fillna(0, inplace=True)
            df_total['difference'] = abs(
                df_total[ticker_list[i - 1]].astype(float) - df_total[ticker_list[i + 1]].astype(float))
            turn_over_ratio = round(df_total['difference'].sum(), 4)
            time_list.append(ticker_list[i][:10])
            turn_over_list.append(turn_over_ratio)
        df_turnover = pd.DataFrame()
        df_turnover['valuation_date'] = time_list
        df_turnover['turnover_ratio'] = turn_over_list
        df_turnover.fillna(0, inplace=True)
        df_turnover2 = df_turnover.copy()
        df_turnover2['year'] = df_turnover2['valuation_date'].apply(lambda x: str(x)[:4])
        group = pd.DataFrame(df_turnover2.groupby('year')['turnover_ratio'].mean())
        if date_type == 'weekly':
            group = round(group * 52, 1)
        else:
            group = round(group * 252, 1)
        group.reset_index(inplace=True)
        group.rename(columns={'turnover_ratio': '年化换手率'}, inplace=True)
        return df_turnover, group
    def draw_gapth(self,df, outputpath, title):  # 画折线图
        df.plot()
        plt.title(title)
        plt.ylabel('净值')
        plt.xlabel('时间')
        file_path = os.path.join(outputpath, "{}图.png".format(title))
        plt.savefig(file_path)
        plt.close()
    def portfolio_return_processing(self,index_type, df_selecting, df_weight, df_turn_over, cost):
        df_turn_over['valuation_date'] = pd.to_datetime(df_turn_over['valuation_date'])
        df_index=self.df_index_return[['valuation_date',index_type]]
        df_stock=self.df_stock_return
        df_stock['valuation_date'] = pd.to_datetime(df_stock['valuation_date'])
        df_index['valuation_date'] = pd.to_datetime(df_index['valuation_date'])
        trading_date = df_selecting.columns.tolist()
        trading_date = pd.to_datetime(trading_date)
        start_date = trading_date[0]
        end_date = trading_date[-1]
        df_stock = df_stock[(df_stock['valuation_date'] >= start_date) & (df_stock['valuation_date'] <= end_date)]
        df_index = df_index[(df_index['valuation_date'] >= start_date) & (df_index['valuation_date'] <= end_date)]
        valuation_date = df_index['valuation_date'].tolist()
        df_stock = df_stock[df_stock['valuation_date'].isin(valuation_date)]
        df_weight.columns = trading_date
        df_selecting.columns = trading_date
        portfolio_return_list = []
        for i in range(1, len(trading_date)):
            this_time = trading_date[i]
            last_time = trading_date[i - 1]
            slice_df_weight = df_weight[last_time]
            slice_df_weight.dropna(inplace=True, axis=0)
            slice_df_selecting = df_selecting[last_time]
            slice_df_selecting.dropna(inplace=True, axis=0)
            list_weight = slice_df_weight.tolist()
            selecting_code = slice_df_selecting.tolist()
            stock_return = \
            df_stock[(df_stock['valuation_date'] > last_time) & (df_stock['valuation_date'] <= this_time)][
                selecting_code].fillna(0).values
            portfolio_return = np.dot(np.asmatrix(list_weight), np.asmatrix(stock_return).T).tolist()[0]
            if portfolio_return == []:
                portfolio_return = [0]
            portfolio_return_list.append(portfolio_return)
        portfolio_return2 = list(np.concatenate(portfolio_return_list))
        df_backtesting = pd.DataFrame()
        df_backtesting['valuation_date'] = valuation_date[1:]
        df_backtesting['portfolio'] = portfolio_return2
        df_turn_over['turnover_ratio'] = df_turn_over['turnover_ratio'] * cost
        df_backtesting['valuation_date'] = pd.to_datetime(df_backtesting['valuation_date'])
        df_backtesting = df_backtesting.merge(df_turn_over, on='valuation_date', how='left')
        df_backtesting = df_backtesting.merge(df_index, on='valuation_date', how='left')
        df_backtesting.fillna(0, inplace=True)
        df_backtesting['portfolio'] = df_backtesting['portfolio'] - df_backtesting['turnover_ratio']
        df_backtesting['ex_return'] = df_backtesting['portfolio'] - df_backtesting[index_type]
        df_backtesting['组合净值'] = (1 + df_backtesting['portfolio']).cumprod()
        df_backtesting['超额净值'] = (1 + df_backtesting['ex_return']).cumprod()
        df_backtesting['基准净值'] = (1 + df_backtesting[index_type]).cumprod()
        df_backtesting.rename(columns={index_type: 'index'}, inplace=True)
        df_h = df_backtesting[['valuation_date', 'portfolio', 'index']]
        df_h2 = df_backtesting[['valuation_date', '组合净值', '超额净值', '基准净值']]
        df_h2.fillna(method='ffill', inplace=True)
        return df_h, df_h2
    def PDF_Creator(self,outputpath, df2, df3, df4, score_type, analyse_type, index_type, total_rank,
                    total_score, df_component):  # df2收益率 df3为权重矩阵 df4为净值
        if analyse_type == 'history':
            pdf_filename = os.path.join(outputpath,
                                        '{}回测分析报告.pdf'.format(str(score_type)))
        else:
            pdf_filename = os.path.join(outputpath,
                                        '{}压力测试.pdf'.format(str(score_type)))
        pdf = PDFCreator(pdf_filename)
        pdf.title('<b>{}指增分析</b>'.format(index_type))
        pdf.text('参数:{}'.format(str(score_type)))
        if analyse_type == 'update':
            pdf.text('组合综合排名:{}'.format(str(total_rank)))
            pdf.text('组合综合分数:{}'.format(str(total_score)))
        pdf.h1('<b>一、策略表现</b>')
        df2['year'] = df2['valuation_date'].apply(lambda x: str(x)[:4])
        result_df = pd.DataFrame()
        df_component['year'] = df_component['valuation_date'].apply(lambda x: str(x)[:4])
        a = df_component.groupby(['year'])[['沪深300', '中证500', '中证1000', '中证2000', '微盘股','中证A500']].mean()
        a = pd.DataFrame(a)
        a[['沪深300', '中证500', '中证1000', '中证2000', '微盘股','中证A500']] = a[
            ['沪深300', '中证500', '中证1000', '中证2000', '微盘股','中证A500']].apply(lambda x: round(x, 3))
        a.reset_index(inplace=True)
        for year in df2['year'].unique().tolist():
            df = df2[df2['year'] == year]
            df_result = self.cal_fund_performance2(df, year)
            result_df = pd.concat([result_df, df_result])
        table_data = [result_df.columns.tolist()] + result_df.values.tolist()
        pdf.table(table_data, highlight_first_row=False)
        if analyse_type == 'history':
            pdf.h1('<b>回测区间策略表现</b>')
            mean_huanshou = round(df3['年化换手率'].mean(), 3)
            pdf.text('回测区间平均年化换手率:{}'.format(str(mean_huanshou)))
            df_result2 = self.cal_fund_performance2(df2, year='回测区间')
            table_data = [df_result2.columns.tolist()] + df_result2.values.tolist()
            pdf.table(table_data, highlight_first_row=False)
        pdf.h1('<b>二、成分股占比</b>')
        table_data2 = [a.columns.tolist()] + a.values.tolist()
        pdf.table(table_data2, highlight_first_row=False)
        pdf.h1('<b>三、净值回测图</b>')
        df4['valuation_date'] = pd.to_datetime(df4['valuation_date'])
        df4.set_index('valuation_date', drop=True, inplace=True)
        slice_df1 = df4[['组合净值', '基准净值']]
        slice_df2 = df4['超额净值']
        self.draw_gapth(slice_df1, outputpath, '组合基准对比')
        fig_filename = os.path.join(outputpath, f"组合基准对比图.png")
        pdf.image(fig_filename)
        self.draw_gapth(slice_df2, outputpath, '超额净值')
        fig_filename = os.path.join(outputpath, f"超额净值图.png")
        pdf.image(fig_filename)
        if analyse_type == 'history':
            pdf.h1('<b>四、年化换手率分析</b>')
            table_data = [df3.columns.tolist()] + df3.values.tolist()
            pdf.table(table_data, highlight_first_row=False)
        pdf.build()
        return
    def back_testing_history(self,df_final,df_final_code,df_final_weight, outpath_optimizer_result2, index_type, score_type,
                                 cost=0.00085):  # 计算analyse_type为history的回测function
            outputpath2=outpath_optimizer_result2
            outputpath_huice = os.path.join(outputpath2, str(score_type)+'回测.xlsx')
            outputpath_quanzhong = os.path.join(outputpath2,
                                                str(score_type) + '_weight.xlsx')
            df_component = self.index_component_calculate_history(df_final)
            df_turn_over, df_turn_over2 = self.turn_over_ratio_calculate(df_final, date_type='daily')
            df_h, df_h2 = self.portfolio_return_processing(index_type, df_final_code, df_final_weight, df_turn_over, cost)
            df_final.to_excel(outputpath_quanzhong, index=False)
            df_h2.to_excel(outputpath_huice, index=False)  # 储存文件需要自己定义
            df_h.rename(columns={'portfolio': 'return', 'index': 'index_return'}, inplace=True)
            self.PDF_Creator(outputpath=outputpath2, df2=df_h,
                        df3=df_turn_over2, df4=df_h2, score_type=score_type,
                        analyse_type='history', index_type=index_type, total_rank=None,
                        total_score=None, df_component=df_component)
    def back_testing_update(self,inputpath,outpath_optimizer_result2, index_type, score_type, df_score,trading):  # 压力测试代码
        outpath_optimizer_result=glv.get('output_weight')
        inputpath_top500 = os.path.join(inputpath, 'stock_code.csv')
        df_top500 = pd.read_csv(inputpath_top500, dtype=str)
        inputpath_quanzhong = os.path.join(inputpath, 'weight.csv')
        df_weight = pd.read_csv(inputpath_quanzhong, header=None)
        df_weight.columns = df_top500.columns.tolist()
        df_final = self.weight_matrix_update(df_weight, df_top500)
        available_date = df_score['valuation_date'].unique().tolist()[0]
        tommorow = gt.next_workday_calculate(available_date)
        tommorow = pd.to_datetime(tommorow)
        tommorow2 = tommorow.strftime('%Y-%m-%d')
        tommorow = tommorow.strftime('%Y%m%d')
        outputpath = os.path.join(outpath_optimizer_result, 'portfolio_weight')
        outputpath = os.path.join(outputpath, score_type)
        try:
            gt.folder_creator2(outputpath)
        except:
            pass
        outputpath = os.path.join(outputpath, str(tommorow2) + '.csv')
        df_final.to_csv(outputpath, index=False)
        if trading==True:
            list_rank = []
            for i in range(len(df_score)):
                list_rank.append(i + 1)
            df_score['rank'] = list_rank
            slice_df_stock = self.df_stock_return[self.df_stock_return['valuation_date'] >= '2016-01-01']
            slice_df_stock['valuation_date'] = pd.to_datetime(slice_df_stock['valuation_date'])
            date_list = slice_df_stock['valuation_date'].tolist()
            df_factordata = self.df_index_return[['valuation_date', index_type]]
            if index_type == '中证1000':
                index_type2 = 'zz1000'
            elif index_type == '中证2000':
                index_type2 = 'zz2000'
            elif index_type == '中证500':
                index_type2 = 'zz500'
            else:
                index_type2 = 'hs300'
            df_factordata['valuation_date'] = pd.to_datetime(df_factordata['valuation_date'])
            df_index = df_factordata
            df_index = df_index[df_index['valuation_date'].isin(date_list)]
            outputpath2 = os.path.join(outpath_optimizer_result2, str(tommorow))
            gt.folder_creator(outputpath2)
            outputpath_quanzhong = os.path.join(outputpath2,
                                                str(index_type2) + '_' + 'weight' + '_' + str(
                                                    tommorow) + '.csv')
            outputpath_huice = os.path.join(outputpath2,
                                            str(index_type) + '_historical_testing' + '.xlsx')
            df_component = self.index_component_calculate_update(df_weight, df_top500)
            ticker_list2 = df_final[df_final.columns.tolist()[0]].tolist()
            weight_list2 = df_final[df_final.columns.tolist()[1]].tolist()
            stock_return = slice_df_stock[ticker_list2]
            stock_return.fillna(0, inplace=True)
            stock_return_matrix = stock_return.values
            stock_return_portfolio = np.dot(np.mat(weight_list2), np.mat(stock_return_matrix).T).tolist()[0]
            df_h = pd.DataFrame()
            df_h['portfolio'] = stock_return_portfolio
            df_h['index'] = df_index[index_type].tolist()
            df_h['valuation_date'] = date_list
            df_h2 = df_h.copy()
            df_h2.fillna(0, inplace=True)
            df_h2['ex_return'] = df_h2['portfolio'] - df_h2['index']
            df_h2['基准净值'] = (1 + df_h2['index']).cumprod()
            df_h2['组合净值'] = (1 + df_h2['portfolio']).cumprod()
            df_h2['超额净值'] = (1 + df_h2['ex_return']).cumprod()
            df_h2 = df_h2[['valuation_date', '组合净值', '基准净值', '超额净值']]
            df_final.columns = ['code', 'weight']
            df_final2 = df_final.merge(df_score, on='code', how='left')
            df_final2['total_rank'] = df_final2['rank'] * df_final2['weight']
            df_final2['total_mark'] = df_final2['weight'] * df_final2['final_score']
            total_rank = int(df_final2['total_rank'].sum())
            total_mark = df_final2['total_mark'].sum()
            df_final.to_csv(outputpath_quanzhong, index=False)
            df_h2.to_excel(outputpath_huice, index=False)  # 储存文件需要自己定义
            df_h.rename(columns={'portfolio': 'return', 'index': 'index_return'}, inplace=True)
            self.PDF_Creator(outputpath=outputpath2, df2=df_h, df3=None,
                             df4=df_h2, score_type=score_type, analyse_type='update', index_type=index_type,
                             total_rank=total_rank, total_score=total_mark, df_component=df_component)

