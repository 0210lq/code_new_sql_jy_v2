from optimizer_main_python import Optimizr_main
from data_prepare.data_prepare import data_preparing
import pandas as pd
from tools_func.tools import next_workday_calculate,chunks,is_workday2
import os
from datetime import date
import sys
import datetime
import global_tools_func.global_tools as gt
inputpath_perfattribution,inputpath_output,inputpath_input,inputpath_score,\
 inputpath_weight,inputpath_holding,inputpath_raw_input,inputpath_raw_output,\
outputpath_trading_shipan,outpath_backtesting_shipan,outpath_optimizer_python,outpath_optimizer_result=path()
def target_date_decision():
    if gt.is_workday2() == True:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        critical_time = '22:30'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            return today
    else:
        today = date.today()
        next_day=gt.next_workday_calculate(today)
        return next_day
def main_update_auto(priority): #priority为true的时候先跑,新版本
    target_date = date.today()
    target_date = target_date.strftime('%Y-%m-%d')
    if is_workday2()==True:
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, 'config.xlsx')
        df_config = pd.read_excel(inputpath)
        inputpath_trading = df_config['inputpath_trading_config'].tolist()[0]
        inputpath_mode_dic = df_config['inputpath_mode_dic'].tolist()[0]
        outputpath_list = []
        df_index_return, df_index_exposure_300, df_index_exposure_500, df_index_exposure_1000, df_index_zz2000, df_stock_return, df_st, df_stock_pool, df_stock, df_index_hs300, df_index_zz500, df_index_zz1000, df_lnmodel = data_preparing()
        Optimizer_main_V4 = Optimizr_main(df_index_return, df_index_exposure_300, df_index_exposure_500,
                                          df_index_exposure_1000, df_index_zz2000, df_stock_return, df_st,
                                          df_stock_pool,
                                          df_stock, df_index_hs300, df_index_zz500, df_index_zz1000, df_lnmodel)
        # 把priority和trading合并
        df_priority_hs300 = pd.read_excel(inputpath_trading, '沪深300')
        df_priority_zz500 = pd.read_excel(inputpath_trading, '中证500')
        df_priority_zz1000 = pd.read_excel(inputpath_trading, '中证1000')
        score_list1 = df_priority_hs300['score_name'].tolist()
        score_list2 = df_priority_zz500['score_name'].tolist()
        score_list3 = df_priority_zz1000['score_name'].tolist()
        score_type_list_final = list(set(score_list1) | set(score_list2) | set(score_list3))
        df_total = pd.read_excel(inputpath_mode_dic)
        df_priority = df_total[df_total['score_name'].isin(score_type_list_final)]
        df_total = df_total[~df_total['score_name'].isin(score_type_list_final)]
        if priority == True:
            for i in range(len(df_priority)):
                score_type = df_priority['score_name'].tolist()[i]
                outputpath = Optimizer_main_V4.main_updating(score_type, target_date)
                outputpath_list.append(outputpath)
            chunks_outputpath_list = chunks(outputpath_list, n=4)
            j = 1
            for i in chunks_outputpath_list:
                outputpath = ",".join(f"'{item}'" for item in i)
                with open(f"output{j}.txt", "w") as f:
                    f.write(outputpath)
                j += 1
        else:
            for i in range(len(df_total)):
                score_type = df_total['score_name'].tolist()[i]
                outputpath = Optimizer_main_V4.main_updating(score_type, target_date)
                outputpath_list.append(outputpath)
            chunks_outputpath_list = chunks(outputpath_list, n=6)
            j = 4
            for i in chunks_outputpath_list:
                outputpath = ",".join(f"'{item}'" for item in i)
                with open(f"output{j}.txt", "w") as f:
                    f.write(outputpath)
                j += 1
    else:
        print('today is not working day')

