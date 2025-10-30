import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from Time_tools.time_tools import time_tools
from Data_checking.data_check import DataCheck
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main

def MarketData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_mkt()
    date = gt.strdate_transfer(date)
    #回滚
    start_date2=date
    for i in range(10):
        start_date2=gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    MktData_update_main(start_date, date,is_sql)

def ScoreData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_score()
    date = gt.strdate_transfer(date)
    score_type = 'fm'
    score_update_main(score_type, date, date,is_sql)
def FactorData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_factor()
    date = gt.strdate_transfer(date)
    # 回滚
    start_date2 = date
    for i in range(10):
        start_date2 = gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    fu = FactorData_update(start_date, date,is_sql)
    fu.FactorData_update_main()
def L4Data_update_main(is_sql=True):
    L4_update_main(is_sql)
def daily_update_auto():
    fm = File_moving()
    fm.file_moving_update_main()
    MarketData_update_main()
    ScoreData_update_main()
    FactorData_update_main()
    DC=DataCheck()
    DC.DataCheckmain()
if __name__ == '__main__':
    # fm = File_moving()
    # fm.file_moving_update_main()
    daily_update_auto()
    # DC = DataCheck()
    # DC.DataCheckmain()
    pass
