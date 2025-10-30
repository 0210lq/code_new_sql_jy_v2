import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from Optimizer_python.main.optimizer_main_python import Optimizer_main
from Optimizer_python.data_prepare.data_prepare import stable_data_preparing
from call_matlab_opt import call_matlab_running_main
from Optimizer_Backtesting.optimizer_backtesting_main import history_running_main
from Optimizer_Backtesting.updating.portfolio_history import portfolio_updating
def history_config_withdraw():
    inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    inputpath=os.path.join(inputpath,'config_history.xlsx')
    df = pd.read_excel(inputpath)
    return df
def history_optimizer_main(is_sql=True): #部署自动化
    df_config=history_config_withdraw()
    stable_data=stable_data_preparing()
    df_st, df_stockuniverse=stable_data.stable_data_preparing()
    opm=Optimizer_main(df_st, df_stockuniverse)
    outputpath_list,outputpath_list_yes=opm.optimizer_history_main(df_config)
    call_matlab_running_main(outputpath_list, outputpath_list_yes)
    history_running_main()
   # portfolio_updating(df_config,is_sql)
if __name__ == '__main__':
    history_optimizer_main(False)