from Optimizer_python.Optimizer.optimizer_V5 import Optimizer_python
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
import global_setting.global_dic as glv
class Optimizer_main:
    def __init__(self,df_st, df_stock_universe):
            self.df_st=df_st
            self.df_stock_pool=df_st
            self.df_stock_universe=df_stock_universe

    #日更function组
    def optimizer_history_main(self,df_config):
        outputpath_list=[]
        outputpath_list_yes=[]
        df_config['start_date']=pd.to_datetime(df_config['start_date'])
        df_config['end_date']=pd.to_datetime(df_config['end_date'])
        start_date_min=df_config['start_date'].min()
        end_date_max=df_config['end_date'].max()
        start_date_min=gt.strdate_transfer(start_date_min)
        end_date_max=gt.strdate_transfer(end_date_max)
        target_date_list = gt.working_days_list(start_date_min, end_date_max)
        df_config['start_date']=df_config['start_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_config['end_date'] = df_config['end_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        for target_date in target_date_list:
              score_name_list=df_config[(df_config['start_date']<=target_date)&(df_config['end_date']>=target_date)]['score_name'].tolist()
              if len(score_name_list)!=0:
                  print(target_date)
                  Optimizer_V5 = Optimizer_python(target_date, self.df_st,self.df_stock_universe)
                  for score_name in score_name_list:
                         print(score_name)
                         outputpath,outputpath_yes = Optimizer_V5.main_optimizer(score_name)
                         outputpath_list.append(outputpath)
                         outputpath_list_yes.append(outputpath_yes)
        outputpath_list.sort()
        outputpath_list_yes.sort()
        return outputpath_list,outputpath_list_yes
    def optimizer_update_main(self,target_date,score_name_list):
        outputpath_list = []
        outputpath_list_yes=[]
        Optimizer_V5 = Optimizer_python(target_date, self.df_st, self.df_stock_universe)
        for score_name in score_name_list:
            outputpath,outputpath_yes = Optimizer_V5.main_optimizer(score_name)
            if outputpath != None:
                outputpath_list.append(outputpath)
                outputpath_list_yes.append(outputpath_yes)
        return outputpath_list,outputpath_list_yes




