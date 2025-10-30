from datetime import date
import pandas as pd
import os
from scipy.io import loadmat
import numpy as np
import warnings
import global_setting.global_dic as glv
from Optimizer_python.parameters.parameters_withdraw import factor_constraint_withdraw,optimizer_args_withdraw,valid_factor_withdraw
from Optimizer_python.Score.score_withdraw import score_withdraw_main
from Optimizer_python.data_prepare.data_prepare import cross_section_data_preparing
from Optimizer_python.weight_processing.weight_constraint import weight_constraint
warnings.filterwarnings("ignore")
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
def config_path_finding():
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath_output=None
    should_break=False
    for i in range(10):
        if should_break:
            break
        inputpath = os.path.dirname(inputpath)
        input_list = os.listdir(inputpath)
        for input in input_list:
            if should_break:
                break
            if str(input)=='config':
                inputpath_output=os.path.join(inputpath,input)
                should_break=True
    return inputpath_output
global global_config_path
global_config_path=config_path_finding()
class Optimizer_python:
    def __init__(self,target_date,df_st, df_stock_universe):
        available_date=gt.last_workday_calculate(target_date)
        dp=cross_section_data_preparing(available_date)
        self.dp=dp
        self.available_date=available_date
        self.df_hs300,self.df_zz500,self.df_zz1000,self.df_zz2000,self.df_zzA500=dp.index_component_withdraw()
        self.df_hs300_exposure,self.df_zz500_exposure,self.df_zz1000_exposure,self.df_zz2000_exposure,self.df_zzA500_exposure=dp.index_exposure_withdraw()
        self.df_stockpool=dp.stock_pool_withdraw()
        self.df_stock_factor=dp.stock_factor_exposure_withdraw()
        self.df_st=df_st
        self.df_stock_universe=df_stock_universe
        self.df_cov=dp.factor_cov_withdraw()
        self.df_specificrisk=dp.factor_risk_withdraw()
    def optimizer_args_processing(self,optimizer_args,score_name):
        inputpath_modedic = os.path.join(global_config_path,'Score_config\mode_dictionary.xlsx')
        df_mode_dic = pd.read_excel(inputpath_modedic)
        slice_df_mode_dic=df_mode_dic.iloc[df_mode_dic[df_mode_dic['score_name']==score_name].index]
        score_type=slice_df_mode_dic['base_score'].tolist()[0]
        index_type=slice_df_mode_dic['index_type'].tolist()[0]
        mode_type=slice_df_mode_dic['mode_type'].tolist()[0]
        optimizer_args['score_type']=score_type
        optimizer_args['index_type']=index_type
        optimizer_args['mode_type']=mode_type
        optimizer_args['score_name']=score_name
        optimizer_args['available_date']=self.available_date
        return optimizer_args
    def top_N_stock_selecting(self,optimizer_args):  # 返回的是股票打分#目前只支持rr和y_hat
        score_type=optimizer_args.get('score_type')
        mode_type=optimizer_args.get('mode_type')
        index_type=optimizer_args.get('index_type')
        opt_type=optimizer_args.get('opt_type')
        st_list = self.df_st['code'].tolist()
        df_score=score_withdraw_main(score_type,self.available_date,mode_type,index_type,self.df_hs300,self.df_zz500,self.df_zz1000,self.df_zz2000,self.df_zzA500)
        # print(mode_type)
        # print(df_score[df_score['code']=='000009.SZ'])
        stock_pool = self.df_stockpool['code']
        stock_pool.dropna(inplace=True, axis=0)
        stock_pool2 = stock_pool.tolist()
        df_score = df_score[df_score['code'].isin(stock_pool2)]
        df_score = df_score[~df_score['code'].isin(st_list)]

        df_score.sort_values(by='final_score', ascending=False, inplace=True)
        if index_type=='沪深300':
             df_weight1 = self.df_hs300
        elif index_type=='中证500':
             df_weight1 = self.df_zz500
        elif index_type=='中证A500':
            df_weight1=self.df_zzA500
        elif index_type=='中证2000':
            df_weight1=self.df_zz2000
        else:
             df_weight1 = self.df_zz1000
        df_selecting_score = pd.DataFrame()
        df_selecting_code = pd.DataFrame()
        df_selecting_initial_weight = pd.DataFrame()
        df_selecting_upper_weight = pd.DataFrame()
        df_selecting_lower_weight = pd.DataFrame()
        df_weight = pd.DataFrame()
        stock_list = df_weight1['code'].tolist()
        stock_list=list(set(stock_list)&set(stock_pool2))
        df_weight['code'] = stock_list
        df_weight = df_weight.merge(df_score, on='code', how='left')
        df_weight = df_weight.merge(df_weight1, on='code', how='left')
        ws=weight_constraint(df_score,df_weight,optimizer_args)
        df_final,df_initial=ws.weight_constraint_main()
        initial_weight = df_final['initial_weight'].tolist()
        upper_weight = df_final['weight_upper'].tolist()
        lower_weight = df_final['weight_lower'].tolist()
        final_code = df_final['code'].tolist()
        score_list = df_final['final_score'].tolist()
        df_selecting_initial_weight[self.available_date] = initial_weight
        df_selecting_upper_weight[self.available_date] = upper_weight
        df_selecting_lower_weight[self.available_date] = lower_weight
        df_selecting_code[self.available_date] = final_code
        df_selecting_score[self.available_date] = score_list
        return df_score,df_initial, df_selecting_code, df_selecting_score,  df_selecting_initial_weight, df_selecting_upper_weight, df_selecting_lower_weight

    def Top_N_risk_exposure_withdraw(self,optimizer_args):
        df_score,df_initial, df_selecting_code, df_selecting_score,  df_selecting_initial_weight, df_selecting_upper_weight, df_selecting_lower_weight = self.top_N_stock_selecting(optimizer_args)
        selecting_code_list=df_selecting_code[self.available_date].tolist()
        df_factor_exposure=self.df_stock_factor[self.df_stock_factor['code'].isin(selecting_code_list)]
        df_factor_exposure.fillna(0,inplace=True)
        df_factor_exposure.drop(columns='code',inplace=True)
        df_risk=self.df_specificrisk[selecting_code_list]
        return df_score,df_initial,df_risk, df_factor_exposure, df_selecting_score, df_selecting_code, df_selecting_initial_weight, df_selecting_upper_weight, df_selecting_lower_weight

    def data_processing_main(self,optimizer_args, outpath_optimizer_python):
        style_list,industry_list=valid_factor_withdraw()
        valid_factor_list=style_list+industry_list
        df_factorcov = self.df_cov[self.df_cov['covariance'].isin(valid_factor_list)]
        df_factorcov = df_factorcov[['covariance'] + valid_factor_list]
        
        # 确保covariance列的顺序与valid_factor_list一致
        df_factorcov['covariance'] = pd.Categorical(df_factorcov['covariance'], categories=valid_factor_list, ordered=True)
        df_factorcov = df_factorcov.sort_values('covariance')
        df_factorcov.set_index('covariance',inplace=True,drop=True)
        df_parameter = pd.DataFrame(optimizer_args.items())
        index_type=optimizer_args.get('index_type')
        score_name=optimizer_args.get('score_name')
        outputpath = outpath_optimizer_python
        target_date = gt.next_workday_calculate(self.available_date)
        outputpath_yes=os.path.join(outputpath,self.available_date)
        outputpath = os.path.join(outputpath, target_date)
        try:
            gt.folder_creator2(outputpath)
        except:
            print('已经存在')
        df_score,df_initial,df_risk, df_stock_exposure, df_selecting_score, df_selecting_code,df_selecting_initial_weight, df_selecting_upper_weight, df_selecting_lower_weight = self.Top_N_risk_exposure_withdraw(optimizer_args)
        if index_type=='沪深300':
            df_index_exposure=self.df_hs300_exposure
        elif index_type=='中证500':
            df_index_exposure=self.df_zz500_exposure
        elif index_type=='中证A500':
            df_index_exposure=self.df_zzA500_exposure
        elif index_type=='中证2000':
            df_index_exposure=self.df_zz2000_exposure
        else:
            df_index_exposure=self.df_zz1000_exposure
        constraint_list=['TE']+style_list+industry_list
        df_style=pd.DataFrame()
        df_industry=pd.DataFrame()
        df_style['factor_name']=style_list
        df_industry['factor_name']=industry_list
        df_constraint = factor_constraint_withdraw(score_name)
        df_constraint=df_constraint[df_constraint['factor_name'].isin(constraint_list)]
        df_constraint_upper = df_constraint[['factor_name', 'upper']]
        df_constraint_upper.set_index('factor_name', inplace=True)
        df_constraint_lower = df_constraint[['factor_name', 'lower']]
        df_constraint_lower.set_index('factor_name', inplace=True)
        df_constraint_upper = df_constraint_upper.T
        df_constraint_lower = df_constraint_lower.T
        df_stock_exposure=df_stock_exposure[valid_factor_list]
        df_index_exposure=df_index_exposure[valid_factor_list]
        df_initial.set_index('code',inplace=True,drop=True)
        df_risk.fillna(0,inplace=True)
        df_initial.fillna(0,inplace=True)
        df_factorcov.fillna(0,inplace=True)
        outputpath_stock_score = os.path.join(outputpath, 'Stock_total_score.csv')
        outputpath_stock_exposure = os.path.join(outputpath, 'Stock_risk_exposure.csv')
        outputpath_index_exposure = os.path.join(outputpath, 'index_risk_exposure.csv')
        outputpath_selecting_code = os.path.join(outputpath, 'Stock_code.csv')
        outputpath_selecting_score = os.path.join(outputpath, 'Stock_score.csv')
        outputpath_parameter = os.path.join(outputpath, 'parameter_selecting.xlsx')
        outputpath_selecting_initial_weight = os.path.join(outputpath, 'Stock_initial_weight.csv')
        outputpath_selecting_upper_weight = os.path.join(outputpath, 'Stock_upper_weight.csv')
        outputpath_selecting_lower_weight = os.path.join(outputpath, 'Stock_lower_weight.csv')
        outputpath_specific_risk = os.path.join(outputpath, 'Stock_specific_risk.csv')
        outputpath_factor_cov = os.path.join(outputpath, 'factor_cov.csv')
        outputpath_index_initial_weight = os.path.join(outputpath, 'index_initial_weight.csv')
        outputpath_constraint_upper = os.path.join(outputpath, 'factor_constraint_upper.csv')
        outputpath_constraint_lower = os.path.join(outputpath, 'factor_constraint_lower.csv')
        df_stock_exposure.to_csv(outputpath_stock_exposure, index=False, encoding='utf_8_sig')
        df_index_exposure.to_csv(outputpath_index_exposure, index=False, encoding='utf_8_sig')
        df_selecting_code.to_csv(outputpath_selecting_code, index=False)
        df_selecting_score.to_csv(outputpath_selecting_score, index=False)
        df_risk.to_csv(outputpath_specific_risk, index=False, encoding='utf_8_sig')
        df_initial.to_csv(outputpath_index_initial_weight, index=False)
        df_factorcov.to_csv(outputpath_factor_cov, index=False)
        df_selecting_initial_weight.to_csv(outputpath_selecting_initial_weight, index=False)
        df_selecting_upper_weight.to_csv(outputpath_selecting_upper_weight, index=False)
        df_selecting_lower_weight.to_csv(outputpath_selecting_lower_weight, index=False)
        df_constraint_upper.to_csv(outputpath_constraint_upper, index=False, encoding='utf_8_sig')
        df_constraint_lower.to_csv(outputpath_constraint_lower, index=False, encoding='utf_8_sig')
        df_score.to_csv(outputpath_stock_score, index=False)
        with pd.ExcelWriter(outputpath_parameter) as writer:
            df_parameter.to_excel(writer, header=None, index=False,sheet_name='parameters')
            df_style.to_excel(writer,sheet_name='style',index=False)
            df_industry.to_excel(writer,sheet_name='industry',index=False)
        return outputpath,outputpath_yes

    def main_optimizer(self,score_name):
        optimizer_args=optimizer_args_withdraw(score_name)
        optimizer_args= self.optimizer_args_processing(optimizer_args,score_name)
        outputpath_optimizer_python=glv.get('output_optimizer')
        outputpath_optimizer_python2 = os.path.join(outputpath_optimizer_python, score_name)
        try:
            gt.folder_creator2(outputpath_optimizer_python2)
        except:
            pass
        outputpath_final,outputpath_yes = self.data_processing_main(optimizer_args, outputpath_optimizer_python2)
        return outputpath_final,outputpath_yes


