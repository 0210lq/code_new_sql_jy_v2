import sys
import os
import pandas as pd
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import json
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
global global_config_path,source,config_path
source,config_path= source_getting()
global_config_path=config_path_finding()
class portfolio_analysis:
    def __init__(self,df_index_return,df_stock_return,index_type,df_code,df_weight,score_name,top_number,inputpath_backtesting):
        self.df_index_return=df_index_return
        self.df_stock_return=df_stock_return
        self.index_name=index_type
        index_type=gt.index_mapping(index_type,type='code')
        self.index_type=index_type
        self.df_code=df_code
        self.df_weight=df_weight
        self.score_name=score_name
        self.base_score=self.portfolio_index_finding(self.score_name)
        self.top_number=top_number
        self.inputpath=inputpath_backtesting

    def crossSection_stock_return_withdraw(self,target_date):
        slice_df_stock=self.df_stock_return[self.df_stock_return['valuation_date']==target_date]
        slice_df_stock.set_index('valuation_date',inplace=True,drop=True)
        slice_df_stock=slice_df_stock.T
        slice_df_stock.reset_index(inplace=True)
        slice_df_stock.columns=['code','return']
        return slice_df_stock

    def crossSection_index_return_withdraw(self,target_date):
        slice_df_index=self.df_index_return[self.df_index_return['valuation_date']==target_date]
        return slice_df_index
    def portfolio_index_finding(self,score_name):
        inputpath_mode_dic=os.path.join(global_config_path,'Score_config\mode_dictionary.xlsx')
        df_mode=pd.read_excel(inputpath_mode_dic)
        base_score=df_mode[df_mode['score_name']==score_name]['base_score'].tolist()[0]
        return base_score
    def score_withdraw(self,target_date):
        available_date=gt.intdate_transfer(gt.last_workday_calculate(target_date))
        available_date2=gt.strdate_transfer(available_date)
        inputpath_original=glv.get('score')
        inputpath_score = glv.get('input_score')
        if source == 'local':
            inputpath_score2 = os.path.join(inputpath_score, self.base_score)
            inputpath_score2 = gt.file_withdraw(inputpath_score2, available_date)
        else:
            inputpath_score2 = str(
                inputpath_score) + f" WHERE valuation_date = '{available_date2}' AND score_name = '{self.base_score}' "
        df3= gt.data_getting(inputpath_score2, config_path)
        df3.rename(columns={'final_score':'original_score'},inplace=True)
        inputpath=os.path.join(self.inputpath,target_date)
        inputpath_score=os.path.join(inputpath,'Stock_score.csv')
        inputpath_code=os.path.join(inputpath,'Stock_code.csv')
        df1=gt.readcsv(inputpath_score)
        df2 = gt.readcsv(inputpath_code)
        df=pd.concat([df2,df1],axis=1)
        df.columns=['code','final_score']
        df=df.merge(df3,on='code',how='left')
        return df
    def weight_construct(self,available_date):
        slice_df_code=self.df_code[available_date]
        slice_df_weight=self.df_weight[available_date]
        df_weight=pd.concat([slice_df_code,slice_df_weight],axis=1)
        df_weight.columns=['code','weight']
        df_weight.dropna(inplace=True,axis=0)
        df_weight['weight']=df_weight['weight']/df_weight['weight'].sum()
        return df_weight
    def portfolio_performance_calculate(self,df,df_index_return,df_stock_return):#df是weight
        df = df.merge(df_stock_return, on='code', how='left')
        df.fillna(0, inplace=True)
        index_return = df_index_return[self.index_type].tolist()[0]
        df['excess_return'] = df['return'] - float(index_return)
        df['weight_difference'] = df['weight'].astype(float) - df['component_weight'].astype(float)
        df['contribution'] = df['excess_return'] * df['weight_difference']
        contribution = df['contribution'].sum()
        weight = (df['weight'].astype(float)-df['component_weight'].astype(float)).sum()
        return contribution, weight

    def portfolio_analyse(self,available_date):
        target_date=gt.next_workday_calculate(available_date)
        df_score = self.score_withdraw(target_date)
        df_weight =self.weight_construct(available_date)
        df_index = gt.index_weight_withdraw(self.index_name, target_date)
        df_index.rename(columns={'weight': 'component_weight'}, inplace=True)
        df_weight = df_weight.merge(df_index, on='code', how='outer')
        df_weight = df_weight.merge(df_score, on='code', how='left')
        df_stock_return=self.crossSection_stock_return_withdraw(target_date)
        df_index_return = self.crossSection_index_return_withdraw(target_date)
        #提取missing的weight并计算contribution
        df_missing = df_weight[df_weight['original_score'].isna()]
        df_missing = df_missing[['code', 'weight', 'component_weight']]
        contribution_missing, weight_missing = self.portfolio_performance_calculate(df_missing, df_index_return,df_stock_return)
        df_weight = df_weight[~(df_weight['original_score'].isna())]
        df_weight.sort_values(by='final_score', ascending=False, inplace=True)
        #提取top的weight拆解并计算分别的contribution
        df_weight2=df_weight.copy()
        df_weight2=df_weight2[df_weight2['component_weight'].isna()]
        df_weight2.reset_index(inplace=True, drop=True)
        df_top = df_weight2.loc[:self.top_number] #需要改
        df_top = df_top[['code', 'weight', 'component_weight']]
        df_top_1 = df_top[df_top['component_weight'].isna()]
        df_weight = df_weight[~(df_weight['component_weight'].isna())]
        contribution_top_1, weight_top_1 = self.portfolio_performance_calculate(df_top_1, df_index_return, df_stock_return)
        #将权重股分quantile分别计算contribution
        contribution_list = [contribution_missing, contribution_top_1]
        analyse_list = ['missing', 'top']
        weight_list = [weight_missing, weight_top_1]
        for i in range(0, 10, 2):
            j = i / 10
            k = 0.2 + 0.1 * i
            quantile_lower = df_weight['final_score'].quantile(1 - k)
            quantile_upper = df_weight['final_score'].quantile(1 - j)
            slice_df_weight = df_weight[
                (df_weight['final_score'] < quantile_upper) & (df_weight['final_score'] >= quantile_lower)]
            slice_df_weight = slice_df_weight[['code', 'weight', 'component_weight']]
            contribution, weight = self.portfolio_performance_calculate(slice_df_weight, df_index_return, df_stock_return)
            contribution_list.append(contribution)
            analyse_list.append('component_' + str(round(1 - j, 2)) + '_' + str(round(1 - k,2)))
            weight_list.append(weight)
        df_final = pd.DataFrame()
        df_final['analyse_name'] = analyse_list
        df_final['contribution_return'] = contribution_list
        df_final.set_index('analyse_name', inplace=True, drop=True)
        df_final2 = pd.DataFrame()
        df_final2['analyse_name'] = analyse_list
        df_final2['weight'] = weight_list
        df_final2.set_index('analyse_name', inplace=True, drop=True)
        df_final = df_final.T
        df_final['valuation_date'] = target_date
        df_final.reset_index(inplace=True, drop=True)
        df_final = df_final[['valuation_date'] + df_final.columns.tolist()[:-1]]
        df_final2 = df_final2.T
        df_final2['valuation_date'] = target_date
        df_final2.reset_index(inplace=True, drop=True)
        df_final2 = df_final2[['valuation_date'] + df_final2.columns.tolist()[:-1]]
        return df_final, df_final2
    def portfolio_analyse_main(self,start_date,end_date):
        df_final = pd.DataFrame()
        df_final2 = pd.DataFrame()
        working_days_list=gt.working_days_list(start_date,end_date)
        for target_date in working_days_list:
            available_date=gt.last_workday_calculate(target_date)
            slice_df1,slice_df2=self.portfolio_analyse(available_date)
            df_final = pd.concat([df_final, slice_df1])
            df_final2 = pd.concat([df_final2, slice_df2])
        df_final[df_final.columns.tolist()[1:]] = (1 + df_final[df_final.columns.tolist()[1:]]).cumprod()
        df_final.set_index('valuation_date',inplace=True,drop=True)
        df_final2.set_index('valuation_date', inplace=True, drop=True)
        return df_final,df_final2

