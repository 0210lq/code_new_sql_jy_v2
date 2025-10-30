import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import json
import global_tools as gt
import global_setting.global_dic as glv
from Optimizer_Backtesting.backtesting.backtesting_history import Back_testing_processing
global source,config_path,config_path2
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
source,config_path= source_getting()
config_path2=config_path_finding()
class backtesting_main:
    def __init__(self,start_date,end_date):
        self.df_index_return=self.index_return_withdraw(start_date,end_date)
        self.df_stock_return=self.stock_return_withdraw(start_date,end_date)
    def portfolio_index_finding(self,score_name):
        inputpath_mode_dic=os.path.join(config_path2,'Score_config\\mode_dictionary.xlsx')
        df_mode=pd.read_excel(inputpath_mode_dic)
        index_type=df_mode[df_mode['score_name']==score_name]['index_type'].tolist()[0]
        return index_type
    def index_return_withdraw(self,start_date,end_date):
        df=gt.indexData_withdraw(start_date=start_date, end_date=end_date, columns=['pct_chg'])
        df=gt.sql_to_timeseries(df)
        return df
    def stock_return_withdraw(self,start_date,end_date):
        df = gt.stockData_withdraw(start_date=start_date, end_date=end_date, columns=['pct_chg'])
        df=gt.sql_to_timeseries(df)
        df['valuation_date'] = pd.to_datetime(df['valuation_date'])
        df['valuation_date'] = df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df.set_index('valuation_date',inplace=True,drop=True)
        df=df.astype(float)
        df.reset_index(inplace=True)
        return df

    def optimizer_history_backtesting_main(self,df_config):
        bt = Back_testing_processing(self.df_index_return, self.df_stock_return)
        df_config['start_date']=pd.to_datetime(df_config['start_date'])
        df_config['end_date'] = pd.to_datetime(df_config['end_date'])
        df_config['start_date']=df_config['start_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_config['end_date'] = df_config['end_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        for i in range(len(df_config)):
            start_date=df_config['start_date'].tolist()[i]
            end_date=df_config['end_date'].tolist()[i]
            score_name=df_config['score_name'].tolist()[i]
            index_type=self.portfolio_index_finding(score_name)
            bt.back_testing_main_history(index_type, score_name, start_date, end_date)

if __name__ == '__main__':
    bm=backtesting_main()
    print(bm.df_index_return())
    print(bm.df_stock_return())


