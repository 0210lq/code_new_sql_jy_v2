import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from setup_logger.logger_setup import setup_logger
import io
import contextlib
import time
from datetime import datetime
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Portfolio_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class scorePortfolio_update:
    def __init__(self, start_date, end_date,is_sql):
        self.is_sql=is_sql
        self.start_date = start_date
        self.end_date = end_date
        self.logger = setup_logger('ScorePortfolio_update')
        self.logger.info('\n' + '*'*50 + '\nSCORE PORTFOLIO UPDATE PROCESSING\n' + '*'*50)
    def se_date_withdraw(self,input_list):
        input_s = input_list[0]
        index = input_s.rindex('_')
        start_date = gt.strdate_transfer(str(input_s)[index + 1:-4])
        return start_date
    def rr_top_update_main(self):
        self.logger.info('\nProcessing RR top portfolio update...')
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        inputpath = glv.get('output_score')
        inputpath_a1 = os.path.join(inputpath, 'a1')
        inputpath_a3 = os.path.join(inputpath, 'a3')
        outputpath = glv.get('output_portfolio')
        outputpath_test = os.path.join(outputpath, 'a1_top' + str(5))
        try:
            os.listdir(outputpath_test)
        except:
            start_date = '2024-01-06'
            self.logger.info('No existing files found, setting start date to 2024-01-06')
        working_list = gt.working_days_list(start_date, end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
        for date in working_list:
            self.logger.info(f'Processing date: {date}')
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_workday_calculate(date)
            available_date = gt.intdate_transfer(available_date)
            daily_a1 = gt.file_withdraw(inputpath_a1, available_date)
            daily_a3 = gt.file_withdraw(inputpath_a3, available_date)
            df_a1 = gt.readcsv(daily_a1)
            df_a3 = gt.readcsv(daily_a3)
            for number in [5, 100, 200, 300]:
                outputpath_a1 = os.path.join(outputpath, 'a1_top' + str(number))
                outputpath_a3 = os.path.join(outputpath, 'a3_top' + str(number))
                gt.folder_creator2(outputpath_a1)
                gt.folder_creator2(outputpath_a3)
                daily_outputpath_a1 = os.path.join(outputpath_a1, 'a1_top' + str(number) + '_' + str(date2) + '.csv')
                daily_outputpath_a3 = os.path.join(outputpath_a3, 'a3_top' + str(number) + '_' + str(date2) + '.csv')
                slice_a1 = df_a1.loc[:number - 1]
                slice_a3 = df_a3.loc[:number - 1]
                slice_a1['weight'] = slice_a1['final_score'] / slice_a1['final_score'].sum()
                slice_a3['weight'] = slice_a3['final_score'] / slice_a3['final_score'].sum()
                slice_a1 = slice_a1[['code', 'weight']]
                slice_a3 = slice_a3[['code', 'weight']]
                slice_a1['portfolio_name']='a1_top'+ str(number)
                slice_a1['valuation_date']=date
                slice_a3['portfolio_name']='a3_top'+ str(number)
                slice_a3['valuation_date']=date
                slice_a1=slice_a1[['valuation_date','portfolio_name','code','weight']]
                slice_a3 = slice_a3[['valuation_date', 'portfolio_name', 'code', 'weight']]
                slice_a1.to_csv(daily_outputpath_a1, index=False)
                slice_a3.to_csv(daily_outputpath_a3, index=False)
                if self.is_sql==True:
                    now = datetime.now()
                    slice_a1['update_time'] = now
                    slice_a3['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, slice_a1,'portfolio_name','a1_top' + str(number))
                    capture_file_withdraw_output(sm.df_to_sql, slice_a3,'portfolio_name','a3_top' + str(number))
                self.logger.info(f'Successfully saved top {number} portfolio data for date: {date2}')
        self.logger.info('Completed RR top portfolio update')
    def rr_hs300_top_update_main(self):
        self.logger.info('\nProcessing RR HS300 top portfolio update...')
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        inputpath = glv.get('output_score')
        inputpath_a1 = os.path.join(inputpath, 'a1')
        inputpath_a3 = os.path.join(inputpath, 'a3')
        outputpath = glv.get('output_portfolio')
        outputpath_test = os.path.join(outputpath, 'a1_hs300_top' + str(100))
        try:
            os.listdir(outputpath_test)
        except:
            start_date = '2024-01-06'
            self.logger.info('No existing files found, setting start date to 2024-01-06')
        working_list = gt.working_days_list(start_date, end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
        for date in working_list:
            self.logger.info(f'Processing date: {date}')
            yes=gt.last_workday_calculate(date)
            yes=gt.last_workday_calculate(yes)
            df_hs300=gt.index_weight_withdraw('沪深300',yes)
            code_list=df_hs300['code'].tolist()
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_workday_calculate(date)
            available_date = gt.intdate_transfer(available_date)
            daily_a1 = gt.file_withdraw(inputpath_a1, available_date)
            daily_a3 = gt.file_withdraw(inputpath_a3, available_date)
            df_a1 = gt.readcsv(daily_a1)
            df_a3 = gt.readcsv(daily_a3)
            df_a1=df_a1[df_a1['code'].isin(code_list)]
            df_a3=df_a3[df_a3['code'].isin(code_list)]
            df_a1.reset_index(inplace=True)
            df_a3.reset_index(inplace=True)
            for number in [100]:
                outputpath_a1 = os.path.join(outputpath, 'a1_hs300_top' + str(number))
                outputpath_a3 = os.path.join(outputpath, 'a3_hs300_top' + str(number))
                gt.folder_creator2(outputpath_a1)
                gt.folder_creator2(outputpath_a3)
                daily_outputpath_a1 = os.path.join(outputpath_a1, 'a1_hs300_top' + str(number) + '_' + str(date2) + '.csv')
                daily_outputpath_a3 = os.path.join(outputpath_a3, 'a3_hs300_top' + str(number) + '_' + str(date2) + '.csv')
                slice_a1 = df_a1.loc[:number - 1]
                slice_a3 = df_a3.loc[:number - 1]
                slice_a1['weight'] = slice_a1['final_score'] / slice_a1['final_score'].sum()
                slice_a3['weight'] = slice_a3['final_score'] / slice_a3['final_score'].sum()
                slice_a1 = slice_a1[['code', 'weight']]
                slice_a1['valuation_date']=date
                slice_a3 = slice_a3[['code', 'weight']]
                slice_a3['valuation_date']=date
                slice_a1['portfolio_name']='a1_hs300_top' + str(number)
                slice_a3['portfolio_name']='a1_hs300_top' + str(number)
                slice_a1=slice_a1[['valuation_date','portfolio_name','code','weight']]
                slice_a3 = slice_a3[['valuation_date', 'portfolio_name', 'code', 'weight']]
                if self.is_sql==True:
                    now = datetime.now()
                    slice_a1['update_time'] = now
                    slice_a3['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, slice_a1,'portfolio_name','a1_hs300_top' + str(number))
                    capture_file_withdraw_output(sm.df_to_sql, slice_a3,'portfolio_name','a3_hs300_top' + str(number))
                slice_a1.to_csv(daily_outputpath_a1, index=False)
                slice_a3.to_csv(daily_outputpath_a3, index=False)

                self.logger.info(f'Successfully saved HS300 top {number} portfolio data for date: {date2}')
        self.logger.info('Completed RR HS300 top portfolio update')
    def rr_zz2000_top_update_main(self):
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        inputpath = glv.get('output_score')
        inputpath_a3 = os.path.join(inputpath, 'a3')
        outputpath = glv.get('output_portfolio')
        outputpath_test2 = os.path.join(outputpath, 'a3_zz2000_top' + str(100))
        try:
            os.listdir(outputpath_test2)
        except:
            start_date = '2024-01-01'
        working_list = gt.working_days_list(start_date, end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
        for date in working_list:
            yes=gt.last_workday_calculate(date)
            yes=gt.last_workday_calculate(yes)
            if date<='2024-01-01':
                 df_hs300=gt.index_weight_withdraw('国证2000',yes)
            else:
                df_hs300 = gt.index_weight_withdraw('中证2000', yes)
            code_list=df_hs300['code'].tolist()
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_workday_calculate(date)
            available_date = gt.intdate_transfer(available_date)
            daily_a3 = gt.file_withdraw(inputpath_a3, available_date)
            df_a3 = gt.readcsv(daily_a3)
            df_a3=df_a3[df_a3['code'].isin(code_list)]
            df_a3.reset_index(inplace=True)
            for number in [100,200]:
                outputpath_a3 = os.path.join(outputpath, 'a3_zz2000_top' + str(number))
                gt.folder_creator2(outputpath_a3)
                daily_outputpath_a3 = os.path.join(outputpath_a3, 'a3_zz2000_top' + str(number) + '_' + str(date2) + '.csv')
                slice_a3 = df_a3.loc[:number - 1]
                slice_a3['final_score']=(slice_a3['final_score']-slice_a3['final_score'].min())/(slice_a3['final_score'].max()-slice_a3['final_score'].min())
                slice_a3['weight'] = slice_a3['final_score']/slice_a3['final_score'].sum()
                slice_a3 = slice_a3[['code', 'weight']]
                slice_a3['valuation_date']=date
                slice_a3['portfolio_name'] = 'a3_zz2000_top' + str(number)
                if self.is_sql==True:
                    now = datetime.now()
                    slice_a3['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, slice_a3,'portfolio_name','a3_zz2000_top' + str(number))
                slice_a3.to_csv(daily_outputpath_a3, index=False)
    def ubp_top_update_main(self):
        self.logger.info('\nProcessing UBP top portfolio update...')
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        outputpath = glv.get('output_portfolio')
        outputpath = os.path.join(outputpath, 'ubp500')
        gt.folder_creator2(outputpath)
        inputpath_score = glv.get('input_score')
        inputpath_score = os.path.join(inputpath_score, 'rr_score')
        inputpath_score = os.path.join(inputpath_score, 'UBP_500alpha')
        input_list = os.listdir(outputpath)
        if len(input_list) == 0:
            input_list2=os.listdir(inputpath_score)
            input_list2.sort()
            start_date = self.se_date_withdraw(input_list2)
            self.logger.info(f'No existing files found, setting start date to {start_date}')
        working_days_list = gt.working_days_list(start_date, end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
        for date in working_days_list:
            self.logger.info(f'Processing date: {date}')
            date2 = gt.intdate_transfer(date)
            available_date = gt.last_weeks_lastday(date)
            available_date = gt.intdate_transfer(available_date)
            daily_inputpath = gt.file_withdraw(inputpath_score, available_date)
            try:
                df = pd.read_excel(daily_inputpath, header=None)
            except:
                df = pd.DataFrame()
            if len(df) != 0:
                df = df.iloc[3:]
                df.columns = ['code', 'chiname', 'weight']
                df = df[df['weight'] != 0]
                df = df[['code', 'weight']]
                df['valuation_date'] = date
                df['portfolio_name'] = 'ubp500'
                df = df[['valuation_date', 'portfolio_name', 'code', 'weight']]
                daily_outputpath = os.path.join(outputpath, 'UBP500alpha_' + str(date2) + '.csv')
                df.to_csv(daily_outputpath, index=False)
                self.logger.info(f'Successfully saved UBP top portfolio data for date: {date2}')
                if self.is_sql==True:
                    now = datetime.now()
                    df['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df,'portfolio_name','ubp500')
            else:
                self.logger.warning(f'ubp_500 {date} 暂时没有数据')
        self.logger.info('Completed UBP top portfolio update')
    def etf_update_main(self):
        self.logger.info('\nProcessing ETF portfolio update...')
        inputpath = glv.get('inputpath_gms')
        update_time=time.ctime(os.path.getmtime(inputpath))
        d = datetime.strptime(update_time, "%a %b %d %H:%M:%S %Y")
        d=d.strftime('%Y-%m-%d')
        outputpath = glv.get('output_portfolio')
        outputpath = os.path.join(outputpath, 'gms')
        gt.folder_creator2(outputpath)
        start_date = gt.strdate_transfer(self.start_date)
        end_date = gt.strdate_transfer(self.end_date)
        working_days_list = gt.working_days_list(start_date, end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
        for date in working_days_list:
            self.logger.info(f'Processing date: {date}')
            difference_date=(pd.to_datetime(date)-pd.to_datetime(d)).days
            if difference_date>5:
                print(inputpath,'上一次更新时间为:'+d,'时间间隔为:'+str(difference_date)+'天')
            date2 = gt.intdate_transfer(date)
            outputpath_daily = os.path.join(outputpath, 'gms_' + str(date2) + '.csv')
            df = pd.read_excel(inputpath)
            index_start=df[df[df.columns.tolist()[1]]=='ALD ETF Signal - UBP Bric'].index.tolist()[0]
            index_end_list = df[df[df.columns.tolist()[1]].isna()].index.tolist()
            index_end_list=[i for i in index_end_list if i>index_start]
            index_end=index_end_list[0]
            df=df.iloc[index_start+1:index_end]
            df.dropna(inplace=True,axis=1)
            df.fillna(0,inplace=True)
            df.columns=['chi_name','code','money']
            df['money']=df['money']
            df['weight']=df['money']/df['money'].sum()
            df=df[['code','weight']]
            if len(df) > 0:
                df['portfolio_name'] = 'gms'
                df['valuation_date'] = date
                df=df[['valuation_date']+df.columns.tolist()[:-1]]
                df.to_csv(outputpath_daily, index=False, encoding='gbk')
                self.logger.info(f'Successfully saved ETF portfolio data for date: {date2}')
                if self.is_sql == True:
                    now = datetime.now()
                    df['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df,'portfolio_name','gms')
                else:
                    self.logger.warning(f'ubpETF {date} ETF_tracking 为空')
            self.logger.info('Completed ETF portfolio update')
    def scorePortfolio_update_main(self):
        self.logger.info('\nStarting score portfolio update main process...')
        self.rr_top_update_main()
        self.rr_hs300_top_update_main()
        self.rr_zz2000_top_update_main()
        # self.ubp_top_update_main()
        try:
            self.etf_update_main()
        except:
            print('gms更新有误')
        self.logger.info('Completed all score portfolio updates')