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
from datetime import datetime
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Score_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class combineScore_update:
    def __init__(self, start_date, end_date,is_sql):
        self.is_sql=is_sql
        self.start_date = start_date
        self.end_date = end_date
        self.logger = setup_logger('Score_update')
        self.logger.info('\n' + '*'*50 + '\nSCORE COMBINATION UPDATE PROCESSING\n' + '*'*50)

    def valid_score_withdraw(self):
        self.logger.info('Retrieving valid score modes...')
        inputpath = glv.get('score_mode')
        df = pd.read_excel(inputpath)
        mode_a1 = df[df['mode_name'] == 'a1']['score_mode'].tolist()[0]
        mode_a3 = df[df['mode_name'] == 'a3']['score_mode'].tolist()[0]
        mode_a1 = 'rr_' + str(mode_a1)
        mode_a3 = 'rr_' + str(mode_a3)
        self.logger.info(f'Retrieved score modes: {mode_a1} and {mode_a3}')
        return mode_a1, mode_a3

    def raw_score_withdraw(self,target_date):
        self.logger.info(f'Retrieving raw scores for date: {target_date}')
        mode_a1, mode_a3 = self.valid_score_withdraw()
        inputpath = glv.get('output_score')
        available_date = gt.intdate_transfer(gt.last_workday_calculate(target_date))
        inputpath_a1 = os.path.join(inputpath, mode_a1)
        inputpath_a3 = os.path.join(inputpath, mode_a3)
        inputpath_a1 = gt.file_withdraw(inputpath_a1, available_date)
        inputpath_a3 = gt.file_withdraw(inputpath_a3, available_date)
        df_a1 = gt.readcsv(inputpath_a1)
        df_a3 = gt.readcsv(inputpath_a3)
        df_a1.drop(columns='score_name',inplace=True)
        df_a3.drop(columns='score_name', inplace=True)
        self.logger.info('Successfully retrieved raw scores')
        return df_a1, df_a3

    def score_processing(self,target_date, index_type):
        self.logger.info(f'Processing scores for date: {target_date} and index type: {index_type}')
        df_result = pd.DataFrame()
        available_date = gt.last_workday_calculate(target_date)
        df_index = gt.index_weight_withdraw(index_type, available_date)
        index_code = df_index['code'].tolist()
        df_a1, df_a3 = self.raw_score_withdraw(target_date)
        df_a3.rename(columns={'final_score': 'a3_score'}, inplace=True)
        df_a3 = df_a3[df_a3['code'].isin(index_code)]
        df_a1_original = df_a1.copy()
        df_a1 = df_a1[df_a1['code'].isin(index_code)]
        df_a1.drop(columns='valuation_date', inplace=True)
        df_a3.drop(columns='valuation_date', inplace=True)
        code_list_short = df_a3[df_a3['a3_score'] < df_a3['a3_score'].quantile(0.2)]['code'].tolist()
        df_final = df_a1.merge(df_a3, on='code', how='outer')
        df_final.loc[(df_final['code'].isin(code_list_short)) & (df_final['final_score'].isna()), ['final_score']] = -2
        df_final.dropna(inplace=True)
        df_final.set_index('code', drop=True, inplace=True)
        df_final = (df_final - df_final.mean()) / df_final.std()
        df_final.reset_index(inplace=True)
        df_final.loc[(df_final['a3_score'] > df_final['a3_score'].quantile(0.2)), ['a3_score']] = 0
        df_final['final_score_combine'] = df_final[['final_score', 'a3_score']].mean(axis=1)
        df_top = df_a1_original[~(df_a1_original['code'].isin(index_code))]
        df_top.sort_values(by='final_score', inplace=True, ascending=False)
        code_list_top = df_top['code'].tolist()[:300]
        df_final.sort_values(by='final_score_combine', inplace=True, ascending=False)
        code_list = df_final['code'].tolist()
        df_result['code'] = code_list_top + code_list
        df_result['valuation_date'] = available_date
        df_result = gt.rank_score_processing(df_result)
        df_result = df_result[['valuation_date', 'code', 'final_score']]
        self.logger.info('Successfully processed scores')
        return df_result

    def score_combination_main(self):
        self.logger.info('\nStarting score combination main process...')
        outputpath = glv.get('output_score')
        outputpath = os.path.join(outputpath, 'combine_zz500')
        gt.folder_creator2(outputpath)
        output_list = os.listdir(outputpath)
        if len(output_list) == 0:
            start_date = '2024-01-04'
            self.logger.info('No existing files found, setting start date to 2024-01-04')
        else:
            start_date=self.start_date
        working_days_list = gt.working_days_list(start_date, self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm = gt.sqlSaving_main(inputpath_configsql, 'Score',delete=True)
        for target_date in working_days_list:
            self.logger.info(f'Processing date: {target_date}')
            available_date = gt.intdate_transfer(gt.last_workday_calculate(target_date))
            outputpath_1 = os.path.join(outputpath, 'Combine_' + str(available_date) + '.csv')
            try:
                df_result = self.score_processing(target_date, '中证500')
                df_result['score_name']='combine_zz500'
                df_result.to_csv(outputpath_1, index=False)
                self.logger.info(f'Successfully saved combined scores for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_result['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_result,'score_name','combine_zz500')
            except:
                self.logger.warning(f'index_component没有更新导致{target_date}的combine_zz500没有更新')
        self.logger.info('Completed score combination process')




