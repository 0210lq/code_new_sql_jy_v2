import os
import pandas as pd
import global_setting.global_dic as glv
# import stock.stock_data_preparing as st
from FactorData_update.factor_preparing import FactorData_prepare
import sys
import logging
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import numpy as np
from setup_logger.logger_setup import setup_logger
import io
import contextlib
from datetime import datetime
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Factordata_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class FactorData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Factor_update')
        self.logger.info('\n' + '*'*50 + '\nFACTOR UPDATE PROCESSING\n' + '*'*50)

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='factor')
        return df_config

    def index_dic_processing(self):
        dic_index = {'上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
                     '中证2000': 'zz2000', '中证A500': 'zzA500','国证2000':'gz2000'}
        return dic_index

    def factor_update_main(self):
        self.logger.info('\nProcessing factor_update_main...')
        outputpath_factor_exposure_base = glv.get('output_factor_exposure')
        outputpath_factor_return_base = glv.get('output_factor_return')
        outputpath_factor_stockpool_base = glv.get('output_factor_stockpool')
        outputpath_factor_cov_base = glv.get('output_factor_cov')
        outputpath_factor_risk_base = glv.get('output_factor_specific_risk')
        gt.folder_creator2(outputpath_factor_exposure_base)
        gt.folder_creator2(outputpath_factor_return_base)
        gt.folder_creator2(outputpath_factor_stockpool_base)
        gt.folder_creator2(outputpath_factor_cov_base)
        gt.folder_creator2(outputpath_factor_risk_base)
        input_list1=os.listdir(outputpath_factor_exposure_base)
        input_list2 = os.listdir(outputpath_factor_return_base)
        input_list3 = os.listdir(outputpath_factor_stockpool_base)
        input_list4 = os.listdir(outputpath_factor_cov_base)
        input_list5 = os.listdir(outputpath_factor_risk_base)
        if len(input_list1)==0 or len(input_list2)==0 or len(input_list3)==0 or len(input_list4)==0 or len(input_list5)==0:
            if self.start_date>'2023-06-01':
                start_date='2023-06-01'
            else:
               start_date=self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm1=gt.sqlSaving_main(inputpath_configsql,'FactorExposrue',delete=True)
            sm2=gt.sqlSaving_main(inputpath_configsql,'FactorReturn',delete=True)
            sm3=gt.sqlSaving_main(inputpath_configsql,'FactorPool',delete=True)
            sm4 = gt.sqlSaving_main(inputpath_configsql, 'FactorCov',delete=True)
            sm5 = gt.sqlSaving_main(inputpath_configsql, 'FactorSpecificrisk',delete=True)
        for available_date in working_days_list:

            self.logger.info(f'\nProcessing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            outputpath_factor_exposure = os.path.join(outputpath_factor_exposure_base,
                                                      'factorExposure_' + available_date + '.csv')
            outputpath_factor_return = os.path.join(outputpath_factor_return_base, 'factorReturn_' + available_date + '.csv')
            outputpath_factor_stockpool = os.path.join(outputpath_factor_stockpool_base,
                                                       'factorStockPool_' + available_date + '.csv')
            outputpath_factor_cov = os.path.join(outputpath_factor_cov_base, 'factorCov_' + available_date + '.csv')
            outputpath_factor_risk = os.path.join(outputpath_factor_risk_base,
                                                  'factorSpecificRisk_' + available_date + '.csv')
            df_config = self.source_priority_withdraw()
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            fc = FactorData_prepare(available_date)
            for source_name in source_name_list:
                if source_name == 'jy':
                    df_factorexposure = fc.jy_factor_exposure_update()
                    df_factorreturn = fc.jy_factor_return_update()
                    df_stockpool = fc.jy_factor_stockpool_update()
                    df_factorcov = fc.factor_jy_covariance_update()
                    df_factorrisk = fc.factor_jy_SpecificRisk_update()
                else:
                    raise ValueError
                if len(df_factorexposure) != 0 and len(df_factorreturn) != 0 and len(df_stockpool) != 0 and len(
                        df_factorcov) != 0 and len(df_factorrisk) != 0:
                    self.logger.info(f'factor使用的数据源是: {source_name}')
                    break
            if len(df_factorexposure) != 0 and len(df_factorreturn) != 0 and len(df_stockpool) != 0 and len(
                    df_factorcov) != 0 and len(df_factorrisk) != 0:
                df_factorexposure.to_csv(outputpath_factor_exposure, index=False, encoding='gbk')
                df_factorreturn.to_csv(outputpath_factor_return, index=False, encoding='gbk')
                df_stockpool.to_csv(outputpath_factor_stockpool, index=False, encoding='gbk')
                df_factorcov.to_csv(outputpath_factor_cov, index=False, encoding='gbk')
                df_factorrisk.to_csv(outputpath_factor_risk, index=False, encoding='gbk')

                self.logger.info(f'Successfully saved factor data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_factorexposure['update_time'] = now
                    df_factorreturn['update_time'] = now
                    df_stockpool['update_time'] = now
                    df_factorcov['update_time'] = now
                    df_factorrisk['update_time'] = now
                    capture_file_withdraw_output(sm1.df_to_sql, df_factorexposure)
                    capture_file_withdraw_output(sm2.df_to_sql, df_factorreturn)
                    capture_file_withdraw_output(sm3.df_to_sql,  df_stockpool)
                    capture_file_withdraw_output(sm4.df_to_sql, df_factorcov)
                    capture_file_withdraw_output(sm5.df_to_sql, df_factorrisk)
            else:
                self.logger.warning(f'factor_data在{available_date}数据存在缺失')

    def index_factor_update_main(self):
        self.logger.info('\nProcessing index_factor_update_main...')
        dic_index = self.index_dic_processing()
        outputpath_factor_index = glv.get('output_indexexposure')
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'FactorIndexExposure')
        for index_type in ['上证50', '沪深300', '中证500', '中证1000', '中证2000', '中证A500','国证2000']:
            self.logger.info(f'\nProcessing index type: {index_type}')
            index_short = dic_index[index_type]
            outputpath_factor_index1_base = os.path.join(outputpath_factor_index, index_short)
            gt.folder_creator2(outputpath_factor_index1_base)
            input_list=os.listdir(outputpath_factor_index1_base)
            if len(input_list)==0:
                if self.start_date > '2025-07-29':
                    start_date = '2025-07-29'
                else:
                    start_date = self.start_date
            else:
                    start_date=self.start_date
            df_config = self.source_priority_withdraw()
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            working_days_list=gt.working_days_list(start_date,self.end_date)
            for available_date in working_days_list:
                self.logger.info(f'Processing date: {available_date} for index {index_type}')
                available_date=gt.intdate_transfer(available_date)
                fc=FactorData_prepare(available_date)
                for source_name in source_name_list:
                    outputpath_factor_index1 = os.path.join(outputpath_factor_index1_base,
                                                            str(index_short) + 'IndexExposure_' + available_date + '.csv')
                    if source_name == 'jy':
                        df_index_exposure = fc.jy_factor_index_exposure_update(index_type)

                    else:
                        raise ValueError
                    if len(df_index_exposure) != 0:
                        self.logger.info(f'{index_type}factor_exposure使用的数据源是: {source_name}')
                        break
                if len(df_index_exposure) != 0:
                    df_index_exposure['organization']=index_short
                    df_index_exposure.to_csv(outputpath_factor_index1, index=False, encoding='gbk')
                    self.logger.info(f'Successfully saved index exposure data for {index_type} on {available_date}')
                    if self.is_sql==True:
                        now = datetime.now()
                        df_index_exposure['update_time'] = now
                        capture_file_withdraw_output(sm.df_to_sql, df_index_exposure)
                else:
                    self.logger.warning(f'{index_type}index_factor在{available_date}数据存在缺失')






    def FactorData_update_main(self):
        self.logger.info('\n' + '='*50 + '\nSTARTING FACTOR DATA UPDATE PROCESS\n' + '='*50)
        self.factor_update_main()
        self.index_factor_update_main()
        self.logger.info('\n' + '='*50 + '\nFACTOR DATA UPDATE PROCESS COMPLETED\n' + '='*50)

def FactorData_history_main(start_date,end_date,is_sql):
    fu=FactorData_update(start_date,end_date,is_sql)
    fu.FactorData_update_main()
if __name__ == '__main__':
    FactorData_history_main('2020-01-01','2025-07-14',True)



