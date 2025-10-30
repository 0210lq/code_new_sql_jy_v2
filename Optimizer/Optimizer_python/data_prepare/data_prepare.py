import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import json
import pandas as pd
import os
import warnings
import global_setting.global_dic as glv
from utils_log.logger import setup_logger

warnings.filterwarnings("ignore")
global source,config_path

# Setup logger for this module
logger = setup_logger('data_prepare')

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
        logger.info(f"Successfully loaded data source configuration: {source}")
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        source = 'local'
    return source,config_path

source,config_path= source_getting()

class cross_section_data_preparing:
    def __init__(self,available_date):
        self.available_date=available_date
        logger.info(f"Initializing cross_section_data_preparing for date: {available_date}")

    def index_component_withdraw(self):
        logger.info("Withdrawing index components...")
        df_hs300 = gt.index_weight_withdraw(index_type='沪深300', available_date=self.available_date)
        df_zz500 = gt.index_weight_withdraw(index_type='中证500', available_date=self.available_date)
        df_zz1000 = gt.index_weight_withdraw(index_type='中证1000', available_date=self.available_date)
        df_zz2000 = gt.index_weight_withdraw(index_type='中证2000', available_date=self.available_date)
        df_zzA500 = gt.index_weight_withdraw(index_type='中证A500', available_date=self.available_date)
        logger.info("Successfully withdrew all index components")
        return df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500

    def index_exposure_withdraw(self):
        logger.info("Withdrawing index exposures...")
        df_hs300 = gt.indexFactor_withdraw(index_type='沪深300', start_date=self.available_date, end_date=self.available_date)
        df_zz500 = gt.indexFactor_withdraw(index_type='中证500', start_date=self.available_date, end_date=self.available_date)
        df_zz1000 = gt.indexFactor_withdraw(index_type='中证1000', start_date=self.available_date, end_date=self.available_date)
        df_zz2000 = gt.indexFactor_withdraw(index_type='中证2000', start_date=self.available_date, end_date=self.available_date)
        df_zzA500=gt.indexFactor_withdraw(index_type='中证A500', start_date=self.available_date, end_date=self.available_date)
        logger.info("Successfully withdrew all index exposures")
        return df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500

    def stock_pool_withdraw(self):
        logger.info("Withdrawing stock pool...")
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_stockpool = glv.get('input_factorstockpool')
        if '\\' in inputpath_stockpool:
              inputpath_stockpool = gt.file_withdraw(inputpath_stockpool, available_date2)
        else:
            inputpath_stockpool=str(inputpath_stockpool)+f" WHERE valuation_date = '{self.available_date}'"
        df_stockpool = gt.data_getting(inputpath_stockpool,config_path)
        logger.info("Successfully withdrew stock pool")
        return df_stockpool

    def stock_factor_exposure_withdraw(self):
        logger.info("Withdrawing stock factor exposures...")
        available_date2 = gt.intdate_transfer(self.available_date)
        inputpath_factor = glv.get('input_factorexposure')
        if source=='local':
             inputpath_factor = gt.file_withdraw(inputpath_factor, available_date2)
        else:
             inputpath_factor=str(inputpath_factor)+f" WHERE valuation_date = '{self.available_date}'"
        df_factor = gt.data_getting(inputpath_factor,config_path)
        df_factor.sort_values(by='code',inplace=True)
        logger.info("Successfully withdrew stock factor exposures")
        return df_factor

    def factor_cov_withdraw(self):
        logger.info("Withdrawing factor covariance...")
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorcov')
        if source == 'local':
              inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        else:
              inputpath_cov=str(inputpath_cov)+f" WHERE valuation_date = '{self.available_date}'"
        df= gt.data_getting(inputpath_cov,config_path)
        df.rename(columns={'factor_name':'covariance'},inplace=True)
        logger.info("Successfully withdrew factor covariance")
        return df

    def factor_risk_withdraw(self):
        logger.info("Withdrawing factor risk...")
        available_date2=gt.intdate_transfer(self.available_date)
        inputpath_cov=glv.get('input_factorrisk')
        if source == 'local':
               inputpath_cov=gt.file_withdraw(inputpath_cov,available_date2)
        else:
              inputpath_cov=str(inputpath_cov)+f" WHERE valuation_date = '{self.available_date}'"
        df=gt.data_getting(inputpath_cov,config_path)
        df.set_index('code',inplace=True,drop=True)
        df=df[['specificrisk']]
        df=df.T
        df.reset_index(inplace=True,drop=True)
        logger.info("Successfully withdrew factor risk")
        return df

class stable_data_preparing:
    def stable_data_preparing(self):  # only need to run one time
        logger.info("Preparing stable data...")
        # 静态文件
        inputpath_st = glv.get('st_stock')
        df_st = gt.data_getting(inputpath_st,config_path)
        df_st.columns=['code']
        inputpath_stockuniverse = glv.get('stock_universe_new')
        if source=='sql':
            inputpath_stockuniverse=str(inputpath_stockuniverse)+" WHERE type = 'stockuni_new'"
        df_stockuniverse = gt.data_getting(inputpath_stockuniverse,config_path)
        logger.info("Successfully prepared stable data")
        return  df_st, df_stockuniverse

if __name__ == '__main__':
    dp=cross_section_data_preparing('2025-05-22')
    print(dp.weight_yes_withdraw('combine_zz500_HB','2025-05-24'))