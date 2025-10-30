from datetime import date
import datetime
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from Optimizer_python.utils_log.logger import setup_logger

# Setup logger for this module
logger = setup_logger('portfolio_history')

def config_path_finding():
    logger.info("Finding config path...")
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
    logger.info(f"Config path found: {inputpath_output}")
    return inputpath_output

global global_config_path
global_config_path=config_path_finding()

def history_config_withdraw():
    logger.info("Withdrawing history configuration...")
    inputpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    inputpath = os.path.join(inputpath, 'config_history.xlsx')
    df = pd.read_excel(inputpath)
    logger.info("History configuration loaded successfully")
    return df

def portfolio_updating(df_config,is_sql):
    logger.info("Starting portfolio history updating")
    score_name_list=df_config['score_name'].tolist()
    inputpath_portfolio=glv.get('output_optimizer')
    outputpath_weight=glv.get('output_weight')
    
    if is_sql==True:
        logger.info("SQL mode enabled, initializing SQL connection")
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        inputpath_configsql = os.path.join(current_dir, 'global_setting\\optimizer_sql.yaml')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
    
    for score_name in score_name_list:
        logger.info(f"Processing portfolio: {score_name}")
        daily_outputpath = os.path.join(outputpath_weight, score_name)
        gt.folder_creator2(daily_outputpath)
        start_date=df_config[df_config['score_name']==score_name]['start_date'].tolist()[0]
        end_date = df_config[df_config['score_name'] == score_name]['end_date'].tolist()[0]
        working_days_list=gt.working_days_list(start_date,end_date)
        
        logger.info(f"Processing {len(working_days_list)} dates for {score_name}")
        for target_date in working_days_list:
            logger.info(f"Processing date: {target_date}")
            target_date2 = gt.intdate_transfer(target_date)
            daily_outputpath2 = os.path.join(daily_outputpath, str(score_name) + '_' + target_date2 + '.csv')
            daily_inputpath = os.path.join(inputpath_portfolio, score_name)
            daily_inputpath2 = os.path.join(daily_inputpath, target_date)
            inputpath_weight = os.path.join(daily_inputpath2, 'weight.csv')
            inputpath_code = os.path.join(daily_inputpath2, 'Stock_code.csv')
            
            try:
                df_weight = pd.read_csv(inputpath_weight, header=None)
                df_code = pd.read_csv(inputpath_code)
            except Exception as e:
                logger.error(f"Failed to update {score_name} for date {target_date}: {str(e)}")
                continue
                
            df_weight.columns = ['weight']
            df_code.columns = ['code']
            df_final = pd.concat([df_code, df_weight], axis=1)
            
            if df_final['weight'].sum() < 0.99 or df_final['weight'].sum() > 1.01:
                logger.warning(f"Portfolio {score_name} weights sum is not 1 for date {target_date}: {df_final['weight'].sum()}")
                continue
                
            df_final['weight'] = df_final['weight'] / df_final['weight'].sum()
            df_final.to_csv(daily_outputpath2, index=False)
            
            if is_sql==True:
                df_final['portfolio_name'] = score_name
                df_final['valuation_date'] = target_date
                df_final = df_final[['valuation_date', 'portfolio_name'] + df_final.columns.tolist()[:-2]]
                sm.df_to_sql(df_final,'portfolio_name',score_name)
                logger.info(f"Successfully saved {score_name} to SQL database for date {target_date}")
            
            logger.info(f"Successfully updated portfolio {score_name} for date {target_date}")
        
        logger.info(f"Completed processing portfolio: {score_name}")
    
    logger.info("Portfolio history updating completed")

def all_score_name_list_withdraw():
    logger.info("Withdrawing all score names...")
    inputpath=os.path.join(global_config_path,'Score_config\\mode_dictionary.xlsx')
    df=pd.read_excel(inputpath)
    score_name_list=df['score_name'].tolist()
    logger.info(f"Found {len(score_name_list)} score names")
    return score_name_list

def all_portfolio_updating(start_date,end_date,is_sql):
    logger.info(f"Starting all portfolio updating from {start_date} to {end_date}")
    score_name_list=all_score_name_list_withdraw()
    portfolio_updating(score_name_list, start_date, end_date,is_sql)
    logger.info("All portfolio updating completed")

if __name__ == '__main__':
    all_portfolio_updating('2025-05-19','2025-05-19', False)
