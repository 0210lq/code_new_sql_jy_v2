import sys
from datetime import date
import datetime
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
from Optimizer_Backtesting.portfolio_checking.portfolio_checking import portfolio_checking,portfolio_Error_raising
import os
from Optimizer_python.utils_log.logger import setup_logger
from datetime import datetime
# Setup logger for this module
logger = setup_logger('portfolio_updating')

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

def score_name_withdraw(score_type):
    logger.info(f"Withdrawing score names for type: {score_type}")
    inputpath_mode_dic = os.path.join(global_config_path,'Score_config\\mode_dictionary.xlsx')
    df_mode_dic = pd.read_excel(inputpath_mode_dic)
    if len(score_type)==4:
         df_mode_dic['score_type']=df_mode_dic['score_name'].apply(lambda x: str(x)[:4])
    else:
        df_mode_dic['score_type'] = df_mode_dic['score_name'].apply(lambda x: str(x)[:2])
    score_list2 = df_mode_dic[df_mode_dic['score_type'] == 'co']['score_name'].tolist()
    df_mode_dic=df_mode_dic[(df_mode_dic['score_type']==score_type)]
    score_list=df_mode_dic['score_name'].tolist()
    if score_type=='fm':
        score_list=score_list+score_list2
    logger.info(f"Found {len(score_list)} score names")
    return score_list

def target_date_decision():
    logger.info("Deciding target date...")
    if gt.is_workday_auto() == True:
        today = date.today()
        # today = date(2025, 8, 5)
        next_day = gt.next_workday_calculate(today)
        critical_time = '20:00'
        # time_now = datetime.datetime.now().strftime("%H:%M")
        time_now = datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            logger.info(f"After critical time, using next workday: {next_day}")
            return next_day
        else:
            today=today.strftime('%Y-%m-%d')
            logger.info(f"Before critical time, using today: {today}")
            return today
    else:
        today = date.today()
        next_day=gt.next_workday_calculate(today)
        logger.info(f"Not a workday, using next workday: {next_day}")
        return next_day

def score_type_decision():
    logger.info("Deciding score type based on current time...")
    critical_time_fm_start = '19:30'
    critical_time_fm_end = '24:00'
    time_now = datetime.datetime.now().strftime("%H:%M")
    if critical_time_fm_start<=time_now<=critical_time_fm_end:
        logger.info("Time is within FM window")
        return 'fm'
    else:
        logger.info("Time is outside of any scoring window")
        return None

def portfolio_updating(score_type,is_sql):
    logger.info(f"Starting portfolio updating for score type: {score_type}")
    inputpath_portfolio=glv.get('output_optimizer')
    outputpath_weight=glv.get('output_weight')
    outputpath_check=glv.get('output_check')
    score_name_list = score_name_withdraw(score_type)
    
    if is_sql==True:
        logger.info("SQL mode enabled, initializing SQL connection")
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        inputpath_configsql = os.path.join(current_dir, 'global_setting\\optimizer_sql.yaml')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
    
    target_date = target_date_decision()
    logger.info(f"Processing {len(score_name_list)} portfolios for date: {target_date}")
    
    for score_name in score_name_list:
        logger.info(f"Processing portfolio: {score_name}")
        target_date2=gt.intdate_transfer(target_date)
        daily_outputpath=os.path.join(outputpath_weight,score_name)
        daily_check=os.path.join(outputpath_check,score_name)
        daily_style=os.path.join(daily_check,'style')
        daily_industry=os.path.join(daily_check,'industry')
        
        gt.folder_creator2(daily_style)
        gt.folder_creator2(daily_industry)
        gt.folder_creator2(daily_outputpath)
        
        daily_outputpath=os.path.join(daily_outputpath,str(score_name)+'_'+target_date2+'.csv')
        daily_style = os.path.join(daily_style, str(score_name) + '_StyleCheck_' + target_date2 + '.csv')
        daily_industry = os.path.join(daily_industry, str(score_name) + '_IndustryCheck_' + target_date2 + '.csv')
        daily_inputpath=os.path.join(inputpath_portfolio,score_name)
        daily_inputpath=os.path.join(daily_inputpath,target_date)
        inputpath_weight=os.path.join(daily_inputpath,'weight.csv')
        inputpath_code=os.path.join(daily_inputpath,'Stock_code.csv')
        
        try:
            df_weight=pd.read_csv(inputpath_weight,header=None)
            df_code=pd.read_csv(inputpath_code)
        except Exception as e:
            logger.error(f"Failed to update {score_name}: {str(e)}")
            continue
            
        df_weight.columns=['weight']
        df_code.columns=['code']
        df_final=pd.concat([df_code,df_weight],axis=1)
        
        if df_final['weight'].sum()<0.99 or df_final['weight'].sum()>1.01:
            logger.warning(f"Portfolio {score_name} weights sum is not 1: {df_final['weight'].sum()}")
            
        df_final['weight']=df_final['weight']/df_final['weight'].sum()
        df_style, df_industry = portfolio_checking(score_name, target_date)
        
        df_style.to_csv(daily_style,encoding='gbk',index=False)
        df_industry.to_csv(daily_industry,encoding='gbk',index=False)
        df_final.to_csv(daily_outputpath,index=False)
        
        if is_sql==True:
            df_final['portfolio_name'] = score_name
            df_final['valuation_date'] = target_date
            df_final = df_final[['valuation_date', 'portfolio_name'] + df_final.columns.tolist()[:-2]]
            now = datetime.now()
            df_final['update_time'] = now
            sm.df_to_sql(df_final,'portfolio_name',score_name)
            logger.info(f"Successfully saved {score_name} to SQL database")
        
        logger.info(f"Successfully updated portfolio: {score_name}")

def portfolio_updating2(score_name,start_date,end_date,is_sql):
    logger.info(f"Starting historical portfolio updating for {score_name} from {start_date} to {end_date}")
    inputpath_portfolio=glv.get('portfolio_data')
    outputpath_weight=glv.get('output_weight')
    working_days_list=gt.working_days_list(start_date,end_date)
    
    if is_sql==True:
        logger.info("SQL mode enabled, initializing SQL connection")
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        inputpath_configsql = os.path.join(current_dir, 'global_setting\\optimizer_sql.yaml')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio',delete=True)
    
    for target_date in working_days_list:
        logger.info(f"Processing date: {target_date}")
        target_date2=gt.intdate_transfer(target_date)
        daily_outputpath=os.path.join(outputpath_weight,score_name)
        gt.folder_creator2(daily_outputpath)
        daily_outputpath=os.path.join(daily_outputpath,str(score_name)+'_'+target_date2+'.csv')
        daily_inputpath=os.path.join(inputpath_portfolio,score_name)
        daily_inputpath=os.path.join(daily_inputpath,target_date)
        inputpath_weight=os.path.join(daily_inputpath,'weight.csv')
        inputpath_code=os.path.join(daily_inputpath,'Stock_code.csv')
        
        try:
            df_weight=pd.read_csv(inputpath_weight,header=None)
            df_code=pd.read_csv(inputpath_code)
        except Exception as e:
            logger.error(f"Failed to update {score_name} for date {target_date}: {str(e)}")
            continue
            
        df_weight.columns=['weight']
        df_code.columns=['code']
        df_final=pd.concat([df_code,df_weight],axis=1)
        
        if df_final['weight'].sum()<0.99 or df_final['weight'].sum()>1.01:
            logger.warning(f"Portfolio {score_name} weights sum is not 1 for date {target_date}: {df_final['weight'].sum()}")
            continue
            
        df_final['weight']=df_final['weight']/df_final['weight'].sum()
        df_final.to_csv(daily_outputpath,index=False)
        
        if is_sql==True:
            df_final['portfolio_name'] = score_name
            df_final['valuation_date'] = target_date
            df_final = df_final[['valuation_date', 'portfolio_name'] + df_final.columns.tolist()[:-2]]
            now = datetime.now()
            df_final['update_time'] = now
            sm.df_to_sql(df_final,'portfolio_name',score_name)
            logger.info(f"Successfully saved {score_name} to SQL database for date {target_date}")
        
        logger.info(f"Successfully updated portfolio {score_name} for date {target_date}")

def portfolio_updating_auto(is_sql):
    logger.info("Starting automatic portfolio updating")
    score_type='fm'
    if score_type!=None:
        portfolio_updating(score_type,is_sql)
        target_date = target_date_decision()
        portfolio_Error_raising(target_date,is_sql)
    logger.info("Automatic portfolio updating completed")

def portfolio_updating_bu(is_sql):
    logger.info("Starting batch update of portfolios")
    df=history_config_withdraw()
    for i in range(len(df)):
        start_date=df['start_date'].tolist()[i]
        end_date = df['end_date'].tolist()[i]
        start_date=gt.strdate_transfer(start_date)
        end_date = gt.strdate_transfer(end_date)
        score_name = df['score_name'].tolist()[i]
        logger.info(f"Processing batch update for {score_name} from {start_date} to {end_date}")
        portfolio_updating2(score_name,start_date,end_date,is_sql)
    logger.info("Batch update completed")

if __name__ == '__main__':
    portfolio_updating_auto(True)
    #print(gt.is_workday2())
