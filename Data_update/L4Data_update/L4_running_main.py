from L4Data_update.L4Data_preparing import L4Data_preparing
from L4Data_update.L4Holding_update import L4Holding_update
from L4Data_update.L4Info_update import L4Info_update
import global_setting.global_dic as glv
import pandas as pd
from L4Data_update.tools_func import tools_func
import datetime
from datetime import date
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from setup_logger.logger_setup import setup_logger

logger = setup_logger('L4Data_update')

def valid_productCode_withdraw():
    """获取有效的产品代码列表"""
    logger.info("Retrieving valid product codes")
    inputpath = glv.get('L4_config')
    df = pd.read_excel(inputpath)
    product_code_list = df['product_code'].tolist()
    logger.info(f"Found {len(product_code_list)} valid product codes")
    return product_code_list

def valid_productName_withdraw():
    """获取有效的产品名称列表"""
    logger.info("Retrieving valid product names")
    inputpath = glv.get('L4_config')
    df = pd.read_excel(inputpath)
    product_name_list = df['product_name'].tolist()
    logger.info(f"Found {len(product_name_list)} valid product names")
    return product_name_list

def target_date_decision_L4():
    logger.info("Determining target date for L4 update")
    today = date.today()
    today=gt.strdate_transfer(today)
    available_date = gt.last_workday_calculate(today)
    logger.info(f"Target date determined: {available_date}")
    return available_date

def L4_running_main(product_code_list,start_date,end_date,is_sql):
    """L4数据更新主函数"""
    logger.info('\n' + '*'*50 + '\nL4 RUNNING MAIN PROCESS\n' + '*'*50)
    logger.info(f"Starting L4 update process from {start_date} to {end_date}")
    tf=tools_func()
    working_days_list=gt.working_days_list(start_date,end_date)
    logger.info(f"Processing {len(working_days_list)} working days")
    
    for available_date in working_days_list:
        logger.info(f"\nProcessing date: {available_date}")
        for product_code in product_code_list:
            logger.info(f"Processing product code: {product_code}")
            available_date2=gt.intdate_transfer(available_date)
            lp = L4Data_preparing(product_code, available_date2)
            try:
                daily_df = lp.raw_L4_withdraw()
            except:
                product_name=tf.product_NameCode_transfer(product_code)
                logger.warning(f"No data found for {product_name} on {available_date}")
                daily_df=pd.DataFrame()
            if len(daily_df)!=0:
                logger.info(f"Processing data for {product_code} on {available_date}")
                lh = L4Holding_update(product_code, available_date2, daily_df,is_sql)
                li = L4Info_update(product_code, available_date2, daily_df,is_sql)
                available_date_yes=gt.last_workday_calculate(available_date)
                available_date_yes=gt.intdate_transfer(available_date_yes)
                lh.L4Holding_processing()
                li.L4Info_processing()
                logger.info(f"Successfully processed {product_code} for {available_date}")
            else:
                logger.warning(f"No data to process for {product_code} on {available_date}")

def L4_update_main(is_sql):
    logger.info('\n' + '*'*50 + '\nL4 UPDATE MAIN PROCESS\n' + '*'*50)
    logger.info("Starting L4 update main process")
    product_code_list=valid_productCode_withdraw()
    target_date=target_date_decision_L4()
    target_date2 = target_date
    for i in range(3):
         target_date2=gt.last_workday_calculate(target_date2)
    logger.info(f"Processing data from {target_date2} to {target_date}")
    L4_running_main(product_code_list, target_date2, target_date,is_sql)
    logger.info("L4 update main process completed")

def L4_history_main(mode,product_name_list,start_date,end_date,is_sql):
    """L4历史数据更新主函数"""
    logger.info('\n' + '*'*50 + '\nL4 HISTORY UPDATE PROCESS\n' + '*'*50)
    logger.info(f"Starting L4 history update process from {start_date} to {end_date}")
    outputpath=glv.get('output_l4')
    tf = tools_func()
    product_code_list=[]
    if mode=='all':
        logger.info("Processing all products")
        product_name_list=valid_productName_withdraw()
    for product_name in product_name_list:
        product_code=tf.product_CodeName_transfer(product_name)
        product_code_list.append(product_code)
    try:
        os.listdir(outputpath)
    except:
        if start_date>'2024-06-01':
            start_date='2025-02-27'
            logger.warning(f"Start date adjusted to {start_date}")
    logger.info(f"Processing {len(product_code_list)} products")
    L4_running_main(product_code_list, start_date, end_date,is_sql)
    logger.info("L4 history update process completed")

if __name__ == '__main__':
    L4_history_main('all',[], '2023-12-13','2025-08-13',True)
