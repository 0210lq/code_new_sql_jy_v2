"""
交易订单处理模块

这个模块负责处理交易订单的生成，包括宣夜和仁睿两个产品的交易订单。
协调数据准备、交易订单生成等各个子模块的执行。

主要功能：
1. 目标日期决策
2. 非交易股票列表获取
3. 宣夜交易订单生成
4. 仁睿交易订单生成
5. 资金重平衡

依赖模块：
- pandas：数据处理
- global_setting.global_dic：全局配置
- global_tools：全局工具函数
- data_prepared：数据准备
- trading_order.trading_order_xuanye：宣夜交易订单
- trading_order.trading_order_renrui：仁睿交易订单

作者：[作者名]
创建时间：[创建时间]
"""

import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import datetime
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
global source,config_path
from datetime import date
import datetime
source=glv.get('source')
config_path=glv.get('config_path')
from data_prepared import data_prepared
from trading_order.trading_order_xuanye import trading_xuanye
from trading_order.trading_order_renrui import trading_renrui

def target_date_decision():
    """
    目标日期决策函数
    
    根据当前时间和工作日判断来确定目标交易日期。
    如果当前是工作日且时间在20:00之前，则使用当天日期；
    否则使用下一个工作日作为目标日期。
    
    Returns:
        str: 目标日期字符串，格式为'YYYY-MM-DD'
    """
    if gt.is_workday_auto() == True:
        today = date.today()
        today = date(2025,8,14)
        next_day = gt.next_workday_calculate(today)
        critical_time = '20:00'
        time_now = datetime.datetime.now().strftime("%H:%M")
        if time_now >= critical_time:
            return next_day
        else:
            today = gt.strdate_transfer(today)
            return today
    else:
        today = date.today()
        next_day = gt.next_workday_calculate(today)
        return next_day

def nontrading_list_getting(product_code):
    """
    获取非交易股票列表
    
    从交易配置文件中读取指定产品的非交易股票列表。
    这些股票将不会在交易订单中出现。
    
    Args:
        product_code (str): 产品代码
        
    Returns:
        list: 非交易股票代码列表
    """
    inputpath=glv.get('config_trading')
    df=pd.read_excel(inputpath,sheet_name=product_code)
    if len(df)>0:
        df=gt.code_transfer(df)
    nontrading_list=df['code'].tolist()
    return nontrading_list

def trading_xy_main(trading_mode,is_realtime):
    """
    宣夜交易订单生成主函数
    
    生成宣夜产品的交易订单，支持不同的交易模式。
    
    Args:
        trading_mode (str): 交易模式，'v1'为TWAP模式，'v2'为VWAP模式
        is_realtime (bool): 是否为实时模式
    """
    # to_mode选v1是跃然的t0 选v2是景泰的t0
    # trading_mode选v1是twap 选v2是vwap
    target_time=target_date_decision()
    dp=data_prepared(target_time,is_realtime)
    stock_money=dp.productInfo_withdraw('SGS958')
    df_mkt,etf_pool=dp.mktData_withdraw()
    df_weight=dp.productTargetWeight_withdraw('SGS958')
    df_holding=dp.productHolding_withdraw('SGS958')
    df_holding=df_holding[~(df_holding['code']=='204001')]
    nontrading_code_list=nontrading_list_getting('SGS958')
    trading_time=dp.tradingTime_withdraw('SGS958')
    
    # 处理非交易股票的资金锁定
    if len(nontrading_code_list)>0:
         df_nontrading=df_holding[df_holding['code'].isin(nontrading_code_list)]
         df_nontrading=df_nontrading.merge(df_mkt,on='code',how='left')
         df_nontrading['mkt_value']=df_nontrading['holding']*df_nontrading['close']
         lock_money=df_nontrading['mkt_value'].sum()
         df_holding=df_holding[~(df_holding['code'].isin(nontrading_code_list))]
         df_weight = df_weight[~(df_weight['code'].isin(nontrading_code_list))]
         df_weight['weight']=df_weight['weight']/df_weight['weight'].sum()
         stock_money=stock_money-lock_money
    
    trading_xy=trading_xuanye(df_weight,df_holding,df_mkt,target_time,stock_money,etf_pool,trading_time)
    trading_xy.trading_order_xy_main(trading_mode)

def trading_rr_main(is_realtime):
    """
    仁睿交易订单生成主函数
    
    生成仁睿产品的交易订单。
    
    Args:
        is_realtime (bool): 是否为实时模式
    """
    target_date = target_date_decision()
    target_date2=gt.intdate_transfer(target_date)
    target_time = target_date_decision()
    dp = data_prepared(target_time, is_realtime)
    stock_money = dp.productInfo_withdraw('SLA626')
    df_mkt, etf_pool = dp.mktData_withdraw()
    df_weight = dp.productTargetWeight_withdraw('SLA626')
    df_holding = dp.productHolding_withdraw('SLA626')
    df_holding = df_holding[~(df_holding['code'] == '204001')]
    nontrading_code_list = nontrading_list_getting('SLA626')
    
    # 处理非交易股票的资金锁定
    if len(nontrading_code_list) > 0:
        df_nontrading = df_holding[df_holding['code'].isin(nontrading_code_list)]
        df_nontrading = df_nontrading.merge(df_mkt, on='code', how='left')
        df_nontrading['mkt_value'] = df_nontrading['holding'] * df_nontrading['close']
        lock_money = df_nontrading['mkt_value'].sum()
        df_holding = df_holding[~(df_holding['code'].isin(nontrading_code_list))]
        df_weight = df_weight[~(df_weight['code'].isin(nontrading_code_list))]
        df_weight['weight'] = df_weight['weight'] / df_weight['weight'].sum()
        stock_money = stock_money - lock_money
    
    trading_renr=trading_renrui(df_weight,df_holding,df_mkt,target_date,stock_money)
    trading_renr.trading_order_renrui()
