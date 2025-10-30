"""
交易检查模块

这个模块用于检查交易订单的差异，比较生产环境和测试环境的交易订单，
确保交易订单的正确性和一致性。

主要功能：
1. 目标日期决策
2. 仁睿交易订单差异检查
3. 宣夜交易订单差异检查
4. 持仓差异分析

依赖模块：
- pandas：数据处理
- global_tools：全局工具函数
- datetime：日期时间处理

作者：[作者名]
创建时间：[创建时间]
"""

import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import datetime
from datetime import date
# pd.set_option('display.max_columns', None)

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

def renrHolding_check():
    """
    仁睿持仓检查函数
    
    比较仁睿产品的生产环境和测试环境交易订单差异。
    分析交易数量、交易方向的差异，并输出差异统计信息。
    """
    target_date=target_date_decision()
    target_date=gt.intdate_transfer(target_date)
    inputpath2 = 'D:\Trading_data_test\\trading_order\仁睿价值精选1号'
    inputpath = 'D:\Trading_data_new\\trading_order\仁睿'
    inputpath = os.path.join(inputpath, 'renr_'+str(target_date)+'_trading_list.csv')
    inputpath2 = os.path.join(inputpath2, '仁睿价值精选1号_'+str(target_date)+'_trading_list.csv')
    df=gt.readcsv(inputpath)
    df2=gt.readcsv(inputpath2)
    df.sort_values('代码',inplace=True)
    df2.sort_values('代码', inplace=True)
    df2 = df2[['代码', '数量', '方向']]
    df2.columns = ['代码', 'quantity_prod', '方向_prod']
    df = df.merge(df2, on='代码', how='outer')
    df.fillna(0, inplace=True)
    df['difference_quantity'] = abs(df['数量'].astype(float) - df['quantity_prod'].astype(float))
    df['difference_direction']=abs(df['方向'].astype(float) - df['方向_prod'].astype(float))
    df = df[(df['difference_quantity'] != 0)|(df['difference_direction'])!=0]
    print('任瑞trading差异')
    print(len(df[df['difference_quantity']>100]))
    print(df[df['difference_quantity']>200])
    print(len(df[df['difference_quantity']>200]))
    print(df)

def xyHolding_check():
    """
    宣夜持仓检查函数
    
    比较宣夜产品的生产环境和测试环境交易订单差异。
    分析交易数量、交易方向的差异，并输出差异统计信息。
    """
    target_date=target_date_decision()
    target_date=gt.intdate_transfer(target_date)
    inputpath2 = 'D:\Trading_data_test\\trading_order\宣夜惠盈1号'
    inputpath = 'D:\Trading_data_new\\trading_order\宣夜'
    inputpath = os.path.join(inputpath, 'xy_'+str(target_date)+'_trading_list.csv')
    inputpath2 = os.path.join(inputpath2, '宣夜惠盈1号_'+str(target_date)+'_trading_list.csv')
    df = pd.read_csv(inputpath, header=None)
    df2 = pd.read_csv(inputpath2, header=None)
    df = df.loc[7:]
    df2 = df2.loc[7:]
    df = df[[1, 2, 3]]
    df2 = df2[[1, 2, 3]]
    df.columns = ['代码', '数量', '方向']
    df2.columns = ['代码', '数量', '方向']
    
    def direction_decision(x):
        """
        方向决策函数
        
        将中文方向转换为数字编码。
        
        Args:
            x (str): 方向字符串（'买入'或'卖出'）
            
        Returns:
            int: 方向编码（0表示买入，1表示卖出）
        """
        if x=='买入':
            return 0
        else:
            return 1
    
    df['方向']=df['方向'].apply(lambda x: direction_decision(x))
    df2['方向']=df2['方向'].apply(lambda x: direction_decision(x))
    df2 = df2[['代码', '数量', '方向']]
    df2.columns = ['代码', 'quantity_prod', '方向_prod']
    df = df.merge(df2, on='代码', how='outer')
    df.fillna(0, inplace=True)
    df['difference_quantity'] = abs(df['数量'].astype(float) - df['quantity_prod'].astype(float))
    df['difference_direction']=abs(df['方向'].astype(float) - df['方向_prod'].astype(float))
    df = df[(df['difference_quantity'] != 0)|(df['difference_direction']!=0)]
    print('宣夜trading差异')
    print(df)

def Holding_checking_main():
    """
    持仓检查主函数
    
    执行所有产品的持仓检查，包括仁睿和宣夜两个产品。
    """
    renrHolding_check()
    xyHolding_check()