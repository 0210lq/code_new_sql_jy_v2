"""
主运行模块

这个模块是交易系统的主入口，负责协调各个子模块的执行。
主要功能包括目标日期决策、投资组合保存和交易订单生成。

主要功能：
1. 目标日期决策逻辑
2. 投资组合数据保存
3. 交易订单生成（宣夜和仁睿）
4. 持仓检查

依赖模块：
- trading_order.trading_order_processing：交易订单处理
- portfolio_saving：投资组合保存
- global_setting.global_dic：全局配置
- global_tools：全局工具函数
- trading_check：交易检查

作者：[作者名]
创建时间：[创建时间]
"""

from trading_order.trading_order_processing import trading_xy_main,trading_rr_main
from portfolio_saving import portfolio_saving_main
import os
import pandas as pd
import global_setting.global_dic as glv
import sys
from datetime import date
import datetime
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from trading_check import Holding_checking_main
global source,config_path
from datetime import date
source=glv.get('source')
config_path=glv.get('config_path')

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

def PortfolioSaving_main(is_realtime=False):
    """
    投资组合保存主函数
    
    执行投资组合数据的保存操作，包括投资组合信息、持仓数据等。
    
    Args:
        is_realtime (bool): 是否为实时模式，默认为False
    """
    target_date=target_date_decision()
    ps=portfolio_saving_main(target_date,is_realtime)
    ps.sqlSaving_main()

def TradingOder_main(is_realtime=False):
    """
    交易订单生成主函数
    
    生成宣夜和仁睿两个产品的交易订单。
    
    Args:
        is_realtime (bool): 是否为实时模式，默认为False
    """
    trading_xy_main('v2',is_realtime)
    trading_rr_main(is_realtime)
    if is_realtime==False:
        Holding_checking_main()

if __name__ == '__main__':
    # PortfolioSaving_main()
    TradingOder_main()
    # Holding_checking_main()