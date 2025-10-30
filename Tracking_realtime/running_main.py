"""
实时跟踪系统主程序
功能：在交易时间内自动执行投资组合和产品的实时跟踪计算
作者：[作者名]
创建时间：[创建时间]
"""

import pandas as pd
import os
import sys

# 添加全局工具函数路径到系统路径
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)

import datetime
import global_tools as gt
import global_setting.global_dic as glv
from calculate_main.product_calculate import product_tracking
from calculate_main.portfolio_calculate import portfolio_tracking
from history_sql_saving import historySql_saving

# 获取配置文件路径
config_path = glv.get('config_path')

def tracking_realtime_main():
    """
    实时跟踪主函数
    功能：
    1. 检查是否为工作日
    2. 在9:00-9:30期间清理实时数据表
    3. 执行投资组合跟踪计算
    4. 循环执行各产品的跟踪计算
    5. 在16:00后执行历史数据保存
    """
    # 检查是否为工作日，只有在工作日才执行跟踪计算
    if gt.is_workday_auto() == True:
        # 获取当前时间
        time = datetime.datetime.now()
        
        # 在开盘前（9:00-9:30）清理实时数据表，确保数据新鲜度
        if time.hour == 9 and time.minute < 30:
            # 清理期货期权持仓表
            gt.table_manager2(config_path, 'tracking_realtime', 'realtime_futureoptionholding')
            # 清理持仓变化表
            gt.table_manager2(config_path, 'tracking_realtime', 'realtime_holdingchanging')
            # 清理投资组合收益表
            gt.table_manager2(config_path, 'tracking_realtime', 'realtime_portfolioreturn')
            # 清理产品信息表
            gt.table_manager2(config_path, 'tracking_realtime', 'realtime_proinfo')
        
        # 执行投资组合级别的跟踪计算
        pt = portfolio_tracking()
        pt.portfolioTracking_main()
        
        # 定义需要跟踪的产品代码列表
        product_list = ['SGS958', 'SLA626', 'SNY426', 'SSS044', 'SVU353', 'STH580', 'SST132']
        # 循环执行每个产品的跟踪计算
        for product_code in product_list:
            try:
                # 创建产品跟踪对象并执行跟踪计算
                pt2 = product_tracking(product_code)
                pt2.productTracking_main()
            except Exception as e:
                # 如果某个产品更新失败，打印错误信息但不影响其他产品
                print(f"{product_code}更新有误: {str(e)}")
        
        # 在收盘后（16:00后）执行历史数据保存
        if time.hour >= 17:
            hs = historySql_saving()
            hs.historySql_main()

# 程序入口点
if __name__ == "__main__":
    tracking_realtime_main()
    # gt.table_manager2(config_path, 'tracking_realtime', 'realtime_futureoptionholding')
    # # 清理持仓变化表
    # gt.table_manager2(config_path, 'tracking_realtime', 'realtime_holdingchanging')
    # # 清理投资组合收益表
    # gt.table_manager2(config_path, 'tracking_realtime', 'realtime_portfolioreturn')
    # # 清理产品信息表
    # gt.table_manager2(config_path, 'tracking_realtime', 'realtime_proinfo')
    # gt.table_manager2(config_path, 'tracking_realtime', 'history_futureoptionholding')
    # # 清理持仓变化表
    # gt.table_manager2(config_path, 'tracking_realtime', 'history_holdingchanging')
    # # 清理投资组合收益表
    # gt.table_manager2(config_path, 'tracking_realtime', 'history_portfolioreturn')
    # # 清理产品信息表
    # gt.table_manager2(config_path, 'tracking_realtime', 'history_proinfo')