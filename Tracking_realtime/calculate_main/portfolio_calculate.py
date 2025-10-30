"""
投资组合计算模块
功能：处理投资组合级别的计算，包括纸面投资组合和产品投资组合的分析
主要类：
- portfolio_tracking: 投资组合跟踪计算类，负责投资组合级别的完整计算流程
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
import numpy as np
from data.data_prepared import weight_withdraw, prod_info

def sql_path():
    """
    获取SQL配置文件路径
    功能：构建SQL配置文件的完整路径
    
    Returns:
        str: SQL配置文件的完整路径
    """
    yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path

# 全局变量定义
global inputpath_sql
inputpath_sql = sql_path()

class portfolio_tracking:
    """
    投资组合跟踪计算类
    功能：处理投资组合级别的完整计算流程，包括纸面投资组合和产品投资组合的分析
    """
    
    def __init__(self):
        """
        初始化方法
        功能：初始化投资组合跟踪对象，设置日期和权重获取器
        """
        today = datetime.date.today()
        self.ww = weight_withdraw()  # 权重数据获取器
        self.date = gt.strdate_transfer(today)  # 当前日期字符串
        self.now = datetime.datetime.now().replace(tzinfo=None)  # 当前时间
    
    def index_type_decision(self, x):
        """
        指数类型判断
        功能：根据投资组合名称判断对应的指数类型
        
        参数：
            x (str): 投资组合名称
            
        返回：
            str: 对应的指数类型，如果无法判断则返回None
        """
        if 'hs300' in x:
            return '沪深300'
        elif 'sz50' in x:
            return '上证50'
        elif 'zz500' in x:
            return '中证500'
        elif 'zzA500' in x:
            return '中证A500'
        elif 'zz1000' in x:
            return '中证1000'
        elif 'zz2000' in x:
            return '中证2000'
        elif 'top' in x:
            return '中证500'
        else:
            return None
    
    def paperportfolio_withdraw(self):
        """
        获取纸面投资组合数据
        功能：获取所有纸面投资组合的权重数据
        
        返回：
            DataFrame: 包含所有纸面投资组合权重数据的DataFrame
        """
        df_final = pd.DataFrame()
        # 获取投资组合列表
        portfolio_list = self.ww.portfolio_list_getting()
        
        # 遍历每个投资组合
        for portfolio_name in portfolio_list:
            # 判断指数类型
            index_type = self.index_type_decision(portfolio_name)
            # 获取投资组合权重数据
            df = self.ww.portfolio_withdraw(portfolio_name)
            # 添加日期、投资组合名称和指数类型
            df['valuation_date'] = self.date
            df['portfolio_name'] = portfolio_name
            df['index_type'] = index_type
            # 合并到最终数据框
            df_final = pd.concat([df_final, df])
        
        return df_final
    
    def productportfolio_withdraw(self):
        """
        获取产品投资组合数据
        功能：获取所有产品投资组合的权重数据
        
        返回：
            DataFrame: 包含所有产品投资组合权重数据的DataFrame
        """
        # 获取产品列表
        product_list = self.ww.product_list_getting()
        df_final = pd.DataFrame()
        
        # 遍历每个产品
        for product_code in product_list:
            # 获取产品信息
            pi = prod_info(product_code)
            index_type = pi.get_product_detail('index')
            # 获取产品权重数据
            df = self.ww.product_withdraw(product_code)
            # 添加日期、产品代码和指数类型
            df['valuation_date'] = self.date
            df['portfolio_name'] = product_code
            df['index_type'] = index_type
            # 合并到最终数据框
            df_final = pd.concat([df_final, df])
        
        return df_final
    
    def portfolioTracking_main(self):
        """
        投资组合跟踪主函数
        功能：执行投资组合级别的完整计算流程，包括数据获取、分析、处理和保存
        """
        # 获取纸面投资组合权重数据
        df_port_weight = self.paperportfolio_withdraw()
        portfolio_name_list = df_port_weight['portfolio_name'].unique().tolist()
        
        # 获取产品投资组合权重数据
        df_prod_weight = self.productportfolio_withdraw()
        
        # 合并所有投资组合数据
        df_cal = pd.concat([df_port_weight, df_prod_weight])
        
        # 进行投资组合分析
        df_info, df_detail = gt.portfolio_analyse(df_cal, cost_stock=0, realtime=True)
        
        # 处理投资组合信息数据
        df_info = df_info[['valuation_date', 'portfolio_name', 'paper_return', 'excess_paper_return']]
        df_info['update_time'] = self.now
        # 将收益率转换为基点单位
        df_info['Excess_Return_bp'] = round(df_info['excess_paper_return'] * 10000, 2)
        df_info['Portfolio_Return_bp'] = round(df_info['paper_return'] * 10000, 2)
        df_info = df_info[['valuation_date', 'portfolio_name', 'Excess_Return_bp', 'Portfolio_Return_bp', 'update_time']]
        
        # 分离纸面投资组合和产品投资组合数据
        df_port = df_info[df_info['portfolio_name'].isin(portfolio_name_list)]
        df_prod = df_info[~(df_info['portfolio_name'].isin(portfolio_name_list))]
        df_prod.rename(columns={'portfolio_name': 'product_code'}, inplace=True)
        
        # 保存纸面投资组合数据到数据库
        if len(df_port) > 0:
            sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn', delete=True)
            sm.df_to_sql(df_port)
        
        # 保存产品投资组合数据到数据库
        if len(df_prod) > 0:
            df_prod['valuation_date']= self.date
            sm = gt.sqlSaving_main(inputpath_sql, 'productreturn', delete=True)
            sm.df_to_sql(df_prod)


if __name__ == '__main__':
    # 获取配置文件路径
    config_path = glv.get('config_path')
    # 清理实时期货期权持仓表
    gt.table_manager(config_path, 'tracking_realtime', 'realtime_futureoptionholding')
    # pt = portfolio_tracking()
    # pt.portfolioTracking_main()
