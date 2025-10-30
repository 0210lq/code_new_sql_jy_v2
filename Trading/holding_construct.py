"""
持仓构建模块

这个模块负责根据投资组合权重和市场数据构建持仓数据。
计算每个股票的目标持仓数量，用于后续的交易订单生成。

主要功能：
1. 根据权重计算目标持仓数量
2. 处理缺失的市场数据
3. 持仓数量的标准化（100股为单位）

依赖模块：
- pandas：数据处理
- global_setting.global_dic：全局配置
- global_tools：全局工具函数

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
source=glv.get('source')
config_path=glv.get('config_path')

class holding_construction:
    """
    持仓构建类
    
    根据投资组合权重和可用资金计算每个股票的目标持仓数量。
    处理市场数据缺失的情况，并确保持仓数量符合交易规则。
    """
    
    def __init__(self,df_portfolio,df_mkt,stock_money):
        """
        初始化持仓构建对象
        
        Args:
            df_portfolio (pandas.DataFrame): 投资组合权重数据，包含code和weight列
            df_mkt (pandas.DataFrame): 市场数据，包含code和close列
            stock_money (float): 可用于股票投资的资金金额
        """
        self.df_portfolio=df_portfolio
        self.df_mkt=df_mkt
        self.stock_money=stock_money
    
    def consturction_main(self):
        """
        持仓构建主函数
        
        根据投资组合权重计算每个股票的目标持仓数量。
        处理缺失的市场数据，并将持仓数量标准化为100股的整数倍。
        
        Returns:
            pandas.DataFrame: 持仓数据，包含code和quantity列
        """
        df=self.df_portfolio.copy()
        df_mkt=self.df_mkt.copy()
        
        # 计算每个股票的目标投资金额
        df['target_money']=df['weight']*self.stock_money
        
        # 合并市场数据获取收盘价
        df=df.merge(df_mkt,on='code',how='left')
        df['close']=df['close'].astype(float)
        
        # 检查缺失的市场数据
        slice_df=df[(df['close'].isna())|(df['close']==0)]
        missing_list=[]
        if len(slice_df)>0:
            print('以上数据为空')
            missing_list=slice_df['code'].tolist()
        
        # 移除缺失数据的股票
        if len(missing_list)>0:
            df=df[~(df['code'].isin(missing_list))]
        
        # 计算持仓数量并标准化为100股的整数倍
        df['quantity']=df['target_money']/df['close']
        df['quantity']=round(df['quantity']/100,0)*100
        
        # 返回最终的持仓数据
        df=df[['code','quantity']]
        return df

