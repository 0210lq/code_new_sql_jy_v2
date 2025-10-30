"""
产品计算模块
功能：处理单个产品的持仓数据计算，包括期货期权、股票ETF可转债等各类资产的分析
主要类：
- product_tracking: 产品跟踪计算类，负责单个产品的完整计算流程
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
from data.data_prepared import futureoption_position, security_position, prod_info

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

class product_tracking:
    """
    产品跟踪计算类
    功能：处理单个产品的完整计算流程，包括数据获取、处理、分析和结果输出
    """
    
    def __init__(self, product_code):
        """
        初始化方法
        功能：初始化产品跟踪对象，获取所有必要的持仓数据和产品信息
        
        参数：
            product_code (str): 产品代码
        """
        self.product_code = product_code
        
        # 获取期货期权持仓数据
        fp = futureoption_position(product_code)
        self.df_future_ori, self.df_option_ori = fp.futureoption_withdraw()
        
        # 处理期货数据，分离今日和昨日数据
        df_future, df_future_yes = self.df_processing(self.df_future_ori)
        self.df_indexFuture, self.df_commFuture, self.df_bond = self.future_split(df_future)
        self.df_indexFuture_yes, self.df_commFuture_yes, self.df_bond_yes = self.future_split(df_future_yes)
        
        # 处理期权数据
        self.df_option, self.df_option_yes = self.df_processing(self.df_option_ori)
        
        # 获取股票ETF可转债持仓数据
        sp = security_position(product_code)
        self.df_stock_ori, self.df_etf_ori, self.df_cb_ori = sp.security_withdraw()
        self.df_stock, self.df_stock_yes = self.df_processing(self.df_stock_ori)
        self.df_etf, self.df_etf_yes = self.df_processing(self.df_etf_ori)
        self.df_cb, self.df_cb_yes = self.df_processing(self.df_cb_ori)
        
        # 获取产品资产价值
        pi = prod_info(product_code)
        self.asset_value,self.asset_value_yes = pi.assetvalue_withdraw()
        
        # 设置当前日期和时间
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)
        self.now = datetime.datetime.now().replace(tzinfo=None)
    
    def direction_prossing(self, x):
        """
        处理持仓方向
        功能：将中文方向标识转换为数值
        
        参数：
            x (str): 中文方向标识
            
        返回：
            int: 方向数值（1表示多头，-1表示空头）
        """
        x=str(x)
        if '空' in x:
            return -1
        elif '多' in x:
            return 1
        else:
            return 'error'
    
    def futureOption_processing(self, df):
        """
        处理期货期权数据
        功能：将数值方向转换为英文标识，并取绝对值
        
        参数：
            df (DataFrame): 期货期权数据框
            
        返回：
            DataFrame: 处理后的数据框
        """
        def direction_processing2(x):
            if x > 0:
                return 'long'
            else:
                return 'short'
        
        df['direction'] = df['quantity'].apply(lambda x: direction_processing2(x))
        df['quantity'] = abs(df['quantity'])
        return df
    
    def df_processing(self, df):
        """
        处理数据框
        功能：处理持仓数据，分离今日和昨日数据，并应用方向处理
        
        参数：
            df (DataFrame): 原始数据框
            
        返回：
            DataFrame: 今日数据框
            DataFrame: 昨日数据框
        """
        if len(df) > 0:
            # 处理持仓方向
            if 'direction' in df.columns:
                df['direction'] = df['direction'].apply(lambda x: self.direction_prossing(x))
                df=df[~(df['direction']=='error')]
            else:
                df['direction'] = 1
            
            # 应用方向到数量
            df['quantity'] = df['quantity'] * df['direction']
            df['pre_quantity'] = df['pre_quantity'] * df['direction']
            
            # 分离今日和昨日数据
            df_today = df[['code', 'quantity']]
            df_yes = df[['code', 'pre_quantity']]
            df_yes.columns = ['code', 'quantity']
        else:
            # 如果数据为空，创建空数据框
            df_today = pd.DataFrame()
            df_yes = pd.DataFrame()
        
        return df_today, df_yes
    
    def future_split(self, df):
        """
        分离期货类型
        功能：根据代码前缀分离指数期货、商品期货和债券期货
        
        参数：
            df (DataFrame): 期货数据框
            
        返回：
            DataFrame: 指数期货数据框
            DataFrame: 商品期货数据框
            DataFrame: 债券期货数据框
        """
        # 根据代码前缀分类
        df['new_code'] = df['code'].apply(lambda x: str(x)[:1])
        df_future = df[~(df['new_code'] == 'T')]  # 非债券期货
        df_indexFuture = df_future[df_future['new_code'] == 'I']  # 指数期货
        df_commFuture = df_future[~(df_future['new_code'] == 'I')]  # 商品期货
        df_bond = df[df['new_code'] == 'T']  # 债券期货
        
        # 删除临时列
        df_indexFuture.drop(columns='new_code', inplace=True)
        df_commFuture.drop(columns='new_code', inplace=True)
        df_bond.drop(columns='new_code', inplace=True)
        
        return df_indexFuture, df_commFuture, df_bond
    
    def partial_analysis(self):
        """
        部分分析
        功能：对各类资产进行部分分析，计算风险指标和收益
        
        返回：
            DataFrame: 投资组合信息
            DataFrame: 详细持仓信息
        """
        # 为各类资产添加投资组合名称
        df_indexFuture = self.df_indexFuture.copy()
        df_indexFuture['portfolio_name'] = 'indexFuture'
        df_commFuture = self.df_commFuture.copy()
        df_commFuture['portfolio_name'] = 'commFuture'
        df_bond = self.df_bond.copy()
        df_bond['portfolio_name'] = 'bond'
        df_stock = self.df_stock.copy()
        df_stock['portfolio_name'] = 'stock'
        df_option = self.df_option.copy()
        df_option['portfolio_name'] = 'option'
        df_etf = self.df_etf.copy()
        df_etf['portfolio_name'] = 'etf'
        df_cb = self.df_cb.copy()
        df_cb['portfolio_name'] = 'convertible_bond'
        
        # 合并所有资产数据
        df_port = pd.concat([df_indexFuture, df_commFuture, df_bond, df_stock, df_option, df_etf, df_cb])
        df_port['valuation_date'] = self.date
        
        # 进行投资组合分析
        df_info, df_detail = gt.portfolio_analyse(df_port, cost_stock=0, cost_etf=0, cost_future=0, cost_option=0, cost_convertiblebond=0, realtime=True, weight_standardize=True)
        
        # 处理详细数据
        df_detail = df_detail[['code', 'quantity', 'delta', 'risk_mkt_value', 'profit', 'portfolio_name']]
        df_detail.rename(columns={'risk_mkt_value': 'mkt_value'}, inplace=True)
        df_detail = self.futureOption_processing(df_detail)
        
        return df_info, df_detail
    
    def info_split(self, df_info, df_detail):
        """
        信息分离
        功能：将分析结果按资产类型分离
        
        参数：
            df_info (DataFrame): 投资组合信息
            df_detail (DataFrame): 详细持仓信息
            
        返回：
            dict: 各类资产的收益和市值信息
        """
        # 分离指数期货信息
        df_indexfuture_info = df_info[df_info['portfolio_name'] == 'indexFuture']
        df_indexfuture = df_detail[df_detail['portfolio_name'] == 'indexFuture']
        if len(df_indexfuture_info) == 0:
            indexfuture_profit = 0
            indexfuture_mktvalue = 0
        else:
            indexfuture_profit = df_indexfuture_info['portfolio_profit'].tolist()[0]
            indexfuture_mktvalue = df_indexfuture_info['portfolio_mktvalue'].tolist()[0]
        df_commfuture_info = df_info[df_info['portfolio_name'] == 'commFuture']
        df_commfuture = df_detail[df_detail['portfolio_name'] == 'commFuture']
        if len(df_commfuture_info) == 0:
            commfuture_profit = 0
            commfuture_mktvalue = 0
        else:
            commfuture_profit = df_commfuture_info['portfolio_profit'].tolist()[0]
            commfuture_mktvalue = df_commfuture_info['portfolio_mktvalue'].tolist()[0]
        df_option_info = df_info[df_info['portfolio_name'] == 'option']
        df_option = df_detail[df_detail['portfolio_name'] == 'option']
        if len(df_option_info) == 0:
            option_profit = 0
            option_mktvalue = 0
        else:
            option_profit = df_option_info['portfolio_profit'].tolist()[0]
            option_mktvalue = df_option_info['portfolio_mktvalue'].tolist()[0]
        df_etf_info = df_info[df_info['portfolio_name'] == 'etf']
        if len(df_etf_info) == 0:
            etf_profit = 0
            etf_mktvalue = 0
        else:
            etf_profit = df_etf_info['portfolio_profit'].tolist()[0]
            etf_mktvalue = df_etf_info['portfolio_mktvalue'].tolist()[0]
        df_cb_info = df_info[df_info['portfolio_name'] == 'convertible_bond']
        if len(df_cb_info) == 0:
            cb_profit = 0
            cb_mktvalue = 0
        else:
            cb_profit = df_cb_info['portfolio_profit'].tolist()[0]
            cb_mktvalue = df_cb_info['portfolio_mktvalue'].tolist()[0]
        df_stock_info = df_info[df_info['portfolio_name'] == 'stock']
        if len(df_stock_info) == 0:
            stock_profit = 0
            stock_mktvalue = 0
        else:
            stock_profit = df_stock_info['portfolio_profit'].tolist()[0]
            stock_mktvalue = df_stock_info['portfolio_mktvalue'].tolist()[0]
        df_bond_info = df_info[df_info['portfolio_name'] == 'bond']
        if len(df_bond_info) == 0:
            bond_profit = 0
            bond_mktvalue = 0
        else:
            bond_profit = df_bond_info['portfolio_profit'].tolist()[0]
            bond_mktvalue = df_bond_info['portfolio_mktvalue'].tolist()[0]
        df_detail=pd.concat([df_indexfuture,df_commfuture,df_option])
        return stock_profit,stock_mktvalue,indexfuture_profit,indexfuture_mktvalue,df_detail,commfuture_profit,commfuture_mktvalue,df_commfuture, \
            option_profit,option_mktvalue,df_option,etf_profit,etf_mktvalue,cb_profit,cb_mktvalue,bond_profit,bond_mktvalue
    
    def trading_action_processing(self):
        """
        交易行为处理
        功能：分析持仓变化，识别交易行为（买入、卖出、持仓不变）
        
        返回：
            DataFrame: 包含交易行为分析的数据框
        """
        # 为各类资产添加类型标识
        df_stock = self.df_stock_ori.copy()
        df_stock['type'] = 'stock'
        df_future = self.df_future_ori.copy()
        df_future['type'] = 'future'
        df_option = self.df_option_ori.copy()
        df_option['type'] = 'option'
        df_etf = self.df_etf_ori.copy()
        df_etf['type'] = 'etf'
        
        # 合并所有资产数据
        df_final = pd.concat([df_stock, df_future, df_option, df_etf])
        df_final = df_final[['code', 'quantity', 'pre_quantity', 'direction', 'type']]
        
        # 计算持仓变化
        df_final['difference'] = df_final['quantity'] - df_final['pre_quantity']
        
        def action_decision(x):
            """
            交易行为判断
            功能：根据持仓变化判断交易行为
            
            参数：
                x (float): 持仓变化量
                
            返回：
                str: 交易行为类型
            """
            if x > 0:
                return '开仓'
            elif x < 0:
                return '平仓'
            else:
                return '不变'
        
        # 添加交易行为列
        df_final['action'] = df_final['difference'].apply(action_decision)
        df_final=df_final[~(df_final['action']=='不变')]
        df_final['simulation']='False'
        df_final['product_code']=self.product_code
        df_final['valuation_date'] = self.date
        df_final.rename(columns={'quantity':'HoldingQty','pre_quantity':'HoldingQty_yes'},inplace=True)
        return df_final

    def product_info_processing(self):
        """
        产品信息处理
        功能：处理产品的基本信息，包括资产价值、各类资产收益等
        
        返回：
            DataFrame: 产品信息数据框
        """
        # 获取各类资产分析结果
        stock_profit, stock_mktvalue, indexfuture_profit, indexfuture_mktvalue, df_detail, commfuture_profit, commfuture_mktvalue, df_commfuture, \
            option_profit, option_mktvalue, df_option, etf_profit, etf_mktvalue, cb_profit, cb_mktvalue, bond_profit, bond_mktvalue = self.info_split(*self.partial_analysis())
        
        # 计算总收益和总市值
        total_profit = stock_profit + indexfuture_profit + commfuture_profit + option_profit + etf_profit + cb_profit + bond_profit
        total_mktvalue = stock_mktvalue + indexfuture_mktvalue + commfuture_mktvalue + option_mktvalue + etf_mktvalue + cb_mktvalue + bond_mktvalue
        proportion_stock=stock_mktvalue/self.asset_value
        proportion_option=option_mktvalue/self.asset_value
        proportion_future=indexfuture_mktvalue/self.asset_value
        proportion_cb=cb_mktvalue/self.asset_value
        leverage_ratio=total_mktvalue/self.asset_value_yes
        product_return=total_profit/self.asset_value_yes
        # 创建产品信息数据框
        df_info = pd.DataFrame({
            '资产总值': [self.asset_value],
            '总盈亏': [total_profit],
            '总市值': [total_mktvalue],
            '股票盈亏': [stock_profit],
            '股票市值': [stock_mktvalue],
            '股指期货盈亏': [indexfuture_profit],
            '股指期货市值': [indexfuture_mktvalue],
            '商品期货盈亏': [commfuture_profit],
            '商品期货市值': [commfuture_mktvalue],
            '期权盈亏': [option_profit],
            '期权市值': [option_mktvalue],
            'ETF_盈亏': [etf_profit],
            'ETF市值': [etf_mktvalue],
            '可转债盈亏': [cb_profit],
            '可转债市值': [cb_mktvalue],
            '国债盈亏': [bond_profit],
            '国债市值': [bond_mktvalue],
            '股票占比': [proportion_stock],
            '期权占比': [proportion_option],
            '期货占比': [proportion_future],
            '可转债占比': [proportion_cb],
            '杠杆率': [leverage_ratio],
            '总资产预估收益率(bp)': [round(10000*product_return,2)],
        })
        df_info=df_info.T
        df_info.reset_index(inplace=True)
        df_info.columns=['type','value']
        df_info['valuation_date']=self.date
        df_info['product_code']=self.product_code
        df_info['simulation']=False
        df_info['update_time']=self.now
        return df_info,df_detail

    def productTracking_main(self):
        """
        产品跟踪主函数
        功能：执行产品级别的完整计算流程，包括数据获取、分析、处理和保存
        """
        # 获取产品信息
        df_info,df_detail = self.product_info_processing()
        
        # 获取交易行为数据
        df_action = self.trading_action_processing()

        # 保存产品信息到数据库
        if len(df_info) > 0:
            sm = gt.sqlSaving_main(inputpath_sql, 'proinfo', delete=True)
            sm.df_to_sql(df_info,'product_code',self.product_code)
        if len(df_action)>0:
            sm3 = gt.sqlSaving_main(inputpath_sql, 'holding_changing', delete=True)
            sm3.df_to_sql(df_action, 'product_code', self.product_code)
        if len(df_detail) > 0:
            df_indexfuture=df_detail[['code','direction','quantity','delta','mkt_value','profit']]
            df_indexfuture['simulation']='False'
            df_indexfuture['product_code']=self.product_code
            df_indexfuture.rename(columns={'quantity':'HoldingQty','profit':'daily_profit'},inplace=True)
            df_indexfuture['valuation_date'] = self.date
            df_indexfuture['update_time'] = self.now
            sm = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding', delete=True)
            sm.df_to_sql(df_indexfuture,'product_code',self.product_code)

        return df_info, df_action, df_indexfuture
if __name__ == '__main__':
    pt=product_tracking('SST132')
    #print(pt.product_info_processing())
    print(pt.productTracking_main())















