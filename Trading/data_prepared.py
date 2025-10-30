"""
数据准备模块

这个模块负责从各种数据源获取和准备交易所需的数据。
包括市场数据、投资组合权重、产品信息、持仓数据等。

主要功能：
1. 市场数据获取（股票和ETF）
2. 投资组合权重数据获取
3. 产品信息获取
4. 持仓数据获取
5. 目标权重数据获取

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

class data_prepared:
    """
    数据准备类
    
    负责从各种数据源获取交易系统所需的数据，包括市场数据、投资组合数据、
    产品信息、持仓数据等。
    """
    
    def __init__(self,target_date,realtime=False):
        """
        初始化数据准备对象
        
        Args:
            target_date (str): 目标日期，格式为'YYYY-MM-DD'
            realtime (bool): 是否为实时模式，默认为False
        """
        self.target_date=target_date
        self.realtime=realtime
        if realtime==True:
            self.target_date=gt.strdate_transfer(datetime.today())
    
    def indexType_getting(self,product_code):
        """
        获取产品对应的指数类型
        
        从产品配置文件中读取指定产品代码对应的指数类型。
        
        Args:
            product_code (str): 产品代码
            
        Returns:
            str: 指数类型
        """
        inputpath_config = glv.get('config_product')
        excel_file = pd.ExcelFile(inputpath_config)
        sheet_names = excel_file.sheet_names
        detail_name = sheet_names[0]
        df_info = pd.read_excel(inputpath_config, detail_name)
        index_type=df_info[df_info['product_code']==product_code]['index_type'].tolist()[0]
        return index_type
    
    def mktData_withdraw(self):
        """
        获取市场数据
        
        获取股票和ETF的收盘价数据，用于计算持仓市值和交易数量。
        
        Returns:
            tuple: (市场数据DataFrame, ETF代码列表)
        """
        if self.realtime==True:
            available_date=self.target_date
        else:
            available_date=gt.last_workday_calculate(self.target_date)
        df_stock=gt.stockData_withdraw(start_date=available_date,end_date=available_date,realtime=self.realtime)
        df_etf=gt.etfData_withdraw(start_date=available_date,end_date=available_date,realtime=self.realtime)
        df_stock=df_stock[['code','close']]
        df_etf=df_etf[['code','close']]
        etf_pool=df_etf['code'].tolist()
        df_mkt=pd.concat([df_stock,df_etf])
        df_mkt['close']=df_mkt['close'].astype(float)
        return df_mkt,etf_pool
    
    def portfolioList_withdraw(self):
        """
        获取投资组合列表
        
        从投资组合权重数据中获取所有投资组合的名称列表。
        
        Returns:
            list: 投资组合名称列表
        """
        inputpath = glv.get('portfolio_weight')
        inputpath = str(inputpath) + f" Where valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        portfolio_list = df['portfolio_name'].unique().tolist()
        return portfolio_list
    
    def portfolioWeight_withdraw(self,portfolio_name):
        """
        获取指定投资组合的权重数据
        
        Args:
            portfolio_name (str): 投资组合名称
            
        Returns:
            pandas.DataFrame: 投资组合权重数据
        """
        inputpath = glv.get('portfolio_weight')
        inputpath = str(inputpath) + f" Where portfolio_name='{portfolio_name}' And valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        return df
    
    def productCode_withdraw(self):
        """
        获取产品代码列表
        
        从交易配置文件中获取所有产品代码。
        
        Returns:
            list: 产品代码列表
        """
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        productCode_list = df['product_code'].unique().tolist()
        return productCode_list
    
    def tradingTime_withdraw(self,product_code):
        """
        获取产品的交易时间
        
        Args:
            product_code (str): 产品代码
            
        Returns:
            str: 交易时间
        """
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        trading_time = df[df['product_code']==product_code]['trading_time'].tolist()[0]
        return trading_time
    
    def productInfo_withdraw(self, product_code):
        """
        获取产品信息
        
        获取指定产品的资金信息，包括账户资金、T0资金等。
        支持自动计算和手动设置两种模式。
        
        Args:
            product_code (str): 产品代码
            
        Returns:
            float: 股票资金金额
        """
        inputpath_config = glv.get('config_trading')
        df = pd.read_excel(inputpath_config, sheet_name='info_sheet')
        slice_df = df[df['product_code'] == product_code]
        account_money = slice_df['account_money'].tolist()[0]
        t0_money = slice_df['t0_money'].tolist()[0]
        if account_money == 'auto':
            if product_code in ['SGS958', 'SNY426', 'SSS044','SLA626']:
                if self.realtime == False:
                    inputpath_info = glv.get('input_info')
                    available_date = gt.last_workday_calculate(self.target_date)
                else:
                    inputpath_info = glv.get('input_info_temp')
                    available_date = self.target_date
                account_name = 'asset_value'
            elif product_code in ['SVU353', 'STH580', 'SST132', 'SLA626']:
                available_date2 = gt.last_workday_calculate(self.target_date)
                available_date = gt.last_workday_calculate(available_date2)
                inputpath_info = glv.get('input_info_l4')
                account_name = 'StockValue'
            else:
                print('product_code不在list当中')
                raise ValueError
            inputpath_info = str(
                inputpath_info) + f" Where product_code='{product_code}' And valuation_date='{available_date}'"
            df_info = gt.data_getting(inputpath_info, config_path)
            account_money = df_info[account_name].tolist()[0]
            if ',' in str(account_money):
                account_money = int(float(account_money.replace(",", "")))
            if account_name == 'StockValue':
                index_type=self.indexType_getting(product_code)
                df_index=gt.indexData_withdraw(index_type, start_date=available_date2,end_date=available_date2,columns=['pct_chg'])
                index_return=df_index['pct_chg'].tolist()[0]
                account_money=(1+index_return)*account_money
        else:
            account_money = account_money
        stock_money = account_money - t0_money
        return stock_money
    
    def productHolding_withdraw(self,product_code):
        """
        获取产品持仓数据
        
        获取指定产品的当前持仓信息，包括股票代码和持仓数量。
        
        Args:
            product_code (str): 产品代码
            
        Returns:
            pandas.DataFrame: 持仓数据，包含code和holding列
        """
        if self.realtime==False:
             inputpath=glv.get('product_Realholding_daily')
             available_date=gt.last_workday_calculate(self.target_date)
        else:
            inputpath=glv.get('product_Realholding_realtime')
            available_date=self.target_date
        inputpath=str(inputpath)+f" Where product_code='{product_code}' And valuation_date='{available_date}'"
        df=gt.data_getting(inputpath,config_path)
        df=df[['code','quantity']]
        df['code']=df['code'].astype(float)
        df=gt.code_transfer(df)
        df.columns=['code','holding']
        df['holding']=df['holding'].astype(float)
        df['new_code']=df['code'].apply(lambda x: str(x)[:2])
        df=df[~(df['new_code']=='20')]
        df=df[['code','holding']]
        return df
    
    def productTargetWeight_withdraw(self,product_code):
        """
        获取产品目标权重数据
        
        获取指定产品的目标权重配置。
        
        Args:
            product_code (str): 产品代码
            
        Returns:
            pandas.DataFrame: 目标权重数据
        """
        inputpath=glv.get('productTarget_weight')
        inputpath = str(inputpath) + f" Where product_code='{product_code}' And valuation_date='{self.target_date}'"
        df = gt.data_getting(inputpath, config_path)
        return df

if __name__ == '__main__':
    #print(gt.crossSection_index_return_withdraw('中证A500', '2025-06-11'))
    dp=data_prepared('2025-07-07',realtime=False)
    dp.productTargetWeight_withdraw('SGS958')