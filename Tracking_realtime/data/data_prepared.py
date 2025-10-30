"""
数据预处理模块
功能：处理期货期权持仓数据、股票ETF数据、可转债数据等，提供数据标准化和分类功能
主要类：
- futureoption_position: 期货期权持仓数据处理
- security_position: 股票ETF可转债持仓数据处理  
- prod_info: 产品信息处理
作者：[作者名]
创建时间：[创建时间]
"""

import pandas as pd
import os
import sys

# 添加全局工具函数路径到系统路径
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)

import datetime as datetime
import global_tools as gt
import global_setting.global_dic as glv
import yaml
import numpy as np

# 全局变量定义
global source, config_path
config_path = glv.get('path_config')
source = gt.source_getting2(config_path)

class futureoption_position:
    """
    期货期权持仓数据处理类
    功能：处理期货和期权的持仓数据，包括数据标准化、分类、方向转换等
    """
    
    def __init__(self, product_code):
        """
        初始化方法
        参数：
            product_code (str): 产品代码
        """
        self.product_code = product_code
    
    def standardize_column_names_future(self, df):
        """
        标准化期货期权数据的列名
        功能：将不同来源的期货期权数据列名统一标准化
        
        参数：
            df (DataFrame): 原始数据框
            
        返回：
            DataFrame: 标准化列名后的数据框
        """
        # 创建列名映射字典，将各种可能的列名映射到标准列名
        column_mapping = {
            # 代码相关
            '合约代码': 'code',
            'InstrumentID': 'code',
            '合约': 'code',
            '市场代码': 'mkt_code',
            '合约名称': 'chi_name',
            '多空': 'direction',
            '买卖': 'direction',
            'PosiDirection': 'direction',
            '总持仓': 'quantity',
            'Position': 'quantity',
            '昨仓': 'pre_quantity',
            'YdPosition': 'pre_quantity',
            '今仓': 'today_quantity',
            '持仓均价': 'unit_cost',
            '持仓成本': 'cost',
            '合约价值': 'mkt_value',
            '持仓盈亏': 'profit',
            'PositionProfit': 'profit',
            '最新价': 'price',
            'SettlementPrice': 'price',
            '当日涨幅': 'pct_chg'
        }
        
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        
        # 定义固定的列顺序
        fixed_columns = ['code', 'price', 'pct_chg', 'mkt_value', 'mkt_code', 'direction', 'chi_name', 'quantity',
                         'pre_quantity', 'today_quantity', 'unit_cost', 'cost', 'profit']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        
        return df
    
    def read_csv_file(self, file_name):
        """
        读取CSV文件并解析键值对格式的数据
        
        参数：
            file_name (str): CSV文件路径
            
        返回：
            DataFrame: 解析后的数据框
        """
        df = pd.read_csv(file_name, header=None)
        # 将第一行作为列名，提取等号前的部分
        df.columns = df.iloc[0, :].map(lambda x: x.split('=')[0])
        # 提取等号后的值
        df = df.map(lambda x: x.split('=')[1])
        return df
    
    def df_classification(self, df):
        """
        对期货期权数据进行资产类型分类
        功能：根据代码中是否包含'-'来判断是期权还是期货
        
        参数：
            df (DataFrame): 数据框
            
        返回：
            DataFrame: 添加了asset_type列的数据框
        """
        def get_asset_type(code):
            if '-' in str(code):
                return 'option'
            else:
                return 'future'
        
        df['asset_type'] = df['code'].apply(get_asset_type)
        return df
    
    def direction_transfer(self, x):
        """
        转换持仓方向编码
        功能：将数字编码转换为中文方向标识
        
        参数：
            x (str): 方向编码
            
        返回：
            str: 中文方向标识
        """
        if x == '2':
            return '多'
        else:
            return '空'
    
    def position_withdraw_hy(self):
        """
        获取期货期权持仓数据
        功能：从本地文件或数据库获取指定产品的期货期权持仓数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        inputpath_holding = glv.get('future_info_xy')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, today2)
            inputpath_holding_today = os.path.join(inputpath_holding_today, 'GLDH5000602925.position.dat')
            df_holding = self.read_csv_file(inputpath_holding_today)
            df_holding = self.standardize_column_names_future(df_holding)
            df_holding['direction'] = df_holding['direction'].apply(lambda x: self.direction_transfer(x))
            df_holding['valuation_date'] = gt.strdate_transfer(today)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(inputpath_holding) + f" Where product_code='SGS958' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        df_future = self.fill_quantity_with_pre_quantity(df_future)
        df_option = self.fill_quantity_with_pre_quantity(df_option)
        return df_future, df_option
    
    def position_withdraw_renr(self):
        """
        获取期货期权持仓数据
        功能：从本地文件或数据库获取指定产品的期货期权持仓数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        inputpath_holding = glv.get('future_info_renr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, today2)
            inputpath_holding_today = os.path.join(inputpath_holding_today, 'GLDH5000603056.position.dat')
            df_holding = self.read_csv_file(inputpath_holding_today)
            df_holding = self.standardize_column_names_future(df_holding)
            df_holding['direction'] = df_holding['direction'].apply(lambda x: self.direction_transfer(x))
            df_holding['valuation_date'] = gt.strdate_transfer(today)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(inputpath_holding) + f" Where product_code='SLA626' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        df_future = self.fill_quantity_with_pre_quantity(df_future)
        df_option = self.fill_quantity_with_pre_quantity(df_option)
        return df_future, df_option
    
    def position_withdraw_rrjx(self):
        """
        获取期货期权持仓数据
        功能：从本地文件或数据库获取指定产品的期货期权持仓数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        inputpath_holding = glv.get('future_info_rr')
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding = os.path.join(inputpath_holding, 'PositionDetail(金砖1号)' + f"({today2}).csv")
            df_holding = gt.data_getting(inputpath_holding, config_path)
            df_holding = self.standardize_column_names_future(df_holding)
            df_holding['valuation_date'] = today
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SNY426' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    
    def position_withdraw_rr500(self):
        """
        获取期货期权持仓数据
        功能：从本地文件或数据库获取指定产品的期货期权持仓数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        inputpath_holding = glv.get('future_info_rr')
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding = os.path.join(inputpath_holding, 'PositionDetail(量化500增强2)' + f"({today2}).csv")
            df_holding = gt.data_getting(inputpath_holding, config_path)
            df_holding = self.standardize_column_names_future(df_holding)
            df_holding['valuation_date'] = today
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SSS044' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    
    def position_withdraw_other(self):
        """
        获取期货期权持仓数据
        功能：从本地文件或数据库获取指定产品的期货期权持仓数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        if self.product_code == 'STH580':
            inputpath = glv.get('future_info_rj')
        elif self.product_code == 'SST132':
            inputpath = glv.get('future_info_zx')
        elif self.product_code == 'SVU353':
            inputpath = glv.get('future_info_gy')
        else:
            raise ValueError
        
        target_date = datetime.date.today()
        for i in range(2):
            target_date = gt.last_workday_calculate(target_date)
            last_day = gt.last_workday_calculate(target_date)
            target_date2 = gt.intdate_transfer(target_date)
            last_day2 = gt.intdate_transfer(last_day)
            
            if source == 'local':
                # 从本地文件读取数据
                inputpath_holding = gt.file_withdraw(inputpath, target_date2)
                inputpath_holding_yes = gt.file_wtidhraw(inputpath, last_day2)
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes = gt.data_getting(inputpath_holding_yes, config_path)
            else:
                # 从数据库读取数据
                inputpath_holding = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{target_date}'"
                inputpath_holding_yes = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{last_day}'"
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes = gt.data_getting(inputpath_holding_yes, config_path)
            
            if len(df_holding) != 0:
                break
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[(df_holding['asset_type'] == 'future') | (df_holding['asset_type'] == 'option')]
        df_holding_yes = df_holding_yes[(df_holding_yes['asset_type'] == 'future') | (df_holding_yes['asset_type'] == 'option')]
        df_holding = df_holding[['valuation_date', 'code', 'quantity', 'mkt_value']]
        df_holding_yes = df_holding_yes[['code', 'quantity']]
        df_holding_yes.columns = ['code', 'pre_quantity']
        
        def direction_transfer(x):
            if x > 0:
                return '多'
            else:
                return '空'
        
        df_holding['direction'] = df_holding['mkt_value'].apply(lambda x: direction_transfer(x))
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity']]
        df_holding = df_holding.merge(df_holding_yes, on='code', how='outer')
        df_holding.fillna(0, inplace=True)
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    
    def fill_quantity_with_pre_quantity(self, df):
        """
        填充持仓数量为昨仓数量
        功能：如果持仓数量为空，则用昨仓数量填充
        
        参数：
            df (DataFrame): 数据框
            
        返回：
            DataFrame: 填充后的数据框
        """
        if 'quantity' in df.columns and 'pre_quantity' in df.columns:
            df['quantity'] = df['quantity'].replace([None, 'None', 'nan', '', np.nan], np.nan)
            df['quantity'] = df['quantity'].astype(float)
            df['quantity'] = df['quantity'].fillna(df['pre_quantity'])
        return df
    
    def futureoption_withdraw(self):
        """
        根据产品代码调用不同的持仓数据获取方法
        功能：根据产品代码调用相应的持仓数据获取方法，并返回处理后的数据
        
        返回：
            DataFrame: 处理后的持仓数据
        """
        if self.product_code == 'SGS958':
            df_future, df_option = self.position_withdraw_hy()
        elif self.product_code == 'SLA626':
            df_future, df_option = self.position_withdraw_renr()
        elif self.product_code == 'SNY426':
            df_future, df_option = self.position_withdraw_rrjx()
        elif self.product_code == 'SSS044':
            df_future, df_option = self.position_withdraw_rr500()
        else:
            df_future, df_option = self.position_withdraw_other()
        return df_future, df_option

class security_position:
    """
    股票ETF可转债持仓数据处理类
    功能：处理股票、ETF、可转债的持仓数据，包括数据标准化、分类等
    """
    
    def __init__(self, product_code):
        """
        初始化方法
        参数：
            product_code (str): 产品代码
        """
        self.product_code = product_code
    
    def pool_withdraw(self):
        """
        获取股票ETF可转债代码池
        功能：从数据库获取股票、ETF、可转债的代码池
        
        返回：
            list: 股票代码列表
            list: ETF代码列表
            list: 可转债代码列表
        """
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        df_etf = gt.etfData_withdraw(today, realtime=True)
        df_stock = gt.stockData_withdraw(today, realtime=True)
        df_cb = gt.cbData_withdraw(today, realtime=True)
        etf_pool = df_etf['code'].unique().tolist()
        stock_pool = df_stock['code'].unique().tolist()
        cb_pool = df_cb['code'].unique().tolist()
        return stock_pool, etf_pool, cb_pool
    
    def df_classification(self, df):
        """
        对股票ETF可转债数据进行资产类型分类
        功能：根据代码判断是股票、ETF还是可转债
        
        参数：
            df (DataFrame): 数据框
            
        返回：
            DataFrame: 添加了asset_type列的数据框
        """
        stock_pool, etf_pool, cb_pool = self.pool_withdraw()
        def get_asset_type(code):
            if code in stock_pool:
                return 'stock'
            elif code in etf_pool:
                return 'etf'
            elif code in cb_pool:
                return 'convertible_bond'
            else:
                return None
        
        df['asset_type'] = df['code'].apply(get_asset_type)
        return df

    def standardize_column_names_stock(self, df):
        """
        标准化股票数据的列名
        功能：将不同来源的股票数据列名统一标准化
        
        参数：
            df (DataFrame): 原始数据框
            
        返回：
            DataFrame: 标准化列名后的数据框
        """
        # 创建列名映射字典，将各种可能的列名映射到标准列名
        column_mapping = {
            # 代码相关
            '证券代码': 'code',
            '代码': 'code',
            '市场名称': 'mkt_name',
            '证券名称': 'chi_name',
            '当前拥股': 'quantity',
            '数量': 'quantity',
            '成本价': 'unit_cost',
            '当前成本': 'cost',
            '市值': 'mkt_value',
            '盈亏': 'profit',
            '可用余额': 'valid_quantity',
            '昨夜拥股': 'pre_quantity',
            '最新价': 'price',
            '当日涨幅': 'pct_chg'
        }
        
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        
        # 定义固定的列顺序
        fixed_columns = ['code', 'price', 'pct_chg', 'mkt_value', 'mkt_name', 'chi_name', 'quantity', 'unit_cost',
                         'cost', 'profit', 'valid_quantity', 'pre_quantity']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df
    
    def position_withdraw_hy(self):
        """
        获取股票持仓数据
        功能：从本地文件或数据库获取指定产品的股票持仓数据
        
        返回：
            DataFrame: 处理后的股票数据
        """
        inputpath_holding = glv.get('stock_info_xy')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail({today2}).csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(inputpath_holding) + f" Where product_code='SGS958' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['code', 'quantity', 'pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb

    def position_withdraw_renr(self):
        """
        获取股票持仓数据
        功能：从本地文件或数据库获取指定产品的股票持仓数据
        
        返回：
            DataFrame: 处理后的股票数据
        """
        inputpath_holding = glv.get('stock_info_renr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionStatics-{today2}.csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SLA626' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['code', 'quantity', 'pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb

    def position_withdraw_rrjx(self):
        """
        获取股票持仓数据
        功能：从本地文件或数据库获取指定产品的股票持仓数据
        
        返回：
            DataFrame: 处理后的股票数据
        """
        inputpath_holding = glv.get('stock_info_rr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail(金砖1号)({today2}).csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SNY426' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['code', 'quantity', 'pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb

    def position_withdraw_rr500(self):
        """
        获取股票持仓数据
        功能：从本地文件或数据库获取指定产品的股票持仓数据
        
        返回：
            DataFrame: 处理后的股票数据
        """
        inputpath_holding = glv.get('stock_info_rr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        
        if source == 'local':
            # 从本地文件读取数据
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail(量化中证500)({today2}).csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            # 从数据库读取数据
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SSS044' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['code', 'quantity', 'pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb
    
    def position_withdraw_other(self):
        """
        获取股票持仓数据
        功能：从本地文件或数据库获取指定产品的股票持仓数据
        
        返回：
            DataFrame: 处理后的股票数据
        """
        if self.product_code == 'STH580':
            inputpath = glv.get('future_info_rj')
        elif self.product_code == 'SST132':
            inputpath = glv.get('future_info_zx')
        elif self.product_code == 'SVU353':
            inputpath = glv.get('future_info_gy')
        else:
            raise ValueError
        
        target_date = datetime.date.today()
        for i in range(2):
            target_date = gt.last_workday_calculate(target_date)
            target_date2 = gt.intdate_transfer(target_date)
            last_date = gt.last_workday_calculate(target_date)
            last_date2 = gt.intdate_transfer(last_date)
            
            if source == 'local':
                # 从本地文件读取数据
                inputpath_holding = gt.file_withdraw(inputpath, target_date2)
                inputpath_holding_yes = gt.file_withdraw(inputpath, last_date2)
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes = gt.data_getting(inputpath_holding_yes, config_path)
            else:
                # 从数据库读取数据
                inputpath_holding = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{target_date}'"
                inputpath_holding_yes = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{last_date}'"
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes = gt.data_getting(inputpath_holding_yes, config_path)
            
            if len(df_holding) != 0:
                break
        # 选择需要的列并进行资产分类
        df_holding = df_holding[['valuation_date', 'code', 'quantity', 'asset_type']]
        df_holding_yes = df_holding_yes[['code', 'quantity']]
        df_holding_yes.columns = ['code', 'pre_quantity']
        df_holding = df_holding.merge(df_holding_yes, on='code', how='outer')
        df_holding.fillna(0, inplace=True)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'cbond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb
    
    def security_withdraw(self):
        """
        根据产品代码调用不同的持仓数据获取方法
        功能：根据产品代码调用相应的持仓数据获取方法，并返回处理后的数据
        
        返回：
            DataFrame: 处理后的股票数据
            DataFrame: 处理后的ETF数据
            DataFrame: 处理后的可转债数据
        """
        if self.product_code == 'SGS958':
            df_stock, df_etf, df_cb = self.position_withdraw_hy()
        elif self.product_code == 'SLA626':
            df_stock, df_etf, df_cb = self.position_withdraw_renr()
        elif self.product_code == 'SNY426':
            df_stock, df_etf, df_cb = self.position_withdraw_rrjx()
        elif self.product_code == 'SSS044':
            df_stock, df_etf, df_cb = self.position_withdraw_rr500()
        else:
            df_stock, df_etf, df_cb = self.position_withdraw_other()
        return df_stock, df_etf, df_cb

class prod_info:
    """
    产品信息处理类
    功能：处理产品信息，包括获取产品名称、可用日期、资产价值等
    """
    
    def __init__(self, product_code):
        """
        初始化方法
        参数：
            product_code (str): 产品代码
        """
        self.product_code = product_code
    
    def get_product_detail(self, field):
        """
        获取产品详细信息
        功能：从产品配置文件中获取指定字段的产品信息
        
        参数：
            field (str): 要获取的字段名
            
        返回：
            dict: 产品详细信息
        """
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'product_detail.yaml')
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        if self.product_code not in data:
            raise ValueError(f"Product code {self.product_code} not found in product_detail.yaml")
        product_info = data[self.product_code]
        if field not in product_info:
            raise ValueError(f"Field '{field}' not found for product code {self.product_code}")
        return product_info[field]
    
    def availableDate_withdraw(self):
        """
        获取产品可用日期
        功能：从本地文件或数据库获取产品的可用日期
        
        返回：
            str: 可用日期字符串
        """
        if source == 'local':
            inputpath = glv.get('prod_info')
            product_name = self.get_product_detail('name')
            inputpath = os.path.join(inputpath, product_name)
            inputlist = os.listdir(inputpath)
            inputlist = [str(i)[:8] for i in inputlist]
            inputlist.sort()
            available_date = inputlist[-1]
        else:
            inputpath = glv.get('prod_info')
            inputpath = str(inputpath) + f" Where product_code='{self.product_code}'"
            df = gt.data_getting(inputpath, config_path)
            df.sort_values(by='valuation_date', inplace=True)
            available_date = df['valuation_date'].unique().tolist()[-1]
        available_date = gt.strdate_transfer(available_date)
        return available_date
    
    def assetvalue_processing(self, asset_value, available_date):
        """
        资产价值处理
        功能：对资产价值进行格式化、转换和计算
        
        参数：
            asset_value (str or float): 原始资产价值
            available_date (str): 可用日期
            
        返回：
            float: 处理后的资产价值
        """
        if asset_value is not None and asset_value != '':
            if isinstance(asset_value, str):
                asset_value = asset_value.replace(',', '')
                try:
                    asset_value = float(asset_value)
                except ValueError:
                    pass  # 如果不能转换为float则保持原值
        else:
            asset_value = float(asset_value)
        asset_value_yes=asset_value
        asset_type = self.get_product_detail('type')
        index_type = self.get_product_detail('index')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if asset_type == '中性':
            asset_value = asset_value
            asset_value_yes=asset_value_yes
        else:
            working_days_list = gt.working_days_list(available_date, today)
            working_days_list = working_days_list[1:]
            for days in working_days_list:
                if days != today:
                    index_return = gt.indexData_withdraw(index_type, days, days, ['pct_chg'])
                    index_return = index_return['pct_chg'].tolist()[0]
                    asset_value_yes = (1 + index_return) * asset_value_yes
                else:
                    index_return = gt.indexData_withdraw(index_type, days, days, ['pct_chg'], True)
                    index_return = index_return['pct_chg'].tolist()[0]
                index_return = float(index_return)
                asset_value = (1 + index_return) * asset_value
        return asset_value,asset_value_yes

    def assetvalue_withdraw(self):
        """
        获取产品资产价值
        功能：从本地文件或数据库获取产品的资产价值，并进行处理
        
        返回：
            float: 处理后的资产价值
        """
        available_date = self.availableDate_withdraw()
        available_date2 = gt.intdate_transfer(available_date)
        if source == 'local':
            inputpath = glv.get('prod_info')
            product_name = self.product_code # Assuming productName_transfer is a typo and should be product_code
            inputpath = os.path.join(inputpath, product_name)
            inputpath = gt.file_withdraw(inputpath, available_date2)
        else:
            inputpath = glv.get('prod_info')
            inputpath = str(
                inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{available_date}'"
        df = gt.data_getting(inputpath, config_path)
        asset_value = df['NetAssetValue'].unique().tolist()[0]
        asset_value,asset_value_yes = self.assetvalue_processing(asset_value, available_date)

        return asset_value,asset_value_yes

class weight_withdraw:
    """
    权重数据处理类
    功能：处理权重数据，包括获取产品列表、获取单个产品权重等
    """
    
    def __init__(self):
        """
        初始化方法
        """
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        self.available_date = today
        self.yes = gt.last_workday_calculate(self.available_date)
        self.realtime = True
    
    def portfolio_list_getting(self, yes=False):
        """
        获取产品组合列表
        功能：从本地文件或数据库获取产品组合列表
        
        参数：
            yes (bool): 是否获取昨日数据
            
        返回：
            list: 产品组合名称列表
        """
        inputpath = glv.get('portfolio_weight')
        if yes == True:
            available_date = self.yes
        else:
            available_date = self.available_date
        if source == 'local':
            portfolio_list = os.listdir(inputpath)
        else:
            inputpath = str(inputpath) + f" Where valuation_date='{available_date}'"
            df = gt.data_getting(inputpath, config_path, update_time=False)
            portfolio_list = df['portfolio_name'].unique().tolist()
        return portfolio_list
    
    def product_list_getting(self):
        """
        获取产品列表
        功能：获取所有产品代码列表
        
        返回：
            list: 产品代码列表
        """
        product_list = ['SGS958', 'SVU353', 'SNY426', 'SSS044', 'STH580', 'SST132', 'SLA626']
        return product_list
    
    def portfolio_withdraw(self, portfolio_name, yes=False):
        """
        获取单个产品组合权重
        功能：从本地文件或数据库获取指定产品组合的权重数据
        
        参数：
            portfolio_name (str): 产品组合名称
            yes (bool): 是否获取昨日数据
            
        返回：
            DataFrame: 产品组合权重数据
        """
        inputpath = glv.get('portfolio_weight')
        if yes == True:
            available_date = self.yes
        else:
            available_date = self.available_date
        if source == 'local':
            inputpath = os.path.join(inputpath, portfolio_name)
            inputpath = gt.file_withdraw(inputpath, gt.intdate_transfer(available_date))
        else:
            inputpath = str(inputpath) + f" Where valuation_date='{available_date}' And portfolio_name='{portfolio_name}'"
        df = gt.data_getting(inputpath, config_path)
        df = df[['code', 'weight']]
        return df
    
    def product_withdraw(self, product_code, yes=False):
        """
        获取单个产品权重
        功能：从本地文件或数据库获取指定产品的权重数据
        
        参数：
            product_code (str): 产品代码
            yes (bool): 是否获取昨日数据
            
        返回：
            DataFrame: 产品权重数据
        """
        inputpath = glv.get('product_weight')
        if yes == True:
            available_date = self.yes
        else:
            available_date = self.available_date
        if source == 'local':
            inputpath = os.path.join(inputpath, product_code)
            inputpath = gt.file_withdraw(inputpath, gt.intdate_transfer(available_date))
        else:
            inputpath = str(inputpath) + f" Where valuation_date='{available_date}' And product_code='{product_code}'"
        df = gt.data_getting(inputpath, config_path)
        df = df[['code', 'weight']]
        return df

def fill_quantity_with_pre_quantity(df):
    """
    填充持仓数量为昨仓数量
    功能：如果持仓数量为空，则用昨仓数量填充
    
    参数：
        df (DataFrame): 数据框
        
    返回：
        DataFrame: 填充后的数据框
    """
    if 'quantity' in df.columns and 'pre_quantity' in df.columns:
        df['quantity'] = df['quantity'].replace([None, 'None', 'nan', '', np.nan], np.nan)
        df['quantity'] = df['quantity'].astype(float)
        df['quantity'] = df['quantity'].fillna(df['pre_quantity'])
    return df


if __name__ == '__main__':
    # 测试权重获取功能
    ww =   futureoption_position('SST132')
    print(ww.position_withdraw_other())
