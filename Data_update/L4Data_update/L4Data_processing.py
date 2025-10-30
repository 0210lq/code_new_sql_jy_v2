import pandas as pd
from L4Data_update.tools_func import tools_func
import numpy as np
from setup_logger.logger_setup import setup_logger
from collections import defaultdict
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
# 添加警告记录器
_warning_cache = defaultdict(set)
class L4Data_processing:
    def __init__(self,df,date,product_code):
        self.logger = setup_logger('L4Data_update')
        self.logger.info('\n' + '*'*50 + '\nL4 DATA PROCESSING\n' + '*'*50)
        self.logger.info(f"Initializing L4Data_processing - Product code: {product_code}, Date: {date}")
        self.product_code=product_code
        self.df=df
        self.date=date
        self.tf = tools_func()
        self.product_name = self.tf.product_NameCode_transfer(product_code)
        self.tf = tools_func()
        self.future_len=self.tf.optionFuture_len_withdraw(product_code,'future_len')
        self.option_len =self.tf.optionFuture_len_withdraw(product_code,'option_len')
        self.stock_len = self.tf.optionFuture_len_withdraw(product_code, 'stock_len')
        self.stock_code = self.tf.optionFuture_len_withdraw(product_code, 'stock_code')
        self.future_code = self.tf.optionFuture_len_withdraw(product_code, 'future_code')
        self.c_bond_code = self.tf.optionFuture_len_withdraw(product_code, 'c_bond_code')
        self.option_code = self.tf.optionFuture_len_withdraw(product_code, 'option_code')
        self.bond_code = self.tf.optionFuture_len_withdraw(product_code, 'bond_code')
        self.c_bond_len = self.tf.optionFuture_len_withdraw(product_code, 'c_bond_len')
        self.etf_code = self.tf.optionFuture_len_withdraw(product_code, 'etf_code')
        self.etf_len = self.tf.optionFuture_len_withdraw(product_code, 'etf_len')
        self.bond_len = self.tf.optionFuture_len_withdraw(product_code, 'bond_len')
        self.stock_code=[str(self.stock_code[0])]
        self.future_code=[str(self.future_code[0])]
        self.option_code=[str(self.option_code[0])]
        self.c_bond_code=[str(self.c_bond_code[0])]
        self.bond_code=[str(self.bond_code[0])]
        self.etf_code=[str(self.etf_code[0])]
    def safe_float_convert(self,x):
            if pd.isnull(x):
                return None
            try:
                return float(str(x).replace(',', ''))
            except (ValueError, TypeError):
                return None
    def remove_duplicate_trades(self, df,duplicate_cols):
        """
        去除科目名称、方向、数量都相同的行
        Args:
            df: 包含交易数据的DataFrame
        Returns:
            处理后的DataFrame
        """
        # 确保输入是DataFrame
        if not isinstance(df, pd.DataFrame):
            return df
        
        # 检查所需列是否存在
        if not all(col in df.columns for col in duplicate_cols):
            return df
            
        # 保留重复行中的第一条记录
        df_cleaned = df.drop_duplicates(subset=duplicate_cols, keep='first')
        
        return df_cleaned
    def create_output_df(self,data_dict):
        """通过字典直接构建输出DataFrame，避免多次insert"""
        return pd.DataFrame(data_dict)

    def process_stock_sheet(self):
        """处理股票相关Sheet（Sheet1）"""
        df=self.df.copy()
        df.reset_index(inplace=True)
        date=self.date
        df['科目代码2']=df['科目代码'].apply(lambda x:str(x)[:4])
        df['科目代码长度']=df['科目代码'].apply(lambda x:len(str(x)))
        #df['科目代码2']=df['科目代码2'].astype(int)
        df=df[(df['科目代码2'].isin(self.stock_code))&(df['科目代码长度']>self.stock_len)]
        df['科目代码']=df['科目代码'].apply(lambda x:self.tf.process_stock_input(x))
        
        # 转换市值，移除无法转换的行
        def safe_float_convert(x):
            if pd.isnull(x):
                return None
            try:
                return float(str(x).replace(',', ''))
            except (ValueError, TypeError):
                return None
                
        df['市值'] = df['市值'].apply(self.safe_float_convert)
        df = df.dropna(subset=['市值'])
        df['数量']=df['数量'].apply(self.safe_float_convert)
        df=df.dropna(subset=['数量'])
        df['市价']=df['市价'].apply(self.safe_float_convert)
        df['单位成本']=df['单位成本'].apply(self.safe_float_convert)
        df=df.dropna(subset=['市价'])
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': df['科目代码'].tolist(),
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist(),
            '单位成本': df['单位成本'].tolist()
        }
        df=self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df

    def process_future_sheet(self):
        """处理期货Sheet（Sheet2）"""
        df=self.df.copy()
        df=df.reset_index()
        date=self.date
        df['科目代码2'] = df['科目代码'].apply(lambda x: str(x)[:4])
        df['科目代码长度'] = df['科目代码'].apply(lambda x: len(str(x)))
        #df['科目代码2']=df['科目代码2'].astype(int)
        df = df[(df['科目代码2'].isin(self.future_code)) & (df['科目代码长度'] > self.future_len)&(df['科目代码长度'] < self.option_len)]
        df = df.drop(df[df['科目名称'].str.contains('国债')].index)
        future_df = df.drop(df[df['科目名称'].str.contains('T2')].index)
        future_df = future_df.drop(future_df[future_df['科目名称'].str.contains('TS')].index)
        future_df['科目名称']=future_df['科目代码'].apply(lambda x: self.tf.process_future_input(x))
        # 处理带逗号的数字格式，先替换逗号，再转换为float
        future_df['市值'] = future_df['市值'].apply(self.safe_float_convert)
        future_df = future_df.dropna(subset=['市值'])
        future_df['数量']=future_df['数量'].apply(self.safe_float_convert)
        future_df=future_df.dropna(subset=['数量'])
        future_df['市价']=future_df['市价'].apply(self.safe_float_convert)
        future_df=future_df.dropna(subset=['市价'])
        type2 = []
        direction = []
        for k in future_df['市值'].tolist():
            if k > 0:
                b = 'long'
                direction.append(b)
                type2.append('中金所_多头')
            if k < 0:
                b = 'short'
                direction.append(b)
                type2.append('中金所_空头')
        dic={
            '日期': [date] * len(future_df),
            '产品名称': [self.product_code] * len(future_df),
            '种类': ['衍生工具'] * len(future_df),
            '种类名称': type2,
            '代码': future_df['科目代码'].tolist(),
            '方向': direction,
            '科目名称': future_df['科目名称'].tolist(),
            '数量': future_df['数量'].tolist(),
            '单位成本': future_df['单位成本'].tolist(),
            '成本': future_df['成本'].tolist(),
            '成本占净值%': future_df['成本占净值%'].tolist(),
            '市价': future_df['市价'].tolist(),
            '市值': future_df['市值'].tolist(),
            '市值占净值%': future_df['市值占净值%'].tolist(),
            '估值增值': future_df['估值增值'].tolist(),
            '停牌信息': future_df['停牌信息'].tolist()
        }
        df = self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df

    def process_c_bond_sheet(self):
        """处理可转债Sheet（Sheet3）"""
        df = self.df.copy()
        df=df.reset_index()
        date = self.date
        df['科目代码2'] = df['科目代码'].apply(lambda x: str(x)[:4])
        #df['科目代码2']=df['科目代码2'].astype(int)
        df['科目代码长度'] = df['科目代码'].apply(lambda x: len(str(x)))
        df = df[(df['科目代码2'].isin(self.c_bond_code)) & (df['科目代码长度'] > self.c_bond_len)]
        df['科目代码'] = df['科目代码'].apply(lambda x: self.tf.process_cbond_input(x))
        df['市值'] = df['市值'].apply(self.safe_float_convert)
        df = df.dropna(subset=['市值'])
        df['数量']=df['数量'].apply(self.safe_float_convert)
        df=df.dropna(subset=['数量'])
        df['市价']=df['市价'].apply(self.safe_float_convert)
        df=df.dropna(subset=['市价'])
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': df['科目代码'].tolist(),
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist(),
            '单位成本' : df['单位成本'].tolist()
        }
        df = self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df

    def process_option_sheet(self):
        """处理期权Sheet（Sheet4）"""
        df = self.df.copy()
        df = df.reset_index()
        date = self.date
        product_name = self.product_name
        df['科目代码2'] = df['科目代码'].apply(lambda x: str(x)[:4])
        #df['科目代码2']=df['科目代码2'].astype(int)
        df['科目代码长度'] = df['科目代码'].apply(lambda x: len(str(x)))
        df = df[(df['科目代码2'].isin(self.option_code))  & (
                    df['科目代码长度'] > self.option_len)]
        df['科目名称']=df['科目代码'].apply(lambda x: self.tf.process_option_input(x))
        df['市值'] = df['市值'].apply(self.safe_float_convert)
        df = df.dropna(subset=['市值'])
        df['数量']=df['数量'].apply(self.safe_float_convert)
        df=df.dropna(subset=['数量'])
        df['市价']=df['市价'].apply(self.safe_float_convert)
        df=df.dropna(subset=['市价'])
        option_value = df['市值'].tolist()
        direction = []
        for j in option_value:
            if j > 0:
                z = 'long'
                direction.append(z)
            if j < 0:
                z = 'short'
                direction.append(z)
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '种类': ['衍生工具'] * len(df),
            '代码': df['科目代码'].tolist(),
            '科目名称': df['科目名称'].tolist(),
            '方向': direction,
            '数量': df['数量'].tolist(),
            '单位成本': df['单位成本'].tolist(),
            '成本': df['成本'].tolist(),
            '成本占净值%': df['成本占净值%'].tolist(),
            '市价': df['市价'].tolist(),
            '市值': df['市值'].tolist(),
            '市值占净值%': df['市值占净值%'].tolist(),
            '估值增值': df['估值增值'].tolist(),
            '停牌信息': df['停牌信息'].tolist(),
        }
        df = self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df
    
    def process_bond_sheet(self):
        """处理债券Sheet"""
        df = self.df.copy()
        date = self.date
        df=df.reset_index()
        df['科目代码2'] = df['科目代码'].apply(lambda x: str(x)[:4])
        #df['科目代码2']=df['科目代码2'].astype(int)
        df['科目代码长度'] = df['科目代码'].apply(lambda x: len(str(x)))
        df = df[(df['科目代码2'].isin(self.bond_code)) & (df['科目代码长度'] > self.bond_len-3)&(df['科目代码长度'] < self.option_len)]
        # condition = np.logical_or(df['科目名称'].str.contains('国债'), df['科目名称'].str.contains('T2'),df['科目名称'].str.contains('TS'))
        df = df[(df['科目名称'].str.contains('国债'))|(df['科目名称'].str.contains('T2'))|df['科目名称'].str.contains('TS')]
        df['市值'] = df['市值'].apply(self.safe_float_convert)
        df = df.dropna(subset=['市值'])
        df['数量']=df['数量'].apply(self.safe_float_convert)
        df=df.dropna(subset=['数量'])
        df['市价']=df['市价'].apply(self.safe_float_convert)
        df=df.dropna(subset=['市价'])
        df['科目代码']=df['科目代码'].apply(lambda x: self.tf.process_bond_input(x))
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': df['科目代码'].tolist(),
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '单位成本': df['单位成本'].tolist(),
            '市值': df['市值'].tolist()
        }
        df = self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df

    def process_etf_sheet(self):
        """处理ETF Sheet"""
        df = self.df.copy()
        date = self.date
        df=df.reset_index()
        df['科目代码2'] = df['科目代码'].apply(lambda x: str(x)[:4])
        #df['科目代码2']=df['科目代码2'].astype(int)
        df['科目代码长度'] = df['科目代码'].apply(lambda x: len(str(x)))
        df = df[(df['科目代码2'].isin(self.etf_code)) & (df['科目代码长度'] > self.etf_len)]
        df['科目代码']=df['科目代码'].apply(lambda x: self.tf.process_etf_input(x))
        df['市值'] = df['市值'].apply(self.safe_float_convert)
        df = df.dropna(subset=['市值'])
        df['数量']=df['数量'].apply(self.safe_float_convert)
        df=df.dropna(subset=['数量'])
        df['市价']=df['市价'].apply(self.safe_float_convert)
        df=df.dropna(subset=['市价'])
        dic= {
            '日期': [date] * len(df),
            '产品名称': [self.product_code] * len(df),
            '代码': df['科目代码'].tolist(),
            '名称': df['科目名称'].tolist(),
            '数量': df['数量'].tolist(),
            '市价': df['市价'].tolist(),
            '单位成本': df['单位成本'].tolist(),
            '市值': df['市值'].tolist()
        }
        df = self.create_output_df(dic)
        df = self.remove_duplicate_trades(df,['科目名称','方向','数量'])
        return df

    def data_check(self,date,product_name,pure_value,property,liabilities,security,stock,bond,derivative,unit_pure,accumulated_pure,accumulated_pure_d,shuhui_value,shengou_value):
        """数据检查"""
        # 定义所有参数及其中文名称的映射
        params = {
            '日期': date,
            '产品名称': product_name,
            '资产净值': pure_value,
            '资产总值': property,
            '负债': liabilities,
            '证券投资': security,
            '股票投资': stock,
            '债券投资': bond,
            '衍生工具': derivative,
            '单位净值': unit_pure,
            '累计净值': accumulated_pure,
            '日增长率': accumulated_pure_d,
            '赎回金额': shuhui_value,
            '申购金额': shengou_value
        }

        # 检查并处理每个参数
        missing_fields = []
        for name, value in params.items():
            if len(value) == 0 or (len(value) > 0 and value[0] is None):
                params[name] = [None]
                missing_fields.append(name)

        # 如果有缺失值，记录信息
        if missing_fields:
            self.logger.warning(f"Missing fields in {self.product_name} for date {self.date}:")
            for field in missing_fields:
                self.logger.warning(f"- {field}")

        # 返回处理后的值，保持原有顺序
        return (params['日期'], params['产品名称'], params['资产净值'],
                params['资产总值'], params['负债'], params['证券投资'],
                params['股票投资'], params['债券投资'], params['衍生工具'],
                params['单位净值'], params['累计净值'],
                params['日增长率'], params['赎回金额'],
                params['申购金额'])

    def process_info_sheet(self):
        """处理信息表数据"""
        try:
            self.logger.info(f"Processing info sheet for {self.product_name} on {self.date}")
            df = self.df.copy()
            
            # 从L4Data_preparing获取字段名
            from L4Data_update.L4Data_preparing import L4Data_preparing
            self.L4_preparing = L4Data_preparing(self.product_code, self.date)
            
            # 获取字段名称
            (pure_value_name, property_name, liabilities_name, security_name,
             stock_name, bond_name, derivative_name, unit_pure_name,
             accumulated_pure_name, accumulated_pure_d_name,
             shuhui_value_name, shengou_value_name) = self.L4_preparing.product_info_transfer(df)
            
            # 定义获取值的函数
            def get_value(field_name, column='市值'):
                """获取指定字段的值"""
                if field_name is None:
                    return np.array([None])
                try:
                    if '科目代码' in df.columns:
                        matched_rows = df[df['科目代码'] == field_name]
                        return np.array([matched_rows[column].iloc[0] if not matched_rows.empty else None])
                    else:
                        return np.array([df.loc[field_name, column]])
                except (KeyError, IndexError):
                    return np.array([None])

            # 获取各字段值
            result = {
                'pure_value': get_value(pure_value_name),
                'property': get_value(property_name),
                'liabilities': get_value(liabilities_name),
                'security': get_value(security_name),
                'stock': get_value(stock_name),
                'bond': get_value(bond_name),
                'derivative': get_value(derivative_name),
                'unit_pure': get_value(unit_pure_name, '科目名称'),
                'accumulated_pure': get_value(accumulated_pure_name, '科目名称'),
                'accumulated_pure_d': get_value(accumulated_pure_d_name, '科目名称'),
                'shuhui_value': get_value(shuhui_value_name),
                'shengou_value': get_value(shengou_value_name)
            }

            # 数据检查
            date, product_name, pure_value, property, liabilities, security, stock, bond, derivative, unit_pure, accumulated_pure, accumulated_pure_d, shuhui_value, shengou_value = self.data_check(
                [self.date], [self.product_code],
                result['pure_value'], result['property'], result['liabilities'],
                result['security'], result['stock'], result['bond'],
                result['derivative'], result['unit_pure'], result['accumulated_pure'],
                result['accumulated_pure_d'], result['shuhui_value'], result['shengou_value']
            )
            # pure_value=self.safe_float_convert(pure_value)
            # 创建输出字典
            output_dict = {
                '持仓日期': date,
                '产品名称': product_name,
                '资产净值': pure_value,
                '资产总值': property,
                '资产负债': liabilities,
                '证券市值': security,
                '股票市值': stock,
                '债券市值': bond,
                '其他衍生工具市值': derivative,
                '产品净值': unit_pure,
                '产品累计净值': accumulated_pure,
                '产品日涨跌幅': accumulated_pure_d,
                '赎回金额': shuhui_value,
                '申购金额': shengou_value
            }

            self.logger.info(f"Successfully processed info sheet for {self.product_name}")
            return self.create_output_df(output_dict)
        except Exception as e:
            self.logger.error(f"Error processing info sheet: {str(e)}", exc_info=True)
            return None
