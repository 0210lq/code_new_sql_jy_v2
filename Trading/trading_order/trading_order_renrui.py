"""
仁睿交易订单模块

这个模块负责生成仁睿产品的交易订单。
根据目标权重和当前持仓计算交易差异，生成相应的交易订单文件。

主要功能：
1. 资金重平衡计算
2. 交易订单生成
3. 交易订单文件输出
4. 数据库保存

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

class trading_renrui:
    """
    仁睿交易订单类
    
    负责生成仁睿产品的交易订单。
    根据目标权重和当前持仓计算交易差异，生成标准格式的交易订单文件。
    """
    
    def __init__(self,df_weight,df_holding,df_mkt,target_date,stock_money):
        """
        初始化仁睿交易订单对象
        
        Args:
            df_weight (pandas.DataFrame): 目标权重数据
            df_holding (pandas.DataFrame): 当前持仓数据
            df_mkt (pandas.DataFrame): 市场数据
            target_date (str): 目标日期
            stock_money (float): 可用股票资金
        """
        self.df_weight=df_weight
        self.df_holding=df_holding
        self.df_mkt=df_mkt
        self.target_date=target_date
        self.stock_money=self.stockmoney_rebalance(stock_money)
    
    def stockmoney_rebalance(self,stock_money):
        """
        资金重平衡函数
        
        根据买卖差异调整可用资金，确保买卖金额基本平衡。
        如果买卖差异小于50万，则自动调整资金。
        
        Args:
            stock_money (float): 原始可用资金
            
        Returns:
            float: 调整后的可用资金
        """
        df_today = self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = pd.to_numeric(df_today['weight'], errors='coerce')
        df_today['close'] = pd.to_numeric(df_today['close'], errors='coerce')
        df_today = df_today.dropna(subset=['weight', 'close'])
        df_today = df_today[~(df_today['close'] == 0.0)]
        df_today['quantity'] = stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
        df_today['mkt'] = df_today['quantity'] * df_today['close']
        df_today = df_today[['code', 'quantity']]
        df_weight = pd.DataFrame()
        if len(self.df_holding)>0:
            code_list_yes = self.df_holding['code'].tolist()
            code_list = list(set(code_list_yes) | set(code_list_today))
            df_weight['code'] = code_list
            df_weight = df_weight.merge(self.df_holding, on='code', how='left')
        else:
            df_weight=df_today
            df_weight['holding']=0
        df_weight = df_weight.merge(self.df_mkt, on='code', how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['difference'] = df_weight['quantity'] - df_weight['holding']
        df_weight_check = df_weight.copy()
        df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
        df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
        df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
        df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
        mkt_buying = df_buying_check['mkt_value'].sum()
        mkt_selling = df_selling_check['mkt_value'].sum()
        if abs((mkt_buying-mkt_selling))<500000:
            stock_money=stock_money-(mkt_buying-mkt_selling)
        print('rr1号试运行买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling)+'将自动把stock_money调整为:'+str(stock_money))
        return stock_money

    def trading_order_renrui(self):
        """
        仁睿交易订单生成函数
        
        生成仁睿产品的交易订单，使用特定的模板格式。
        计算交易差异，生成交易订单文件并保存到数据库。
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Trading_renr',delete=True)
        outputpath=glv.get('product_trading')
        outputpath=os.path.join(outputpath,'仁睿')
        gt.folder_creator2(outputpath)
        outputpath=os.path.join(outputpath,'renr_' + str(gt.intdate_transfer(self.target_date)) + '_trading_list.csv')
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, '模板')
        inputpath = os.path.join(inputpath, '仁睿')
        inputpath = os.path.join(inputpath, '默认篮子格式.csv')
        df = gt.readcsv(inputpath)
        df_today=self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = pd.to_numeric(df_today['weight'], errors='coerce')
        df_today['close'] = pd.to_numeric(df_today['close'], errors='coerce')
        df_error = df_today[(df_today['close'] == 0.0) | (df_today['close'].isna()) | (df_today['weight'].isna())]
        df_today = df_today.dropna(subset=['weight', 'close'])
        df_today = df_today[~(df_today['close'] == 0.0)]
        if len(df_error) > 0:
            print('以下数据close或weight出现问题')
            print(df_error)
        df_today['quantity'] = self.stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
        df_today['mkt']=df_today['quantity']*df_today['close']
        df_today = df_today[['code', 'quantity']]
        df_weight = pd.DataFrame()
        if len(self.df_holding)>0:
            code_list_yes = self.df_holding['code'].tolist()
            code_list = list(set(code_list_yes) | set(code_list_today))
            df_weight['code'] = code_list
            df_weight = df_weight.merge(self.df_holding, on='code', how='left')
            df_weight = df_weight.merge(df_today, on='code', how='left')
        else:
            df_weight = df_today
            df_weight['holding'] = 0
        df_weight=df_weight.merge(self.df_mkt,on='code',how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['difference'] = df_weight['quantity'] - df_weight['holding']
        df_weight_check = df_weight.copy()
        df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
        df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
        df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
        df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
        mkt_buying = df_buying_check['mkt_value'].sum()
        mkt_selling = df_selling_check['mkt_value'].sum()
        df_weight_check['mkt_holding'] = df_weight_check['close'] * df_weight_check['holding']
        mkt_holding = df_weight_check['mkt_holding'].sum()
        print('rr1号买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling),
              'holding总市值为:' + str(mkt_holding))
        list_difference = df_weight['difference'].tolist()
        list_action = []
        for i in list_difference:
            if i > 0:
                list_action.append('0')
            elif i < 0:
                list_action.append('1')
            else:
                list_action.append('不动')
        df_weight['action'] = list_action
        df_weight['difference'] = abs(df_weight['difference'])
        df_weight = df_weight[df_weight['action'] != '不动']
        df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
        df_weight.loc[
            (df_weight['new_code'] == '68') & (df_weight['action'] == '0') & (df_weight['difference'] == 100), [
                'difference']] = 200
        code_list_today = df_weight['code'].tolist()
        quantity_list = df_weight['difference'].tolist()
        action_list = df_weight['action'].tolist()
        df['代码'] = code_list_today
        df['数量'] = quantity_list
        df['方向'] = action_list
        df_final = df
        df['代码']=df['代码'].apply(lambda x: str(x)[:-3])
        df_final.to_csv(outputpath, index=False, encoding='utf_8_sig')
        df.rename(columns={'代码':'code'},inplace=True)
        df_final['valuation_date']=self.target_date
        df_final['update_time']=current_time
        sm.df_to_sql(df_final)