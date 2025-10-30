"""
宣夜交易订单模块

这个模块负责生成宣夜产品的交易订单，支持TWAP和VWAP两种交易模式。
根据目标权重和当前持仓计算交易差异，生成相应的交易订单文件。

主要功能：
1. 资金重平衡计算
2. TWAP模式交易订单生成
3. VWAP模式交易订单生成
4. 交易订单文件输出
5. ETF和股票分类处理

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

class trading_xuanye:
    """
    宣夜交易订单类
    
    负责生成宣夜产品的交易订单，支持不同的交易模式。
    根据目标权重和当前持仓计算交易差异，生成标准格式的交易订单文件。
    """
    
    def __init__(self,df_weight,df_holding,df_mkt,target_date,stock_money,etf_pool,trading_time):
        """
        初始化宣夜交易订单对象
        
        Args:
            df_weight (pandas.DataFrame): 目标权重数据
            df_holding (pandas.DataFrame): 当前持仓数据
            df_mkt (pandas.DataFrame): 市场数据
            target_date (str): 目标日期
            stock_money (float): 可用股票资金
            etf_pool (list): ETF代码池
            trading_time (str): 交易时间
        """
        self.df_weight=df_weight
        self.df_holding=df_holding
        self.df_mkt=df_mkt
        self.target_date=target_date
        self.stock_money=self.stockmoney_rebalance(stock_money)
        self.etf_pool=etf_pool
        self.trading_time=trading_time
        self.current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    
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
            df_weight = df_weight.merge(df_today, on='code', how='left')
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
        print('xy试运行买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling)+'将自动把stock_money调整为:'+str(stock_money))
        return stock_money
    
    def trading_order_xuanye_mode_1(self):
        """
        TWAP模式交易订单生成函数
        
        生成TWAP模式的交易订单，使用特定的模板格式。
        将交易订单分为股票和ETF两部分，分别生成不同的文件。
        """
        df_final = pd.DataFrame()
        etf_code =self.etf_pool
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, '模板')
        inputpath = os.path.join(inputpath, '宣夜')
        inputpath = os.path.join(inputpath, 'trading_list模板.csv')
        outputpath = glv.get('product_trading')
        outputpath = os.path.join(outputpath, '宣夜')
        gt.folder_creator2(outputpath)
        target_date2 = gt.intdate_transfer(self.target_date)
        outputpath3 = os.path.join(outputpath, 'xy_' + target_date2 + '_ETF_trading_list.csv')
        outputpath2 = os.path.join(outputpath, 'xy_' + target_date2 + '_trading_list.csv')
        df = gt.readcsv(inputpath)
        df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN1', 'open_action', 'UN2', '涨停', 'UN3', 'UN4']
        df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴TWAP-' + str(self.target_date)
        df[df.columns.tolist()[2]].iloc[3] = self.trading_time
        df_today=self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = df_today['weight'].astype(float)
        df_today['close'] = df_today['close'].astype(float)
        df_error = df_today[(df_today['close'].astype(float) == 0)|(df_today['close'].isna())]
        if len(df_error) > 0:
            print('以下数据close出现问题')
            print(df_error)
        df_today['weight'] = df_today['weight'] / df_today['weight'].sum()
        df_today['quantity'] = self.stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
        df_today = df_today[['code', 'quantity']]
        df_weight=pd.DataFrame()
        if len(self.df_holding)>0:
            code_list_yes = self.df_holding['code'].tolist()
            code_list = list(set(code_list_yes) | set(code_list_today))
            df_weight['code'] = code_list
            df_weight= df_weight.merge(self.df_holding, on='code', how='left')
            df_weight = df_weight.merge(df_today, on='code', how='left')
        else:
            df_weight=df_today
            df_weight['holding'] = 0
        df_weight = df_weight.merge(self.df_mkt, on='code', how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['difference'] = df_weight['quantity'] - df_weight['holding']
        list_difference = df_weight['difference'].tolist()
        df_weight_check = df_weight.copy()
        df_buying_check = df_weight_check[df_weight_check['difference'] > 0]
        df_selling_check = df_weight_check[df_weight_check['difference'] < 0]
        df_buying_check['mkt_value'] = df_buying_check['close'] * abs(df_buying_check['difference'])
        df_selling_check['mkt_value'] = df_selling_check['close'] * abs(df_selling_check['difference'])
        mkt_buying = df_buying_check['mkt_value'].sum()
        mkt_selling = df_selling_check['mkt_value'].sum()
        # inputpath_configsql = glv.get('config_sql')
        # sm = gt.sqlSaving_main(inputpath_configsql, 'Trading_xy', delete=True)
        print('hy1号买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling))
        list_action = []
        for i in list_difference:
            if i > 0:
                list_action.append('买入')
            elif i < 0:
                list_action.append('卖出')
            else:
                list_action.append('不动')
        df_weight['action'] = list_action
        df_weight['difference'] = abs(df_weight['difference'])
        df_weight = df_weight[df_weight['action'] != '不动']
        df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
        df_weight.loc[
            (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
                'difference']] = 200
        df_weight['code'] = df_weight['code'].apply(lambda x: str(x)[:-3])
        df_weight1 = df_weight[df_weight['code'].isin(etf_code)]
        df_weight1 = df_weight1[['code', 'difference', 'action']]
        df_weight1.columns = ['code', 'quantity', '方向']
        df_weight2 = df_weight[~(df_weight['code'].isin(etf_code))]
        code_list_today = df_weight2['code'].tolist()
        quantity_list = df_weight2['difference'].tolist()
        action_list = df_weight2['action'].tolist()
        for i in range(len(df.columns.tolist())):
            columns_name = df.columns.tolist()[i]
            initial_list = df[columns_name].tolist()
            if i == 1:
                final_list = initial_list + code_list_today
            elif i == 2:
                final_list = initial_list + quantity_list
            elif i == 3:
                final_list = initial_list + action_list
            elif i == 5:
                final_list = initial_list + [0] * len(df_weight)
            elif i == 7:
                final_list = initial_list + ['FALSE'] * len(df_weight2)
            else:
                final_list = initial_list + [None] * len(df_weight2)
            df_final[columns_name] = final_list
        df_final.columns = df.columns.tolist()[:2] + [None] * 8
        df_final.to_csv(outputpath2, index=False, encoding='utf_8_sig')
        # df_final['valuation_date'] = self.target_date
        # df_final['update_time'] = self.current_time
        # sm.df_to_sql(df_final)
        df_weight1.to_csv(outputpath3, index=False, encoding='gbk')

    def trading_order_xuanye_mode_2(self):
        """
        VWAP模式交易订单生成函数
        
        生成VWAP模式的交易订单，使用特定的模板格式。
        将交易订单分为股票和ETF两部分，分别生成不同的文件。
        """
        df_final = pd.DataFrame()
        etf_code = self.etf_pool
        inputpath = os.path.split(os.path.realpath(__file__))[0]
        inputpath = os.path.join(inputpath, '模板')
        inputpath = os.path.join(inputpath, '宣夜')
        inputpath = os.path.join(inputpath, 'trading_list_v2模板.csv')
        outputpath = glv.get('product_trading')
        outputpath = os.path.join(outputpath, '宣夜')
        gt.folder_creator2(outputpath)
        target_date2 = gt.intdate_transfer(self.target_date)
        outputpath3 = os.path.join(outputpath, 'xy_' + target_date2 + '_ETF_trading_list.csv')
        outputpath2 = os.path.join(outputpath, 'xy_' + target_date2 + '_trading_list.csv')
        df = gt.readcsv(inputpath)
        df.columns = df.columns.tolist()[:2] + ['Time', 'Direction', 'UN2', '涨停', 'UN3', 'UN4']
        target_date2 = str(self.target_date)[:4] + str(self.target_date)[5:7] + str(self.target_date)[8:10]
        df[df.columns.tolist()[1]].iloc[1] = '宇量皓兴VWAP-' + str(target_date2)
        df[df.columns.tolist()[2]].iloc[3] = self.trading_time
        df_today=self.df_weight.copy()
        code_list_today = df_today['code'].tolist()
        df_today = df_today.merge(self.df_mkt, on='code', how='left')
        df_today['weight'] = pd.to_numeric(df_today['weight'], errors='coerce')
        df_today['close'] = pd.to_numeric(df_today['close'], errors='coerce')
        df_error = df_today[(df_today['close'] == 0.0) | (df_today['close'].isna()) | (df_today['weight'].isna())]
        df_today = df_today.dropna(subset=['weight', 'close'])
        df_today = df_today[~(df_today['close'] == 0.0)]
        if len(df_error) > 0:
            print('以下数据close出现问题')
            print(df_error)
        df_today['quantity'] = self.stock_money * df_today['weight'] / df_today['close']
        df_today['quantity'] = round(df_today['quantity'] / 100, 0) * 100
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
        df_weight_check['mkt_holding'] = df_weight_check['close'] * df_weight_check['holding']
        mkt_holding = df_weight_check['mkt_holding'].sum()
        # inputpath_configsql = glv.get('config_sql')
        # sm = gt.sqlSaving_main(inputpath_configsql, 'Trading_xy', delete=True)
        print('hy1号买额为:' + str(mkt_buying),
              '卖额为' + str(mkt_selling) + '买卖额差为' + str(mkt_buying - mkt_selling),
              'holding总市值为:' + str(mkt_holding))
        list_difference = df_weight['difference'].tolist()
        list_action = []
        for i in list_difference:
            if i > 0:
                list_action.append('买入')
            elif i < 0:
                list_action.append('卖出')
            else:
                list_action.append('不动')
        df_weight['action'] = list_action
        df_weight['difference'] = abs(df_weight['difference'])
        df_weight = df_weight[df_weight['action'] != '不动']
        df_weight['new_code'] = df_weight['code'].apply(lambda x: str(x)[:2])
        df_weight.loc[
            (df_weight['new_code'] == '68') & (df_weight['action'] == '买入') & (df_weight['difference'] == 100), [
                'difference']] = 200
        df_weight['code'] = df_weight['code'].apply(lambda x: str(x)[:-3])
        df_weight1 = df_weight[df_weight['code'].isin(etf_code)]
        df_weight1 = df_weight1[['code', 'difference', 'action']]
        df_weight1.columns = ['code', 'quantity', '方向']

        df_weight2 = df_weight[~(df_weight['code'].isin(etf_code))]
        code_list_today = df_weight2['code'].tolist()
        quantity_list = df_weight2['difference'].tolist()
        action_list = df_weight2['action'].tolist()
        for i in range(len(df.columns.tolist())):
            columns_name = df.columns.tolist()[i]
            initial_list = df[columns_name].tolist()
            if i == 1:
                final_list = initial_list + code_list_today
            elif i == 2:
                final_list = initial_list + quantity_list
            elif i == 3:
                final_list = initial_list + action_list
            elif i == 5:
                final_list = initial_list + ['FALSE'] * len(df_weight2)
            else:
                final_list = initial_list + [None] * len(df_weight2)
            df_final[columns_name] = final_list
        df_final.columns = df.columns.tolist()[:2] + [None] * 6
        df_final.to_csv(outputpath2, index=False, encoding='utf_8_sig')
        df_weight1.to_csv(outputpath3, index=False, encoding='gbk')
        # df_final['valuation_date']=self.target_date
        # df_final['update_time']=self.current_time
        # sm.df_to_sql(df_final)
    
    def trading_order_xy_main(self,trading_mode):
        """
        宣夜交易订单主函数
        
        根据交易模式选择相应的交易订单生成方法。
        
        Args:
            trading_mode (str): 交易模式，'mode_v1'为TWAP模式，其他为VWAP模式
        """
        if trading_mode == 'mode_v1':
            self.trading_order_xuanye_mode_1()
        else:
            self.trading_order_xuanye_mode_2()

