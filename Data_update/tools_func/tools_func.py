import pandas as pd
import numpy as np
from scipy import stats
from scipy.optimize import newton
from scipy.stats import norm
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
class delta_calculator:
    def __init__(self,available_date,df,df_future):
        self.available_date = available_date
        df = self.future_option_code_processing(df)
        df_future = self.future_option_code_processing(df_future)
        self.df = df
        self.df_future = df_future
    def future_option_code_processing(self, df):
        def extract_dot_field(code):
            code_str = str(code)
            if '.' in code_str:
                return code_str.split('.')[0]
            else:
                return code_str
        df['code'] = df['code'].apply(extract_dot_field)
        return df

    def process_df(self):
        df = self.df.copy()
        df_future = self.df_future.copy()
        df_future = df_future[['code', 'close']]
        df_future.columns = ['future_code', 'future_price']
        df['future_code'] = df['code'].apply(lambda x: self.future_option_mapping(x))
        df = df.merge(df_future, on='future_code', how='left')
        df['option_type'] = df['code'].apply(lambda x: 'call' if 'C' in x else 'put')
        df['strike_price'] = df['code'].apply(lambda x: float(x.split('-')[-1].split()[0]))
        df['date'] = df['code'].apply(lambda x: '20' + str(x)[2:4] + '-' + str(x)[4:6])
        df['maturity_date'] = df['date'].apply(lambda x: self.get_third_friday(x))
        df['maturity_date'] = pd.to_datetime(df['maturity_date'])
        df['T'] = (df['maturity_date'] - pd.to_datetime(self.available_date)).dt.days / 365
        df = self.calculate_sigma(df)
        return df
        
    def future_option_mapping(self,x):
        if str(x)[:2] == 'HO':
            return 'IH' + str(x)[2:6]
        elif str(x)[:2] == 'IO':
            return 'IF' + str(x)[2:6]
        elif str(x)[:2] == 'MO':
            return 'IM' + str(x)[2:6]
        else:
            print('qnmd')
            raise ValueError
    def bsm_option_price(self,S, K, r, T, sigma, option_type='call'):
    # 计算B-S模型下的期权价格
    # :param S: 标的资产价格
    # :param K: 行权价格
    # :param r: 无风险利率
    # :param T: 到期时间（年）
    # :param sigma: 波动率
    # :param option_type: 期权类型（'call'表示看涨期权，'put'表示看跌期权）
    # :return: 期权价格

        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
    
        if option_type == 'call':
            option_price = S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
        elif option_type == 'put':
            option_price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)
    
        return option_price

    def BM_IV(self,P, S, K, r, T, option_type,option_name):
        sigma_min = 0.0000001
        sigma_max = 1.000
        sigma_mid = (sigma_min + sigma_max) / 2
        status='normal'
        if option_type=='call':
            def call_bs(S, K, sigma, r, T):
                d1 = (np.log(S / K) + (r + pow(sigma, 2) / 2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                call_value = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
                return call_value
            call_min = call_bs(S, K, sigma_min, r, T)
            call_max = call_bs(S, K, sigma_max, r, T)
            call_mid = call_bs(S, K, sigma_mid, r, T)
            diff = P - call_mid
            if P < call_min or P > call_max:
                print('-----------------------------------------')
                print(option_name,'输入参数为',P, S, K, r, T, option_type)
                print('理论最小价格为:',call_min, '理论最大价格为:',call_max,'市场价格为:', P)
                print('-----------------------------------------')
                status='error'
            if status=='error':
                sigma_mid=None
            else:
                while abs(diff) > 1e-6:
                    diff = P - call_bs(S, K, sigma_mid, r, T)
                    sigma_mid = (sigma_min + sigma_max) / 2
                    call_mid = call_bs(S, K, sigma_mid, r, T)
                    if P > call_mid:
                        sigma_min = sigma_mid
                    else:
                        sigma_max = sigma_mid
        else:
            def put_bs(S, K, sigma, r, T):
                d1 = (np.log(S / K) + (r + pow(sigma, 2) / 2) * T) / (sigma * np.sqrt(T))
                d2 = d1 - sigma * np.sqrt(T)
                put_value = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                return put_value
            put_min = put_bs(S, K, sigma_min, r, T)
            put_max = put_bs(S, K, sigma_max, r, T)
            put_mid = put_bs(S, K, sigma_mid, r, T)
            diff = P - put_mid
            if P < put_min or P > put_max:
                print('-----------------------------------------')
                print(option_name,'输入参数为',P, S, K, r, T, option_type)
                print('理论最小价格为:',put_min, '理论最大价格为:',put_max,'市场价格为:', P)
                print('-----------------------------------------')
                status='error'
            if status=='error':
                sigma_mid=None
            else:
                while abs(diff) > 1e-6:
                    diff = P - put_bs(S, K, sigma_mid, r, T)
                    sigma_mid = (sigma_min + sigma_max) / 2
                    put_mid = put_bs(S, K, sigma_mid, r, T)
                    if P > put_mid:
                        sigma_min = sigma_mid
                    else:
                        sigma_max = sigma_mid
        return sigma_mid

    def implied_volatility(self,option_price, S, K, T, r, option_type,option_name):
        def difference(sigma):
            return self.bsm_option_price(S, K, T, r, sigma, option_type) - option_price
        iv=self.BM_IV(option_price, S, K, r, T, option_type,option_name)
        if iv==None:
            try:
                print('开始使用牛顿法')
                iv=newton(difference, 0.2, tol=1E-2)
            except:
                print('无解')
                if str(option_name)[:2]=='IO':
                     iv = 0.1
                else:
                     iv = 0.2
        return iv
    def calculate_sigma(self, df):
        df['sigma'] = df.apply(lambda row: self.implied_volatility(
            S=row['future_price'],
            K=row['strike_price'],
            r=0.02,
            T=row['T'],
            option_price=row['close'],
            option_type=row['option_type'],
            option_name=row['code'],
        ), axis=1)
        return df
    def d(self,s,k,r,T,sigma):
        d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        return (d1,d2)
    def delta(self,s,k,r,T,sigma,n):
        if n=='put':
            n2=-1
        else:
            n2=1
        d1 = self.d(s, k, r, T, sigma)[0]
        delta = n2 * stats.norm.cdf(n2 * d1)
        return delta
    def get_third_friday(self,year_month):
    # 将YYYY_MM转换为datetime
        year = int(year_month.split('-')[0])
        month = int(year_month.split('-')[1])
        first_day = pd.Timestamp(f'{year}-{month:02d}-01')
    
    # 获取当月第一个星期五
        friday = first_day + pd.Timedelta(days=(4 - first_day.weekday() + 7) % 7)
    
    # 获取第三个星期五
        third_friday = friday + pd.Timedelta(weeks=2)
    
    # 检查是否为工作日，如果不是则往后顺延到下一个工作日
        if gt.is_workday(third_friday)==False:
                third_friday = gt.next_workday_calculate(third_friday)
    
        return third_friday

    def delta_calculator_main(self):
        ori_list=self.df.columns.tolist()
        df=self.process_df()
        df['Delta'] = df.apply(lambda row:  self.delta(
            s=row['future_price'],
            k=row['strike_price'],
            r=0.02,
            T=row['T'],
            sigma=row['sigma'],
            n=row['option_type']
        ), axis=1)
        df=df[ori_list[:-2]+['Delta','sigma']+ori_list[-2:]]
        return df


class cb_delta_calculator:
    def __init__(self, available_date, df, df_stock,df_stock_return):
        self.available_date = gt.strdate_transfer(available_date)
        df_stock.columns=['stock_code','stock_price']
        self.df=df.merge(df_stock,on='stock_code',how='left')
        self.df_stock_return=df_stock_return
    def black_scholes_delta(self,S, K, T, r, sigma):
        # S: 正股价格
        # K: 转股价格·
        # T: 到期时间（以年为单位）
        # r: 无风险利率
        # sigma: 标的股票的波动率
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        delta = stats.norm.cdf(d1)
        return delta
    def CB_delta_calculate(self):
        df_holding=self.df.copy()
        df_holding.sort_values(by='stock_code',ascending=True,inplace=True)
        code_list=df_holding['stock_code'].tolist()
        df_std=gt.stock_volatility_calculate(self.df_stock_return,self.available_date)
        code_list2=df_std.columns.tolist()
        code_list_final=list(set(code_list)&set(code_list2))
        code_list_final.sort()
        df_std=df_std[code_list_final]
        df_std.reset_index(inplace=True)
        df_holding=df_holding[df_holding['stock_code'].isin(code_list_final)]
        df1=df_std[df_std['valuation_date']==self.available_date]
        df1.drop('valuation_date',axis=1,inplace=True)
        df1=df1.T
        df1.reset_index(inplace=True)
        df1.columns=['stock_code','std']
        df_holding=df_holding.merge(df1,on='stock_code',how='left')
        delta_list=[]
        for i in range(len(df_holding)):
            stock_price=df_holding['stock_price'].tolist()[i]
            cb_price = df_holding['conv_price'].tolist()[i]
            T = df_holding['maturity'].tolist()[i]
            std = df_holding['std'].tolist()[i]*np.sqrt(252)
            delta_today=self.black_scholes_delta(stock_price,cb_price, T, r=0.02, sigma=std)
            delta_list.append(delta_today)
        df_holding['delta']=delta_list
        return df_holding




    
