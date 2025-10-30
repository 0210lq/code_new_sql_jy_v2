"""
投资组合保存模块

这个模块负责将投资组合相关的数据保存到数据库和本地文件。
包括投资组合信息、持仓数据、产品权重等数据的保存操作。

主要功能：
1. 投资组合信息保存
2. 投资组合持仓保存
3. 产品权重保存
4. 产品持仓保存
5. SQL数据库保存

依赖模块：
- pandas：数据处理
- global_setting.global_dic：全局配置
- global_tools：全局工具函数
- data_prepared：数据准备
- holding_construct：持仓构建

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
from data_prepared import data_prepared
from holding_construct import holding_construction
global source,config_path
source=glv.get('source')
config_path=glv.get('config_path')
if source=='local':
    print('暂不支持local模式')
    raise ValueError

class portfolio_saving_main:
    """
    投资组合保存主类
    
    负责将投资组合相关的数据保存到数据库和本地文件系统。
    包括投资组合信息、持仓数据、产品权重等。
    """
    
    def __init__(self,target_date,realtime=False):
        """
        初始化投资组合保存对象
        
        Args:
            target_date (str): 目标日期，格式为'YYYY-MM-DD'
            realtime (bool): 是否为实时模式，默认为False
        """
        self.realtime=realtime
        self.dp=data_prepared(target_date,realtime)
        if realtime==True:
             self.target_date=gt.strdate_transfer(datetime.today())
        else:
            self.target_date=target_date
        self.df_mkt,self.etfpool=self.dp.mktData_withdraw()
    
    def PortfolioInfo_saving(self):
        """
        投资组合信息保存函数
        
        从产品配置文件中读取投资组合信息，包括产品名称、指数类型、
        产品代码等，并保存到数据库中。
        
        Returns:
            pandas.DataFrame: 保存的投资组合信息数据
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        inputpath_config = glv.get('config_product')
        excel_file = pd.ExcelFile(inputpath_config)
        sheet_names = excel_file.sheet_names
        detail_name = sheet_names[0]
        df_info = pd.read_excel(inputpath_config, detail_name)
        df_final = pd.DataFrame()
        for sheet_name in sheet_names[1:]:
            slice_df_info = df_info[df_info['product_name'] == sheet_name]
            slice_df_info = slice_df_info[['product_name', 'index_type', 'product_code']]
            df = pd.read_excel(inputpath_config, sheet_name=sheet_name)
            df.rename(columns={'score_name': 'portfolio_name'}, inplace=True)
            df['product_name'] = sheet_name
            df = df.merge(slice_df_info, on='product_name', how='left')
            df_final = pd.concat([df_final, df])
        df_final['valuation_date'] = self.target_date
        df_final['update_time'] = current_time
        df_final = df_final[
            ['valuation_date', 'product_name', 'product_code', 'portfolio_name', 'weight', 'index_type', 'update_time']]
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Portfolio_product',delete=True)
        sm.df_to_sql(df_final)
        return df_final

    def PortfolioHolinng_saving(self):
        """
        投资组合持仓保存函数
        
        计算并保存所有产品的投资组合持仓数据。
        为每个产品和投资组合计算持仓数量，并保存到数据库。
        
        Returns:
            pandas.DataFrame: 保存的投资组合持仓数据
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        productCode_list = self.dp.productCode_withdraw()
        portfolioList = self.dp.portfolioList_withdraw()
        inputpath_configsql = glv.get('config_sql')
        sm=gt.sqlSaving_main(inputpath_configsql, 'Portfolio_holding',delete=True)
        df_total=pd.DataFrame()
        for product_code in productCode_list:
            print(product_code)
            for portfolio_name in portfolioList:
                stock_money = self.dp.productInfo_withdraw(product_code)
                df_portfolio=self.dp.portfolioWeight_withdraw(portfolio_name)
                hs = holding_construction(df_portfolio,self.df_mkt,stock_money)
                df_final=hs.consturction_main()
                df_final=df_final[df_final['quantity']!=0]
                df_final['valuation_date']=self.target_date
                df_final['product_code']=product_code
                df_final['portfolio_name']=portfolio_name
                df_final=df_final[['valuation_date','product_code','portfolio_name','code','quantity']]
                df_final['update_time']=current_time
                df_total=pd.concat([df_total,df_final])
                df_total['update_time']=current_time
                sm.df_to_sql(df_final,'portfolio_name',portfolio_name)
        return df_total
    
    def ProductWeight_saving(self,df_prodInfo):
        """
        产品权重保存函数
        
        根据投资组合权重计算每个产品的目标权重，并保存到数据库和本地文件。
        
        Args:
            df_prodInfo (pandas.DataFrame): 产品信息数据
        """
        target_date2=gt.intdate_transfer(self.target_date)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        product_code_list=df_prodInfo['product_code'].unique().tolist()
        inputpath_configsql = glv.get('config_sql')
        outputpath_local=glv.get('productTarget_weight_local')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Product_weight',delete=True)
        for product_code in product_code_list:
            outputpath_local_daily=os.path.join(outputpath_local,product_code)
            gt.folder_creator2(outputpath_local_daily)
            outputpath_local_daily=os.path.join(outputpath_local_daily,product_code+'_'+str(target_date2)+'.csv')
            df_final=pd.DataFrame()
            count=0
            slice_df=df_prodInfo[df_prodInfo['product_code']==product_code]
            portfolio_list=slice_df['portfolio_name'].tolist()
            for portfolio_name in portfolio_list:
                weight=slice_df[slice_df['portfolio_name']==portfolio_name]['weight'].tolist()[0]
                df_portfolio=self.dp.portfolioWeight_withdraw(portfolio_name)
                df_portfolio=df_portfolio[['code','weight']]
                df_portfolio['weight']=df_portfolio['weight']*weight
                df_portfolio.rename(columns={'weight':portfolio_name},inplace=True)
                if count==0:
                    df_final=df_portfolio
                    count+=1
                else:
                    df_final=df_final.merge(df_portfolio,on='code',how='outer')
            df_final.fillna(0,inplace=True)
            df_final.set_index('code', inplace=True, drop=True)
            df_final['weight'] = df_final.apply(lambda x: x.sum(), axis=1)
            df_final = df_final[df_final['weight'] != 0]
            df_final.reset_index(inplace=True)
            df_final['product_code']=product_code
            df_final['valuation_date']=self.target_date
            df_final=df_final[['valuation_date','product_code','code','weight']]
            df_final['update_time']=current_time
            df_final.to_csv(outputpath_local_daily,index=False)
            sm.df_to_sql(df_final,'product_code',product_code)
    
    def ProductHolding_saving(self,df_prodInfo,df_portHolding):
        """
        产品持仓保存函数
        
        根据投资组合持仓计算每个产品的实际持仓，并保存到数据库。
        
        Args:
            df_prodInfo (pandas.DataFrame): 产品信息数据
            df_portHolding (pandas.DataFrame): 投资组合持仓数据
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        product_code_list=df_prodInfo['product_code'].unique().tolist()
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Product_holding',delete=True)
        for product_code in product_code_list:
            slice_portHolding=df_portHolding[df_portHolding['product_code']==product_code]
            df_final=pd.DataFrame()
            count=0
            slice_df=df_prodInfo[df_prodInfo['product_code']==product_code]
            portfolio_list=slice_df['portfolio_name'].tolist()
            for portfolio_name in portfolio_list:
                weight=slice_df[slice_df['portfolio_name']==portfolio_name]['weight'].tolist()[0]
                df_portfolio=slice_portHolding[slice_portHolding['portfolio_name']==portfolio_name]
                df_portfolio['quantity']=df_portfolio['quantity']*weight
                df_portfolio = df_portfolio[['code', 'quantity']]
                df_portfolio.rename(columns={'quantity':portfolio_name},inplace=True)
                if count==0:
                    df_final=df_portfolio
                    count+=1
                else:
                    df_final=df_final.merge(df_portfolio,on='code',how='outer')
            df_final.fillna(0,inplace=True)
            df_final.set_index('code', inplace=True, drop=True)
            df_final['quantity'] = df_final.apply(lambda x: x.sum(), axis=1)
            df_final['quantity']=round(df_final['quantity']/100,0)*100
            df_final = df_final[df_final['quantity'] != 0]
            df_final.reset_index(inplace=True)
            df_final['product_code']=product_code
            df_final['valuation_date']=self.target_date
            df_final=df_final[['valuation_date','product_code','code','quantity']]
            df_final['update_time']=current_time
            sm.df_to_sql(df_final,'product_code',product_code)
    
    def sqlSaving_main(self):
        """
        SQL保存主函数
        
        执行所有数据保存操作，包括投资组合信息、产品权重、投资组合持仓、
        产品持仓等。使用异常处理确保单个模块失败不影响整体执行。
        """
        # df_info = self.PortfolioInfo_saving()
        # df_holding = self.PortfolioHolinng_saving()
        # self.ProductWeight_saving(df_info)
        # self.ProductHolding_saving(df_info, df_holding)
        print('------------------------PortfolioInfo------------------------------------')
        try:
            df_info=self.PortfolioInfo_saving()
        except:
            print('PortfolioInfo更新有误')
        print('----------------------ProductWeight--------------------------------------')
        try:
            self.ProductWeight_saving(df_info)
        except:
            print('ProductWeight更新有误')
        print('-------------------------PortfolioHoling-----------------------------------')
        if self.realtime==False:
            try:
                df_holding = self.PortfolioHolinng_saving()
            except:
                print('PortfolioHoling更新有误')
            print('----------------------ProductHolding--------------------------------------')
            try:
                self.ProductHolding_saving(df_info, df_holding)
            except:
                print('ProductHolding更新有误')
