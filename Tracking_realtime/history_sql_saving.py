"""
历史数据SQL保存模块
功能：将实时数据保存到历史数据表中，包括期货期权持仓、投资组合收益、产品收益、产品信息、持仓变化等
主要类：
- historySql_saving: 历史数据保存类，负责将实时数据迁移到历史表
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

def sql_path():
    """
    获取SQL配置文件路径
    功能：构建SQL配置文件的完整路径
    
    Returns:
        str: SQL配置文件的完整路径
    """
    yaml_path = os.path.join(os.path.dirname(__file__), 'project_config', 'trackingrealtime_sql.yaml')
    return yaml_path

# 全局变量定义
global inputpath_sql, config_path
config_path = glv.get('config_path')
inputpath_sql = sql_path()

class historySql_saving:
    """
    历史数据保存类
    功能：将实时数据保存到历史数据表中，确保数据的持久化存储
    """
    
    def __init__(self):
        """
        初始化方法
        功能：初始化历史数据保存对象，设置当前日期和时间
        """
        today = datetime.date.today()
        self.date = gt.strdate_transfer(today)  # 当前日期字符串
        self.now = datetime.datetime.now().replace(tzinfo=None)  # 当前时间
    
    def foHolding_saving(self):
        """
        保存期货期权持仓历史数据
        功能：从实时表读取期货期权持仓数据并保存到历史表
        
        Returns:
            DataFrame: 保存的期货期权持仓数据
        """
        # 从实时表查询期货期权持仓数据
        inputpath = f"Select * from tracking_realtime.realtime_futureoptionholding Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            # 添加更新时间
            df_final['update_time'] = self.now
            # 保存到历史表
            sm = gt.sqlSaving_main(inputpath_sql, 'optionfuture_holding_his', delete=True)
            sm.df_to_sql(df_final)
        
        return df_final
    
    def foHolding_check(self):
        """
        检查期货期权持仓历史数据是否存在
        功能：检查指定日期的期货期权持仓历史数据是否已存在
        
        Returns:
            str: 'exist'表示存在，'not_exist'表示不存在
        """
        # 查询历史表中的期货期权持仓数据
        inputpath = f"Select * from tracking_realtime.history_futureoptionholding Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            status = 'exist'
        else:
            status = 'not_exist'
        
        return status
    
    def portfolioreturn_saving(self):
        """
        保存投资组合收益历史数据
        功能：从实时表读取投资组合收益数据并保存到历史表
        
        Returns:
            DataFrame: 保存的投资组合收益数据
        """
        # 从实时表查询投资组合收益数据
        inputpath = f"Select * from tracking_realtime.realtime_portfolioreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            # 添加更新时间
            df_final['update_time'] = self.now
            # 保存到历史表
            sm = gt.sqlSaving_main(inputpath_sql, 'portfolioreturn_his', delete=True)
            sm.df_to_sql(df_final)
        
        return df_final
    
    def portfolioreturn_check(self):
        """
        检查投资组合收益历史数据是否存在
        功能：检查指定日期的投资组合收益历史数据是否已存在
        
        Returns:
            str: 'exist'表示存在，'not_exist'表示不存在
        """
        # 查询历史表中的投资组合收益数据
        inputpath = f"Select * from tracking_realtime.history_portfolioreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            status = 'exist'
        else:
            status = 'not_exist'
        
        return status
    
    def productreturn_saving(self):
        """
        保存产品收益历史数据
        功能：从实时表读取产品收益数据并保存到历史表
        
        Returns:
            DataFrame: 保存的产品收益数据
        """
        # 从实时表查询产品收益数据
        inputpath = f"Select * from tracking_realtime.realtime_productstockreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            # 添加更新时间
            df_final['update_time'] = self.now
            # 保存到历史表
            sm = gt.sqlSaving_main(inputpath_sql, 'productreturn_his', delete=True)
            sm.df_to_sql(df_final)
        
        return df_final
    
    def productreturn_check(self):
        """
        检查产品收益历史数据是否存在
        功能：检查指定日期的产品收益历史数据是否已存在
        
        Returns:
            str: 'exist'表示存在，'not_exist'表示不存在
        """
        # 查询历史表中的产品收益数据
        inputpath = f"Select * from tracking_realtime.history_productstockreturn Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            status = 'exist'
        else:
            status = 'not_exist'
        
        return status
    
    def proinfo_saving(self):
        """
        保存产品信息历史数据
        功能：从实时表读取产品信息数据并保存到历史表
        
        Returns:
            DataFrame: 保存的产品信息数据
        """
        # 从实时表查询产品信息数据
        inputpath = f"Select * from tracking_realtime.realtime_proinfo Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            # 添加更新时间
            df_final['update_time'] = self.now
            # 保存到历史表
            sm = gt.sqlSaving_main(inputpath_sql, 'proinfo_his', delete=True)
            sm.df_to_sql(df_final)
        
        return df_final
    
    def proinfo_check(self):
        """
        检查产品信息历史数据是否存在
        功能：检查指定日期的产品信息历史数据是否已存在
        
        Returns:
            str: 'exist'表示存在，'not_exist'表示不存在
        """
        # 查询历史表中的产品信息数据
        inputpath = f"Select * from tracking_realtime.history_proinfo Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            status = 'exist'
        else:
            status = 'not_exist'
        
        return status
    
    def holdingchanging_saving(self):
        """
        保存持仓变化历史数据
        功能：从实时表读取持仓变化数据并保存到历史表
        
        Returns:
            DataFrame: 保存的持仓变化数据
        """
        # 从实时表查询持仓变化数据
        inputpath = f"Select * from tracking_realtime.realtime_holdingchanging Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            # 添加更新时间
            df_final['update_time'] = self.now
            # 保存到历史表
            sm = gt.sqlSaving_main(inputpath_sql, 'holding_changing_his', delete=True)
            sm.df_to_sql(df_final)
        
        return df_final
    
    def holdingchanging_check(self):
        """
        检查持仓变化历史数据是否存在
        功能：检查指定日期的持仓变化历史数据是否已存在
        
        Returns:
            str: 'exist'表示存在，'not_exist'表示不存在
        """
        # 查询历史表中的持仓变化数据
        inputpath = f"Select * from tracking_realtime.history_holdingchanging Where valuation_date='{self.date}'"
        df_final = gt.data_getting(inputpath, config_path)
        
        if len(df_final) > 0:
            status = 'exist'
        else:
            status = 'not_exist'
        
        return status

    def historySql_main(self):
        """
        历史数据保存主函数
        功能：执行完整的历史数据保存流程，检查并保存所有类型的历史数据
        """
        # 检查并保存期货期权持仓历史数据
        status1 = self.foHolding_check()
        if status1 == 'not_exist':
            self.foHolding_saving()
        
        # 检查并保存投资组合收益历史数据
        status2 = self.portfolioreturn_check()
        if status2 == 'not_exist':
            self.portfolioreturn_saving()
        
        # 检查并保存产品收益历史数据
        status3 = self.productreturn_check()
        if status3 == 'not_exist':
            self.productreturn_saving()
        
        # 检查并保存产品信息历史数据
        status4 = self.proinfo_check()
        if status4 == 'not_exist':
            self.proinfo_saving()
        
        # 检查并保存持仓变化历史数据
        status5 = self.holdingchanging_check()
        if status5 == 'not_exist':
            self.holdingchanging_saving()


if __name__ == '__main__':
    # 创建历史数据保存对象并执行保存流程
    hs = historySql_saving()
    hs.historySql_main()