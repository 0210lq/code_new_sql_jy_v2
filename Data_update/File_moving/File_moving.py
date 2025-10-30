import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
import global_setting.global_dic as glv
# 添加数据库管理器导入
from global_setting.db_manager import DBSyncManager
from setup_logger.logger_setup import setup_logger
import io
import contextlib
from datetime import datetime

def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('DataOther_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result

# 数据库同步类
class DatabaseSync:
    def __init__(self, source_config=None, target_config=None):
        """
        初始化数据库同步类
        
        参数:
            source_config: 源数据库配置名称或直接配置字典
            target_config: 目标数据库配置名称或直接配置字典
        """
        # 从全局配置获取数据库配置，如果没有提供则使用默认配置
        self.source_config = source_config or glv.get('source_db_config', 'source_db')
        self.target_config = target_config or glv.get('target_db_config', 'target_db')
        self.sync_manager = DBSyncManager(self.source_config, self.target_config)
        self.logger = setup_logger('DatabaseSync')
    
    def sync_table(self, table_name, where_clause=None, params=None, truncate_first=False):
        """
        同步单个表
        
        参数:
            table_name: 表名
            where_clause: 筛选条件
            params: 查询参数
            truncate_first: 是否先清空目标表
        """
        try:
            self.logger.info(f"开始同步表: {table_name}")
            count = self.sync_manager.sync_table(table_name, where_clause, params, truncate_first)
            self.logger.info(f"表 {table_name} 同步完成，共同步 {count} 条记录")
            return count
        except Exception as e:
            self.logger.error(f"同步表 {table_name} 失败: {str(e)}")
            raise
    
    def sync_multiple_tables(self, tables, truncate_first=False):
        """
        同步多个表
        
        参数:
            tables: 表名列表或字典
            truncate_first: 是否先清空目标表
        """
        try:
            self.logger.info(f"开始同步多个表: {', '.join(tables)}")
            results = self.sync_manager.sync_multiple_tables(tables, truncate_first)
            
            # 记录结果
            for table_name, result in results.items():
                if result['status'] == 'success':
                    self.logger.info(f"表 {table_name} 同步成功，共同步 {result['count']} 条记录")
                else:
                    self.logger.error(f"表 {table_name} 同步失败: {result['message']}")
            
            return results
        except Exception as e:
            self.logger.error(f"同步多个表失败: {str(e)}")
            raise
    
    def sync_main(self):
        """
        主同步方法，可以在这里配置需要同步的表
        """
        # 示例：同步多个表
        tables_to_sync = {
            'ChineseValuationDate': {},
            'STstock': {},
            'Stock_uni': {},
            'SpecialDay': {}
        }
        return self.sync_multiple_tables(tables_to_sync)

class File_moving:
    def data_other_moving(self):
        input = glv.get('input_destination')
        output = glv.get('output_destination')
        gt.folder_creator2(output)
        status=0
        if len(os.listdir(output))==0:
            status=1
        gt.move_specific_files(input, output)
        if status==1:
            DOS = DataOther_sql()
            DOS.Dataother_main()
    
    def data_product_moving(self):
        input=glv.get('input_prod')
        gt.folder_creator2(input)
        if len(os.listdir(input))!=0:
            output = glv.get('output_prod')
            gt.move_specific_files2(input, output)
            print('product_detail已经复制完成')
    
    def file_moving_update_main(self):
        self.data_other_moving()
        self.data_product_moving()
        # 可以在这里添加数据库同步调用
        # sync = DatabaseSync()
        # sync.sync_main()

class DataOther_sql:
    def __init__(self):
        self.inputpath=glv.get('output_destination')
    
    def valuationData_sql(self):
        inputpath=os.path.join(self.inputpath,'chinese_valuation_date.xlsx')
        df=pd.read_excel(inputpath)
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'ChineseValuationDate')
        capture_file_withdraw_output(sm.df_to_sql, df)
    
    def st_stock_sql(self):
        inputpath=os.path.join(self.inputpath,'st_stock.xlsx')
        df=pd.read_excel(inputpath)
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'STstock')
        capture_file_withdraw_output(sm.df_to_sql, df)
    
    def stockuni_sql(self):
        inputpath=os.path.join(self.inputpath,'StockUniverse_new.csv')
        df=gt.readcsv(inputpath)
        inputpath2=os.path.join(self.inputpath,'StockUniverse.csv')
        df2=gt.readcsv(inputpath2)
        df=df[['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']]
        df2=df2[['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']]
        df2['type']='stockuni_old'
        df['type']='stockuni_new'
        df_final=pd.concat([df,df2])
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Stock_uni')
        capture_file_withdraw_output(sm.df_to_sql, df_final)
    
    def specialdate_sql(self):
        inputpath=os.path.join(self.inputpath,'month_first_6days.xlsx')
        df=pd.read_excel(inputpath)
        inputpath2=os.path.join(self.inputpath,'weeks_firstday.xlsx')
        df2=pd.read_excel(inputpath2)
        inputpath3 = os.path.join(self.inputpath, 'weeks_lastday.xlsx')
        df3 = pd.read_excel(inputpath3)
        df['type']='monthFirst6Days'
        df2['type']='weeksFirstDay'
        df3['type']='weeksLastDay'
        df_final=pd.concat([df,df2,df3])
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'SpecialDay')
        capture_file_withdraw_output(sm.df_to_sql, df_final)
    
    def Dataother_main(self):
        self.valuationData_sql()
        self.stockuni_sql()
        self.specialdate_sql()
        self.st_stock_sql()

# 创建__init__.py文件以便模块导入
# d:\code_new_SQL_jy_new\Data_update\global_setting\__init__.py

if __name__ == '__main__':
    # 可以选择运行数据导入或数据库同步
    # DOS = DataOther_sql()
    # DOS.Dataother_main()
    
    # 运行数据库同步
    sync = DatabaseSync()
    sync.sync_main()



