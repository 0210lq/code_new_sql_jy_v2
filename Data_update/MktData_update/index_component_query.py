"""
指数成分股查询通用类
从聚源数据库（jydb）查询指数成分股权重数据
"""
import os
import pandas as pd
from datetime import datetime
import sys

path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from global_setting.db_manager import MySQLManager
from setup_logger.logger_setup import setup_logger


class IndexComponent:
    """指数成分股数据查询通用类"""
    
    def __init__(self, dic_index, db_config='source_db'):
        """
        初始化指数成分股查询类
        
        参数:
            dic_index: 指数代码字典，格式 {'3145': 'hs300', '3146': 'zz500', ...}
                      key为指数代码（字符串），value为指数英文简称
            db_config: 数据库配置名称，默认为'source_db'
        """
        self.dic_index = dic_index
        self.db_manager = MySQLManager(db_config)
        self.logger = setup_logger('IndexComponent_Query')
    
    def query_index_component(self, date):
        """
        查询指数成分股数据并返回DataFrame
        
        参数:
            date: 日期，可以是以下格式之一：
                  - 整数格式 YYYYMMDD，如 20251030
                  - 字符串格式 'YYYY-MM-DD'，如 '2025-10-30'
                  - 字符串格式 'YYYYMMDD'，如 '20251030'
        
        返回:
            DataFrame，包含所有指数的成分股数据
            列：valuation_date, organization, code, weight, status, update_time
        """
        try:
            # 连接数据库
            self.db_manager.connect()
            
            # 统一转换日期格式
            if isinstance(date, int):
                # 整数格式 YYYYMMDD
                dt_str = str(date)
                dt_date = datetime.strptime(dt_str, '%Y%m%d').strftime('%Y-%m-%d')
            elif isinstance(date, str):
                if len(date) == 8:
                    # 字符串格式 YYYYMMDD
                    dt_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                elif len(date) == 10:
                    # 字符串格式 YYYY-MM-DD
                    dt_date = date
                else:
                    raise ValueError(f"日期格式不正确: {date}，应为 YYYYMMDD 或 YYYY-MM-DD")
            else:
                raise ValueError(f"日期类型不正确: {type(date)}，应为 int 或 str")
            
            # 估值日期和查询日期使用同一个日期
            valuation_date_str = dt_date
            
            # 获取当前时间作为update_time
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 查询参数
            eod_flag = 1  # 收盘标志
            
            # 存储所有指数的数据
            all_data = []
            
            # 遍历所有指数代码
            for index_code_str, organization in self.dic_index.items():
                index_code = int(index_code_str)
                self.logger.info(f'查询指数 {organization} (代码: {index_code}) 的成分股数据')
                
                # 查询普通市场数据（沪深主板和创业板）
                sql_string = """
                SELECT 
                    CASE 
                        WHEN t2.SecuMarket = 83 THEN CONCAT(t2.SecuCode, '.SH') 
                        WHEN t2.SecuMarket = 90 THEN CONCAT(t2.SecuCode, '.SZ') 
                        ELSE NULL 
                    END AS code,
                    t1.weight AS weight,
                    %s AS status
                FROM jydb.LC_IndexComponentsWeight t1
                LEFT JOIN jydb.SecuMain t2 ON t2.InnerCode = t1.InnerCode
                LEFT JOIN jydb.LC_ShareStru t3 ON t3.CompanyCode = t2.CompanyCode
                LEFT JOIN jydb.QT_DailyQuote t4 ON t4.InnerCode = t1.InnerCode
                WHERE t1.EndDate = (
                    SELECT MAX(EndDate) 
                    FROM jydb.LC_IndexComponentsWeight 
                    WHERE IndexCode = %s AND EndDate < %s
                )
                AND t3.EndDate = (
                    SELECT MAX(EndDate) 
                    FROM jydb.LC_ShareStru s1 
                    WHERE s1.CompanyCode = t3.CompanyCode AND s1.EndDate < %s
                )
                AND t4.TradingDay = %s
                AND t1.IndexCode = %s
                ORDER BY code, status
                """
                
                # 查询科创板数据
                sql_string_kc = """
                SELECT 
                    CASE WHEN t2.SecuMarket = 83 THEN CONCAT(t2.SecuCode, '.SH') END AS code,
                    t1.weight AS weight,
                    %s AS status
                FROM jydb.LC_IndexComponentsWeight t1
                LEFT JOIN jydb.SecuMain t2 ON t2.InnerCode = t1.InnerCode
                LEFT JOIN jydb.LC_STIBShareStru t3 ON t3.CompanyCode = t2.CompanyCode
                LEFT JOIN jydb.LC_STIBDailyQuote t4 ON t4.InnerCode = t1.InnerCode
                WHERE t1.EndDate = (
                    SELECT MAX(EndDate) 
                    FROM jydb.LC_IndexComponentsWeight 
                    WHERE IndexCode = %s AND EndDate < %s
                )
                AND t3.EndDate = (
                    SELECT MAX(EndDate) 
                    FROM jydb.LC_STIBShareStru s1 
                    WHERE s1.CompanyCode = t3.CompanyCode AND s1.EndDate < %s
                )
                AND t4.TradingDay = %s
                AND t1.IndexCode = %s
                ORDER BY code, status
                """
                
                # 执行查询 - 普通市场
                params_normal = (
                    eod_flag,           # %s 1: status
                    index_code,         # %s 2: IndexCode (查询权重表)
                    dt_date,            # %s 3: EndDate (查询权重表) - 字符串格式 'YYYY-MM-DD'
                    dt_date,            # %s 4: EndDate (查询股本结构表) - 字符串格式 'YYYY-MM-DD'
                    dt_date,            # %s 5: TradingDay (查询行情表) - 字符串格式 'YYYY-MM-DD'
                    index_code,         # %s 6: IndexCode (过滤条件)
                )
                
                df_normal = pd.DataFrame()
                try:
                    result_normal = self.db_manager.execute_query(sql_string, params_normal)
                    if result_normal:
                        df_normal = pd.DataFrame(result_normal)
                        self.logger.info(f"{organization} 普通市场查询到 {len(df_normal)} 条记录")
                except Exception as e:
                    self.logger.warning(f"{organization} 普通市场查询失败: {e}")
                    df_normal = pd.DataFrame()
                
                # 执行查询 - 科创板
                params_kc = (
                    eod_flag,           # %s 1: status
                    index_code,         # %s 2: IndexCode (查询权重表)
                    dt_date,            # %s 3: EndDate (查询权重表) - 字符串格式 'YYYY-MM-DD'
                    dt_date,            # %s 4: EndDate (查询股本结构表) - 字符串格式 'YYYY-MM-DD'
                    dt_date,            # %s 5: TradingDay (查询行情表) - 字符串格式 'YYYY-MM-DD'
                    index_code,         # %s 6: IndexCode (过滤条件)
                )
                
                df_kc = pd.DataFrame()
                try:
                    result_kc = self.db_manager.execute_query(sql_string_kc, params_kc)
                    if result_kc:
                        df_kc = pd.DataFrame(result_kc)
                        self.logger.info(f"{organization} 科创板查询到 {len(df_kc)} 条记录")
                except Exception as e:
                    self.logger.warning(f"{organization} 科创板查询失败: {e}")
                    df_kc = pd.DataFrame()
                
                # 合并当前指数的数据
                df_combined = pd.DataFrame()
                if not df_normal.empty or not df_kc.empty:
                    df_combined = pd.concat([df_normal, df_kc], ignore_index=True)
                    
                    # 过滤掉code为None的行
                    df_combined = df_combined[df_combined['code'].notna()]
                    
                    if not df_combined.empty:
                        # 添加必要列
                        df_combined['valuation_date'] = valuation_date_str
                        df_combined['organization'] = organization
                        df_combined['update_time'] = update_time
                        
                        # 重新排列列顺序
                        df_combined = df_combined[['valuation_date', 'organization', 'code', 'weight', 'status', 'update_time']]
                        
                        all_data.append(df_combined)
                        self.logger.info(f"{organization} 成功获取 {len(df_combined)} 条成分股记录")
                    else:
                        self.logger.warning(f"{organization} 合并后数据为空")
                else:
                    self.logger.warning(f"{organization} 日期 {dt_date} 没有查询到数据")
            
            # 合并所有指数的数据
            if all_data:
                df_final = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"总共获取 {len(df_final)} 条成分股记录，涉及 {len(self.dic_index)} 个指数")
                return df_final
            else:
                self.logger.warning(f'日期 {dt_date} 所有指数都没有查询到数据')
                return pd.DataFrame()
        
        except Exception as e:
            self.logger.error(f"查询指数成分股数据时发生错误: {e}")
            raise
        
        # TEST: 临时注释掉自动关闭连接
        # finally:
        #     # 关闭数据库连接
        #     self.db_manager.disconnect()


if __name__ == '__main__':
    # 使用示例
    # 定义指数代码字典
    dic_index = {
        '3145': 'hs300',  # 沪深300
        # '3146': 'zz500',  # 中证500（示例，可继续添加）
        # '3147': 'sz50',   # 上证50（示例，可继续添加）
    }
    
    # 查询并获取DataFrame
    index_component = IndexComponent(dic_index, 'source_db')
    
    # 支持多种日期格式
    # df = index_component.query_index_component(20251030)  # 整数格式
    # df = index_component.query_index_component('20251030')  # 字符串格式 YYYYMMDD
    df = index_component.query_index_component('2025-10-30')  # 字符串格式 YYYY-MM-DD
    df.to_csv('indexcom.csv')
    print(df)

