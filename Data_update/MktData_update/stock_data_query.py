"""
股票数据查询通用类
从聚源数据库（jydb）查询股票行情数据
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


class StockData:
    """股票数据查询通用类"""
    
    def __init__(self, db_config='source_db'):
        """
        初始化股票数据查询类
        
        参数:
            db_config: 数据库配置名称，默认为'source_db'
        """
        self.db_manager = MySQLManager(db_config)
        self.logger = setup_logger('StockData_Query')
    
    def query_stock_data(self, date):
        """
        查询股票行情数据并返回DataFrame（按照MATLAB mktData逻辑）
        
        参数:
            date: 日期，可以是以下格式之一：
                  - 整数格式 YYYYMMDD，如 20251030
                  - 字符串格式 'YYYY-MM-DD'，如 '2025-10-30'
                  - 字符串格式 'YYYYMMDD'，如 '20251030'
        
        返回:
            DataFrame，包含所有股票的行情数据
            列：qtid, prevClose, open, hi, lo, close, volume, value, ret, 
                vwap, adjFactor, tradeStatus
        """
        try:
            # 连接数据库
            self.db_manager.connect()
            
            # 统一转换日期格式
            if isinstance(date, int):
                # 整数格式 YYYYMMDD
                dt_str = str(date)
                dt_date = datetime.strptime(dt_str, '%Y%m%d').strftime('%Y-%m-%d')
                dtstr_yyyy_mm_dd = dt_str  # 保持 YYYYMMDD 格式用于某些查询
            elif isinstance(date, str):
                if len(date) == 8:
                    # 字符串格式 YYYYMMDD
                    dt_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
                    dtstr_yyyy_mm_dd = date
                elif len(date) == 10:
                    # 字符串格式 YYYY-MM-DD
                    dt_date = date
                    dtstr_yyyy_mm_dd = date.replace('-', '')
                else:
                    raise ValueError(f"日期格式不正确: {date}，应为 YYYYMMDD 或 YYYY-MM-DD")
            else:
                raise ValueError(f"日期类型不正确: {type(date)}，应为 int 或 str")
            
            self.logger.info(f'开始查询日期 {dt_date} 的股票数据')
            
            # 存储所有数据
            all_data = []
            
            # 1. 查询普通市场数据（沪深主板和创业板）
            sql_normal = """
            SELECT 
                CASE 
                    WHEN t3.SecuMarket = 83 THEN CONCAT(t3.SecuCode, '.SH') 
                    WHEN t3.SecuMarket = 90 THEN CONCAT(t3.SecuCode, '.SZ') 
                    WHEN t3.SecuMarket = 18 THEN CONCAT(t3.SecuCode, '.BJ') 
                    ELSE NULL 
                END AS qtid,
                t1.PrevClosePrice AS prevClose,
                t1.OpenPrice AS open,
                t1.HighPrice AS hi,
                t1.LowPrice AS lo,
                t1.ClosePrice AS close,
                t1.TurnoverVolume * 10000 AS volume,
                t1.TurnoverValue * 10000 AS value,
                CAST(t1.ClosePrice AS DECIMAL(10,6)) / t1.PrevClosePrice - 1 AS ret,
                CASE WHEN t1.TurnoverVolume > 0 THEN t1.TurnoverValue / t1.TurnoverVolume ELSE NULL END AS vwap,
                IFNULL(t2.AdjustingFactor, 1) AS adjFactor,
                CASE WHEN t1.TurnoverVolume > 0 THEN 1 ELSE 0 END AS tradeStatus
            FROM jydb.qt_performance t1
            LEFT JOIN jydb.QT_AdjustingFactor t2
                ON t2.InnerCode = t1.InnerCode
                AND t2.ExDiviDate = (
                    SELECT ExDiviDate 
                    FROM jydb.QT_AdjustingFactor 
                    WHERE InnerCode = t1.InnerCode 
                        AND ExDiviDate <= t1.TradingDay 
                    ORDER BY ExDiviDate DESC 
                    LIMIT 1
                )
            JOIN jydb.SecuMain t3
                ON t3.InnerCode = t1.InnerCode
            WHERE t1.TradingDay = %s
                AND t3.SecuCategory = 1
                AND t3.SecuMarket IN (18, 83, 90)
                AND t3.ListedSector IN (1, 2, 6, 8)
                AND t3.ListedState = 1
            ORDER BY t3.SecuCode
            """
            
            params_normal = (dt_date,)
            
            df_normal = pd.DataFrame()
            try:
                result_normal = self.db_manager.execute_query(sql_normal, params_normal)
                if result_normal:
                    df_normal = pd.DataFrame(result_normal)
                    self.logger.info(f"普通市场查询到 {len(df_normal)} 条记录")
            except Exception as e:
                self.logger.warning(f"普通市场查询失败: {e}")
                df_normal = pd.DataFrame()
            
            if not df_normal.empty:
                all_data.append(df_normal)
            
            # 2. 查询科创板行情数据
            sql_stib = """
            SELECT 
                CASE WHEN t3.SecuMarket = 83 THEN CONCAT(t3.SecuCode, '.SH') END AS qtid,
                t1.PrevClosePrice AS prevClose,
                t1.OpenPrice AS open,
                t1.HighPrice AS hi,
                t1.LowPrice AS lo,
                t1.ClosePrice AS close,
                t1.TurnoverVolume AS volume,
                t1.TurnoverValue AS value,
                CAST(t1.ClosePrice AS DECIMAL(10,6)) / t1.PrevClosePrice - 1 AS ret,
                CASE WHEN t1.TurnoverVolume > 0 THEN t1.TurnoverValue / t1.TurnoverVolume ELSE NULL END AS vwap,
                IFNULL(t2.AdjustingFactor, 1) AS adjFactor,
                CASE WHEN t1.TurnoverVolume > 0 THEN 1 ELSE 0 END AS tradeStatus
            FROM jydb.LC_STIBDailyQuote t1
            LEFT JOIN jydb.LC_STIBAdjustingFactor t2
                ON t2.InnerCode = t1.InnerCode
                AND t2.ExDiviDate = (
                    SELECT ExDiviDate 
                    FROM jydb.QT_AdjustingFactor 
                    WHERE InnerCode = t1.InnerCode 
                        AND ExDiviDate <= t1.TradingDay 
                    ORDER BY ExDiviDate DESC 
                    LIMIT 1
                )
            JOIN jydb.SecuMain t3
                ON t3.InnerCode = t1.InnerCode
            WHERE t1.TradingDay = %s
                AND t3.SecuCategory = 1
                AND t3.SecuMarket = 83
                AND t3.ListedSector = 7
                AND t3.ListedState = 1
            ORDER BY t3.SecuCode
            """
            
            params_stib = (dt_date,)
            
            df_stib = pd.DataFrame()
            try:
                result_stib = self.db_manager.execute_query(sql_stib, params_stib)
                if result_stib:
                    df_stib = pd.DataFrame(result_stib)
                    self.logger.info(f"科创板查询到 {len(df_stib)} 条记录")
            except Exception as e:
                self.logger.warning(f"科创板查询失败: {e}")
                df_stib = pd.DataFrame()
            
            if not df_stib.empty:
                all_data.append(df_stib)
            
            # 合并所有数据
            if all_data:
                df_final = pd.concat(all_data, ignore_index=True)
                
                # 重新排列列顺序，确保顺序一致
                columns_order = ['qtid', 'prevClose', 'open', 'hi', 'lo', 'close', 
                               'volume', 'value', 'ret', 'vwap', 
                               'adjFactor', 'tradeStatus']
                
                # 只保留存在的列
                existing_columns = [col for col in columns_order if col in df_final.columns]
                df_final = df_final[existing_columns]
                
                # 按qtid排序
                if 'qtid' in df_final.columns:
                    df_final = df_final.sort_values('qtid').reset_index(drop=True)
                
                self.logger.info(f"总共获取 {len(df_final)} 条股票数据记录")
                return df_final
            else:
                self.logger.warning(f'日期 {dt_date} 没有查询到数据')
                return pd.DataFrame()
        
        except Exception as e:
            self.logger.error(f"查询股票数据时发生错误: {e}")
            raise
        
        # TEST: 临时注释掉自动关闭连接
        # finally:
        #     # 关闭数据库连接
        #     self.db_manager.disconnect()


if __name__ == '__main__':
    # 使用示例
    # 创建股票数据查询对象
    stock_data = StockData('source_db')
    
    # 支持多种日期格式
    # df = stock_data.query_stock_data(20251030)  # 整数格式
    # df = stock_data.query_stock_data('20251030')  # 字符串格式 YYYYMMDD
    df = stock_data.query_stock_data('2025-10-30')  # 字符串格式 YYYY-MM-DD
    
    # 保存结果
    df.to_csv('stock_data.csv', index=False, encoding='utf-8-sig')
    print(df)
    print(f"\n查询完成，共 {len(df)} 条记录")

