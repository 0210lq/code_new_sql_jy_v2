try:
    import pymysql
    # 将 pymysql 的异常映射为 mysql.connector 兼容的异常类
    class MySQLConnectorError(Exception):
        pass
    pymysql.Error = MySQLConnectorError
    USE_PYMYSQL = True
except ImportError:
    try:
        import mysql.connector
        USE_PYMYSQL = False
    except ImportError:
        raise ImportError("请安装 mysql-connector-python 或 pymysql: pip install mysql-connector-python 或 pip install pymysql")

import pandas as pd
import numpy as np
from .mysql_config import get_db_config

class MySQLManager:
    """MySQL数据库连接管理类"""
    
    def __init__(self, config_name='default'):
        """
        初始化数据库连接管理器
        
        参数:
            config_name: 配置名称，默认为'default'
        """
        self.config_name = config_name
        self.config = get_db_config(config_name)
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            # 如果已经有连接且处于连接状态，直接返回
            if self.connection:
                try:
                    if USE_PYMYSQL:
                        # PyMySQL 检查连接状态
                        if self.connection.open:
                            return
                    else:
                        # mysql.connector 检查连接状态
                        if self.connection.is_connected():
                            return
                except:
                    # 连接状态检查失败，需要重新连接
                    pass
            
            # 关闭旧连接（如果存在）
            if self.connection or self.cursor:
                try:
                    self.disconnect()
                except:
                    pass
            
            # 建立新连接
            print(f"[connect] 正在连接数据库 {self.config['host']}:{self.config['port']}...")
            try:
                if USE_PYMYSQL:
                    # 使用 PyMySQL 连接
                    # 移除或转换 PyMySQL 不支持的参数
                    pymysql_config = self.config.copy()
                    # PyMySQL 不支持 use_unicode 参数（默认就是 unicode）
                    if 'use_unicode' in pymysql_config:
                        del pymysql_config['use_unicode']
                    # PyMySQL 使用 connect_timeout 而不是 connection_timeout
                    if 'connection_timeout' in pymysql_config:
                        pymysql_config['connect_timeout'] = pymysql_config.pop('connection_timeout')
                    # PyMySQL 不支持 raise_on_warnings
                    if 'raise_on_warnings' in pymysql_config:
                        del pymysql_config['raise_on_warnings']
                    
                    self.connection = pymysql.connect(**pymysql_config)
                    print(f"[connect] PyMySQL 连接对象创建成功")
                    # PyMySQL 使用 pymysql.cursors.DictCursor 来获取字典格式结果
                    from pymysql.cursors import DictCursor
                    self.cursor = self.connection.cursor(DictCursor)
                    print(f"[connect] PyMySQL 游标创建成功")
                else:
                    # 使用 mysql.connector 连接
                    self.connection = mysql.connector.connect(**self.config)
                    print(f"[connect] 连接对象创建成功")
                    self.cursor = self.connection.cursor(dictionary=True, buffered=True)
                    print(f"[connect] 游标创建成功")
                
                print(f"成功连接到数据库: {self.config['host']}:{self.config['port']}/{self.config['database']}")
            except Exception as conn_err:
                print(f"[connect] 连接过程中发生错误: {conn_err}")
                import traceback
                traceback.print_exc()
                raise
        except Exception as err:
            print(f"数据库连接错误: {err}")
            self.connection = None
            self.cursor = None
            raise
    
    def disconnect(self):
        """关闭数据库连接"""
        try:
            if self.cursor:
                try:
                    self.cursor.close()
                except Exception as e:
                    print(f"关闭游标时出错: {e}")
                finally:
                    self.cursor = None
            
            if self.connection:
                try:
                    if USE_PYMYSQL:
                        # PyMySQL 检查连接状态
                        if hasattr(self.connection, 'open') and self.connection.open:
                            self.connection.close()
                            print("数据库连接已关闭")
                    else:
                        # mysql.connector 检查连接状态
                        if self.connection.is_connected():
                            self.connection.close()
                            print("数据库连接已关闭")
                except Exception as e:
                    print(f"关闭连接时出错: {e}")
                finally:
                    self.connection = None
        except Exception as e:
            print(f"断开数据库连接时发生未知错误: {e}")
            self.cursor = None
            self.connection = None
    
    def execute_query(self, query, params=None):
        """
        执行SQL查询
        
        参数:
            query: SQL查询语句
            params: 查询参数，默认为None
        
        返回:
            查询结果（如果有）
        """
        # 检查连接状态
        need_reconnect = False
        if not self.connection:
            need_reconnect = True
        else:
            try:
                if USE_PYMYSQL:
                    if not (hasattr(self.connection, 'open') and self.connection.open):
                        need_reconnect = True
                else:
                    if not self.connection.is_connected():
                        need_reconnect = True
            except:
                need_reconnect = True
        
        if need_reconnect:
            self.connect()
        
        try:
            self.cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as err:
            print(f"查询执行错误: {err}")
            self.connection.rollback()
            raise
    
    def get_table_data(self, table_name, where_clause=None, params=None):
        """
        获取表数据
        
        参数:
            table_name: 表名
            where_clause: WHERE子句，默认为None
            params: 查询参数，默认为None
        
        返回:
            表数据DataFrame
        """
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        data = self.execute_query(query, params)
        return pd.DataFrame(data)
    
    def table_exists(self, table_name):
        """
        检查表是否存在
        
        参数:
            table_name: 表名
        
        返回:
            表是否存在
        """
        query = """
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
        """
        params = (self.config['database'], table_name)
        result = self.execute_query(query, params)
        if result and len(result) > 0:
            if USE_PYMYSQL:
                return result[0].get('count', 0) > 0
            else:
                return result[0].get('COUNT(*)', 0) > 0
        return False
    
    def get_table_structure(self, table_name):
        """
        获取表结构
        
        参数:
            table_name: 表名
        
        返回:
            表结构描述字典列表
        """
        query = """
        SELECT column_name, data_type, column_type, 
               is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        params = (self.config['database'], table_name)
        return self.execute_query(query, params)

class DBSyncManager:
    """数据库同步管理器"""
    
    def __init__(self, source_config='source_db', target_config='target_db'):
        """
        初始化数据库同步管理器
        
        参数:
            source_config: 源数据库配置名称
            target_config: 目标数据库配置名称
        """
        self.source_db = MySQLManager(source_config)
        self.target_db = MySQLManager(target_config)
    
    def sync_table(self, table_name, where_clause=None, params=None, truncate_first=False):
        """
        同步表数据
        
        参数:
            table_name: 表名
            where_clause: 筛选条件，默认为None（同步全部数据）
            params: 查询参数，默认为None
            truncate_first: 是否先清空目标表，默认为False
        
        返回:
            同步的记录数
        """
        try:
            print(f"[sync_table] 开始同步表: {table_name}")
            # 连接数据库
            print(f"[sync_table] 正在连接源数据库...")
            self.source_db.connect()
            print(f"[sync_table] 源数据库连接成功")
            print(f"[sync_table] 正在连接目标数据库...")
            self.target_db.connect()
            print(f"[sync_table] 目标数据库连接成功")
            
            # 检查源表是否存在
            if not self.source_db.table_exists(table_name):
                raise ValueError(f"源数据库中表 '{table_name}' 不存在")
            
            # 如果目标表不存在，创建表
            if not self.target_db.table_exists(table_name):
                self._create_table_from_source(table_name)
            
            # 如果需要先清空目标表
            if truncate_first:
                self.target_db.execute_query(f"TRUNCATE TABLE {table_name}")
                print(f"已清空目标表 '{table_name}'")
            
            # 获取源表数据
            print(f"正在从源表 '{table_name}' 读取数据...")
            df = self.source_db.get_table_data(table_name, where_clause, params)
            
            if df.empty:
                print(f"源表 '{table_name}' 没有数据需要同步")
                return 0
            
            # 插入数据到目标表
            print(f"正在将 {len(df)} 条记录同步到目标表 '{table_name}'...")
            
            # 生成插入语句，使用反引号包裹列名和表名（防止关键字冲突）
            columns = ', '.join([f"`{col}`" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            
            # 批量插入数据，将 NaN 转换为 None（MySQL 的 NULL）
            # 替换 DataFrame 中的 NaN 为 None
            df_cleaned = df.where(pd.notnull(df), None)
            # 转换为元组列表，确保 NaN 被转换为 None
            data = []
            for row in df_cleaned.itertuples(index=False, name=None):
                row_data = []
                for val in row:
                    if pd.isna(val) or (isinstance(val, float) and np.isnan(val)):
                        row_data.append(None)
                    else:
                        row_data.append(val)
                data.append(tuple(row_data))
            
            self.target_db.cursor.executemany(insert_query, data)
            self.target_db.connection.commit()
            
            print(f"成功同步 {len(df)} 条记录到目标表 '{table_name}'")
            return len(df)
        
        except Exception as e:
            print(f"同步表 '{table_name}' 时出错: {e}")
            if self.target_db.connection:
                self.target_db.connection.rollback()
            raise
        
        finally:
            # 安全关闭连接
            try:
                if hasattr(self, 'source_db'):
                    self.source_db.disconnect()
            except Exception as e:
                print(f"关闭源数据库连接时出错: {e}")
            
            try:
                if hasattr(self, 'target_db'):
                    self.target_db.disconnect()
            except Exception as e:
                print(f"关闭目标数据库连接时出错: {e}")
    
    def _create_table_from_source(self, table_name):
        """
        从源表创建目标表
        
        参数:
            table_name: 表名
        """
        # 获取源表结构
        structure = self.source_db.get_table_structure(table_name)
        
        if not structure:
            raise ValueError(f"无法获取表 '{table_name}' 的结构")
        
        # 生成CREATE TABLE语句
        col_defs = []
        for col in structure:
            col_name = col.get('column_name', col.get('COLUMN_NAME', ''))
            col_type = col.get('column_type', col.get('COLUMN_TYPE', ''))
            
            if not col_name or not col_type:
                continue
            
            col_def = f"`{col_name}` {col_type}"
            
            # 处理是否允许 NULL
            is_nullable = col.get('is_nullable', col.get('IS_NULLABLE', 'YES'))
            if is_nullable == 'NO':
                col_def += ' NOT NULL'
            
            # 处理默认值
            col_default = col.get('column_default', col.get('COLUMN_DEFAULT'))
            if col_default is not None and col_default != 'None':
                # 处理字符串默认值
                if isinstance(col_default, str) and not col_default.upper().startswith('CURRENT_'):
                    col_def += f" DEFAULT '{col_default}'"
                else:
                    col_def += f" DEFAULT {col_default}"
            
            col_defs.append(col_def)
        
        if not col_defs:
            raise ValueError(f"表 '{table_name}' 没有有效的列定义")
        
        # 构建 CREATE TABLE 语句
        create_sql = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(col_defs) + "\n)"
        
        # 执行CREATE TABLE语句
        print(f"正在创建表 '{table_name}'，SQL: {create_sql}")
        self.target_db.execute_query(create_sql)
        print(f"已在目标数据库创建表 '{table_name}'")
    
    def sync_multiple_tables(self, tables, truncate_first=False):
        """
        同步多个表
        
        参数:
            tables: 表名列表或字典 {表名: {where_clause, params}}
            truncate_first: 是否先清空目标表，默认为False
        
        返回:
            同步结果字典
        """
        print(f"[sync_multiple_tables] 开始同步多个表，数量: {len(tables)}")
        results = {}
        
        if isinstance(tables, list):
            tables = {table: {} for table in tables}
        
        for idx, (table_name, options) in enumerate(tables.items(), 1):
            print(f"[sync_multiple_tables] 处理第 {idx}/{len(tables)} 个表: {table_name}")
            where_clause = options.get('where_clause')
            params = options.get('params')
            try:
                count = self.sync_table(table_name, where_clause, params, truncate_first)
                results[table_name] = {'status': 'success', 'count': count}
                print(f"[sync_multiple_tables] 表 {table_name} 同步成功，记录数: {count}")
            except Exception as e:
                print(f"[sync_multiple_tables] 表 {table_name} 同步失败: {e}")
                import traceback
                traceback.print_exc()
                results[table_name] = {'status': 'error', 'message': str(e)}
        
        print(f"[sync_multiple_tables] 所有表处理完成")
        return results