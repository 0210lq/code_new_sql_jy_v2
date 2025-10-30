import mysql.connector
import pandas as pd
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
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"成功连接到数据库: {self.config['host']}:{self.config['port']}/{self.config['database']}")
        except mysql.connector.Error as err:
            print(f"数据库连接错误: {err}")
            raise
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("数据库连接已关闭")
    
    def execute_query(self, query, params=None):
        """
        执行SQL查询
        
        参数:
            query: SQL查询语句
            params: 查询参数，默认为None
        
        返回:
            查询结果（如果有）
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        try:
            self.cursor.execute(query, params or ())
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except mysql.connector.Error as err:
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
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
        """
        params = (self.config['database'], table_name)
        result = self.execute_query(query, params)
        return result[0]['COUNT(*)'] > 0
    
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
            # 连接数据库
            self.source_db.connect()
            self.target_db.connect()
            
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
            
            # 生成插入语句
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # 批量插入数据
            data = [tuple(row) for row in df.itertuples(index=False, name=None)]
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
            # 关闭连接
            self.source_db.disconnect()
            self.target_db.disconnect()
    
    def _create_table_from_source(self, table_name):
        """
        从源表创建目标表
        
        参数:
            table_name: 表名
        """
        # 获取源表结构
        structure = self.source_db.get_table_structure(table_name)
        
        # 生成CREATE TABLE语句
        create_sql = f"CREATE TABLE {table_name} ("
        
        for col in structure:
            col_def = f"{col['column_name']} {col['column_type']}"
            if col['is_nullable'] == 'NO':
                col_def += ' NOT NULL'
            if col['column_default'] is not None:
                col_def += f" DEFAULT {col['column_default']}"
            create_sql += col_def + ', '
        
        # 移除最后一个逗号和空格
        create_sql = create_sql[:-2] + ')'
        
        # 执行CREATE TABLE语句
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
        results = {}
        
        if isinstance(tables, list):
            tables = {table: {} for table in tables}
        
        for table_name, options in tables.items():
            where_clause = options.get('where_clause')
            params = options.get('params')
            try:
                count = self.sync_table(table_name, where_clause, params, truncate_first)
                results[table_name] = {'status': 'success', 'count': count}
            except Exception as e:
                results[table_name] = {'status': 'error', 'message': str(e)}
        
        return results