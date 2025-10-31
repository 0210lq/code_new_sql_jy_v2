import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving,DatabaseSync
from Time_tools.time_tools import time_tools
from Data_checking.data_check import DataCheck
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main

def MarketData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_mkt()
    date = gt.strdate_transfer(date)
    #回滚
    start_date2=date
    for i in range(10):
        start_date2=gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    MktData_update_main(start_date, date,is_sql)

def ScoreData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_score()
    date = gt.strdate_transfer(date)
    score_type = 'fm'
    score_update_main(score_type, date, date,is_sql)
def FactorData_update_main(is_sql=True):
    tt = time_tools()
    date = tt.target_date_decision_factor()
    date = gt.strdate_transfer(date)
    # 回滚
    start_date2 = date
    for i in range(10):
        start_date2 = gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    fu = FactorData_update(start_date, date,is_sql)
    fu.FactorData_update_main()
def L4Data_update_main(is_sql=True):
    L4_update_main(is_sql)
def daily_update_auto():
    fm = File_moving()
    fm.file_moving_update_main()
    MarketData_update_main()
    ScoreData_update_main()
    FactorData_update_main()
    DC=DataCheck()
    DC.DataCheckmain()
def create_tables_from_config():
    """
    根据配置文件创建数据库表
    从 dataUpdate_sql.yaml 读取表配置并创建对应的数据库表
    """
    import yaml
    import os
    import re
    from urllib.parse import unquote
    from setup_logger.logger_setup import setup_logger
    
    logger = setup_logger('TableCreator')
    
    try:
        # 读取配置文件
        # update_main.py 在 Data_update 目录下，配置文件在同一目录的 config_project 子目录中
        config_path = os.path.join(os.path.dirname(__file__), 
                                  'config_project', 'Data_update', 'dataUpdate_sql.yaml')
        logger.info(f"读取配置文件: {config_path}")
        
        # 检查文件是否存在
        if not os.path.exists(config_path):
            logger.error(f"配置文件不存在: {config_path}")
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config:
            logger.error("配置文件为空或格式错误")
            return
        
        logger.info(f"找到 {len(config)} 个表配置")
        
        # SQLAlchemy 类型到 MySQL 类型的映射
        type_mapping = {
            'String': 'VARCHAR',
            'Integer': 'INT',
            'Float': 'DOUBLE',
            'DateTime': 'DATETIME',
        }
        
        # 处理每个表的配置
        created_tables = []
        failed_tables = []
        
        for table_key, table_config in config.items():
            table_name = table_config.get('table_name', '').strip('"')
            db_url = table_config.get('db_url', '').strip()
            schema = table_config.get('schema', {})
            private_keys = table_config.get('private_keys', [])
            
            if not table_name or not db_url:
                logger.warning(f"表 {table_key} 配置不完整，跳过")
                continue
            
            logger.info(f"\n处理表: {table_name} (配置键: {table_key})")
            
            try:
                # 从 db_url 解析数据库连接信息
                # 格式: mysql+pymysql://user:password@host:port/database?params
                # 密码中可能包含 # @ 等特殊字符，需要使用正则表达式解析
                import re
                
                # 移除 mysql+pymysql:// 前缀
                if db_url.startswith('mysql+pymysql://'):
                    url_str = db_url.replace('mysql+pymysql://', '', 1)
                else:
                    url_str = db_url
                
                # 使用正则表达式解析：user:password@host:port/database?params
                # 匹配格式：([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)
                pattern = r'([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)'
                match = re.match(pattern, url_str)
                
                if not match:
                    logger.error(f"无法解析 db_url: {db_url}")
                    failed_tables.append((table_key, "URL解析失败"))
                    continue
                
                user = unquote(match.group(1))
                password = unquote(match.group(2))
                host = match.group(3)
                port = int(match.group(4))
                database_with_params = match.group(5)
                
                # 分离数据库名和参数
                if '?' in database_with_params:
                    database = database_with_params.split('?')[0]
                else:
                    database = database_with_params
                
                if not database or not user or not password:
                    logger.error(f"URL中缺少必要信息: {db_url}")
                    failed_tables.append((table_key, "缺少必要信息"))
                    continue
                
                # 创建数据库连接管理器（临时配置）
                # 注意：这里需要为每个表创建独立的连接，因为不同表可能在不同数据库
                temp_config = {
                    'host': host,
                    'port': port,
                    'user': user,
                    'password': password,
                    'database': database,
                    'charset': 'utf8mb4',
                }
                
                # 创建临时 MySQLManager（我们需要创建一个临时的配置）
                # 由于 MySQLManager 从配置文件读取，我们需要直接连接
                import pymysql
                
                conn = pymysql.connect(**temp_config)
                cursor = conn.cursor()
                
                # 检查表是否存在
                check_table_sql = f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
                """
                cursor.execute(check_table_sql, (database, table_name))
                table_exists = cursor.fetchone()[0] > 0
                
                if table_exists:
                    logger.info(f"表 {table_name} 已存在，跳过创建")
                    cursor.close()
                    conn.close()
                    continue
                
                # 生成 CREATE TABLE SQL
                columns_sql = []
                
                for col_name, col_config in schema.items():
                    col_type = col_config.get('type', 'String')
                    col_length = col_config.get('length')
                    
                    # 映射类型
                    mysql_type = type_mapping.get(col_type, 'VARCHAR')
                    
                    # 构建列定义
                    if mysql_type == 'VARCHAR':
                        length = col_length if col_length else 255
                        col_def = f"`{col_name}` {mysql_type}({length})"
                    elif mysql_type == 'DATETIME':
                        col_def = f"`{col_name}` {mysql_type}"
                    else:
                        col_def = f"`{col_name}` {mysql_type}"
                    
                    columns_sql.append(col_def)
                
                # 生成主键约束
                if private_keys:
                    pk_cols = ', '.join([f"`{pk}`" for pk in private_keys])
                    columns_sql.append(f"PRIMARY KEY ({pk_cols})")
                
                # 完整的 CREATE TABLE 语句
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {', '.join(columns_sql)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                
                # 执行创建表
                logger.info(f"正在创建表: {table_name}")
                logger.debug(f"SQL: {create_table_sql}")
                cursor.execute(create_table_sql)
                conn.commit()
                
                logger.info(f"✓ 成功创建表: {table_name}")
                created_tables.append(table_name)
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                logger.error(f"创建表 {table_name} 失败: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                failed_tables.append((table_key, str(e)))
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
        
        # 总结
        logger.info("\n" + "=" * 50)
        logger.info(f"表创建完成统计:")
        logger.info(f"  成功创建: {len(created_tables)} 个表")
        logger.info(f"  失败: {len(failed_tables)} 个表")
        if created_tables:
            logger.info(f"  成功创建的表: {', '.join(created_tables)}")
        if failed_tables:
            logger.warning(f"  失败的表:")
            for table_key, error in failed_tables:
                logger.warning(f"    - {table_key}: {error}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"创建数据库表时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise

def chushihua():#数据库初始化
    import sys
    import traceback
    try:
        print("=" * 50)
        print("开始数据库初始化...")
        print("=" * 50)

        # 根据配置文件创建数据库表
        print("\n正在根据配置文件创建数据库表...")
        create_tables_from_config()
        print("\n数据库表创建完成")

        # 可以选择运行数据导入或数据库同步
        # DOS = DataOther_sql()
        # DOS.Dataother_main()

        # 运行数据库同步
        print("\n正在创建 DatabaseSync 实例...")
        sync = DatabaseSync()
        print("DatabaseSync 实例创建成功")

        print("\n正在执行数据库同步...")
        results = sync.sync_main()
        print(f"\n数据库同步完成，结果: {results}")
        print("=" * 50)

    except KeyboardInterrupt:
        print("\n\n用户中断操作")
        sys.exit(0)
    except Exception as e:
        print("\n" + "=" * 50)
        print(f"发生错误: {str(e)}")
        print("=" * 50)
        print("详细错误信息:")
        traceback.print_exc()
        print("=" * 50)
        sys.exit(1)
if __name__ == '__main__':
    # fm = File_moving()
    # fm.file_moving_update_main()
    # chushihua()
    create_tables_from_config()
    # DC = DataCheck()
    # DC.DataCheckmain()
    pass
