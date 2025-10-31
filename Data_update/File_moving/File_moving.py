import sys
import os

# 添加路径
try:
    path = os.getenv('GLOBAL_TOOLSFUNC_new')
    if path:
        sys.path.append(path)
    import global_tools as gt
    import global_setting.global_dic as glv
except Exception as e:
    print(f"导入基础模块失败: {e}")
    raise

# 添加数据库管理器导入
try:
    from global_setting.db_manager import DBSyncManager
    from setup_logger.logger_setup import setup_logger
except ImportError as e:
    print(f"导入数据库管理器失败: {e}")
    print("请确保已安装 PyMySQL 或 mysql-connector-python:")
    print("  pip install pymysql")
    print("  或")
    print("  pip install mysql-connector-python")
    raise
except Exception as e:
    print(f"导入数据库管理器时发生错误: {e}")
    import traceback
    traceback.print_exc()
    raise

# 数据库同步类
class DatabaseSync:
    def __init__(self, source_config='source_db', target_config='target_db'):
        """
        初始化数据库同步类
        
        参数:
            source_config: 源数据库配置名称，默认为'source_db'
            target_config: 目标数据库配置名称，默认为'target_db'
        """
        self.source_config = source_config
        self.target_config = target_config
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
            print(f"sync_multiple_tables 开始，表数量: {len(tables)}")
            self.logger.info(f"开始同步多个表: {', '.join(tables)}")
            print("正在调用 sync_manager.sync_multiple_tables...")
            results = self.sync_manager.sync_multiple_tables(tables, truncate_first)
            print("sync_manager.sync_multiple_tables 调用完成")
            
            # 记录结果
            for table_name, result in results.items():
                if result['status'] == 'success':
                    self.logger.info(f"表 {table_name} 同步成功，共同步 {result['count']} 条记录")
                    print(f"表 {table_name} 同步成功，共同步 {result['count']} 条记录")
                else:
                    self.logger.error(f"表 {table_name} 同步失败: {result['message']}")
                    print(f"表 {table_name} 同步失败: {result['message']}")
            
            return results
        except Exception as e:
            print(f"sync_multiple_tables 中发生错误: {e}")
            import traceback
            traceback.print_exc()
            self.logger.error(f"同步多个表失败: {str(e)}")
            raise
    
    def sync_main(self):
        """
        主同步方法，可以在这里配置需要同步的表
        """
        try:
            print("正在配置需要同步的表...")
            # 示例：同步多个表
            tables_to_sync = {
                'chinesevaluationdate': {},
                'st_stock': {},
                'stockuniverse': {},
                'specialday': {}
            }
            print(f"配置了 {len(tables_to_sync)} 个表需要同步")
            print("开始调用 sync_multiple_tables...")
            results = self.sync_multiple_tables(tables_to_sync)
            print("sync_multiple_tables 调用完成")
            return results
        except Exception as e:
            print(f"sync_main 中发生错误: {e}")
            import traceback
            traceback.print_exc()
            raise

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
    def __init__(self, source_config='source_db', target_config='target_db'):
        """
        初始化数据库同步类
        
        参数:
            source_config: 源数据库配置名称，默认为'source_db'
            target_config: 目标数据库配置名称，默认为'target_db'
        """
        self.sync_manager = DBSyncManager(source_config, target_config)
        self.logger = setup_logger('DataOther_sql')
    
    def valuationData_sql(self, truncate_first=False):
        """从源数据库同步 ChineseValuationDate 表到目标数据库"""
        try:
            self.logger.info("开始同步 ChineseValuationDate 表")
            count = self.sync_manager.sync_table('ChineseValuationDate', truncate_first=truncate_first)
            self.logger.info(f"ChineseValuationDate 表同步完成，共同步 {count} 条记录")
            return count
        except Exception as e:
            self.logger.error(f"同步 ChineseValuationDate 表失败: {str(e)}")
            raise
    
    def st_stock_sql(self, truncate_first=False):
        """从源数据库同步 STstock 表到目标数据库"""
        try:
            self.logger.info("开始同步 STstock 表")
            count = self.sync_manager.sync_table('STstock', truncate_first=truncate_first)
            self.logger.info(f"STstock 表同步完成，共同步 {count} 条记录")
            return count
        except Exception as e:
            self.logger.error(f"同步 STstock 表失败: {str(e)}")
            raise
    
    def stockuni_sql(self, truncate_first=False):
        """从源数据库同步 Stock_uni 表到目标数据库"""
        try:
            self.logger.info("开始同步 Stock_uni 表")
            count = self.sync_manager.sync_table('Stock_uni', truncate_first=truncate_first)
            self.logger.info(f"Stock_uni 表同步完成，共同步 {count} 条记录")
            return count
        except Exception as e:
            self.logger.error(f"同步 Stock_uni 表失败: {str(e)}")
            raise
    
    def specialdate_sql(self, truncate_first=False):
        """从源数据库同步 SpecialDay 表到目标数据库"""
        try:
            self.logger.info("开始同步 SpecialDay 表")
            count = self.sync_manager.sync_table('SpecialDay', truncate_first=truncate_first)
            self.logger.info(f"SpecialDay 表同步完成，共同步 {count} 条记录")
            return count
        except Exception as e:
            self.logger.error(f"同步 SpecialDay 表失败: {str(e)}")
            raise
    
    def Dataother_main(self, truncate_first=False):
        """
        主同步方法，同步所有表
        
        参数:
            truncate_first: 是否先清空目标表，默认为False
        """
        try:
            self.logger.info("开始执行数据同步任务")
            self.valuationData_sql(truncate_first=truncate_first)
            self.stockuni_sql(truncate_first=truncate_first)
            self.specialdate_sql(truncate_first=truncate_first)
            self.st_stock_sql(truncate_first=truncate_first)
            self.logger.info("所有表同步完成")
        except Exception as e:
            self.logger.error(f"数据同步任务执行失败: {str(e)}")
            raise

# 创建__init__.py文件以便模块导入
# d:\code_new_SQL_jy_new\Data_update\global_setting\__init__.py

if __name__ == '__main__':
    import sys
    import traceback
    
    try:
        print("=" * 50)
        print("开始初始化数据库同步...")
        print("=" * 50)
        
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



