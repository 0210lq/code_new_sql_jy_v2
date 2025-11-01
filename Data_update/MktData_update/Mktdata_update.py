import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from datetime import datetime
from tools_func.tools_func import *
from MktData_update.Mktdata_preparing import (indexdata_prepare, stockData_preparing,
                                              indexComponent_prepare)
from MktData_update.index_component_query import IndexComponent
from MktData_update.stock_data_query import StockData
import re
from setup_logger.logger_setup import setup_logger
import io
import contextlib
import yaml
from urllib.parse import urlparse, unquote

def check_table_exists(db_url, table_name):
    """
    检查表是否存在
    
    参数:
        db_url: 数据库连接URL
        table_name: 表名
    
    返回:
        bool: 表是否存在
    """
    try:
        # 解析数据库URL（处理密码中包含特殊字符的情况）
        # URL格式：mysql+pymysql://user:password@host:port/database?params
        # 密码中可能包含 # @ 等特殊字符，需要使用正则表达式或手动解析
        
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
            print(f"无法解析数据库URL: {db_url}")
            return False
        
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
            return False
        
        # 打印连接信息用于调试
        print(f"[检查表存在性] 数据库连接信息:")
        print(f"  Host: {host}")
        print(f"  Port: {port}")
        print(f"  User: {user}")
        print(f"  Database: {database}")
        print(f"  Table: {table_name}")
        
        # 直接使用PyMySQL检查表
        import pymysql
        conn = pymysql.connect(host=host, port=port, user=user, password=password, 
                             database=database, charset='utf8mb4')
        cursor = conn.cursor()
        
        # 首先列出数据库中所有的表
        list_tables_sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
        """
        cursor.execute(list_tables_sql, (database,))
        all_tables = [row[0] for row in cursor.fetchall()]
        print(f"[检查表存在性] 数据库 {database} 中的所有表: {all_tables}")
        
        # 检查表是否存在（不区分大小写）
        check_table_sql = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)
        """
        cursor.execute(check_table_sql, (database, table_name))
        table_exists = cursor.fetchone()[0] > 0
        
        # 如果使用小写检查不存在，尝试查找实际表名（区分大小写）
        if not table_exists:
            actual_table_sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)
            """
            cursor.execute(actual_table_sql, (database, table_name))
            actual_table = cursor.fetchone()
            if actual_table:
                actual_table_name = actual_table[0]
                print(f"[检查表存在性] 找到实际表名: {actual_table_name} (原查询: {table_name})")
                table_exists = True
        
        print(f"[检查表存在性] 表 {table_name} 存在性: {table_exists}")
        
        cursor.close()
        conn.close()
        
        return table_exists
    except Exception as e:
        import traceback
        print(f"检查表存在性时出错: {e}")
        print(traceback.format_exc())
        return False

def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Mktdata_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class indexData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date = start_date
        self.end_date = end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\n' + '*'*50 + '\nMARKET DATA UPDATE PROCESSING\n' + '*'*50)
    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_data')
        return df_config
    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'prevClose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            'hi' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            'lo' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'value' : 'amt',
            'AMT' : 'amt',
            'return' : 'pct_chg',
            'PCT_CHG' : 'pct_chg',
            'ret' : 'return',
            'turn' :'turn_over',
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code','open','high','low','close','pre_close','pct_chg','volume','amt','turn_over']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df
    def index_code_rename(self,df):
        # Create a mapping dictionary for the specific codes
        code_mapping = {
            '932000': '932000.CSI',
            '000510': '000510.CSI',
            '999004': '999004.SSI'
        }
        # Function to check and replace codes
        def replace_code(code):
            for old_code, new_code in code_mapping.items():
                if old_code in code:
                    return new_code
            return code
        # Apply the replacement function to the code column
        df['code'] = df['code'].apply(replace_code)
        return df
    def index_data_update_main(self):
        self.logger.info('\nProcessing index data update...')
        df_config = self.source_priority_withdraw()
        outputpath_index_return_base = glv.get('output_indexdata')
        gt.folder_creator2(outputpath_index_return_base)
        input_list=os.listdir(outputpath_index_return_base)
        if len(input_list)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'indexData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            ir=indexdata_prepare(available_date)
            outputpath_index_return = os.path.join(outputpath_index_return_base,
                                                   'indexdata_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_indexdata = pd.DataFrame()
            df_index_wind = ir.raw_wind_index_data_withdraw()
            df_index_tushare = ir.raw_tushare_index_data_withdraw()
            df_index_jy = ir.raw_jy_index_data_withdraw()
            try:
                df_index_wind = self.standardize_column_names(df_index_wind)
                df_index_wind=self.index_code_rename(df_index_wind)
            except:
                df_index_wind = pd.DataFrame()
            try:
                df_index_tushare = self.standardize_column_names(df_index_tushare)
                df_index_tushare=self.index_code_rename(df_index_tushare)
            except:
                df_index_tushare = pd.DataFrame()
            try:
                df_index_jy = self.standardize_column_names(df_index_jy)
                df_index_jy=self.index_code_rename(df_index_jy)
                df_index_jy.drop_duplicates('code',keep='first',inplace=True)
            except:
                df_index_jy = pd.DataFrame()
            if len(df_index_wind)>0 and len(df_index_tushare)==0 and len(df_index_jy)==0:
                df_indexdata=df_index_wind
                self.logger.info('indexdata使用的数据源是: wind')
            elif len(df_index_wind)==0 and len(df_index_tushare)>0 and len(df_index_jy)==0:
                df_indexdata=df_index_tushare
                df_indexdata['turn_over']=None
                self.logger.info('indexdata使用的数据源是: tushare')
            elif len(df_index_wind)==0 and len(df_index_tushare)==0 and len(df_index_jy)>0:
                df_indexdata=df_index_jy
                df_indexdata=df_indexdata[df_indexdata['code'].isin(['000016.SH','000076.SH','000300.SH','000510.CSI','000852.SH','000905.SH','399303.SZ','932000.CSI','999004.SSI'])]
                df_indexdata['turn_over']=None
                self.logger.info('indexdata使用的数据源是: jy')
            elif len(df_index_wind)==0 and len(df_index_tushare)==0 and len(df_index_jy)==0:
                df_indexdata = pd.DataFrame()
                self.logger.warning(f'No data available for date: {available_date}')
                continue
            else:
                 # 获取所有唯一的code
                if len(df_index_wind)!=0:
                    if 'code' not in df_index_tushare.columns:
                        all_codes= list(set(df_index_wind['code']))
                    else:
                        all_codes = list(set(df_index_wind['code']) | set(df_index_tushare['code']))
                    all_codes.sort()
                # 保持Wind的列顺序，并添加Tushare独有的列
                    wind_columns = df_index_wind.columns.tolist()
                    tushare_only_columns = [col for col in df_index_tushare.columns if col not in wind_columns]
                    all_columns = wind_columns + tushare_only_columns
                else:
                    all_codes=list(set(df_index_tushare['code']))
                    all_columns=df_index_tushare.columns.tolist()
                # 创建新的DataFrame，包含所有列
                df_indexdata = pd.DataFrame(columns=all_columns)
                # 设置code列
                df_indexdata['code'] = all_codes
                if source_name_list[0]=='wind':
                    df_priority=df_index_wind
                    df_bu=df_index_tushare
                    df_bu2=df_index_jy
                else:
                    df_priority=df_index_tushare
                    df_bu=df_index_wind
                    df_bu2=df_index_jy
                # 对于每一列（除了code列），优先使用Wind的数据，如果Wind的数据有缺失则使用Tushare的数据补充
                for col in all_columns:
                    if col == 'code':
                        continue
                    # 先使用Wind的数据
                    if col in df_priority.columns:
                        # 找出df_stock中在df_priority中存在的代码
                        valid_codes = df_indexdata['code'].isin(df_priority['code'])
                        # 只对存在的代码进行赋值
                        if valid_codes.any():
                            df_indexdata.loc[valid_codes, col] = df_priority.set_index('code').loc[df_indexdata.loc[valid_codes, 'code'], col].values
                        # 对不存在的代码设置为None
                        df_indexdata.loc[~valid_codes, col] = None
                    else:
                        df_indexdata[col] = None
                    
                    # 如果Wind的数据有缺失，用Tushare的数据补充
                    if col in df_bu.columns:
                        # 找出当前列中为NaN的行
                        mask = df_indexdata[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu中存在的行
                            codes_to_update = df_indexdata.loc[mask, 'code']
                            codes_in_bu = df_bu['code'].unique()
                            valid_mask = codes_to_update.isin(codes_in_bu)
                            if valid_mask.any():
                                idx_to_update = df_indexdata.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_indexdata.loc[idx_to_update, col] = df_bu.set_index('code').loc[codes_valid, col].values
                    # 如果Wind的数据有缺失，用jy的数据补充
                    if col in df_bu2.columns:
                        # 找出当前列中为NaN的行
                        mask = df_indexdata[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu2中存在的行
                            codes_to_update = df_indexdata.loc[mask, 'code']
                            codes_in_bu2 = df_bu2['code'].unique()  # 修正：使用df_bu2而不是df_bu
                            valid_mask = codes_to_update.isin(codes_in_bu2)
                            if valid_mask.any():
                                idx_to_update = df_indexdata.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_indexdata.loc[idx_to_update, col] = df_bu2.set_index('code').loc[codes_valid, col].values
                self.logger.info('index_data使用的数据源是: wind和tushare和jy合并数据')
            if len(df_indexdata) != 0:
                df_indexdata['pct_chg']=df_indexdata['pct_chg']/100
                df_indexdata['valuation_date']=gt.strdate_transfer(available_date)
                df_indexdata=df_indexdata[['valuation_date']+df_indexdata.columns.tolist()[:-1]]
                df_indexdata.to_csv(outputpath_index_return, index=False, encoding='gbk')
                self.logger.info(f'Successfully saved index data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_indexdata['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_indexdata)
            else:
                self.logger.warning(f'index_data {available_date} 四个数据源都没有数据')

class indexComponent_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing index component update...')

    def file_name_withdraw(self,index_type):
        if index_type == '上证50':
            return 'sz50'
        elif index_type == '沪深300':
            return 'hs300'
        elif index_type == '中证500':
            return 'zz500'
        elif index_type == '中证1000':
            return 'zz1000'
        elif index_type == '中证2000':
            return 'zz2000'
        elif index_type=='国证2000':
            return 'gz2000'
        else:
            return 'zzA500'

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_component')
        return df_config

    def index_dic_processing(self):
        dic_index = {'上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
                     '中证2000': 'zz2000', '中证A500': 'zzA500','国证2000':'gz2000'}
        # dic_index = { '3145': 'hs300'}
        return dic_index

    def index_component_update_main(self):
        df_config = self.source_priority_withdraw()
        df_config.sort_values(by='rank', inplace=True)
        source_name_list = df_config['source_name'].tolist()

        dic_index = self.index_dic_processing()

        outputpath_component = glv.get('output_indexcomponent')
        outputpath_port = glv.get('output_portfolio')

        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'indexComponent',delete=True)
            sm2=gt.sqlSaving_main(inputpath_configsql,'Portfolio',delete=True)
        for index_type in ['上证50', '沪深300', '中证500', '中证1000', '中证2000', '中证A500','国证2000']:
            index_code = dic_index[index_type]
            self.logger.info(f'\nProcessing index type: {index_type}')
            file_name = self.file_name_withdraw(index_type)
            outputpath_component_update_base = os.path.join(outputpath_component, file_name)
            outputpath_port_update_base = os.path.join(outputpath_port, str(index_code) + '_comp')
            gt.folder_creator2(outputpath_component_update_base)
            gt.folder_creator2(outputpath_port_update_base)
            input_list=os.listdir(outputpath_component_update_base)
            if len(input_list)==0:
                if self.start_date > '2023-06-01':
                    start_date = '2023-06-01'
                else:
                    start_date = self.start_date
            else:
                start_date=self.start_date
            working_days_list=gt.working_days_list(start_date,self.end_date)
            for available_date in working_days_list:
                target_date = gt.next_workday_calculate(available_date)
                target_date = gt.intdate_transfer(target_date)
                self.logger.info(f'Processing date: {available_date}')
                available_date=gt.intdate_transfer(available_date)
                if index_type == '中证2000' and int(available_date) < 20230901:
                    available_date2 = '20230901'
                elif index_type == '中证A500' and int(available_date) < 20241008:
                    available_date2 = '20241008'
                else:
                    available_date2 = available_date
                df_daily = pd.DataFrame()
                outputpath_port_update = os.path.join(outputpath_port_update_base,
                                                      str(index_code) + '_comp_' + target_date + '.csv')
                outputpath_component_update = os.path.join(outputpath_component_update_base,
                                                           index_code + 'ComponentWeight_' + available_date + '.csv')
                ic = indexComponent_prepare(available_date2)
                for source_name in source_name_list:
                    if source_name == 'jy':
                        df_daily =ic.raw_jy_index_component_preparing(index_type)
                    else:
                        self.logger.warning('聚源都暂无数据')
                    if len(df_daily) != 0:
                        self.logger.info(f'{index_type}_component使用的数据源是: {source_name}')
                        break
                if len(df_daily) != 0:
                    df_daily['valuation_date']=gt.strdate_transfer(available_date)
                    df_daily['organization']=index_code
                    other_columns=[i for i in df_daily.columns.tolist() if i!='valuation_date']
                    df_daily=df_daily[['valuation_date']+other_columns]
                    df_daily.to_csv(outputpath_component_update, index=False)
                    df_port = df_daily[['code', 'weight']]
                    df_port['valuation_date']=gt.strdate_transfer(target_date)
                    df_port['portfolio_name']=str(index_code) + '_comp'
                    df_port=df_port[['valuation_date','portfolio_name','code','weight']]
                    df_port.to_csv(outputpath_port_update, index=False)
                    self.logger.info(f'Successfully saved {index_type} component data for date: {available_date}')
                    if self.is_sql == True:
                        now = datetime.now()
                        df_daily['update_time'] = now
                        df_port['update_time']=now
                        capture_file_withdraw_output(sm.df_to_sql, df_daily,'organization',index_code)
                        capture_file_withdraw_output(sm2.df_to_sql, df_port, 'portfolio_name', str(index_code) + '_comp')

                else:
                    self.logger.warning(f'{index_type}_component在{available_date}暂无数据')

    def index_component_update_main2(self):
        working_days_list = gt.working_days_list(self.start_date, self.end_date)
        
        # 定义指数代码字典 '上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
        #                      '中证2000': 'zz2000', '中证A500': 'zzA500','国证2000':'gz2000'
        dic_index = {
            '46': 'sz50',   # 上证50
            '3145': 'hs300',  # 沪深300
            '4978': 'zz500',  # 中证500
            '39144': 'zz1000',  # 中证1000
            '561230': 'zz2000',  # 中证2000
            '636661': 'zzA500',  # 中证A500
            '33792': 'gz2000',  # 国证2000
        }
        
        # 初始化数据库保存器（如果需要）
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            
            # 读取配置文件，检查表是否存在
            try:
                with open(inputpath_configsql, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                # 检查 indexComponent 表
                if 'indexComponent' in config:
                    indexcomp_config = config['indexComponent']
                    db_url = indexcomp_config.get('db_url', '').strip()
                    table_name = indexcomp_config.get('table_name', '').strip('"')
                    self.logger.info(f"[配置信息] indexComponent:")
                    self.logger.info(f"  db_url: {db_url}")
                    self.logger.info(f"  table_name: {table_name}")
                    if db_url and table_name:
                        if not check_table_exists(db_url, table_name):
                            self.logger.error(f"表 {table_name} 不存在，请先运行 chushihua() 创建表")
                            self.logger.error(f"请检查连接信息是否正确，数据库和表名是否匹配")
                            raise Exception(f"表 {table_name} 不存在，请先运行 chushihua() 创建表")
                        else:
                            self.logger.info(f"✓ 确认表 {table_name} 存在")
                
                # 检查 Portfolio 表
                if 'Portfolio' in config:
                    portfolio_config = config['Portfolio']
                    db_url = portfolio_config.get('db_url', '').strip()
                    table_name = portfolio_config.get('table_name', '').strip('"')
                    if db_url and table_name:
                        if not check_table_exists(db_url, table_name):
                            self.logger.warning(f"表 {table_name} 不存在，请先运行 chushihua() 创建表")
                            raise Exception(f"表 {table_name} 不存在，请先运行 chushihua() 创建表")
                        else:
                            self.logger.info(f"✓ 确认表 {table_name} 存在")
            except Exception as e:
                self.logger.error(f"检查表存在性时出错: {e}")
                raise
            
            sm = gt.sqlSaving_main(inputpath_configsql, 'indexComponent', delete=True)
            sm2 = gt.sqlSaving_main(inputpath_configsql, 'Portfolio', delete=True)

        # 查询并获取DataFrame
        index_component = IndexComponent(dic_index, 'source_db')
        for available_date in working_days_list:
            target_date = gt.next_workday_calculate(available_date)
            target_date = gt.intdate_transfer(target_date)
            self.logger.info(f'Processing date: {available_date}')
            
            # 支持多种日期格式
            # df = index_component.query_index_component(20251030)  # 整数格式
            # df = index_component.query_index_component('20251030')  # 字符串格式 YYYYMMDD
            df_daily = index_component.query_index_component(available_date)  # 字符串格式 YYYY-MM-DD
            
            # 调试信息
            self.logger.info(f'Query result for date {available_date}: {len(df_daily)} records')
            if df_daily is None or len(df_daily) == 0:
                self.logger.warning(f'No data returned for date {available_date}, skipping database write')
                continue
            
            # 打印DataFrame信息用于调试
            self.logger.info(f'DataFrame columns: {df_daily.columns.tolist()}')
            self.logger.info(f'DataFrame shape: {df_daily.shape}')
            
            if len(df_daily) > 0:
                # 处理df_port数据
                df_port = df_daily[['code', 'weight','organization','update_time']].copy()
                df_port['valuation_date'] = gt.strdate_transfer(target_date)
                # 将organization列的值添加_comp后缀，然后重命名为portfolio_name
                df_port['organization'] = df_port['organization'] + '_comp'
                df_port = df_port.rename(columns={'organization': 'portfolio_name'})
                # 重新排列列顺序
                df_port = df_port[['valuation_date', 'portfolio_name', 'code', 'weight', 'update_time']]
                
                self.logger.info(f'Successfully queried component data for date: {available_date}, total records: {len(df_daily)}')
                
                # 写入数据库（如果需要）
                if self.is_sql == True:
                    # 按每个指数分别写入数据库
                    for index_code_str, organization in dic_index.items():
                        index_code = organization  # 获取指数英文简称，如 'hs300'
                        
                        # 筛选当前指数的df_daily数据
                        df_daily_filtered = df_daily[df_daily['organization'] == index_code].copy()
                        if len(df_daily_filtered) > 0:
                            try:
                                self.logger.info(f'Writing {len(df_daily_filtered)} records to indexComponent table for {index_code}')
                                capture_file_withdraw_output(sm.df_to_sql, df_daily_filtered, 'organization', index_code)
                                self.logger.info(f'Successfully wrote df_daily for {index_code}')
                            except Exception as e:
                                self.logger.error(f'Failed to write df_daily for {index_code}: {e}')
                                raise
                        else:
                            self.logger.warning(f'No data found for organization {index_code} on date {available_date}')
                        
                        # 筛选当前指数的df_port数据
                        portfolio_name = index_code + '_comp'
                        df_port_filtered = df_port[df_port['portfolio_name'] == portfolio_name].copy()
                        if len(df_port_filtered) > 0:
                            try:
                                self.logger.info(f'Writing {len(df_port_filtered)} records to Portfolio table for {portfolio_name}')
                                capture_file_withdraw_output(sm2.df_to_sql, df_port_filtered, 'portfolio_name', portfolio_name)
                                self.logger.info(f'Successfully wrote df_port for {portfolio_name}')
                            except Exception as e:
                                self.logger.error(f'Failed to write df_port for {portfolio_name}: {e}')
                                raise
                        else:
                            self.logger.warning(f'No data found for portfolio {portfolio_name} on date {available_date}')
            else:
                self.logger.warning(f'Component data is empty for date: {available_date}')
        
        # TEST: 在循环结束后手动关闭连接
        try:
            index_component.db_manager.disconnect()
        except:
            pass

class stockData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing stock data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='stock')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'prevClose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            'hi' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            'lo' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'value' : 'amt',
            'AMT' : 'amt',
            'return' : 'pct_chg',
            'ret' : 'pct_chg',
            'vwap':'vwap',
            'adjfactor':'adjfactor',
            'ratioAdjFactor' : 'adjfactor',
            'tradeStatus' : 'trade_status',
            'tarde_status' : 'trade_status',
            'trade_status': 'trade_status',
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code','open','high','low','close','pre_close','pct_chg','vwap','volume','amt','adjfactor','trade_status']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df

    def stock_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_stock = glv.get('output_stock')
        gt.folder_creator2(outputpath_stock)
        input_list1=os.listdir(outputpath_stock)
        if len(input_list1)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'stockData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            df_stock=pd.DataFrame()
            available_date=gt.intdate_transfer(available_date)
            st = stockData_preparing(available_date)
            outputpath_stock_daily = os.path.join(outputpath_stock, 'stockdata_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_stock_wind=st.raw_jy_stockdata_withdraw()
            df_stock_jy=st.raw_jy_stockdata_withdraw()
            try:
                df_stock_wind = self.standardize_column_names(df_stock_wind)
            except:
                df_stock_wind = pd.DataFrame()
            try:
                df_stock_jy = self.standardize_column_names(df_stock_jy)
            except:
                df_stock_jy = pd.DataFrame()

            if len(df_stock_wind)>0 and len(df_stock_jy)>0:
                df_stock=df_stock_jy
                df_stock.rename(columns={'adjfactor':'adjfactor_jy'},inplace=True)
                df_stock['adjfactor_wind']=None
                self.logger.info('stock_data使用的数据源是: jy')
            else:
                df_stock=pd.DataFrame()
                self.logger.warning(f'stock_data {available_date} 两个数据源都没有数据')
                continue
            if len(df_stock) != 0:
                available_date2=gt.strdate_transfer(available_date)
                df_stock['valuation_date']=available_date2
                # 重新排列列顺序：valuation_date最前，adjfactor_jy和adjfactor_wind最后
                cols = df_stock.columns.tolist()
                # 移除valuation_date, adjfactor_jy, adjfactor_wind
                cols = [c for c in cols if c not in ['valuation_date', 'adjfactor_jy', 'adjfactor_wind']]
                # 只保留实际存在的adjfactor_jy和adjfactor_wind
                adj_cols = [c for c in ['adjfactor_jy', 'adjfactor_wind'] if c in df_stock.columns]
                df_stock = df_stock[['valuation_date'] + cols + adj_cols]
                df_stock.to_csv(outputpath_stock_daily, index=False)
                self.logger.info(f'Successfully saved stock data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_stock['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_stock)
            else:
                self.logger.warning(f'stock_data {available_date} 四个数据源更新有问题')

    def stock_data_update_main2(self):
        working_days_list=gt.working_days_list(self.start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'stockData',delete=True)
        
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            df_stock=pd.DataFrame()
            available_date_int=gt.intdate_transfer(available_date)
            
            # 每次循环创建新的StockData对象，确保连接独立管理
            stock_data = StockData('source_db')
            try:
                # 使用StockData从数据库查询数据，支持多种日期格式
                df_stock = stock_data.query_stock_data(available_date)
                self.logger.info(f'Successfully queried {len(df_stock)} records from database for date: {available_date}')
            except Exception as e:
                self.logger.error(f'Failed to query data from database for date {available_date}: {e}')
                df_stock = pd.DataFrame()
            finally:
                # 确保StockData的连接已关闭（虽然query_stock_data内部会关闭，但这里双重保险）
                try:
                    stock_data.db_manager.disconnect()
                except:
                    pass
            
            if len(df_stock) > 0:
                try:
                    # 标准化列名（与stock_data_update_main保持一致）
                    df_stock = self.standardize_column_names(df_stock)
                    # 重命名adjfactor为adjfactor_jy
                    # df_stock.rename(columns={'adjfactor':'adjfactor_jy'}, inplace=True)
                    # 添加adjfactor_wind列（设为None）
                    # df_stock['adjfactor_wind'] = None
                    self.logger.info('stock_data使用的数据源是: jy (from database)')
                except Exception as e:
                    self.logger.error(f'Failed to standardize column names for date {available_date}: {e}')
                    df_stock = pd.DataFrame()
            else:
                self.logger.warning(f'stock_data {available_date} 从数据库没有查询到数据')
                continue
            
            if len(df_stock) != 0:
                available_date2=gt.strdate_transfer(available_date_int)
                df_stock['valuation_date']=available_date2
                
                # 重新排列列顺序：valuation_date最前，adjfactor_jy和adjfactor_wind最后
                # cols = df_stock.columns.tolist()
                # 移除valuation_date, adjfactor_jy, adjfactor_wind
                # cols = [c for c in cols if c not in ['valuation_date', 'adjfactor_jy', 'adjfactor_wind']]
                # 只保留实际存在的adjfactor_jy和adjfactor_wind
                # adj_cols = [c for c in ['adjfactor_jy', 'adjfactor_wind'] if c in df_stock.columns]
                # df_stock = df_stock[['valuation_date'] + cols + adj_cols]
                
                # 只保存到SQL数据库，不生成本地文件
                if self.is_sql==True:
                    try:
                        now = datetime.now()
                        df_stock['update_time'] = now
                        # 与index_component_update_main2保持一致，不传递删除条件参数
                        # 但确保在StockData连接关闭后再进行SQL写入
                        capture_file_withdraw_output(sm.df_to_sql, df_stock)
                        self.logger.info(f'Successfully saved stock data to SQL for date: {available_date}, records: {len(df_stock)}')
                    except Exception as e:
                        self.logger.error(f'Failed to save stock data to SQL for date {available_date}: {e}')
                        import traceback
                        self.logger.error(traceback.format_exc())
                        # 不raise，继续处理下一个日期
                else:
                    self.logger.warning(f'SQL保存未启用，数据未保存')
            else:
                self.logger.warning(f'stock_data {available_date} 数据更新有问题')
