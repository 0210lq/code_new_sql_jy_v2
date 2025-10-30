import pandas as pd
import os
import global_setting.global_dic as glv
import warnings
warnings.filterwarnings("ignore")
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from L4Data_update.tools_func import tools_func
import yaml
from setup_logger.logger_setup import setup_logger

class L4Data_preparing:
    def __init__(self,product_code,available_date):
        self.logger = setup_logger('L4Data_update')
        self.logger.info('\n' + '*'*50 + '\nL4 DATA PREPARING\n' + '*'*50)
        self.logger.info(f"Initializing L4Data_preparing - Product code: {product_code}, Date: {available_date}")
        self.product_code = product_code
        self.tf = tools_func()
        self.product_name = self.tf.product_NameCode_transfer(product_code)
        self.available_date = available_date
        self._config = None
        self._standard_columns = {}
        self._column_patterns = {}
        self._product_info = {}
        self._load_config()

    def _load_config(self):
        """加载YAML配置文件"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                 'config_project', 'L4_config', 'L4_info_config.yaml')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            self._process_config()
            self.logger.info("Successfully loaded and processed configuration file")
        except Exception as e:
            self.logger.error(f"Failed to load configuration file: {e}")
            self.logger.error(f"Configuration file path: {config_path}")
            raise

    def _process_config(self):
        """处理配置数据"""
        if not self._config:
            self.logger.error("Configuration not loaded")
            raise ValueError("Configuration not loaded")
        
        # 提取标准列名和模式
        for key, value in self._config['standard_columns'].items():
            self._standard_columns[key] = value['name']
            self._column_patterns[key] = value['patterns']
        
        # 提取产品信息配置
        self._product_info = self._config.get('product_info', {})
        self.logger.info("Successfully processed configuration data")

    def get_product_info(self):
        """获取产品配置信息"""
        return self._product_info

    def _find_matching_column(self, column_name, patterns):
        """查找匹配的列名模式"""
        column_name = str(column_name).lower()
        # 首先尝试精确匹配
        for pattern in patterns:
            if str(pattern).lower() == column_name:
                return pattern
        
        # 如果没有精确匹配，再尝试部分匹配
        for pattern in patterns:
            if str(pattern).lower() in column_name:
                return pattern
        return None

    def _standardize_columns(self, df):
        """标准化DataFrame的列名"""
        self.logger.info("Starting column standardization")
        rename_dict = {}
        
        # 首先处理完全匹配的列名
        for col in df.columns:
            col_str = str(col)
            col_lower = col_str.lower()
            for std_col, patterns in self._column_patterns.items():
                if col_str in patterns or col_lower in [p.lower() for p in patterns]:
                    rename_dict[col] = self._standard_columns[std_col]
                    break
        
        # 然后处理部分匹配的列名
        for col in df.columns:
            if col not in rename_dict:  # 只处理还没有匹配的列名
                col_str = str(col)
                for std_col, patterns in self._column_patterns.items():
                    if self._find_matching_column(col_str, patterns):
                        rename_dict[col] = self._standard_columns[std_col]
                        break
        
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)
            self.logger.info(f"Renamed columns: {rename_dict}")
            
        # 为所有标准列名创建缺失的列，并填充空值
        for std_col_name in self._standard_columns.values():
            # 如果是科目代码且已经是索引，则跳过创建该列
            if std_col_name == '科目代码' and df.index.name == '科目代码':
                continue
            if std_col_name not in df.columns:
                df[std_col_name] = None
        
        # 只保留标准列名中定义的列
        standard_columns = set(self._standard_columns.values())
        # 如果科目代码是索引，从标准列中移除它
        if df.index.name == '科目代码':
            standard_columns.discard('科目代码')
        columns_to_keep = [col for col in df.columns if col in standard_columns]
        df = df[columns_to_keep]
        
        self.logger.info("Column standardization completed")
        return df

    def extract(self):
        self.logger.info(f"Extracting data for {self.product_name} on {self.available_date}")
        product_code = self.product_code
        time = self.available_date
        inputpath = glv.get('folder_path')
        filelist = os.listdir(inputpath)
        
        # 将日期转换为两种格式
        time_int = gt.intdate_transfer(time)  # YYYYMMDD格式
        time_str = gt.strdate_transfer(time_int)  # YYYY-MM-DD格式
        
        # 文件名检索
        list1 = [i for i in filelist if product_code in i]
        if len(list1) == 0:
            self.logger.warning(f"No files found for product {self.product_name}")
            return None
            
        # 使用两种日期格式匹配文件
        list2 = [j for j in list1 if time_int in j or time_str in j]
        if len(list2) == 0:
            self.logger.warning(f"No files found for date {self.available_date}")
            return None
            
        # 只保留Excel文件
        list2 = [i for i in list2 if '.xlsx' in i or '.xls' in i]
        
        if len(list2) > 0:
            # 选择最短文件名（通常是最简单的那个）
            string1 = min(list2, key=len)
            list2 = [string1]
            
        if len(list2) > 1:
            self.logger.warning('Multiple files found with same name, please check')
            
        return list2[0] if list2 else None

    def raw_L4_adit(self,df):
        self.logger.info("Starting raw L4 data adjustment")
        df = self._standardize_columns(df)
        self.logger.info("Raw L4 data adjustment completed")
        return df

    def raw_L4_withdraw(self):
        self.logger.info(f"Withdrawing raw L4 data for {self.product_name}")
        folder_path = glv.get('folder_path')
        current_file = self.extract()
        if current_file is None:
            self.logger.error(f"No matching file found for {self.product_name} on {self.available_date}")
            return pd.DataFrame()
            
        a = os.path.join(folder_path, current_file)
        a = a.replace('\\', '\\\\')  # 路径拼接
        
        # 读取整个Excel文件
        df = pd.read_excel(a, header=None)
        
        # 找到包含"科目代码"的行
        header_row = None
        for idx, row in df.iterrows():
            if row.astype(str).str.contains('科目代码').any():
                header_row = idx
                break
        
        if header_row is None:
            self.logger.error("No row containing '科目代码' found")
            raise ValueError("No row containing '科目代码' found")
            
        # 使用包含"科目代码"的行作为列名
        df.columns = df.iloc[header_row]
        
        # 从下一行开始取数据
        df = df.iloc[header_row + 1:]
        
        # 删除科目代码为空的行
        df = df.dropna(subset=['科目代码'])
        
        # 删除科目代码列中包含"科目代码"字样的行
        df = df[~df['科目代码'].astype(str).str.contains('科目代码')]
        
        # 重置索引
        df = df.reset_index(drop=True)
        
        # 设置科目代码为索引
        df.set_index('科目代码', inplace=True)
        
        # 标准化列名
        df = self.raw_L4_adit(df)
        df=df.astype(str)
        self.logger.info("Successfully withdrew and processed raw L4 data")
        return df

    def product_info_transfer(self, df):
        """获取产品信息配置并进行精确匹配"""
        self.logger.info(f"Transferring product info for {self.product_name}")
        product_config = self.get_product_info()
        matched_fields = {
            'pure_value': None, 'property': None, 'liabilities': None,
            'security': None, 'stock': None, 'bond': None,
            'derivative': None, 'unit_pure': None, 'accumulated_pure': None,
            'accumulated_pure_d': None, 'shuhui_value': None, 'shengou_value': None
        }
        
        # 获取科目代码列
        search_values = df.index.values if df.index.name == '科目代码' else df['科目代码'].values if '科目代码' in df.columns else []
        
        # 将所有科目代码转换为字符串并去除空格
        search_values = [str(val).strip() for val in search_values]
        
        # 遍历每个字段的配置
        for field_key, field_value in matched_fields.items():
            field_config = product_config.get(field_key, {})
            if field_config and 'patterns' in field_config:
                patterns = field_config['patterns']
                # 遍历每个科目代码
                for val in search_values:
                    # 检查科目代码是否与任何pattern完全匹配
                    if val in [str(pattern).strip() for pattern in patterns]:
                        matched_fields[field_key] = val
                        break
        
        self.logger.info("Successfully transferred product info")
        return tuple(matched_fields.values())

    