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
from L4Data_update.L4Data_processing import L4Data_processing
from setup_logger.logger_setup import setup_logger
import io
import contextlib
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('L4info_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class L4Info_update:
    def __init__(self,product_code,available_date,df,is_sql):
        self.is_sql=is_sql
        self.logger = setup_logger('L4Data_update')
        self.logger.info('\n' + '*'*50 + '\nL4 INFO UPDATE PROCESSING\n' + '*'*50)
        self.logger.info(f"Initializing L4Info_update - Product code: {product_code}, Date: {available_date}")
        self.product_code=product_code
        self.tf=tools_func()
        self.product_name=self.tf.product_NameCode_transfer(product_code)
        self.available_date=available_date
        self.df=df
        self.df=self.df.reset_index()
        self.df=self.df.fillna(0)
        self.lp = L4Data_processing(self.df, self.available_date, self.product_code)

    def outputpath_getting(self):
        """获取输出路径"""
        self.logger.info(f"Getting output path - Product code: {self.product_code}")
        if self.product_code=='SSS044':
            outputpath=glv.get('outputpath1_RR500')
            file_name='_瑞锐500指增信息跟踪.csv'
        elif self.product_code=='SNY426':
            outputpath = glv.get('outputpath1_RRJX')
            file_name='_瑞锐精选信息跟踪.csv'
        elif self.product_code=='SGS958':
            outputpath = glv.get('outputpath1_XYHY_N01')
            file_name = '_惠盈一号指增信息跟踪.csv'
        elif self.product_code=='SZJ339':
            outputpath = glv.get('outputpath1_SF500_N08')
            file_name='_盛元8号指增信息跟踪.csv'
        elif self.product_code=='SVU353':
            outputpath = glv.get('outputpath1_GYZY_N01')
            file_name='_高益振英一号指增信息跟踪.csv'
        elif self.product_code=='SLA626':
            outputpath = glv.get('outputpath1_RenRui_N01')
            file_name='_仁睿价值精选1号信息跟踪.csv'
        elif self.product_code=='STH580':
            outputpath = glv.get('outputpath1_NJ300')
            file_name='_念觉300指增11号信息跟踪.csv'
        elif self.product_code=='SST132':
            outputpath = glv.get('outputpath1_ZJ4')
            file_name='_念觉知行4号信息跟踪.csv'
        else:
            outputpath=None
            file_name=None
            self.logger.warning(f"Product code {self.product_code} is not within script scope")
        if outputpath==None or file_name==None:
            result_path_final=None
        else:
            gt.folder_creator2(outputpath)
            result_path_final = os.path.join(outputpath, str(self.available_date) + file_name)
            self.logger.info(f"Output path: {result_path_final}")
        return result_path_final
    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            '日期' : 'valuation_date',
            '持仓日期':'valuation_date',
            '产品名称' : 'product_code',
            '资产净值' : 'NetAssetValue',
            '资产总值' : 'GrossAssetValue',
            '资产负债' : 'AssetLiabilities',
            '证券市值' : 'SecuritiesValue',
            '股票市值' : 'StockValue',
            '债券市值' : 'BondValue',
            '其他衍生工具市值' : 'DerivativesValue',
            '产品净值' : 'ProductNetValue',
            '产品累计净值' : 'ProductAccumulatedNetValue',
            '产品日涨跌幅' : 'ProductReturn',
            '赎回金额' : 'RedemptionAmount',
            '申购金额' : 'SubscriptionAmount'
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
        fixed_columns = ['valuation_date','product_code','NetAssetValue','GrossAssetValue','AssetLiabilities','SecuritiesValue','StockValue',
                         'BondValue','DerivativesValue','ProductNetValue','ProductAccumulatedNetValue','ProductReturn','RedemptionAmount','SubscriptionAmount']
        # 对于不存在的列，创建并填充空值
        for col in fixed_columns:
            if col not in df.columns:
                df[col] = None
        # 按固定顺序排列所有列
        df = df[fixed_columns]
        return df
    def L4Info_processing(self):
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'L4InfoData')
        """处理L4信息数据"""
        self.logger.info(f"Starting L4 info processing - Product: {self.product_name}, Date: {self.available_date}")
        status = 'save'
        product_name = self.product_name
        try:
            result = self.lp.process_info_sheet()
        except:
            result = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"Product info format changed for {self.available_date} {self.product_name}, please correct")
        result_path = self.outputpath_getting()
        if status == 'not_save':
            self.logger.error('Update failed')
        else:
            result=self.standardize_column_names(result)
            result['valuation_date'] = pd.to_datetime(result['valuation_date'])
            result['valuation_date'] = result['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            result.to_csv(result_path,index=False)
            self.logger.info(f"L4 info processing completed, saved to: {result_path}")
            if self.is_sql == True:
                capture_file_withdraw_output(sm.df_to_sql, result)

