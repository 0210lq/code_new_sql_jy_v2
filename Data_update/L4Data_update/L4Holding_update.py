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
    logger = setup_logger('L4holding_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class L4Holding_update:
    def __init__(self,product_code,available_date,df,is_sql):
        self.is_sql=is_sql
        self.logger = setup_logger('L4Data_update')
        self.logger.info('\n' + '*'*50 + '\nL4 HOLDING UPDATE PROCESSING\n' + '*'*50)
        self.logger.info(f"Initializing L4Holding_update - Product code: {product_code}, Date: {available_date}")
        self.product_code=product_code
        self.tf=tools_func()
        self.product_name=self.tf.product_NameCode_transfer(product_code)
        self.available_date=available_date
        df2=df.copy()
        self.df=df2[~(df2['停牌信息'].isna() | (df2['停牌信息'] == 'None') | (df2['停牌信息'] == 'nan'))]
        self.lp=L4Data_processing(self.df,self.available_date,self.product_code)

    def outputpath_getting(self):
        """获取输出路径"""
        self.logger.info(f"Getting output path - Product code: {self.product_code}")
        if self.product_code=='SSS044':
            outputpath=glv.get('outputpath_RR500')
            file_name='_瑞锐500指增信息跟踪.csv'
        elif self.product_code=='SNY426':
            outputpath = glv.get('outputpath_RRJX')
            file_name='_瑞锐精选信息跟踪.csv'
        elif self.product_code=='SGS958':
            outputpath = glv.get('outputpath_XYHY_N01')
            file_name = '_惠盈一号指增信息跟踪.csv'
        elif self.product_code=='SZJ339':
            outputpath = glv.get('outputpath_SF500_N08')
            file_name='_盛元8号指增信息跟踪.csv'
        elif self.product_code=='SVU353':
            outputpath = glv.get('outputpath_GYZY_N01')
            file_name='_高益振英一号指增信息跟踪.csv'
        elif self.product_code=='SLA626':
            outputpath = glv.get('outputpath_RenRui_N01')
            file_name='_仁睿价值精选1号信息跟踪.csv'
        elif self.product_code=='STH580':
            outputpath = glv.get('outputpath_NJ300')
            file_name='_念觉300指增11号信息跟踪.csv'
        elif self.product_code=='SST132':
            outputpath = glv.get('outputpath_ZJ4')
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
    def standardize_column_names(self,df,type):
        # 创建列名映射字典
        column_mapping = {
            '日期' : 'valuation_date',
            '产品名称' : 'product_code',
            '代码' : 'code',
            '科目名称' :'code',
            '数量' : 'quantity',
            '市价' : 'price',
            '单位成本' :'unit_cost',
            '市值' : 'mkt_value'
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
        df['asset_type']=type
        # 定义固定的列顺序
        fixed_columns = ['valuation_date','product_code','asset_type','code','quantity','price','unit_cost','mkt_value']
        # 对于不存在的列，创建并填充空值
        for col in fixed_columns:
            if col not in df.columns:
                df[col] = None
        # 按固定顺序排列所有列
        df = df[fixed_columns]
        return df
    def output_df_normalization(self,df,type):
        if type=='stock' or type=='cbond' or type=='bond' or type=='etf':
            df['方向']='long'
        if type=='future' or type=='option':
            df.drop(columns='代码',inplace=True)
        df=self.standardize_column_names(df,type)
        if type=='stock' or type=='cbond' or type=='etf':
            try:
                df=gt.code_transfer(df)
            except:
                df=df
        return df

    def L4Holding_processing(self):
        """处理L4持仓数据"""
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'L4HoldingData')
        self.logger.info(f"Starting L4 holding processing - Product: {self.product_name}, Date: {self.available_date}")
        status='save'
        product_name=self.product_name
        date=self.available_date
        try:
            result=self.lp.process_stock_sheet()
            self.logger.info("Successfully processed stock sheet")
        except:
            result=pd.DataFrame()
            status='not_save'
            self.logger.error(f"Stock sheet format changed for {date} {product_name}, please correct")
        try:
            result1 = self.lp.process_future_sheet()
            self.logger.info("Successfully processed future sheet")
        except:
            result1 = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"Future sheet format changed for {date} {product_name}, please correct")
        try:
            result2 = self.lp.process_c_bond_sheet()
            self.logger.info("Successfully processed convertible bond sheet")
        except:
            result2 = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"Convertible bond sheet format changed for {date} {product_name}, please correct")
        try:
            result3 = self.lp.process_option_sheet()
            self.logger.info("Successfully processed option sheet")
        except:
            result3 = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"Option sheet format changed for {date} {product_name}, please correct")
        try:
            result4 = self.lp.process_bond_sheet()
            self.logger.info("Successfully processed bond sheet")
        except:
            result4 = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"Bond sheet format changed for {date} {product_name}, please correct")
        try:
            result5 = self.lp.process_etf_sheet()
            self.logger.info("Successfully processed ETF sheet")
        except:
            result5 = pd.DataFrame()
            status = 'not_save'
            self.logger.error(f"ETF sheet format changed for {date} {product_name}, please correct")
        result_path = self.outputpath_getting()
        if status == 'not_save':
            self.logger.error('Update failed')
        else:
            result=self.output_df_normalization(result,'stock')
            result1=self.output_df_normalization(result1,'future')
            result2=self.output_df_normalization(result2,'cbond')
            result3=self.output_df_normalization(result3,'option')
            result4=self.output_df_normalization(result4,'bond')
            result5=self.output_df_normalization(result5,'etf')
            df_final=pd.concat([result,result1,result2,result3,result4,result5])
            df_final['valuation_date']=pd.to_datetime(df_final['valuation_date'])
            df_final['valuation_date']= df_final['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_final.to_csv(result_path,index=False)
            self.logger.info(f"L4 holding processing completed, saved to: {result_path}")
            if self.is_sql == True:
                capture_file_withdraw_output(sm.df_to_sql, df_final)





