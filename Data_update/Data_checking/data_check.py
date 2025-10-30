import pandas as pd
import os
import sys
import global_setting.global_dic as glv
from Time_tools.time_tools import time_tools
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from setup_logger.logger_setup import setup_logger2
import io
import contextlib
import yaml
import pymysql
from datetime import datetime

class DataCheck:
    def __init__(self,target_date=None): #对应你要交易的日期
        self.target_date=target_date
        self.tt = time_tools()
        self.logger = setup_logger2('DataCheck')
        if target_date==None:
            self.target_date_score = self.tt.target_date_decision_score()
            self.target_date_other = self.tt.target_date_decision_mkt()
        else:
            self.target_date_score = target_date
            self.target_date_other = gt.last_workday_calculate(target_date)
        self.target_date_score=gt.strdate_transfer(self.target_date_score)
        self.target_date_other=gt.strdate_transfer(self.target_date_other)
        self.db_config = self._load_db_config()
        self.conn = None
        self.cursor = None
        self.table_config = self._load_table_config()

    def _load_db_config(self):
        """加载数据库配置信息"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                     'config_project', 'Data_update', 'sql_connection.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            self.logger.info("数据库配置加载成功")
            return config['database']
        except Exception as e:
            self.logger.error(f"加载数据库配置失败: {str(e)}")
            raise

    def _load_check_config(self):
        """加载数据库检查配置信息"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                     'config_project', 'Data_update', 'database_check.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            self.logger.info("数据库检查配置加载成功")
            return config
        except Exception as e:
            self.logger.error(f"加载数据库检查配置失败: {str(e)}")
            raise

    def _load_table_config(self):
        """加载表配置信息"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                     'config_project', 'Data_update', 'dataUpdate_sql.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            self.logger.info("表配置加载成功")
            return config
        except Exception as e:
            self.logger.error(f"加载表配置失败: {str(e)}")
            raise

    def connect_db(self):
        """建立数据库连接"""
        try:
            self.conn = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            self.cursor = self.conn.cursor()
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise

    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("数据库连接已关闭")

    def check_database_updates(self):
        """
        检查数据库中所有表的更新状态
        基于配置文件中定义的检查模式进行检查
        """
        self.logger.info("=================================================================================================================")
        self.logger.info("开始检查数据库更新状态")
        try:
            self.connect_db()
            check_config = self._load_check_config()
            
            for mode, tables in check_config.items():
                self.logger.info(f"\n开始执行 {mode} 检查模式:")
                for table_config in tables:
                    table_name = table_config['table_name']
                    self.logger.info(f"\n检查表 {table_name}:")
                    try:
                        # 检查表字段
                        self.cursor.execute(f"""
                            SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA = '{self.db_config['database']}' 
                            AND TABLE_NAME = '{table_name}'
                        """)
                        columns = [col[0] for col in self.cursor.fetchall()]
                        
                        if mode == 'mode1':
                            # mode1: 检查仅包含valuation_date字段的表的数据维度
                            if 'valuation_date' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date字段，跳过检查")
                                continue
                            self.cursor.execute(f"""
                                SELECT COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                            """)
                            count = self.cursor.fetchone()[0]
                            self.logger.info(f"表 {table_name} 在 {self.target_date_other} 的数据维度:")
                            self.logger.info(f"记录数: {count}")
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode2':
                            # mode2: 检查包含valuation_date和organization字段的表的数据维度
                            if 'valuation_date' not in columns or 'organization' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date或organization字段，跳过检查")
                                continue
                            # 输出目标日期下所有organization的数量
                            self.cursor.execute(f"""
                                SELECT organization, COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                                GROUP BY organization
                            """)
                            org_counts = self.cursor.fetchall()
                            self.logger.info(f"表 {table_name} 在 {self.target_date_other} 按organization分组的维度:")
                            for org, cnt in org_counts:
                                self.logger.info(f"organization: {org}, 记录数: {cnt}")
                            # 输出所有唯一organization
                            org_list = [org for org, _ in org_counts]
                            self.logger.info(f"唯一organization列表: {', '.join([str(o) for o in org_list])}")
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode3':
                            # mode3: 检查包含valuation_date和product_code字段的表的数据维度
                            if 'valuation_date' not in columns or 'product_code' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date或product_code字段，跳过检查")
                                continue
                            # 计算mode3的检查日期
                            check_date = gt.last_workday_calculate(self.target_date_other)
                            # 输出目标日期下所有product_code的数量
                            self.cursor.execute(f"""
                                SELECT product_code, COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{check_date}'
                                GROUP BY product_code
                            """)
                            product_counts = self.cursor.fetchall()
                            self.logger.info(f"表 {table_name} 在 {check_date} 按product_code分组的维度:")
                            for product, cnt in product_counts:
                                self.logger.info(f"product_code: {product}, 记录数: {cnt}")
                            # 输出所有唯一product_code
                            product_list = [product for product, _ in product_counts]
                            self.logger.info(f"唯一product_code列表: {', '.join([str(p) for p in product_list])}")
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode4':
                            # mode4: 检查包含valuation_date、type和organization字段的表的数据维度
                            if 'valuation_date' not in columns or 'type' not in columns or 'organization' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date、type或organization字段，跳过检查")
                                continue
                            # 输出目标日期下按type和organization分组的数量
                            self.cursor.execute(f"""
                                SELECT type, organization, COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                                GROUP BY type, organization
                                ORDER BY type, organization
                            """)
                            type_org_counts = self.cursor.fetchall()
                            self.logger.info(f"表 {table_name} 在 {self.target_date_other} 按type和organization分组的维度:")
                            for type_val, org, cnt in type_org_counts:
                                self.logger.info(f"type: {type_val}, organization: {org}, 记录数: {cnt}")
                            # 输出所有唯一type
                            self.cursor.execute(f"""
                                SELECT DISTINCT type 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                                ORDER BY type
                            """)
                            type_list = [t[0] for t in self.cursor.fetchall()]
                            self.logger.info(f"唯一type列表: {', '.join([str(t) for t in type_list])}")
                            # 输出所有唯一organization
                            self.cursor.execute(f"""
                                SELECT DISTINCT organization 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                                ORDER BY organization
                            """)
                            org_list = [o[0] for o in self.cursor.fetchall()]
                            self.logger.info(f"唯一organization列表: {', '.join([str(o) for o in org_list])}")
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode5':
                            # mode5: 检查包含valuation_date和score_name字段的表的数据维度
                            if 'valuation_date' not in columns or 'score_name' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date或score_name字段，跳过检查")
                                continue
                            # 输出目标日期下按score_name分组的数量
                            self.cursor.execute(f"""
                                SELECT score_name, COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_other}'
                                GROUP BY score_name
                                ORDER BY score_name
                            """)
                            score_counts = self.cursor.fetchall()
                            self.logger.info(f"表 {table_name} 在 {self.target_date_other} 按score_name分组的维度:")
                            for score, cnt in score_counts:
                                self.logger.info(f"score_name: {score}, 记录数: {cnt}")
                            # 输出所有唯一score_name
                            score_list = [score for score, _ in score_counts]
                            self.logger.info(f"唯一score_name列表: {', '.join([str(s) for s in score_list])}")
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode6':
                            # mode6: 检查包含valuation_date和portfolio_name字段的表的数据维度
                            if 'valuation_date' not in columns or 'portfolio_name' not in columns:
                                self.logger.warning(f"表 {table_name} 没有valuation_date或portfolio_name字段，跳过检查")
                                continue
                            # 输出目标日期下按portfolio_name分组的数量
                            self.cursor.execute(f"""
                                SELECT portfolio_name, COUNT(*) 
                                FROM {table_name}
                                WHERE valuation_date = '{self.target_date_score}'
                                GROUP BY portfolio_name
                                ORDER BY portfolio_name
                            """)
                            portfolio_counts = self.cursor.fetchall()
                            self.logger.info(f"表 {table_name} 在 {self.target_date_score} 按portfolio_name分组的维度:")
                            for portfolio, cnt in portfolio_counts:
                                self.logger.info(f"portfolio_name: {portfolio}, 记录数: {cnt}")
                            
                            # 获取所有唯一的portfolio_name，不限制于当前日期的数据
                            self.cursor.execute(f"""
                                SELECT DISTINCT portfolio_name 
                                FROM {table_name}
                                ORDER BY portfolio_name
                            """)
                            portfolio_list = [p[0] for p in self.cursor.fetchall()]
                            self.logger.info(f"所有唯一portfolio_name列表: {', '.join([str(p) for p in portfolio_list])}")
                            
                            # 获取当前日期的portfolio_name列表
                            current_date_portfolios = [p for p, _ in portfolio_counts]
                            self.logger.info(f"当前日期({self.target_date_score})的portfolio_name列表: {', '.join([str(p) for p in current_date_portfolios])}")
                            
                            # 检查是否有缺失的portfolio_name
                            missing_portfolios = set(portfolio_list) - set(current_date_portfolios)
                            if missing_portfolios:
                                self.logger.warning(f"以下portfolio_name在当前日期({self.target_date_score})没有数据: {', '.join([str(p) for p in sorted(missing_portfolios)])}")
                            
                            self.logger.info(f"列名: {', '.join(columns)}")
                        elif mode == 'mode7':
                            # mode7: 不需要日常检查的表，跳过日志输出
                            continue
                    except Exception as e:
                        self.logger.error(f"检查表 {table_name} 时发生错误: {str(e)}")
                        continue
        except Exception as e:
            self.logger.error(f"数据库检查过程中发生错误: {str(e)}")
        finally:
            self.close_db()
            
        self.logger.info("数据库更新状态检查完成")
        self.logger.info("=================================================================================================================")

    def capture_file_withdraw_output(self, func, *args, **kwargs):
        """捕获file_withdraw的输出并记录到日志"""
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            result = func(*args, **kwargs)
            output = buf.getvalue()
            if output.strip():
                self.logger.info(output.strip())
        return result

    def rrScore_updateChecking(self):
        self.logger.info("=================================================================================================================")
        self.logger.info("开始检查 rrScore 更新状态")
        inputpath_score_config = glv.get('score_mode')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['score_type2'] = df_config['score_type'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['score_type2'] == 'rr')]['score_mode'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, 'rr_' + str(mode_type))
            mode_name = df_config[df_config['score_mode'] == mode_type]['mode_name'].tolist()[0]
            outputpath3 = os.path.join(outputpath, mode_name)
            available_date = gt.last_workday_calculate(self.target_date_score)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath2, available_date)
            inputpath_file3 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath3, available_date)
            if inputpath_file2 == None or inputpath_file3 == None:
                self.logger.error(f'fm_score在最新时间:{self.target_date_score}更新出现错误')
            else:
                self.logger.info(f'fm_score已经更新到最新日期:{self.target_date_score}')
        self.logger.info("=================================================================================================================")

    def combineScore_updateChecking(self):
        self.logger.info("=================================================================================================================")
        self.logger.info("开始检查 combineScore 更新状态")
        inputpath_score_config = glv.get('mode_dic')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['base_score2'] = df_config['base_score'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['base_score2'] == 'co')]['base_score'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, mode_type)
            available_date = gt.last_workday_calculate(self.target_date_score)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath2, available_date)
            if inputpath_file2 == None:
                self.logger.error(f'combine_score在最新时间:{self.target_date_score}更新出现错误')
            else:
                self.logger.info(f'combine_score已经更新到最新日期:{self.target_date_score}')
        self.logger.info("=================================================================================================================")

    def config_withdraw(self):
        inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        inputpath_config = os.path.join(inputpath, 'config_path\data_update_path_config.xlsx')
        df=pd.read_excel(inputpath_config)
        def output_finding(x):
            if 'output' in x:
                return 'Yes'
            else:
                return 'False'
        df['is_output']=df['data_type'].apply(lambda x: output_finding(x))
        df=df[df['is_output']=='Yes']
        df=df[~(df['data_type'].isin(['output_score','output_timeseries','output_destination','output_prod','output_l4']))]
        output_list=df['data_type'].tolist()
        return output_list

    def checking_crossSectiondata_main(self):
        self.logger.info("=================================================================================================================")
        self.logger.info("开始截面数据检查主流程")
        outputlist=self.config_withdraw()
        further_list=['output_indexexposure','output_indexcomponent','output_portfolio']
        for data_type in outputlist:
            inputpath=glv.get(data_type)
            if data_type in further_list:
                try:
                    input_list=os.listdir(inputpath)
                except:
                    self.logger.error(f'找不到 {inputpath}')
                    continue
                if data_type=='output_portfolio':
                    available_date=gt.intdate_transfer(self.target_date_score)
                    input_list=[i for i in input_list if 'a1' in i or 'a3' in i or 'gms' in i or 'ubp500' in i]
                else:
                    available_date = gt.intdate_transfer(self.target_date_other)
                for input in input_list:
                    inputpath_daily=os.path.join(inputpath,input)
                    try:
                         inputpath_daily = self.capture_file_withdraw_output(gt.file_withdraw, inputpath_daily, available_date)
                    except:
                        self.logger.error(f'找不到 {inputpath_daily}')
            else:
                try:
                    if 'outputpath' in data_type:
                         available_date = gt.last_workday_calculate(self.target_date_other)
                         available_date=gt.intdate_transfer(available_date)
                    else:
                         available_date = gt.intdate_transfer(self.target_date_other)
                    inputpath_daily = self.capture_file_withdraw_output(gt.file_withdraw, inputpath, available_date)
                except:
                    self.logger.error(f'找不到 {inputpath}')
        self.logger.info("截面数据检查主流程完成")
        self.logger.info("=================================================================================================================")

    def checking_timeseriesdata_main(self):
        self.logger.info("=================================================================================================================")
        self.logger.info("开始时序数据检查主流程")
        self.logger = setup_logger2('TimeseriesDataCheck')
        outputpath_ori = glv.get('output_timeseries')
        file_list = os.listdir(outputpath_ori)
        # 对每个数据类型进行处理
        for file_name in file_list:
            outputpath = os.path.join(outputpath_ori, file_name)

            # 检查目录是否存在
            if not os.path.exists(outputpath):
                self.logger.info(f"Directory {file_name} not found")
                continue

            # 获取该目录下的所有文件
            files = os.listdir(outputpath)
            if not files:
                self.logger.info(f"No files found in {file_name}")
                continue

            self.logger.info(f"\nChecking {file_name}:")
            # 处理该目录下的每个文件
            for file in files:
                try:
                    df = gt.readcsv(os.path.join(outputpath, file))
                    df['valuation_date'] = pd.to_datetime(df['valuation_date'])
                    df['valuation_date']=df['valuation_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
                    start_date = df['valuation_date'].min()
                    end_date = df['valuation_date'].max()
                    expected_dates = gt.working_days_list(start_date, self.target_date_other)
                    actual_dates = df['valuation_date'].unique().tolist()
                    missing_dates = list(set(expected_dates) - set(actual_dates))
                    missing_dates.sort()
                    if missing_dates:
                        # 将长列表分成多行输出
                        self.logger.info(f'Missing data for {file}:')
                        # 每行最多显示10个日期
                        for i in range(0, len(missing_dates), 10):
                            chunk = missing_dates[i:i + 10]
                            self.logger.info(f'Dates {i + 1}-{i + len(chunk)}: {chunk}')
                    else:
                        self.logger.info(f'{file} already updated to the latest date {end_date}')
                except Exception as e:
                    self.logger.error(f"Error processing {file} in {file_name}: {str(e)}")
        self.logger.info("时序数据检查主流程完成")
        self.logger.info("=================================================================================================================")
    def DataCheckmain(self):
        self.checking_crossSectiondata_main()
        # self.checking_timeseriesdata_main()
        self.rrScore_updateChecking()
        self.combineScore_updateChecking()
        self.check_database_updates()


if __name__ == '__main__':
    dc=DataCheck('2025-05-19')
    dc.check_database_updates()