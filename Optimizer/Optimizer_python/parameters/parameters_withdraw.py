import pandas as pd
import os
import global_setting.global_dic as glv

def optimizer_args_withdraw(score_type):
    inputpath_config = glv.get('portfolio_parameters')
    df_config = pd.read_excel(inputpath_config)
    df_config=df_config[df_config['score_name']==score_type]
    df_config.set_index('score_name',inplace=True,drop=True)
    df_config=df_config.T
    # df_config.set_index('factor_name',inplace=True,drop=True)
    parameters=df_config.to_dict()
    parameters=parameters.get(score_type)
    parameters['score_type']=score_type
    return parameters
def factor_constraint_withdraw(score_type):
      inputpath_config = glv.get('factor_constraint')
      df_upper= pd.read_excel(inputpath_config,sheet_name='Upper')
      df_lower = pd.read_excel(inputpath_config, sheet_name='Lower')
      df_upper=df_upper[['factor_name',score_type]]
      df_lower = df_lower[['factor_name', score_type]]
      df_upper.rename(columns={score_type:'upper'},inplace=True)
      df_lower.rename(columns={score_type: 'lower'}, inplace=True)
      df_constraint=df_upper.merge(df_lower,on='factor_name',how='left')
      return df_constraint
def valid_factor_withdraw():
    inputpath_config = glv.get('factor_constraint')
    df_valid = pd.read_excel(inputpath_config, sheet_name='factor_name')
    style_list=df_valid[df_valid['style_valid']==1]['factor_name'].tolist()
    industry_list=df_valid[df_valid['industry_valid']==1]['factor_name'].tolist()
    return style_list,industry_list
