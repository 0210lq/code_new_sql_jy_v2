"""
测试模块

这个模块用于比较两个投资组合权重文件的差异，主要用于测试和验证目的。
比较生产环境和测试环境的投资组合权重配置，计算差异并输出结果。

主要功能：
1. 读取两个不同路径的投资组合权重文件
2. 比较权重差异
3. 计算并输出差异统计信息

依赖模块：
- pandas：数据处理
- os：路径操作

作者：[作者名]
创建时间：[创建时间]
"""

import pandas as pd
import os

# 定义输入路径
inputpath1='D:\Portfolio\portfolio_weight\combine_zz500_HB'
inputpath2='D:\Portfolio_test\portfolio_weight\combine_zz500_HB'
inputpath1=os.path.join(inputpath1,'combine_zz500_HB_20250627.csv')
inputpath2=os.path.join(inputpath2,'combine_zz500_HB_20250627.csv')

# 读取两个投资组合权重文件
df1=pd.read_csv(inputpath1)
df2=pd.read_csv(inputpath2)

# 重命名列以便区分
df1.rename(columns={'weight':'weight_prod'},inplace=True)
df2.rename(columns={'weight':'weight_new'},inplace=True)

# 合并两个数据框，使用outer join确保所有股票都被包含
df_final=df1.merge(df2,on='code',how='outer')
print(df_final)

# 填充缺失值为0
df_final.fillna(0,inplace=True)

# 计算权重差异的绝对值
df_final['difference']=abs(df_final['weight_prod'].astype(float)-df_final['weight_new'].astype(float))

# 按差异大小降序排列
df_final.sort_values('difference',inplace=True,ascending=False)
print(df_final)

# 输出总差异
print(df_final['difference'].sum())