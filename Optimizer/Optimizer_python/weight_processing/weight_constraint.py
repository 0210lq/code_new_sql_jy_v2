import pandas as pd
import os
import global_setting.global_dic as glv
class weight_constraint:
    def __init__(self,df_score,df_weight,optimizer_args):
        self.df_score=df_score
        self.df_weight=df_weight
        self.top_number=int(optimizer_args.get('top_number'))
        self.stock_number=int(optimizer_args.get('stock_number'))
        self.top_weight_upper=optimizer_args.get('top_weight_upper')
        self.top_weight_lower=optimizer_args.get('top_weight_lower')
        self.C19U = optimizer_args.get('component_1_0.9_upper')
        self.C19L = optimizer_args.get('component_1_0.9_lower')
        self.C98U = optimizer_args.get('component_0.9_0.8_upper')
        self.C98L = optimizer_args.get('component_0.9_0.8_lower')
        self.C87U = optimizer_args.get('component_0.8_0.7_upper')
        self.C87L = optimizer_args.get('component_0.8_0.7_lower')
        self.C76U = optimizer_args.get('component_0.7_0.6_upper')
        self.C76L = optimizer_args.get('component_0.7_0.6_lower')
        self.C65U = optimizer_args.get('component_0.6_0.5_upper')
        self.C65L = optimizer_args.get('component_0.6_0.5_lower')
        self.C54U = optimizer_args.get('component_0.6_0.5_upper')
        self.C54L = optimizer_args.get('component_0.6_0.5_lower')
        self.C43U = optimizer_args.get('component_0.4_0.3_upper')
        self.C43L = optimizer_args.get('component_0.4_0.3_lower')
        self.C32U = optimizer_args.get('component_0.3_0.2_upper')
        self.C32L = optimizer_args.get('component_0.3_0.2_lower')
        self.C20U = optimizer_args.get('component_0.2_0_upper')
        self.C20L = optimizer_args.get('component_0.2_0_lower')
        self.constraint_mode=optimizer_args.get('contraint_type')
    def component_weight_processing(self,df):
        quantile_9 = df['final_score'].quantile(0.9)
        quantile_7 = df['final_score'].quantile(0.7)
        quantile_5 = df['final_score'].quantile(0.5)
        quantile_3 = df['final_score'].quantile(0.3)
        quantile_8 = df['final_score'].quantile(0.8)
        quantile_6 = df['final_score'].quantile(0.6)
        quantile_4 = df['final_score'].quantile(0.4)
        quantile_2 = df['final_score'].quantile(0.2)
        df1 = df[(df['final_score'] >= quantile_9)]
        df2 = df[(df['final_score'] < quantile_9) & (df['final_score'] >= quantile_8)]
        df3 = df[(df['final_score'] < quantile_8) & (df['final_score'] >= quantile_7)]
        df4 = df[(df['final_score'] < quantile_7) & (df['final_score'] >= quantile_6)]
        df5= df[(df['final_score'] < quantile_6) & (df['final_score'] >= quantile_5)]
        df6 = df[(df['final_score'] < quantile_5) & (df['final_score'] >= quantile_4)]
        df7 = df[(df['final_score'] < quantile_4) & (df['final_score'] >= quantile_3)]
        df8 = df[(df['final_score'] < quantile_3) & (df['final_score'] >= quantile_2)]
        df9= df[(df['final_score'] < quantile_2)]
        if self.constraint_mode=='v2':
            df1['weight_upper_index'] = df1['initial_weight_index'] + self.C19U
            df1['weight_lower_index'] = df1['initial_weight_index'] - self.C19L
            df2['weight_upper_index'] = df2['initial_weight_index'] + self.C98U
            df2['weight_lower_index'] = df2['initial_weight_index'] - self.C98L
            df3['weight_upper_index'] = df3['initial_weight_index'] + self.C87U
            df3['weight_lower_index'] = df3['initial_weight_index'] - self.C87L
            df4['weight_upper_index'] = df4['initial_weight_index'] + self.C76U
            df4['weight_lower_index'] = df4['initial_weight_index'] - self.C76L
            df5['weight_upper_index'] = df5['initial_weight_index'] + self.C65U
            df5['weight_lower_index'] = df5['initial_weight_index'] - self.C65L
            df6['weight_upper_index'] = df6['initial_weight_index'] + self.C54U
            df6['weight_lower_index'] = df6['initial_weight_index'] - self.C54L
            df7['weight_upper_index'] = df7['initial_weight_index'] + self.C43U
            df7['weight_lower_index'] = df7['initial_weight_index'] - self.C43L
            df8['weight_upper_index'] = df8['initial_weight_index'] + self.C32U
            df8['weight_lower_index'] = df8['initial_weight_index'] - self.C32L
            df9['weight_upper_index'] = df9['initial_weight_index'] + self.C20U
            df9['weight_lower_index'] = df9['initial_weight_index'] - self.C20L
        elif self.constraint_mode=='v1':
            df1['weight_upper_index'] = df1['initial_weight_index'] * (1 + self.C19U)
            df1['weight_lower_index'] = df1['initial_weight_index'] * (1 - self.C19L)
            df2['weight_upper_index'] = df2['initial_weight_index'] * (1 + self.C98U)
            df2['weight_lower_index'] = df2['initial_weight_index'] * (1 - self.C98L)
            df3['weight_upper_index'] = df3['initial_weight_index'] * (1 + self.C87U)
            df3['weight_lower_index'] = df3['initial_weight_index'] * (1 - self.C87L)
            df4['weight_upper_index'] = df4['initial_weight_index'] * (1 + self.C76U)
            df4['weight_lower_index'] = df4['initial_weight_index'] * (1 - self.C76L)
            df5['weight_upper_index'] = df5['initial_weight_index'] * (1 + self.C65U)
            df5['weight_lower_index'] = df5['initial_weight_index'] * (1 - self.C65L)
            df6['weight_upper_index'] = df6['initial_weight_index'] * (1 + self.C54U)
            df6['weight_lower_index'] = df6['initial_weight_index'] * (1 - self.C54L)
            df7['weight_upper_index'] = df7['initial_weight_index'] * (1 + self.C43U)
            df7['weight_lower_index'] = df7['initial_weight_index'] * (1 - self.C43L)
            df8['weight_upper_index'] = df8['initial_weight_index'] * (1 + self.C32U)
            df8['weight_lower_index'] = df8['initial_weight_index'] * (1 - self.C32L)
            df9['weight_upper_index'] = df9['initial_weight_index'] * (1 + self.C20U)
            df9['weight_lower_index'] = df9['initial_weight_index'] * (1 - self.C20L)
        else:
            raise ValueError
        lower_1 = df1['weight_lower_index'].sum()
        lower_2 = df2['weight_lower_index'].sum()
        lower_3 = df3['weight_lower_index'].sum()
        lower_4 = df4['weight_lower_index'].sum()
        lower_5 = df5['weight_lower_index'].sum()
        lower_6 = df6['weight_lower_index'].sum()
        lower_7 = df7['weight_lower_index'].sum()
        lower_8 = df8['weight_lower_index'].sum()
        lower_9 = df9['weight_lower_index'].sum()
        if lower_1+lower_2+lower_3+lower_4+lower_5+lower_6+lower_7+lower_8+lower_9>1-self.top_weight_lower:
            print('weight_lower不满足要求，已经自动缩放')
            sum = lower_1 + lower_2 + lower_3 + lower_4 + lower_5+lower_6+lower_7+lower_8+lower_9
            cha=0.8-self.top_weight_lower-sum
            proportion_1 = 1 - cha * lower_1 / sum
            proportion_2 = 1 - cha * lower_2 / sum
            proportion_3 = 1 - cha * lower_3 / sum
            proportion_4 = 1 - cha * lower_4 / sum
            proportion_5 = 1 - cha * lower_5 / sum
            proportion_6 = 1 - cha * lower_6 / sum
            proportion_7 = 1 - cha * lower_7 / sum
            proportion_8 = 1 - cha * lower_8 / sum
            proportion_9 = 1 - cha * lower_9 / sum
        else:
            proportion_1 = 1
            proportion_2 = 1
            proportion_3 = 1
            proportion_4 = 1
            proportion_5 = 1
            proportion_6 = 1
            proportion_7 = 1
            proportion_8 = 1
            proportion_9 = 1
        df1['weight_lower_index'] = df1['weight_lower_index'] * proportion_1
        df2['weight_lower_index'] = df2['weight_lower_index'] * proportion_2
        df3['weight_lower_index'] = df3['weight_lower_index'] * proportion_3
        df4['weight_lower_index'] = df4['weight_lower_index'] * proportion_4
        df5['weight_lower_index'] = df5['weight_lower_index'] * proportion_5
        df6['weight_lower_index'] = df6['weight_lower_index'] * proportion_6
        df7['weight_lower_index'] = df7['weight_lower_index'] * proportion_7
        df8['weight_lower_index'] = df8['weight_lower_index'] * proportion_8
        df9['weight_lower_index'] = df9['weight_lower_index'] * proportion_9
        df_final=pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9])
        df_final.loc[df_final['weight_lower_index'] < 0, ['weight_lower_index']] = 0
        df_final.loc[df_final['weight_upper_index'] < 0, ['weight_upper_index']] = 0
        df_final.drop(columns='final_score',inplace=True)
        return df_final
    def missing_weight_processing(self,df_weight,score_min):
        df_missing=df_weight[df_weight['final_score'].isna()]
        df_missing['weight_upper']=df_missing['weight']
        df_missing['weight_lower']=0
        df_missing['initial_weight']=0
        df_missing['final_score']=score_min
        #df_missing['final_score']=df_weight['final_score'].quantile(0.25)
        df_missing.rename(columns={'weight':'initial_weight_index'},inplace=True)
        df_missing = df_missing[['code', 'weight_upper', 'weight_lower', 'initial_weight', 'initial_weight_index', 'final_score']]
        return df_missing
    def weight_constraint_main(self):
        df_weight=self.df_weight.copy()
        df_score=self.df_score.copy()
        score_min=df_score['final_score'].min()
        df_missing=self.missing_weight_processing(df_weight,score_min)

        #定义变量
        df_weight.dropna(inplace=True)
        df_weight['weight'] = df_weight['weight'].astype(float)
        component_list=df_weight['code'].unique().tolist()
        df_score.sort_values(by='final_score', inplace=True, ascending=False)
        df_score.reset_index(inplace=True, drop=True)
        df_score=df_score[['code','final_score']]
        df_score2=df_score.copy()
        df_score2=df_score2[~(df_score2['code'].isin(component_list))]
        df_score2.reset_index(inplace=True,drop=True)
        df_top = df_score2.iloc[:self.top_number]
        df_top['top_weight'] = 1/len(df_top)
        df_weight.rename(columns={'weight': 'initial_weight_index'}, inplace=True)
        df_weight=self.component_weight_processing(df_weight)
        df_top['weight_upper_top'] = df_top['top_weight'] * self.top_weight_upper
        df_top['weight_lower_top'] = df_top['top_weight'] * self.top_weight_lower
        df_top = df_top[['code', 'weight_upper_top', 'weight_lower_top']]
        df_top['initial_weight_top'] = df_top['weight_lower_top']
        df_weight = df_weight.merge(df_top, on='code', how='outer')
        df_weight = df_weight.merge(df_score, on='code', how='left')
        df_weight.fillna(0, inplace=True)
        df_weight['weight_upper'] = df_weight['weight_upper_top']+df_weight['weight_upper_index']
        df_weight['weight_lower'] = df_weight['weight_lower_top']+df_weight['weight_lower_index']
        df_weight['initial_weight']=df_weight['weight_lower']
        df_weight.sort_values(by='code',ascending=False,inplace=True)
        code_list_existing=df_weight['code'].unique().tolist()
        df_weight=df_weight[['code','weight_upper','weight_lower','initial_weight','initial_weight_index','final_score']]
        df_score_bu = df_score[~(df_score['code'].isin(code_list_existing))]
        df_score_bu.sort_values(by='final_score', inplace=True, ascending=False)
        df_score_bu.reset_index(inplace=True, drop=True)
        number_need = self.stock_number - len(df_weight)
        if number_need > 0:
            df_score_bu = df_score_bu.iloc[:number_need]
            df_score_bu['weight_upper'] = 0
            df_score_bu['weight_lower'] = 0
            df_score_bu['initial_weight_index']=0
            df_score_bu['initial_weight'] = df_score_bu['weight_lower']
            df_score_bu = df_score_bu[['code','weight_upper','weight_lower','initial_weight','initial_weight_index','final_score']]
            df_weight = pd.concat([df_weight, df_score_bu])
        df_weight=pd.concat([df_weight, df_missing])
        df_weight.sort_values('code', inplace=True)
        df_weight.reset_index(inplace=True, drop=True)
        df_initial=df_weight[['code','initial_weight_index']]
        df_initial.rename(columns={'initial_weight_index': 'weight'}, inplace=True)
        df_initial=df_initial[['code','weight']]
        return df_weight,df_initial