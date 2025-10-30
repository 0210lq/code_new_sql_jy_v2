import pandas as pd
import global_setting.global_dic as glv
import ast
import re
class tools_func:
    def option_name_transfer_NJ300(self,option_name):
        # 输入为 产品下期权名
        # 输出为 对应产品代码
        if option_name[0:5] == '沪深300':
            if option_name[5] == '沽':
                if option_name[6] == '1':
                    option_name_new = 'IO241' + option_name[7] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'IO240' + option_name[6] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[6] == '1':
                    option_name_new = 'IO241' + option_name[7] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'IO240' + option_name[6] + '-C-' + option_name[-4:] + '.CFE'
        elif option_name[0:4] == '上证50':
            if option_name[4] == '沽':
                if option_name[5] == '1':
                    option_name_new = 'HO241' + option_name[6] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'HO240' + option_name[5] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[5] == '1':
                    option_name_new = 'HO241' + option_name[6] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'HO240' + option_name[5] + '-C-' + option_name[-4:] + '.CFE'
        elif option_name[0:6] == '中证1000':
            if option_name[6] == '沽':
                if option_name[7] == '1':
                    option_name_new = 'MO241' + option_name[8] + '-P-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'MO240' + option_name[7] + '-P-' + option_name[-4:] + '.CFE'
            else:
                if option_name[7] == '1':
                    option_name_new = 'MO241' + option_name[8] + '-C-' + option_name[-4:] + '.CFE'
                else:
                    option_name_new = 'MO240' + option_name[7] + '-C-' + option_name[-4:] + '.CFE'
        else:
            print('期权名字格式特殊转换失败，请手动改正')
            option_name_new = option_name
        return option_name_new

    def option_name_transfer(self,option_name):
        # 输入为 盛丰1000外其他产品下期权名
        # 输出为 对应产品代码
        if option_name[0:5] == '沪深300':
            option_name_new = 'IO' + option_name[-11:] + '.CFE'
        elif option_name[0:4] == '上证50':
            option_name_new = 'HO' + option_name[-11:] + '.CFE'
        elif option_name[0:6] == '中证1000':
            option_name_new = 'MO' + option_name[-11:] + '.CFE'
        else:
            print('期权名字格式特殊转换失败，请手动改正')
            option_name_new = option_name
        return option_name_new
    def product_NameCode_transfer(self,product_code):
        inputpath=glv.get('L4_config')
        df=pd.read_excel(inputpath)
        product_name=df[df['product_code']==product_code]['product_name'].tolist()[0]
        return product_name
    def product_CodeName_transfer(self,product_name):
        inputpath=glv.get('L4_config')
        df=pd.read_excel(inputpath)
        product_code=df[df['product_name']==product_name]['product_code'].tolist()[0]
        return product_code
    def product_loc_withdraw(self,product_code):
        inputpath = glv.get('L4_config')
        df = pd.read_excel(inputpath)
        columns = df[df['product_code'] == product_code]['columns'].tolist()[0]
        columns2 = df[df['product_code'] == product_code]['df'].tolist()[0]
        return columns,columns2
    def optionFuture_len_withdraw(self,product_code,type):
        inputpath = glv.get('L4_config')
        df = pd.read_excel(inputpath)
        info_parameters= df[df['product_code'] == product_code][type].tolist()[0]
        try:
            info_parameters = ast.literal_eval(info_parameters)
        except:
            info_parameters=info_parameters
        return info_parameters

    def process_stock_input(self, input_string):
        input_string = str(input_string)
        # 查找以00,60,30,68,83开头的6位数字
        pattern = r'(?:00|60|30|68|83)\d{4}'
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)
        else:
            return " "

    def process_etf_input(self, input_string):
        input_string = str(input_string)
        # 查找以00,60,30,68,83开头的6位数字
        if '.' in input_string:
            return input_string[-9:-3]
        else:
            return input_string[-6:]
    def process_cbond_input(self, input_string):
        input_string = str(input_string)
        # 查找以00,60,30,68,83开头的6位数字
        if '.' in input_string:
            return input_string[-9:-3]
        else:
            return input_string[-6:]
    def process_option_input(self,input_string):
        input_string = str(input_string)
        
        # 查找XXXX-P-XXXX或XXXX-C-XXXX格式
        pattern = r"\d{4}-[PC]-\d{4}"
        match = re.search(pattern, input_string)
        
        if match:
            # 找到匹配的位置
            start_pos = match.start()
            # 获取匹配的内容
            matched_part = match.group(0)
            # 检查前面的字符
            if start_pos >= 2:  # 确保前面有足够的字符来检查
                prefix = input_string[start_pos-2:start_pos]
                # 检查前缀是否为两个字母
                if prefix.isalpha() and len(prefix) == 2 and prefix.isupper():
                    # 如果是两个字母，直接使用
                    return prefix + matched_part
                else:
                    return 'IO' + matched_part
            # 如果前缀不是两个字母或位置不够，使用IO
        
        return " "  # 如果没有找到匹配的模式，返回空格
    def process_future_input(self,input_string):
        input_string = str(input_string)
        pattern = r"[A-Z]{2}\d{4}"
        pattern2 = r"[A-Z]{1}\d{4}"
        pattern3 = r"[A-Z]{2}\d{3}"
        match = re.search(pattern, input_string)
        match2 = re.search(pattern2, input_string)
        match3 = re.search(pattern3, input_string)
        if match:
            return match.group(0)
        else:
            if match2:
                # 检查是否是FXXXX格式
                if match2.group(0).startswith('F'):
                    return 'IF' + match2.group(0)[1:]  # 返回IFXXXX格式
                return match2.group(0)
            else:
                if match3:
                    return match3.group(0)
                else: 
                    return " "
    def process_bond_input(self,input_string):
        pattern = r"[TS]{2}\d{4}"
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)
        else:
            pattern = r"[T]{1}\d{4}"
            match = re.search(pattern, input_string)
            if match:
                return match.group(0)
            else:
                 return " "


