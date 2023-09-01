'''
Description: 
1. 先对字符串进行全局处理
2. 然后判断字符串对应的数据集
3. 先执行ind数据集的所有操作
4. 

version: 
Author: huangzg
LastEditors: huangzg
Date: 2023-08-31 16:04:26
LastEditTime: 2023-09-01 18:20:13
'''
import pandas as pd
import numpy as np
import regex as re
from data_loader import DataLoader
from typing import Union,List

"""用于计算2017年之前的综合变量"""

class SummaryVariables(object):
    def __init__(self,
                 equ_table_path:str="./CHFS数据-2017/CHFS2017综合变量计算过程表.xlsx",
                 read_format:str="txt",
                 used_cols:list=None,
                 data_dir:str="./"
                 ) -> None:
        self.equ_table = pd.read_excel(equ_table_path,header=5)
        self.data_loader = DataLoader(read_format=read_format,used_cols=used_cols)
        self.data_dir = data_dir

    def get_equ_group(self,
                      entity:str="hh",
                      entity_col:str="所在数据集",
                      ):
        """找到指定entity对应公式的行"""
        equ_bool = self.equ_table[entity_col].apply(lambda x:True if isinstance(x,str) 
                                                    and re.search(entity,x) else False)

        return self.equ_table[equ_bool]
    
    def uni_exception_parser(self, 
                       rule_str:str
                       ):
        """用于处理非常规公式,如`...`,`分为三种情况`等

        """
        if "..." in rule_str:
            last_num_str = re.rule_str.split("...")[-1].split("+")[1] # 例如, c2064_6_imp
            num = int(last_num_str.split("_")[1])
            inter_nums = list(range(2,num))
            inter_num_str_list = [last_num_str.replace(str(num),str(i)) for i in inter_nums]
            inter_str = "+".join(inter_num_str_list)
            rule_str = rule_str.replace("...", inter_str)

    def ind_exception_parser(self,
                             rule_str:str,
                            dataset_name:str=None,
                            idx_col_name:str="hhid"
                            )->list:
        """处理带有加总字样且不包含max的ind数据集的公式
        返回一个列表，第一个为一个可直接eval的公式，第二个是一个通过groupby计算的字典{列名：公式}
        """

        rule_str_group, rule_str_ind = rule_str.split("加总") # 
        rule_str_group = re.sub("\u4e00-\u9fa5","",rule_str_group).split("=")
        group_str = f'{dataset_name}.groupby("{idx_col_name}")["{rule_str_group[1]}"].transform("sum")'
        rule_str_dict = {rule_str_group[0]:group_str}
        return [rule_str_ind, rule_str_dict]

    def extreme_exception_parse(self,
                                rule_str:str):
        """处理self.equ_table[self.equ_table["中间变量含义"] == "医疗保险报销"]"""
        rule_str_list = rule_str.split("\n")
        hh_str_list = rule_str_list[:2]
        temp = rule_str_list.pop(hh_str_list[0])
        temp = temp.replace(" } ","}").replace(" ",",")
        hh_str_list.append(temp)
        ind_str = "".join(rule_str_list[2:])
        return hh_str_list, ind_str
    
    def three_condition_parser(self,
                            rule_str:str,):
        """处理`分三种情况`"""

    def if_lacking_parser(self,rule_str:str):
        """处理`若没有缺失值`"""
        pass

    def max_exception_parser(self,rule_str:str):
        """处理计算公式中存在max的情况"""
        rule_str_list = rule_str.split("\n")
        rule_str_list = [i for i in rule_str_list if (i != "") and (i is not None)]
        for idx,string in enumerate(rule_str_list):
            if "max" in string:
                rule_str_list[idx] = re.sub(" +", ",",string)
        return rule_str_list[::-1]
    
    def multi_equ_parser(self,rule_str:str):
        rule_str_list = re.split("\n|[ ]+",rule_str)
        rule_str_list = [i for i in rule_str_list if (i != "") and (i is not None)]
        rule_str_list = rule_str_list[::-1]
        return rule_str_list

    def level_I_parse(self,
                      data:pd.Series,
                      dataset_name:pd.DataFrame=None,
                      idx_col_name:str="hhid"
                      ):
        data = data.dropna()
        data = data.apply(self.uni_exception_parser)
        ind_rule, hh_rule = [],[]
        hh_rule_list = []
        for rule_str in data:
            if "加总" in rule_str and "max{" not in rule_str:
                result = self.ind_exception_parser(rule_str=rule_str,
                                                   dataset_name=dataset_name,
                                                   idx_col_name=idx_col_name)
                ind_rule.append(result)
            elif "加总" in rule_str and "max{" in rule_str:
                hh_str_list,ind_str = self.extreme_exception_parse(rule_str)
                ind_str_result = self.ind_exception_parser(rule_str=ind_str,
                                                           dataset_name=dataset_name,
                                                           idx_col_name=idx_col_name)
                ind_rule.append(ind_str_result)
                hh_rule_list.append(hh_str_list)
            elif "分三种情况" in rule_str:
                pass
            elif len(rule_str.split("=")) > 2:
                # 存在多个表达式
                rule_str_list = self.multi_equ_parser(rule_str)
                hh_rule_list.append(rule_str_list)
            elif len(rule_str.split("=")) == 2:
                hh_rule.append(rule_str)
            else:
                print("未能解析的公式:")
                print(rule_str)
        return ind_rule, hh_rule, hh_rule_list   

    def level_II_parse(self,data:pd.Series):
        data = data.dropna()
        data = data.apply(self.uni_exception_parser)
        hh_rule, hh_rule_list = [], []
        for rule_str in data:
            if max in rule_str:
                rule_str_list = self.max_exception_parser(rule_str)
                hh_rule_list.append(rule_str_list)
            elif len(rule_str.split("")) > 2:
                rule_str_list = self.multi_equ_parser(rule_str)
                hh_rule_list.append(rule_str_list)
            elif len(rule_str.split("=")) == 2:
                hh_rule.append(rule_str)
            else:
                print("未能解析的公式:")
                print(rule_str)
        return hh_rule, hh_rule_list
        

    def equ_parser(self,
                    inter_level_1_col:str="中间变量计算公式",
                    inter_level_2_col:str="计算公式",
                    dataset_name:str=None,
                    idx_col_name:str="hhid") -> list:
        """
        将汇总表中的指定公式解析为一个公式列表
        """
        level_1_equ = self.equ_table[inter_level_1_col]
        level_2_equ = self.equ_table[inter_level_2_col]
        ind_rule, I_hh_rule, I_hh_rule_list = self.level_I_parse(data=level_1_equ,
                                                             dataset_name=dataset_name,
                                                             idx_col_name=idx_col_name)
        II_hh_rule, II_hh_rule_list = self.level_II_parse(data=level_2_equ)
        
        return ind_rule, I_hh_rule, I_hh_rule_list, II_hh_rule, II_hh_rule_list

    def ind_data_cal(self,
                              ind_data:pd.DataFrame,
                              ind_rule:list
                              )-> pd.DataFrame:
        """计算使用ind数据的中间变量"""
        new_cols = []
        for rule in ind_rule:
            try:
                rule_0 = re.sub("\s","",rule[0])
                ind_data = ind_data.eval(rule_0)
                new_cols.append(rule_0.split("=")[0])
                ind_data[rule[1].keys[0]] = eval(rule[1].values[0])
                new_cols.append(rule[1].keys[0])
            except Exception as e:
                print(f"公式计算错误,错误原因:")
                print(e)
                print(f"错误公式:{rule}")

        return ind_data[new_cols]

    def hh_data_cal(self,
                               hh_data:pd.DataFrame,
                               hh_rule_all:Union[list,tuple]):
        """用于计算level I中使用hh数据的中间变量"""
        new_cols = []
        hh_rule, hh_rule_list = hh_rule_all
        for rule_str in hh_rule:
            try:
                rule_str = re.sub("\s","",rule_str)
                hh_data = hh_data.eval(rule_str)
                new_cols.append(rule_str)
            except Exception as e:
                print(f"公式计算错误,错误原因:")
                print(e)
                print(f"错误公式:{rule_str}")
        for rule_str_list in hh_rule_list:
            try:
                for rule_str in rule_str_list:
                    rule_str = re.sub("\s","",rule_str)
                    hh_data = hh_data.eval(rule_str)
                    new_cols.append(rule_str)
            except Exception as e:
                print(f"公式计算错误,错误原因:")
                print(e)
                print(f"错误公式:{rule_str_list}")

        return hh_data[new_cols]
        

    def variable_cal(self,
                     year:int=2015,
                    inter_level_1_col:str="中间变量计算公式",
                    inter_level_2_col:str="计算公式",
                    idx_col_name:str="hhid"
                     ):
        """计算最终数据"""

        hh_data = self.data_loader.year_entity_loader(year=year,
                                                   entity="hh",
                                                   data_dir=self.data_dir,
                                                   drop_dup=False)
        ind_data = self.data_loader.year_entity_loader(year=year,
                                                       entity="ind",
                                                       data_dir=self.data_dir,
                                                       drop_dup=False)
        master_data = self.data_loader.year_entity_loader(year=year,
                                                       entity="master",
                                                       data_dir=self.data_dir,
                                                       drop_dup=False)
        ind_rule, I_hh_rule, I_hh_rule_list, II_hh_rule, II_hh_rule_list = self.equ_parser(
            inter_level_1_col=inter_level_1_col,
            inter_level_2_col=inter_level_2_col,
            dataset_name="ind_data",
            idx_col_name=idx_col_name
            )
        ind_data_new = self.ind_data_cal(ind_data=ind_data,ind_rule=ind_rule)
        hh_data_merge = pd.merge(ind_data_new, hh_data,how="left",on=idx_col_name)
        hh_data_I = self.hh_data_cal(hh_data=hh_data_merge,hh_rule_all=(I_hh_rule,I_hh_rule_list))
        hh_data_II = self.hh_data_cal(hh_data=hh_data_I,hh_rule_all=(II_hh_rule,II_hh_rule_list))
        final_data = pd.merge(hh_data_II, master_data,how="left",on=idx_col_name)
        
        return final_data

    def multi_year_call(self,years:List[int] = [2013,2015]):
        data_list = []
        for year in years:
            data = self.variable_cal(year=year)
            data_list.append(data)
        return data_list

if __name__ == "__main__":
    summary = SummaryVariables()
    hh_equ_group = summary.get_equ_group(entity="hh",entity_col="所在数据集")
    ind_equ_group = summary.get_equ_group(entity="ind",entity_col="所在数据集")
    print("done.")
        
