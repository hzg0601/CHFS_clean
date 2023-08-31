import pandas as pd
import numpy as np
import regex as re
from data_loader import DataLoader
"""用于计算2017年之前的综合变量"""

class SummaryVariables(object):
    def __init__(self,
                 equ_table_path:str="./CHFS数据-2017/CHFS2017综合变量计算过程表.xlsx",
                 read_format:str="txt",
                 used_cols:list=None
                 ) -> None:
        self.equ_table = pd.read_excel(equ_table_path,encoding="utf-8")
        self.data_loader = DataLoader(read_format=read_format,used_cols=used_cols)
        

    def equ_parser(self,equ_table:pd.DataFrame) -> list:
        """
        将汇总表中的指定公式解析为一个公式列表
        """
        pass

    def exception_parser(self):
        """用于处理非常规公式"""
        pass

    def get_equ_group(self,
                      entity:str="hh",
                      entity_col:str="",
                      ):
        """找到指定entity对应公式的行"""
        equ_bool = self.equ_table[entity_col].apply(lambda x:True if isinstance(x,str) 
                                                    and re.searh(entity,x) else False)

        return self.equ_table[equ_bool]


    def variable_cal(self,
                     year:int=2015,
                     entity:str="hh",
                     data_dir:str="./",

                     ):
        data = self.data_loader.year_entity_loader(year=year,
                                                   entity=entity,
                                                   data_dir=data_dir,
                                                   drop_dup=False)
        

if __name__ == "__main__":
    pass

        
