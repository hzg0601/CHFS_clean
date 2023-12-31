"""load data
hh: household,代表家庭变量库，包含了问卷中家庭部分的数据，例如：家庭农业生产经营情况、住房资产拥有情况等
ind: individual,代表个人变量库，包含了问卷中个人部分的数据，例如：人口统计特征，个人工作及收入信息，保险与保障等
master: 代表非问卷变量库，包含了在问卷数据基础上衍生出来的样本地理信息、权重、综合变量等信息
"""
import pandas as pd
import logging
import os
from typing import Union, List, Tuple, Dict
from pathlib import Path
import re
from functools import reduce
import warnings
warnings.filterwarnings("ignore")
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # set logger level

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


class DataLoader(object):
    """一个读取chfs数据的类，
    read_format:读取文件的格式
    save_format:存储文件的格式
    used_cols:选取的变量名列表
    
    """
    def __init__(self,
                 read_format="txt",
                 used_cols:List[str] = None,
                 ) -> None:
        self.read_format = read_format
        self.reader_dict = {"txt": self.txt_reader,
                            "stata13":self.dta_2013_reader,
                            "stata14":self.dta_2014_reader}
        self.used_cols = set(used_cols) if used_cols is not None else None


    def txt_reader(self,
                   file_name:str,
                   data_dir:str,
                   save:bool=False,
                   save_dir:str="./",
                   save_format: str = "csv",
                   save_name:str=None
                   ) -> pd.DataFrame:
        """
        file_names: 文件名，支持以正则表达式匹配
        data_dir: 数据存储的路径
        save: 是否存储的本地
        save_dir:存储到本地的路径
        save_format: 存储到本地的格式,.csv, .xlsx
        """
        assert os.path.exists(data_dir), "数据路径不存在"
        file_path = []
        for file in os.listdir(data_dir):
            if re.search(file_name, file):
                file_path.append(os.path.join(data_dir,file))
        if len(file_path) == 0:
            logger.error("路径下不存在指定文件名的文件")
            raise KeyError
        elif len(file_path) > 1:
            logger.warning("指定文件名下存在多个问题，执行合并...")
            data = []
            for file in file_path:
                data = pd.read_csv(file,sep=",", encoding="utf-8")
            data = pd.concat(data,axis=0)
        else:
            try:
                data = pd.read_csv(file_path[0],sep=',',encoding='utf-8',low_memory=False)
            except UnicodeDecodeError:
                data = pd.read_csv(file_path[0],sep=",",encoding="gbk",low_memory=False)
        if save:
            if not save_name:
                save_name = file_path[0].split(".")[0].replace("/","_").replace("\\","_")
            save_name = ".".join(save_name,save_format)
            file_path = os.path.join(save_dir, save_name)
            data.to_csv(file_path,index=False)

        return data


    def dta_2013_reader(self,
                        file_name:str, 
                        data_dir:str,
                        save:bool=False,
                        save_dir:str=None,
                        save_format: str = "csv",
                        save_name:str=None
                        )-> pd.DataFrame:
        """
        file_names: 文件名，支持以正则表达式匹配
        data_dir: 数据存储的路径
        save: 是否存储的本地
        save_dir:存储到本地的路径
        save_format: 存储到本地的格式,.csv, .xlsx
        """
        pass

    def dta_2014_reader(self,
                        file_name:str, 
                        data_dir:str,
                        save:bool=False,
                        save_dir:str=None,
                        save_format: str = "csv",
                        save_name:str=None
                        )-> pd.DataFrame:
        """
        file_names: 文件名，支持以正则表达式匹配
        data_dir: 数据存储的路径
        save: 是否存储的本地
        save_dir:存储到本地的路径
        save_format: 存储到本地的格式,.csv, .xlsx
        """
        pass

    def match_path(self,parent_dir:str, key:str):
        for path in os.listdir(parent_dir):
            if re.search(key,path):
                second_path = os.path.join(parent_dir,path)
                return second_path


    def year_entity_loader(self,
                    year:Union[int, str]=2011,
                    entity:str="hh", # hh, id, master
                    data_dir:str="./",
                    drop_dup:bool=False,
                    drop_dup_cols:List[str] = ["hhid"]
                    ) -> pd.DataFrame:
        """读取某一年某一entity的数据
        """
        second_path = self.match_path(data_dir,str(year))
        third_path = self.match_path(second_path,self.read_format)

        reader = self.reader_dict[self.read_format]
        data = reader(file_name=entity,data_dir=third_path,save=False)
        # 增加year变量
        data['year'] = int(year)
        print(f"before--year {year},entity:{entity}, shape:{data.shape}")
        # 删除index列
        if "index" in data.columns:
            data = data.drop(columns=["index"])
        # 设置hhid列
        data = self.set_id(data=data,idx_col="hhid")
        # 取出指定的列
        data = self.select_cols(data,used_cols=None)
        # 去重
        if drop_dup:
            data = data.drop_duplicates(subset=drop_dup_cols)
        print(f"after--year {year},entity:{entity}, shape:{data.shape}")
        return data

    def entity_batch_loader(self,
                            entity:str="hh",
                            years: Union[str, List[int]]="all",
                            data_dir: str = "./",
                            drop_dup:bool = False,
                            drop_dup_cols:List[str] = ["hhid"]
                            )-> pd.DataFrame:
        """读取指定某一个entity[hh,ind,master]指定多个年份的数据"""
        if years == "all":
            years = [re.search("20[0-9]{2}",file).group(0) for file in os.listdir(data_dir)
                     if re.search("20[0-9]{2}",file)]
            if len(years) == 0:
                logger.error("路径错误，未匹配到年份")
                raise KeyError
        dataset = []
        for year in years:
            data = self.year_entity_loader(year=year,
                                           entity=entity,
                                           data_dir=data_dir,
                                           drop_dup=drop_dup,
                                           drop_dup_cols=drop_dup_cols)
            dataset.append(data)
        dataset = pd.concat(dataset,axis=1)
        print(f"shape in `entity_batch_loader`:{dataset.shape}")
        return dataset

    def year_batch_loader(self,
                          entities:Union[str,List[str]]="all",
                          year:int=2019,
                          data_dir:str="./",
                          drop_dup:bool=False,
                          drop_dup_cols:List[str]=["hhid"],
                          merge_on:Union[str,List[str]] = ["hhid","year"],
                          save:bool=True,
                          save_file_name:str=None,
                          save_dir:str="./"
                          ):
        """读取指定某一个年份下多个entity["hh","ind","master"]的数据并合并"""
        if entities == "all":
            entities = ["hh","ind","master"]
        dataset = []
        for entity in entities:
            data = self.year_entity_loader(year=year,
                                           entity=entity,
                                           data_dir=data_dir,
                                           drop_dup=drop_dup,
                                           drop_dup_cols=drop_dup_cols)

            dataset.append(data)
        dataset = reduce(lambda left,right: pd.merge(left=left,right=right,on=merge_on,how="outer"),dataset)
        if save:
            save_file_name = str(year)+"_".join(entities)+"_"+str(drop_dup) + "." + "csv"
            save_path = os.path.join(save_dir,save_file_name)
            dataset.to_csv(save_path,index=False,encoding='utf_8_boom')
        print(f"shape in `year_batch_loader`:{dataset.shape}")
        return dataset

    def batch_loader(self,
                     entities:Union[str,List[str]]="all",
                     years:Union[str,List[int]]="all",
                     data_dir="./",
                     drop_dup:bool=False,
                     drop_dup_cols:List[str]=["hhid"],
                     merge_on:Union[str,List[str]] = ["hhid","year"],
                     save:bool=False,
                     save_file_name:str=None,
                     save_dir:str = "./",
                     keep_common_cols:bool=False,
                     keep_common_idx:bool=False,
                     ):
        """批量读取指定多个年份、多个entity[hh,ind,master]的数据"""
        if entities == "all":
            entities = ["hh","ind","master"]
        if years == "all":
            years = [re.search("20[0-9]{2}",file).group(0) for file in os.listdir(data_dir)
                     if re.search("20[0-9]{2}",file)]
        dataset = []
        idx_set_dict = {}
        col_set_dict = {}
        for year in years:
            data = self.year_batch_loader(entities=entities,
                                          year=year,
                                          data_dir=data_dir,
                                          drop_dup=drop_dup,
                                          drop_dup_cols=drop_dup_cols,
                                          merge_on=merge_on)
            idx_set_dict[year] = data['hhid'].to_list()
            col_set_dict[year] = data.columns.to_list()
            dataset.append(data)
        dataset = pd.concat(dataset,axis=1)
        common_idx,_ = self.get_common(idx_set_dict, prefix="hhid") #! 必须是列名
        common_cols,_ = self.get_common(col_set_dict,prefix="cols") 
        if keep_common_cols:
            dataset = self.select_cols(dataset,used_cols=common_cols)
        if keep_common_idx:
            dataset = self.select_idx(dataset,idx_col="hhid",used_idx=common_idx)
        if save:
            if not isinstance(save_file_name,str):
                print("未设置存储到本地的文件名,临时设置为`all_data`")
                save_file_name = "all_data" 
            save_path = os.path.join(save_dir,".".join([save_file_name,"csv"]))
            dataset.to_csv(save_path,index=False)
        print(f"shape in `batch_loader`:{dataset.shape}")
        return dataset

    def set_id(self,data:pd.DataFrame, idx_col:str="hhid"):
        """部分数据没有hhid,而存在hhid_2011,hhid_2013等列，需要从中选出hhid"""
        idx_cols = [col for col in data.columns if re.search(f"{idx_col}_[0-9]{{4}}", col)]
        if idx_col not in data.columns:
            idx_data = data[idx_cols]
            idx_data.rename(columns={raw :int(re.search("[0-9]{4}",raw).group(0)) for raw in idx_cols},inplace=True)
            idx_list = []
            for idx, row in idx_data.iterrows():
                temp = row[row.dropna().idxmin()]
                idx_list.append(temp)
            data[idx_col] = idx_list
        data[idx_col] = data[idx_col].astype(int)
        data = data.drop(columns=idx_cols)
        return data
    
    def select_cols(self,data,used_cols=None) -> pd.DataFrame:
        """根据self.used_cols选择特定的列"""
        if used_cols is not None:
            data_cols = set(data.columns.to_list())
            used_cols = list(data_cols & set(used_cols))
            return data[used_cols]
        else:
            return data
        
    def select_idx(self,data,idx_col="hhid",used_idx=None) -> pd.DataFrame:
        """根据self.used_idx选择特定行"""
        if used_idx is not None:
            used_idx = list(set(used_idx))
            data = data[data[idx_col].isin(used_idx)]
        return data

    def get_common(self, set_dict:dict, save=True, save_path="./",prefix="")->pd.DataFrame:
        longest = max([len(value) for key, value in set_dict.items()])
        common_set = [set(value) for key,value in set_dict.items()]
        common_set = reduce(lambda x,y: x.intersection(y), common_set)
        common_set = pd.Series(list(common_set),name=prefix)
        pad_dict = {key: (value + [0]*(longest - len(value))) for key,value in set_dict.items()}
        pad_dict = pd.DataFrame(pad_dict)
        if save:
            common_set.to_csv(os.path.join(save_path,prefix+"_common_set.csv"),index=False)
            pad_dict.to_csv(os.path.join(save_path,prefix+"_pad_dict.csv"),index=False)

        return common_set, pad_dict


if __name__ == "__main__":
    used_cols = None # 指定需要的变量名
    loader = DataLoader(read_format="txt",
                        used_cols=used_cols)

    # data = loader.year_entity_loader(year=2011,entity="hh")
    # data = loader.year_batch_loader(year=2011,entities="all")
    # data = loader.entity_batch_loader(years="all",entity="hh")
    data = loader.batch_loader(years=[2013,2015,2017,2019], # 
                               entities=["hh","master"],
                               drop_dup=True,
                               merge_on=["hhid","year"],
                               drop_dup_cols=["hhid"],
                               keep_common_idx=True
                               )
    
    # data.to_csv("./all_data.csv",index=None,encoding="utf-8")
    



