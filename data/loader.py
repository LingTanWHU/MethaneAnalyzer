import pandas as pd
import numpy as np
import os
import glob
from typing import List, Optional
import streamlit as st

class DataLoader:
    """数据加载器"""
    
    def __init__(self, data_root_path: str):
        self.data_root_path = data_root_path
        self.numeric_columns = ['CO2_dry', 'CH4_dry', 'H2O']  # 添加 H2O
    
    def load_data_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """加载单个.dat文件"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            header_line = None
            data_start = 0
            
            for i, line in enumerate(lines):
                if line.strip().startswith('DATE'):
                    header_line = line.strip()
                    data_start = i + 1
                    break
            
            if header_line is None:
                return None
            
            headers = [h.strip() for h in header_line.split()]
            data_lines = []
            
            for line in lines[data_start:]:
                line = line.strip()
                if line:
                    parts = line.split()
                    if len(parts) >= len(headers):
                        data_lines.append(parts[:len(headers)])
            
            if not data_lines:
                return None
            
            df = pd.DataFrame(data_lines, columns=headers)
            
            # 转换数值列 - 添加 H2O
            for col in self.numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 转换日期时间
            if 'DATE' in df.columns and 'TIME' in df.columns:
                df['DATETIME'] = pd.to_datetime(
                    df['DATE'] + ' ' + df['TIME'].str.split('.').str[0], 
                    format='%Y-%m-%d %H:%M:%S', 
                    errors='coerce'
                )
                # 假设原始时间是 UTC，标记为 UTC
                df['DATETIME'] = df['DATETIME'].dt.tz_localize('UTC')
            
            return df
            
        except Exception as e:
            st.error(f"读取文件 {file_path} 时出错: {str(e)}")
            return None
    
    def get_filtered_files(self, year: Optional[int] = None, 
                          month: Optional[int] = None, 
                          day: Optional[int] = None) -> List[str]:
        """根据年月日筛选获取数据文件路径"""
        data_files = []
        
        # 遍历年份文件夹
        years = [str(year).zfill(4)] if year else os.listdir(self.data_root_path)
        for year_folder in years:
            year_folder = str(year_folder).zfill(4)
            year_path = os.path.join(self.data_root_path, year_folder)
            if os.path.isdir(year_path) and year_folder.isdigit():
                if year and int(year_folder) != year:
                    continue
                    
                # 遍历月份文件夹
                months = [str(month).zfill(2)] if month else os.listdir(year_path)
                for month_folder in months:
                    month_folder = str(month_folder).zfill(2)
                    month_path = os.path.join(year_path, month_folder)
                    if os.path.isdir(month_path) and month_folder.isdigit():
                        if month and int(month_folder) != month:
                            continue
                            
                        # 遍历日期文件夹
                        days = [str(day).zfill(2)] if day else os.listdir(month_path)
                        for day_folder in days:
                            day_folder = str(day_folder).zfill(2)
                            day_path = os.path.join(month_path, day_folder)
                            if os.path.isdir(day_path) and day_folder.isdigit():
                                if day and int(day_folder) != day:
                                    continue
                                    
                                # 查找.dat文件
                                dat_files = glob.glob(os.path.join(day_path, "*.dat"))
                                for dat_file in dat_files:
                                    data_files.append(dat_file)
        
        return sorted(data_files)
    
    def load_all_files(self, file_paths: List[str]) -> pd.DataFrame:
        """加载所有符合条件的文件并合并"""
        all_data = []
        for file_path in file_paths:
            with st.spinner(f"正在加载数据文件: {os.path.basename(file_path)}"):
                df = self.load_data_file(file_path)
                if df is not None and not df.empty:
                    all_data.append(df)
        
        if not all_data:
            st.error("没有加载到任何有效数据")
            return pd.DataFrame()
        
        return pd.concat(all_data, ignore_index=True)