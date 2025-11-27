import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import pytz
from typing import List, Optional
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp

class DataLoader:
    """数据加载器"""
    
    def __init__(self, data_source: str = 'picarro'):
        if data_source == 'picarro':
            self.data_root_path = os.getenv('PICARRO_DATA_ROOT_PATH', r'D:\Users\why\Documents\DataLog_User')
            self.numeric_columns = ['CO2_dry', 'CH4_dry', 'H2O', 'CO2', 'CH4']  # 添加原始浓度列
            self.data_type = 'picarro'
        elif data_source == 'pico':
            self.data_root_path = os.getenv('PICO_DATA_ROOT_PATH', r'D:\Users\why\Documents\MIRA_Data')
            self.numeric_columns = ['CH4', 'C2H6', 'H2O']  # Pico 数据列
            self.data_type = 'pico'
        else:
            raise ValueError(f"不支持的数据源类型: {data_source}")
        
        # 使用 CPU 核心数的一半作为线程池大小
        self.max_workers = max(1, mp.cpu_count() // 2)
    
    def _load_picarro_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """加载 Picarro .dat 文件"""
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
            
            # 转换数值列 - 包括干基和原始浓度
            for col in ['CO2_dry', 'CH4_dry', 'H2O', 'CO2', 'CH4']:
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
            return None
    
    def _load_pico_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """加载 Pico .txt 文件"""
        try:
            df = pd.read_csv(file_path)
            
            # 重命名列以匹配标准格式
            column_mapping = {
                'Time Stamp': 'DATETIME',
                'CH4 (ppm)': 'CH4',
                'C2H6 (ppb)': 'C2H6',  # C2H6 单位是 ppb
                'H2O (ppm)': 'H2O',
                'Tgas(degC)': 'Tgas'
            }
            
            # 重命名存在的列
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # 转换数值列
            for col in ['CH4', 'C2H6', 'H2O', 'Tgas']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 转换日期时间
            if 'DATETIME' in df.columns:
                df['DATETIME'] = pd.to_datetime(df['DATETIME'], format='%m/%d/%Y %H:%M:%S.%f', errors='coerce')
                # Pico 数据时间已经是 UTC+8，需要先标记为 UTC+8，然后转换为 UTC
                df['DATETIME'] = df['DATETIME'].dt.tz_localize('Asia/Shanghai')
                # 转换为 UTC 以便与其他数据保持一致
                df['DATETIME'] = df['DATETIME'].dt.tz_convert('UTC')
            
            return df
            
        except Exception as e:
            return None
    
    def _load_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """根据数据类型加载单个文件"""
        if self.data_type == 'picarro':
            return self._load_picarro_file(file_path)
        elif self.data_type == 'pico':
            return self._load_pico_file(file_path)
        else:
            return None
    
    def get_filtered_files(self, start_date: datetime, end_date: datetime, timezone: pytz.timezone) -> List[str]:
        """根据起始和终止日期筛选获取数据文件路径"""
        data_files = []
        
        if self.data_type == 'picarro':
            # Picarro 数据的文件结构 - 需要考虑时区转换导致的日期边界问题
            # 将用户指定的时间范围转换为UTC时间，以匹配数据文件中的日期
            # 考虑到时区转换可能导致日期变化，需要扩展搜索范围
            
            # 将用户时间转换为指定时区
            start_date_tz = timezone.localize(datetime.combine(start_date.date(), datetime.min.time()))
            end_date_tz = timezone.localize(datetime.combine(end_date.date(), datetime.max.time()))
            
            # 转换为UTC时间
            start_date_utc = start_date_tz.astimezone(pytz.UTC)
            end_date_utc = end_date_tz.astimezone(pytz.UTC)
            
            # 计算需要搜索的日期范围（考虑时区转换可能跨越的日期）
            search_start_date = start_date_utc.date() - timedelta(days=1)  # 向前扩展一天
            search_end_date = end_date_utc.date() + timedelta(days=1)      # 向后扩展一天
            
            # 遍历年份文件夹
            years = []
            for item in os.listdir(self.data_root_path):
                item_path = os.path.join(self.data_root_path, item)
                if os.path.isdir(item_path) and item.isdigit():
                    year = int(item)
                    if search_start_date.year <= year <= search_end_date.year:
                        years.append(year)
            
            for year in sorted(years):
                year_folder = str(year).zfill(4)
                year_path = os.path.join(self.data_root_path, year_folder)
                
                # 遍历月份文件夹
                months = []
                start_month = search_start_date.month if year == search_start_date.year else 1
                end_month = search_end_date.month if year == search_end_date.year else 12
                
                for item in os.listdir(year_path):
                    item_path = os.path.join(year_path, item)
                    if os.path.isdir(item_path) and item.isdigit():
                        month = int(item)
                        if start_month <= month <= end_month:
                            months.append(month)
                
                for month in sorted(months):
                    month_folder = str(month).zfill(2)
                    month_path = os.path.join(year_path, month_folder)
                    
                    # 遍历日期文件夹
                    days = []
                    start_day = search_start_date.day if year == search_start_date.year and month == search_start_date.month else 1
                    end_day = search_end_date.day if year == search_end_date.year and month == search_end_date.month else 31
                    
                    for item in os.listdir(month_path):
                        item_path = os.path.join(month_path, item)
                        if os.path.isdir(item_path) and item.isdigit():
                            day = int(item)
                            if start_day <= day <= end_day:
                                days.append(day)
                    
                    for day in sorted(days):
                        day_folder = str(day).zfill(2)
                        day_path = os.path.join(month_path, day_folder)
                        
                        # 查找.dat文件
                        dat_files = glob.glob(os.path.join(day_path, "*.dat"))
                        for dat_file in dat_files:
                            data_files.append(dat_file)
        
        elif self.data_type == 'pico':
            # Pico 数据的文件结构 - 需要考虑跨天问题
            # 扩展搜索范围：开始日期前一天到结束日期后一天
            search_start_date = start_date.date() - timedelta(days=1)
            search_end_date = end_date.date() + timedelta(days=1)
            
            # 查找所有匹配的 .txt 文件，但排除不需要的文件
            all_txt_files = glob.glob(os.path.join(self.data_root_path, "*.txt"))
            
            for txt_file in all_txt_files:
                filename = os.path.basename(txt_file)
                # 排除不需要的文件
                if ('Eng.txt' in filename or 
                    'spectralite.txt' in filename or 
                    'config.txt' in filename):
                    continue
                
                # 检查文件名是否符合 Pico 数据格式: Pico101244_251106_185816.txt
                if filename.startswith('Pico') and filename.endswith('.txt'):
                    try:
                        # 从文件名提取时间信息: Pico101244_251106_185816.txt
                        # 提取日期部分: 251106 -> 2025-11-06
                        name_part = filename.replace('Pico', '').replace('.txt', '')
                        if '_' in name_part:
                            date_part = name_part.split('_')[1]  # 251106
                            year = int('20' + date_part[:2])
                            month = int(date_part[2:4])
                            day = int(date_part[4:6])
                            
                            file_date = datetime(year, month, day).date()
                            
                            # 检查文件日期是否在扩展的搜索范围内
                            if search_start_date <= file_date <= search_end_date:
                                data_files.append(txt_file)
                    except:
                        # 如果解析失败，跳过这个文件
                        continue
        
        return sorted(data_files)
    
    def load_all_files(self, file_paths: List[str], start_datetime: datetime = None, 
                      end_datetime: datetime = None, user_timezone: pytz.timezone = None) -> pd.DataFrame:
        """使用多线程加载所有符合条件的文件并合并，同时进行时间筛选"""
        if not file_paths:
            return pd.DataFrame()
        
        all_data = []
        
        # 将用户指定的时间范围转换为UTC时间以匹配数据
        start_utc = None
        end_utc = None
        if start_datetime is not None and end_datetime is not None and user_timezone:
            start_utc = user_timezone.localize(start_datetime).astimezone(pytz.UTC)
            end_utc = user_timezone.localize(end_datetime).astimezone(pytz.UTC)
        
        # 使用线程池并行加载文件
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有文件加载任务
            future_to_file = {executor.submit(self._load_single_file, file_path): file_path 
                             for file_path in file_paths}
            
            # 收集结果
            loaded_data = []
            for future in as_completed(future_to_file):
                df = future.result()
                if df is not None and not df.empty:
                    # 如果指定了时间范围，则进行时间筛选
                    if start_utc is not None and end_utc is not None:
                        if 'DATETIME' in df.columns:
                            df = df[(df['DATETIME'] >= start_utc) & (df['DATETIME'] <= end_utc)]
                    
                    if not df.empty:
                        loaded_data.append(df)
        
        if not loaded_data:
            return pd.DataFrame()
        
        return pd.concat(loaded_data, ignore_index=True)