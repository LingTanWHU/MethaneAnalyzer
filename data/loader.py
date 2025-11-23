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
    
    def __init__(self, data_root_path: str):
        self.data_root_path = data_root_path
        self.numeric_columns = ['CO2_dry', 'CH4_dry', 'H2O']
        # 使用 CPU 核心数的一半作为线程池大小
        self.max_workers = max(1, mp.cpu_count() // 2)
    
    def _load_single_file(self, file_path: str) -> Optional[pd.DataFrame]:
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
            # 不在并行处理中显示错误，避免过多输出
            return None
    
    def get_filtered_files(self, start_date: datetime, end_date: datetime, timezone: pytz.timezone) -> List[str]:
        """根据起始和终止日期筛选获取数据文件路径，考虑时区转换"""
        data_files = []
        
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