import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import pytz
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
from .database_manager import DatabaseManager

class DataLoader:
    """数据加载器 - 存储1分钟平均数据和标准差，支持重采样"""
    
    def __init__(self, data_source: str = 'picarro', use_db: bool = True):
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
        
        # 数据库相关
        self.use_db = use_db
        if use_db:
            self.db_manager = DatabaseManager()
    
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
    
    def get_all_data_files(self) -> List[str]:
        """获取所有数据文件路径"""
        if self.data_type == 'picarro':
            data_files = []
            years = []
            for item in os.listdir(self.data_root_path):
                item_path = os.path.join(self.data_root_path, item)
                if os.path.isdir(item_path) and item.isdigit():
                    years.append(int(item))
            
            for year in sorted(years):
                year_folder = str(year).zfill(4)
                year_path = os.path.join(self.data_root_path, year_folder)
                
                for root, dirs, files in os.walk(year_path):
                    for file in files:
                        if file.endswith('.dat'):
                            data_files.append(os.path.join(root, file))
            
            return sorted(data_files)
        
        elif self.data_type == 'pico':
            all_txt_files = glob.glob(os.path.join(self.data_root_path, "*.txt"))
            filtered_files = []
            for txt_file in all_txt_files:
                filename = os.path.basename(txt_file)
                if not ('Eng.txt' in filename or 
                       'spectralite.txt' in filename or 
                       'config.txt' in filename) and filename.startswith('Pico'):
                    filtered_files.append(txt_file)
            return sorted(filtered_files)
    
    def sync_database(self, time_window: str = '1min', agg_method: str = 'mean'):
        """
        同步数据库与文件系统 - 生成并存储1分钟平均数据和标准差
        """
        print(f"开始同步{self.data_type}数据并生成{time_window}平均数据...")
        
        # 获取所有数据文件
        all_files = self.get_all_data_files()
        print(f"找到 {len(all_files)} 个数据文件")
        
        # 获取数据库中已有的文件记录
        existing_records = self.db_manager.get_existing_file_records()
        print(f"数据库中已有 {len(existing_records)} 个文件记录")
        
        files_to_process = []
        
        for file_path in all_files:
            file_hash = self.db_manager.calculate_file_hash(file_path)
            file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if file_path in existing_records:
                # 文件已存在，检查是否有更新
                existing_hash = existing_records[file_path]['hash']
                existing_modified = datetime.fromisoformat(existing_records[file_path]['modified'])
                
                if file_hash != existing_hash or file_modified > existing_modified:
                    print(f"文件已更改: {file_path}")
                    files_to_process.append((file_path, file_hash))
                else:
                    print(f"文件未更改，跳过: {file_path}")
            else:
                # 新文件
                print(f"新文件: {file_path}")
                files_to_process.append((file_path, file_hash))
        
        print(f"需要处理 {len(files_to_process)} 个文件")
        
        # 先删除已有的预处理数据
        self.db_manager.delete_old_processed_data_by_time_window(self.data_type, time_window, agg_method)
        
        # 处理需要更新的文件
        total_records = 0
        for file_path, file_hash in files_to_process:
            print(f"处理文件: {file_path}")
            
            # 加载数据
            df = self._load_picarro_file(file_path) if self.data_type == 'picarro' else self._load_pico_file(file_path)
            
            if df is not None and not df.empty:
                # 进行时间平均和标准差处理
                df_avg, df_std = self._process_file_data_with_std(df, time_window, agg_method)
                
                if not df_avg.empty:
                    # 插入预处理数据（平均值和标准差）
                    self.db_manager.insert_processed_data_to_db(df_avg, df_std, self.data_type, time_window, agg_method)
                    total_records += len(df_avg)
                    print(f"已处理并存储: {file_path} ({len(df_avg)} 条记录)")
                    
                    # 更新文件记录
                    self.db_manager.update_file_record(file_path, file_hash, self.data_type, len(df_avg))
            else:
                print(f"警告: 无法加载文件 {file_path}")
        
        print(f"同步完成！共处理 {total_records} 条预处理记录")
    
    def _process_file_data_with_std(self, df: pd.DataFrame, time_window: str, agg_method: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """处理单个文件的数据，进行时间平均和标准差计算"""
        if df.empty or 'DATETIME' not in df.columns:
            return pd.DataFrame(), pd.DataFrame()
        
        # 设置时间列为索引进行重采样
        df_with_index = df.set_index('DATETIME')
        
        # 选择需要聚合的数值列
        numeric_cols = df_with_index.select_dtypes(include=[np.number]).columns.tolist()
        
        if not numeric_cols:
            return pd.DataFrame(), pd.DataFrame()
        
        # 只对数值列进行聚合
        df_numeric = df_with_index[numeric_cols]
        
        # 将旧的时间频率别名转换为新格式
        corrected_time_window = time_window.replace('T', 'min')
        
        # 重采样对象
        resampled = df_numeric.resample(corrected_time_window)
        
        # 根据聚合方法选择函数
        if agg_method == 'mean':
            df_avg = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        elif agg_method == 'median':
            df_avg = resampled.apply(lambda x: x.median() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        else:
            df_avg = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        
        # 将时间列从索引中恢复
        df_avg = df_avg.reset_index()
        df_std = df_std.reset_index()
        
        return df_avg, df_std
    
    def load_processed_data(self, start_datetime: datetime, end_datetime: datetime, 
                           user_timezone: pytz.timezone, time_window: str, agg_method: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """从数据库加载预处理数据并根据需要进行重采样，返回平均值和标准差"""
        if start_datetime is not None and end_datetime is not None and user_timezone:
            # 将用户选择的时区时间转换为UTC时间用于数据库查询
            start_utc = user_timezone.localize(start_datetime).astimezone(pytz.UTC)
            end_utc = user_timezone.localize(end_datetime).astimezone(pytz.UTC)
            
            start_utc_str = start_utc.strftime('%Y-%m-%d %H:%M:%S')
            end_utc_str = end_utc.strftime('%Y-%m-%d %H:%M:%S')
            
            # 如果是1分钟数据，直接从数据库获取
            if time_window == '1min':
                df = self.db_manager.query_processed_data_from_db(
                    self.data_type, start_utc_str, end_utc_str, '1min', agg_method)
                print(f"从预处理数据表获取 {len(df)} 条记录")
            else:
                # 如果是其他时间窗口，先获取1分钟数据，然后重采样
                df_1min = self.db_manager.query_processed_data_from_db(
                    self.data_type, start_utc_str, end_utc_str, '1min', agg_method)
                
                if df_1min.empty:
                    print("没有1分钟数据用于重采样")
                    return pd.DataFrame(), pd.DataFrame()
                
                print(f"从预处理数据表获取 {len(df_1min)} 条1分钟记录，准备重采样到 {time_window}")
                
                # 重采样到指定时间窗口
                df_avg, df_std = self._resample_data_with_std(df_1min, time_window, agg_method)
                # 合并平均值和标准差数据
                df = df_avg.copy()
                for col in df_std.columns:
                    if col != 'DATETIME':
                        std_col_name = f"{col}_std"
                        df[std_col_name] = df_std[col]
            
            if not df.empty and 'DATETIME' in df.columns:
                # 添加 DATETIME_DISPLAY 列用于显示（转换为用户选择的时区）
                df['DATETIME_DISPLAY'] = pd.to_datetime(df['DATETIME']).dt.tz_convert(user_timezone)
            
            # 分离平均值和标准差数据
            if not df.empty:
                avg_cols = [col for col in df.columns if not col.endswith('_std')]
                std_cols = [col for col in df.columns if col.endswith('_std')]
                
                df_avg = df[avg_cols].copy()
                
                # 创建标准差DataFrame
                df_std = df[['DATETIME_DISPLAY']].copy()
                for std_col in std_cols:
                    avg_col = std_col.replace('_std', '')
                    df_std[avg_col] = df[std_col]
            else:
                df_avg = pd.DataFrame()
                df_std = pd.DataFrame()
            
            return df_avg, df_std
        else:
            return pd.DataFrame(), pd.DataFrame()
    
    def _resample_data_with_std(self, df: pd.DataFrame, time_window: str, agg_method: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """对已有的数据进行重采样，同时计算平均值和标准差"""
        if df.empty or 'DATETIME' not in df.columns:
            return pd.DataFrame(), pd.DataFrame()
        
        # 设置时间列为索引进行重采样
        df_with_index = df.set_index('DATETIME')
        
        # 选择需要聚合的数值列（排除DATETIME_DISPLAY列和_std列）
        numeric_cols = [col for col in df_with_index.columns if col not in ['DATETIME_DISPLAY'] and not col.endswith('_std')]
        numeric_cols = [col for col in numeric_cols if pd.api.types.is_numeric_dtype(df_with_index[col])]
        
        if not numeric_cols:
            return pd.DataFrame(), pd.DataFrame()
        
        # 只对数值列进行聚合
        df_numeric = df_with_index[numeric_cols]
        
        # 将旧的时间频率别名转换为新格式
        corrected_time_window = time_window.replace('T', 'min')
        
        # 重采样对象
        resampled = df_numeric.resample(corrected_time_window)
        
        # 根据聚合方法选择函数
        if agg_method == 'mean':
            df_avg = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        elif agg_method == 'median':
            df_avg = resampled.apply(lambda x: x.median() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        else:
            df_avg = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        
        # 将时间列从索引中恢复
        df_avg = df_avg.reset_index()
        df_std = df_std.reset_index()
        
        return df_avg, df_std

# 导出同步函数
def update_database_manually(data_source: str = 'picarro', time_window: str = '1min', agg_method: str = 'mean'):
    """手动更新数据库"""
    loader = DataLoader(data_source=data_source, use_db=True)
    loader.sync_database(time_window, agg_method)