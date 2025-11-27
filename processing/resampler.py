import pandas as pd
import numpy as np
import pytz
from typing import Tuple, Optional
import streamlit as st

class DataResampler:
    """数据重采样器"""
    
    def __init__(self):
        # 根据数据源动态设置列名
        pass
    
    def filter_zero_values(self, df: pd.DataFrame, data_source: str = 'picarro') -> pd.DataFrame:
        """过滤掉气体的零值（= 0）"""
        if df.empty:
            return df
        
        condition = True
        if data_source == 'picarro':
            if 'CO2_dry' in df.columns:
                condition = condition & (df['CO2_dry'] != 0)
            if 'CH4_dry' in df.columns:
                condition = condition & (df['CH4_dry'] != 0)
            if 'H2O' in df.columns:
                condition = condition & (df['H2O'] != 0)
        else:  # pico
            if 'CH4' in df.columns:
                condition = condition & (df['CH4'] != 0)
            if 'C2H6' in df.columns:
                condition = condition & (df['C2H6'] != 0)
            if 'H2O' in df.columns:
                condition = condition & (df['H2O'] != 0)
        
        return df[condition].copy()
    
    def resample_data_with_uncertainty(self, df: pd.DataFrame, 
                                     time_window: Optional[str], 
                                     agg_method: str = 'mean', 
                                     time_col: str = 'DATETIME_DISPLAY') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """对数据进行重采样（平均或中位数），并计算标准差作为不确定度"""
        if df.empty or time_col not in df.columns or time_window is None:
            return df, pd.DataFrame()
        
        # 设置时间列为索引进行重采样
        df_with_index = df.set_index(time_col)
        
        # 选择需要聚合的数值列
        numeric_cols = df_with_index.select_dtypes(include=[np.number]).columns.tolist()
        
        if not numeric_cols:
            return df, pd.DataFrame()
        
        # 只对数值列进行聚合
        df_numeric = df_with_index[numeric_cols]
        
        # 将旧的时间频率别名转换为新格式
        if time_window:
            # 将 'T' 替换为 'min'
            corrected_time_window = time_window.replace('T', 'min')
        else:
            corrected_time_window = time_window
        
        # 重采样对象
        resampled = df_numeric.resample(corrected_time_window)
        
        # 根据聚合方法选择函数
        if agg_method == 'mean':
            df_resampled = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        elif agg_method == 'median':
            df_resampled = resampled.apply(lambda x: x.median() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        else:
            df_resampled = resampled.apply(lambda x: x.mean() if len(x.dropna()) > 0 else np.nan)
            df_std = resampled.apply(lambda x: x.std() if len(x.dropna()) > 0 else np.nan)
        
        # 将时间列从索引中恢复
        df_resampled = df_resampled.reset_index()
        df_std = df_std.reset_index()
        
        return df_resampled, df_std
    
    def process_data(self, df: pd.DataFrame, time_window: Optional[str], 
                    agg_method: str, display_tz: pytz.timezone, 
                    filter_zero_values: bool = True, data_source: str = 'picarro') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """完整的数据处理流程"""
        original_count = len(df)
        
        # 转换时区用于显示
        if 'DATETIME' in df.columns:
            # 将UTC时间转换为用户选择的时区用于显示
            df['DATETIME_DISPLAY'] = df['DATETIME'].dt.tz_convert(display_tz)
        else:
            df['DATETIME_DISPLAY'] = df.index
        
        # 应用零值过滤
        if filter_zero_values:
            df = self.filter_zero_values(df, data_source)
            filtered_count = len(df)
            st.info(f"数据过滤 (零值): {original_count} -> {filtered_count} 条记录")
        else:
            st.info(f"原始数据记录数: {original_count}")
        
        if df.empty:
            return df, pd.DataFrame()
        
        # 应用时间平均/中位数和标准差计算
        processed_df, std_df = self.resample_data_with_uncertainty(df, time_window, agg_method, 'DATETIME_DISPLAY')
        
        # 重新计算记录数
        if time_window is not None:
            after_resample_count = len(processed_df)
            st.info(f"应用时间平均后: {len(df)} -> {after_resample_count} 条记录")
        
        # 按时间排序
        if 'DATETIME_DISPLAY' in processed_df.columns:
            processed_df = processed_df.sort_values('DATETIME_DISPLAY').reset_index(drop=True)
        
        return processed_df, std_df