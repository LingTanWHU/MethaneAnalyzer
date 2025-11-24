import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class AppConfig:
    """应用配置"""
    picarro_data_root_path: str = r'Y:\公共空间\Data 数据 结果\监测仪数据\DataLog_User'
    pico_data_root_path: str = r'Y:\公共空间\Data 数据 结果\监测仪数据\MIRA_Data'
    # 从环境变量获取路径，如果不存在则使用默认值
    PICARRO_DATA_ROOT_PATH: str = os.getenv('PICARRO_DATA_ROOT_PATH', picarro_data_root_path)
    PICO_DATA_ROOT_PATH: str = os.getenv('PICO_DATA_ROOT_PATH', pico_data_root_path)

# 时间窗口选项 - 使用新格式
TIME_WINDOW_OPTIONS = {
    "原始 (无平均)": None,
    "30秒平均": "30S",
    "1分钟平均": "1min", 
    "5分钟平均": "5min",
    "10分钟平均": "10min",
    "30分钟平均": "30min",
    "1小时平均": "1H",
}

DEFAULT_TIME_WINDOW_INDEX = 2  # 默认为5分钟平均
DEFAULT_AGG_METHOD_INDEX = 0   # 默认为平均值
DEFAULT_TIMEZONE_INDEX = 0     # 默认为UTC+8

# 聚合方法选项
AGG_METHOD_OPTIONS = {
    "平均值": "mean",
    "中位数": "median"
}

# 时区选项
TIMEZONE_OPTIONS = {
    "UTC+8 (Asia/Shanghai)": "Asia/Shanghai",
    "UTC": "UTC",
    "UTC+0 (London)": "Europe/London",
    "UTC-5 (New York)": "America/New_York",
}

# 数据源选项
DATA_SOURCE_OPTIONS = {
    "Picarro": "picarro",
    "Pico": "pico"
}

# Picarro 数据显示选项
PICARRO_CONCENTRATION_OPTIONS = {
    "干基浓度": "dry",
    "原浓度": "raw"
}