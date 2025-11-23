import streamlit as st
import os
import pytz
from config.settings import (
    TIME_WINDOW_OPTIONS, AGG_METHOD_OPTIONS, TIMEZONE_OPTIONS,
    DEFAULT_TIME_WINDOW_INDEX, DEFAULT_AGG_METHOD_INDEX, DEFAULT_TIMEZONE_INDEX,
    AppConfig
)

def setup_page_config():
    """设置页面配置"""
    st.set_page_config(
        page_title="监测仪数据浏览器",
        layout="wide",
        initial_sidebar_state="expanded"
    )

def setup_sidebar():
    """设置侧边栏并返回配置"""
    config = AppConfig()
    
    st.sidebar.header("数据选择")
    st.sidebar.info(f"数据根路径: {config.DATA_ROOT_PATH}")
    
    # 获取所有年份
    years = []
    if os.path.exists(config.DATA_ROOT_PATH):
        for item in os.listdir(config.DATA_ROOT_PATH):
            item_path = os.path.join(config.DATA_ROOT_PATH, item)
            if os.path.isdir(item_path) and item.isdigit():
                years.append(int(item))
    
    # 年月日筛选 - 使用三列布局
    col1, col2, col3 = st.sidebar.columns([1, 1, 1])
    
    with col1:
        selected_year = st.selectbox(
            "选择年份:", 
            options=sorted(years) if years else [None], 
            index=len(years)-1 if years else None
        )
    
    # 获取月份
    months = []
    if selected_year:
        year_path = os.path.join(config.DATA_ROOT_PATH, str(selected_year).zfill(4))
        if os.path.exists(year_path):
            for item in os.listdir(year_path):
                item_path = os.path.join(year_path, item)
                if os.path.isdir(item_path) and item.isdigit():
                    months.append(int(item))
    
    with col2:
        selected_month = st.selectbox(
            "选择月份:", 
            options=sorted(months) if months else [None], 
            index=len(months)-1 if months else None
        )
    
    # 获取日期
    days = []
    if selected_year and selected_month:
        month_path = os.path.join(config.DATA_ROOT_PATH, str(selected_year).zfill(4), str(selected_month).zfill(2))
        if os.path.exists(month_path):
            for item in os.listdir(month_path):
                item_path = os.path.join(month_path, item)
                if os.path.isdir(item_path) and item.isdigit():
                    days.append(int(item))
    
    with col3:
        selected_day = st.selectbox(
            "选择日期:", 
            options=sorted(days) if days else [None], 
            index=len(days)-1 if days else None
        )
    
    # 数据过滤设置 - 只保留过滤零值的选项
    st.sidebar.header("数据过滤设置")
    filter_zeros = st.sidebar.checkbox("过滤零值", value=True)
    
    # 时区设置
    st.sidebar.header("时区设置")
    selected_tz_key = st.sidebar.selectbox(
        "选择显示时区",
        options=list(TIMEZONE_OPTIONS.keys()),
        index=DEFAULT_TIMEZONE_INDEX
    )
    selected_timezone = pytz.timezone(TIMEZONE_OPTIONS[selected_tz_key])

    # 时间平均设置
    st.sidebar.header("时间平均设置")
    selected_time_window_key = st.sidebar.selectbox(
        "选择时间窗口",
        options=list(TIME_WINDOW_OPTIONS.keys()),
        index=DEFAULT_TIME_WINDOW_INDEX
    )
    selected_time_window = TIME_WINDOW_OPTIONS[selected_time_window_key]
    
    # 聚合方法选择
    selected_agg_method_key = st.sidebar.radio(
        "选择聚合方法",
        options=list(AGG_METHOD_OPTIONS.keys()),
        format_func=lambda x: x,
        index=DEFAULT_AGG_METHOD_INDEX
    )
    selected_agg_method = AGG_METHOD_OPTIONS[selected_agg_method_key]

    # 图形设置
    st.sidebar.header("图形设置")
    # CO2
    use_custom_co2_range = st.sidebar.checkbox("自定义CO2 Y轴范围", value=False)
    if use_custom_co2_range:
        co2_range = st.sidebar.slider(
            "CO2 Y轴范围",
            0.0, 1000.0,
            (0.0, 1000.0)
        )
    else:
        co2_range = None
        
    # CH4
    use_custom_ch4_range = st.sidebar.checkbox("自定义CH4 Y轴范围", value=False)
    if use_custom_ch4_range:
        ch4_range = st.sidebar.slider(
            "CH4 Y轴范围",
            0.0, 10.0,
            (0.0, 10.0)
        )
    else:
        ch4_range = None
        
    # H2O
    use_custom_h2o_range = st.sidebar.checkbox("自定义H2O Y轴范围", value=False)
    if use_custom_h2o_range:
        h2o_range = st.sidebar.slider(
            "H2O Y轴范围",
            0.0, 100.0,
            (0.0, 100.0)
        )
    else:
        h2o_range = None

    # 返回配置字典
    return {
        'data_root_path': config.DATA_ROOT_PATH,
        'selected_year': selected_year,
        'selected_month': selected_month,
        'selected_day': selected_day,
        'filter_zeros': filter_zeros,
        'selected_timezone': selected_timezone,
        'selected_time_window_key': selected_time_window_key,
        'selected_time_window': selected_time_window,
        'selected_agg_method': selected_agg_method,
        'co2_range': co2_range,
        'ch4_range': ch4_range,
        'h2o_range': h2o_range
    }