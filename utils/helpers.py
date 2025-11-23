import streamlit as st
import os
import glob  # 添加这个导入
import pytz
from datetime import datetime, time, timedelta, date
from config.settings import (
    TIME_WINDOW_OPTIONS, AGG_METHOD_OPTIONS, TIMEZONE_OPTIONS, DATA_SOURCE_OPTIONS,
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

def scan_available_dates_picarro(data_root_path: str, start_year: int = None, end_year: int = None) -> set:
    """扫描 Picarro 数据目录，获取所有有数据的日期"""
    available_dates = set()
    
    # 确定年份范围
    years = []
    for item in os.listdir(data_root_path):
        item_path = os.path.join(data_root_path, item)
        if os.path.isdir(item_path) and item.isdigit():
            year = int(item)
            if (start_year is None or year >= start_year) and (end_year is None or year <= end_year):
                years.append(year)
    
    for year in sorted(years):
        year_path = os.path.join(data_root_path, str(year).zfill(4))
        
        for month_item in os.listdir(year_path):
            month_path = os.path.join(year_path, month_item)
            if os.path.isdir(month_path) and month_item.isdigit():
                month = int(month_item)
                
                for day_item in os.listdir(month_path):
                    day_path = os.path.join(month_path, day_item)
                    if os.path.isdir(day_path) and day_item.isdigit():
                        day = int(day_item)
                        
                        # 检查该日期文件夹下是否有 .dat 文件
                        dat_files = [f for f in os.listdir(day_path) if f.endswith('.dat')]
                        if dat_files:  # 如果有 .dat 文件，说明该日期有数据
                            available_dates.add(datetime(year, month, day).date())
    
    return available_dates

def scan_available_dates_pico(data_root_path: str) -> set:
    """扫描 Pico 数据目录，获取所有有数据的日期"""
    available_dates = set()
    
    # 查找所有匹配的 .txt 文件
    all_txt_files = glob.glob(os.path.join(data_root_path, "*.txt"))
    
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
                    available_dates.add(file_date)
            except:
                # 如果解析失败，跳过这个文件
                continue
    
    return available_dates

def display_data_availability(available_dates: set):
    """显示数据可用性表格"""
    if not available_dates:
        st.sidebar.warning("未找到任何数据文件")
        return
    
    st.sidebar.header("数据可用性")
    
    # 获取最近30天的日期
    today = date.today()
    last_30_days = [today - timedelta(days=i) for i in range(29, -1, -1)]  # 最近30天
    
    # 创建一个完整的日历矩阵 (6行 x 7列 = 42个位置，足够显示30天)
    calendar_matrix = [['' for _ in range(7)] for _ in range(6)]
    
    # 获取第一个日期是星期几
    first_date = last_30_days[0]
    first_weekday = first_date.weekday()  # 0=Monday, 6=Sunday
    
    # 填充日历矩阵
    for i, check_date in enumerate(last_30_days):
        # 计算相对于第一个日期的偏移量
        offset_days = (check_date - first_date).days
        # 计算是星期几
        day_of_week = (first_weekday + offset_days) % 7  # 0=Monday, 6=Sunday
        # 计算是第几周
        week_num = (first_weekday + offset_days) // 7
        
        has_data = check_date in available_dates
        color = "🟢" if has_data else "🔴"
        day_str = f"{color} {check_date.day:02d}"
        
        # 在对应位置填入数据
        if 0 <= week_num < 6 and 0 <= day_of_week < 7:  # 确保不超出矩阵范围
            calendar_matrix[week_num][day_of_week] = day_str
    
    # 显示星期标题
    day_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    cols = st.sidebar.columns(7)
    for i, day_name in enumerate(day_names):
        cols[i].write(f"**{day_name}**")
    
    # 显示日历内容
    for week_row in calendar_matrix:
        cols = st.sidebar.columns(7)
        for i, day_content in enumerate(week_row):
            if day_content:  # 如果有内容才显示
                cols[i].write(day_content)
            else:
                cols[i].write("")

def setup_sidebar():
    """设置侧边栏并返回配置"""
    config = AppConfig()
    
    st.sidebar.header("数据选择")
    
    # 数据源切换
    data_source_key = st.sidebar.selectbox(
        "选择数据源",
        options=list(DATA_SOURCE_OPTIONS.keys()),
        index=0  # 默认为 Picarro
    )
    data_source = DATA_SOURCE_OPTIONS[data_source_key]
    
    if data_source == 'picarro':
        data_root_path = config.PICARRO_DATA_ROOT_PATH
        st.sidebar.info(f"数据根路径: {data_root_path}")
        
        # 扫描并显示 Picarro 可用数据日期
        if os.path.exists(data_root_path):
            with st.spinner("正在扫描数据目录..."):
                available_dates = scan_available_dates_picarro(data_root_path)
            
            # 显示数据可用性
            display_data_availability(available_dates)
            
            # 找到最近有数据的日期
            if available_dates:
                latest_data_date = max(available_dates)  # 最近的有数据日期
            else:
                latest_data_date = datetime.now().date() - timedelta(days=1)  # 如果没有数据，使用前一天
        else:
            latest_data_date = datetime.now().date() - timedelta(days=1)
    
    elif data_source == 'pico':
        data_root_path = config.PICO_DATA_ROOT_PATH
        st.sidebar.info(f"数据根路径: {data_root_path}")
        
        # 扫描并显示 Pico 可用数据日期
        if os.path.exists(data_root_path):
            with st.spinner("正在扫描数据目录..."):
                available_dates = scan_available_dates_pico(data_root_path)
            
            # 显示数据可用性
            display_data_availability(available_dates)
            
            # 找到最近有数据的日期
            if available_dates:
                latest_data_date = max(available_dates)  # 最近的有数据日期
            else:
                latest_data_date = datetime.now().date() - timedelta(days=1)  # 如果没有数据，使用前一天
        else:
            latest_data_date = datetime.now().date() - timedelta(days=1)
    
    # 时区设置 - 移到时间设置之前
    st.sidebar.header("时区设置")
    selected_tz_key = st.sidebar.selectbox(
        "选择显示时区",
        options=list(TIMEZONE_OPTIONS.keys()),
        index=DEFAULT_TIMEZONE_INDEX
    )
    selected_timezone = pytz.timezone(TIMEZONE_OPTIONS[selected_tz_key])
    
    # 时间范围设置 - 使用日期时间选择器
    st.sidebar.header("时间范围设置")
    
    # 默认为最近有数据的那一天
    default_start_date = latest_data_date
    default_start_time = time(0, 0)
    default_end_date = latest_data_date
    default_end_time = time(23, 59)
    
    # 起始日期时间
    start_date_col, start_time_col = st.sidebar.columns([1, 1])
    with start_date_col:
        start_date = st.date_input(
            "起始日期",
            value=default_start_date,
            max_value=datetime.now().date()
        )
    with start_time_col:
        start_time = st.time_input(
            "起始时间",
            value=default_start_time
        )
    
    # 终止日期时间
    end_date_col, end_time_col = st.sidebar.columns([1, 1])
    with end_date_col:
        end_date = st.date_input(
            "终止日期",
            value=default_end_date,
            max_value=datetime.now().date()
        )
    with end_time_col:
        end_time = st.time_input(
            "终止时间",
            value=default_end_time
        )
    
    # 合并日期和时间
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    # 确保起始时间不晚于终止时间
    if start_datetime > end_datetime:
        st.sidebar.error("起始时间不能晚于终止时间")
        return None

    # 数据过滤设置 - 过滤非正数，默认开启
    st.sidebar.header("数据过滤设置")
    filter_non_positive = st.sidebar.checkbox("过滤非正数 (≤ 0)", value=True)  # 默认开启

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
    use_custom_co2_range = st.sidebar.checkbox("自定义CO2 Y轴范围", value=False) if data_source == 'picarro' else False
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
        
    # C2H6 (仅 Pico)
    use_custom_c2h6_range = st.sidebar.checkbox("自定义C2H6 Y轴范围", value=False) if data_source == 'pico' else False
    if use_custom_c2h6_range:
        c2h6_range = st.sidebar.slider(
            "C2H6 Y轴范围",
            0.0, 1000.0,
            (0.0, 1000.0)
        )
    else:
        c2h6_range = None

    # 返回配置字典
    config_dict = {
        'data_source': data_source,
        'data_root_path': data_root_path,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'filter_non_positive': filter_non_positive,  # 修改参数名
        'selected_timezone': selected_timezone,
        'selected_time_window_key': selected_time_window_key,
        'selected_time_window': selected_time_window,
        'selected_agg_method': selected_agg_method,
        'co2_range': co2_range,
        'ch4_range': ch4_range,
        'h2o_range': h2o_range
    }
    
    if data_source == 'pico':
        config_dict['c2h6_range'] = c2h6_range
    
    return config_dict