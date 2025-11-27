import streamlit as st
from datetime import datetime
import pytz
import pandas as pd
from config.settings import (
    TIME_WINDOW_OPTIONS, AGG_METHOD_OPTIONS, TIMEZONE_OPTIONS,
    DEFAULT_TIME_WINDOW_INDEX, DEFAULT_AGG_METHOD_INDEX, DEFAULT_TIMEZONE_INDEX
)
from data.loader import DataLoader
from processing.resampler import DataResampler
from visualization.plotter import DataPlotter
from utils.helpers import setup_sidebar, setup_page_config

@st.cache_data(ttl=300)  # 缓存5分钟
def load_and_process_data(data_source, start_datetime, end_datetime, 
                         timezone_str, time_window, agg_method, filter_zero_values,
                         picarro_concentration_type=None):
    """缓存数据加载和处理过程"""
    # 将时区字符串转换为时区对象
    selected_timezone = pytz.timezone(timezone_str)
    
    # 数据加载
    data_loader = DataLoader(data_source)
    all_files = data_loader.get_filtered_files(
        start_date=start_datetime,
        end_date=end_datetime,
        timezone=selected_timezone
    )
    
    if not all_files:
        return pd.DataFrame(), pd.DataFrame()
    
    # 加载所有数据文件 - 传入时间范围和时区进行数据筛选
    combined_df = data_loader.load_all_files(
        all_files,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        user_timezone=selected_timezone
    )
    
    if combined_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # 数据处理
    resampler = DataResampler()
    processed_df, std_df = resampler.process_data(
        df=combined_df,
        time_window=time_window,
        agg_method=agg_method,
        display_tz=selected_timezone,
        filter_zero_values=filter_zero_values,
        data_source=data_source  # 传入数据源类型
    )
    
    # 对 Picarro 的 H2O 数据进行单位转换：乘以 1e4 (从 % 转换为 ppm)
    if data_source == 'picarro' and 'H2O' in processed_df.columns:
        processed_df['H2O'] = processed_df['H2O'] * 1e4
    
    return processed_df, std_df

def main():
    """主应用函数"""
    # 设置页面配置
    setup_page_config()
    
    # 显示标题
    st.title("气体监测仪数据浏览器")
    st.markdown("---")
    
    # 设置侧边栏
    config = setup_sidebar()
    
    # 检查配置是否有效
    if config is None:
        return
    
    # 使用缓存加载数据 - 传入数据源类型而不是路径
    with st.spinner("正在加载和处理数据..."):
        load_kwargs = {
            'data_source': config['data_source'],
            'start_datetime': config['start_datetime'],
            'end_datetime': config['end_datetime'],
            'timezone_str': config['selected_timezone'].zone,
            'time_window': config['selected_time_window'],
            'agg_method': config['selected_agg_method'],
            'filter_zero_values': config['filter_zero_values']
        }
        
        if config['data_source'] == 'picarro':
            load_kwargs['picarro_concentration_type'] = config['picarro_concentration_type']
        
        processed_df, std_df = load_and_process_data(**load_kwargs)
    
    if processed_df.empty:
        st.warning("处理后没有有效数据")
        return
    
    # 数据可视化
    plotter = DataPlotter()
    fig = plotter.create_plots(
        df=processed_df,
        std_df=std_df,
        time_window=config['selected_time_window_key'],
        agg_method=config['selected_agg_method'],
        co2_range=config['co2_range'],
        ch4_range=config['ch4_range'],
        h2o_range=config['h2o_range'],
        c2h6_range=config.get('c2h6_range', None),
        data_source=config['data_source'],  # 传入数据源类型
        picarro_concentration_type=config.get('picarro_concentration_type', 'dry')  # 传入浓度类型
    )
    
    # 显示数据预览
    st.subheader("数据预览")
    st.dataframe(processed_df.head(10))
    
    # 绘制图表
    # 检查是否包含气体数据
    if config['data_source'] == 'picarro':
        gas_cols = ['CH4_dry', 'CO2_dry', 'H2O'] if config['picarro_concentration_type'] == 'dry' else ['CH4', 'CO2', 'H2O']
        has_gas_data = any(col in processed_df.columns for col in gas_cols)
    else:  # pico
        has_gas_data = any(col in processed_df.columns for col in ['CH4', 'C2H6', 'H2O'])
    
    if has_gas_data:
        st.plotly_chart(fig, use_container_width=True)
        
        # 统计信息
        st.subheader("统计信息")
        
        # 创建多列显示统计信息
        if config['data_source'] == 'picarro':
            gas_cols = ['CH4_dry', 'CO2_dry', 'H2O'] if config['picarro_concentration_type'] == 'dry' else ['CH4', 'CO2', 'H2O']
            gas_cols = [col for col in gas_cols if col in processed_df.columns]
        else:  # pico
            gas_cols = [col for col in ['CH4', 'C2H6', 'H2O'] if col in processed_df.columns]
        
        if len(gas_cols) == 1:
            cols = [st.container()]
        elif len(gas_cols) == 2:
            cols = st.columns(2)
        else:  # 3个或更多
            cols = st.columns(3)
        
        for i, gas_col in enumerate(gas_cols):
            with cols[i % len(cols)]:
                st.write(f"**{gas_col} 统计**")
                gas_stats = processed_df[gas_col].describe()
                st.write(gas_stats)
    else:
        st.warning("数据中不包含气体浓度列")
        st.write("可用列:", list(processed_df.columns))

if __name__ == "__main__":
    main()