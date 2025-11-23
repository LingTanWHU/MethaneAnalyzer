import streamlit as st
from datetime import datetime
import pytz
from config.settings import (
    TIME_WINDOW_OPTIONS, AGG_METHOD_OPTIONS, TIMEZONE_OPTIONS,
    DEFAULT_TIME_WINDOW_INDEX, DEFAULT_AGG_METHOD_INDEX, DEFAULT_TIMEZONE_INDEX
)
from data.loader import DataLoader
from processing.resampler import DataResampler
from visualization.plotter import DataPlotter
from utils.helpers import setup_sidebar, setup_page_config

@st.cache_data(ttl=300)  # 缓存5分钟
def load_and_process_data(data_root_path, start_datetime, end_datetime, 
                         timezone_str, time_window, agg_method, filter_zeros):
    """缓存数据加载和处理过程"""
    # 将时区字符串转换为时区对象
    selected_timezone = pytz.timezone(timezone_str)
    
    # 数据加载
    data_loader = DataLoader(data_root_path)
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
        filter_zeros=filter_zeros
    )
    
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
    
    # 使用缓存加载数据 - 传入时区字符串而不是时区对象
    with st.spinner("正在加载和处理数据..."):
        processed_df, std_df = load_and_process_data(
            config['data_root_path'],
            config['start_datetime'],
            config['end_datetime'],
            config['selected_timezone'].zone,  # 传入时区字符串
            config['selected_time_window'],
            config['selected_agg_method'],
            config['filter_zeros']
        )
    
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
        h2o_range=config['h2o_range']
    )
    
    # 显示数据概览
    st.subheader("数据概览")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="总记录数", value=len(processed_df))
    
    with col2:
        if 'DATETIME_DISPLAY' in processed_df.columns:
            start_time = processed_df['DATETIME_DISPLAY'].min()
            end_time = processed_df['DATETIME_DISPLAY'].max()
            # 精简时间显示格式
            time_range = f"{start_time.strftime('%m-%d %H:%M')} 至 {end_time.strftime('%m-%d %H:%M')}"
            st.metric(label="时间范围", value=time_range)
        else:
            st.metric(label="时间范围", value="N/A")
    
    with col3:
        # 精简气体数据统计显示
        gas_counts = []
        if 'CO2_dry' in processed_df.columns:
            gas_counts.append(f"CO2: {processed_df['CO2_dry'].count()}")
        if 'CH4_dry' in processed_df.columns:
            gas_counts.append(f"CH4: {processed_df['CH4_dry'].count()}")
        if 'H2O' in processed_df.columns:
            gas_counts.append(f"H2O: {processed_df['H2O'].count()}")
        
        gas_summary = ", ".join(gas_counts) if gas_counts else "N/A"
        st.metric(label="气体数据", value=gas_summary)
    
    # 显示数据预览
    st.subheader("数据预览")
    st.dataframe(processed_df.head(10))
    
    # 绘制图表
    # 检查是否包含气体数据
    has_gas_data = any(col in processed_df.columns for col in ['CO2_dry', 'CH4_dry', 'H2O'])
    if has_gas_data:
        st.plotly_chart(fig, use_container_width=True)
        
        # 统计信息 - 添加 H2O 统计
        st.subheader("统计信息")
        
        # 创建多列显示统计信息
        gas_cols = [col for col in ['CO2_dry', 'CH4_dry', 'H2O'] if col in processed_df.columns]
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