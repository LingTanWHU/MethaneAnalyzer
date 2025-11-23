import streamlit as st
from config.settings import (
    TIME_WINDOW_OPTIONS, AGG_METHOD_OPTIONS, TIMEZONE_OPTIONS,
    DEFAULT_TIME_WINDOW_INDEX, DEFAULT_AGG_METHOD_INDEX, DEFAULT_TIMEZONE_INDEX
)
from data.loader import DataLoader
from processing.resampler import DataResampler
from visualization.plotter import DataPlotter
from utils.helpers import setup_sidebar, setup_page_config

def main():
    """主应用函数"""
    # 设置页面配置
    setup_page_config()
    
    # 显示标题
    st.title("气体监测仪数据浏览器")
    st.markdown("---")
    
    # 设置侧边栏
    config = setup_sidebar()
    
    # 数据加载
    data_loader = DataLoader(config['data_root_path'])
    all_files = data_loader.get_filtered_files(
        year=config['selected_year'],
        month=config['selected_month'],
        day=config['selected_day']
    )
    
    if not all_files:
        st.warning("未找到符合条件的数据文件")
        return
    
    # 显示找到的文件数量
    st.sidebar.success(f"找到 {len(all_files)} 个数据文件")
    
    # 加载所有数据文件
    combined_df = data_loader.load_all_files(all_files)
    
    if combined_df.empty:
        st.error("无法加载任何数据文件")
        return
    
    # 数据处理
    resampler = DataResampler()
    processed_df, std_df = resampler.process_data(
        df=combined_df,
        time_window=config['selected_time_window'],
        agg_method=config['selected_agg_method'],
        display_tz=config['selected_timezone'],
        filter_zeros=config['filter_zeros']
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
    col1.metric("总记录数", len(processed_df))
    col2.metric("数据时间范围", 
               f"{processed_df['DATETIME_DISPLAY'].min()} 至 {processed_df['DATETIME_DISPLAY'].max()}" 
               if 'DATETIME_DISPLAY' in processed_df.columns else "N/A")
    # 更新气体数据统计显示
    available_gases = []
    if 'CO2_dry' in processed_df.columns:
        available_gases.append(f"CO2_dry: {processed_df['CO2_dry'].count()}")
    if 'CH4_dry' in processed_df.columns:
        available_gases.append(f"CH4_dry: {processed_df['CH4_dry'].count()}")
    if 'H2O' in processed_df.columns:
        available_gases.append(f"H2O: {processed_df['H2O'].count()}")
    col3.metric("可用气体数据", ", ".join(available_gases) if available_gases else "N/A")
    
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