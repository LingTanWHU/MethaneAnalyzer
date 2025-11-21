import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime
import glob
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go

# 加载环境变量
load_dotenv()

def load_data_file(file_path):
    """
    加载.dat文件并解析数据
    """
    try:
        # 读取文件的前几行来确定分隔符和列数
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        # 找到数据开始的行（跳过标题行）
        header_line = None
        data_start = 0
        
        for i, line in enumerate(lines):
            if line.strip().startswith('DATE'):
                header_line = line.strip()
                data_start = i + 1
                break
        
        if header_line is None:
            return None
            
        # 解析标题
        headers = [h.strip() for h in header_line.split()]
        
        # 读取数据行
        data_lines = []
        for line in lines[data_start:]:
            line = line.strip()
            if line:
                # 分割数据行 - 使用空格分割，但保持数据完整性
                parts = line.split()
                if len(parts) >= len(headers):
                    data_lines.append(parts[:len(headers)])
        
        if not data_lines:
            return None
            
        # 创建DataFrame
        df = pd.DataFrame(data_lines, columns=headers)
        
        # 转换数值列
        numeric_columns = ['CO2_dry', 'CH4_dry']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 转换日期时间
        if 'DATE' in df.columns and 'TIME' in df.columns:
            df['DATETIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'].str.split('.').str[0], 
                                         format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        return df
        
    except Exception as e:
        st.error(f"读取文件 {file_path} 时出错: {str(e)}")
        return None

def get_data_files(base_path):
    """
    获取所有数据文件路径
    """
    data_files = []
    
    # 遍历年份文件夹
    for year_folder in os.listdir(base_path):
        year_path = os.path.join(base_path, year_folder)
        if os.path.isdir(year_path) and year_folder.isdigit():
            # 遍历月份文件夹
            for month_folder in os.listdir(year_path):
                month_path = os.path.join(year_path, month_folder)
                if os.path.isdir(month_path) and month_folder.isdigit():
                    # 遍历日期文件夹
                    for day_folder in os.listdir(month_path):
                        day_path = os.path.join(month_path, day_folder)
                        if os.path.isdir(day_path) and day_folder.isdigit():
                            # 查找.dat文件
                            dat_files = glob.glob(os.path.join(day_path, "*.dat"))
                            for dat_file in dat_files:
                                data_files.append(dat_file)
    
    return sorted(data_files)

def main():
    st.set_page_config(
        page_title="监测仪数据浏览器",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("气体监测仪数据浏览器")
    st.markdown("---")
    
    # 从环境变量获取数据根路径
    data_root = os.getenv('DATA_ROOT_PATH', r'Y:\公共空间\Data 数据 结果\监测仪数据\DataLog_User')
    
    if not os.path.exists(data_root):
        st.error(f"数据根路径不存在: {data_root}")
        return
    
    st.sidebar.header("数据选择")
    st.sidebar.info(f"数据根路径: {data_root}")
    
    # 获取所有数据文件
    with st.spinner("正在扫描数据文件..."):
        all_files = get_data_files(data_root)
    
    if not all_files:
        st.warning("未找到任何.dat数据文件")
        return
    
    # 显示找到的文件数量
    st.sidebar.success(f"找到 {len(all_files)} 个数据文件")
    
    # 创建文件选择器
    file_names = []
    file_paths = {}
    
    for file_path in all_files:
        file_name = os.path.basename(file_path)
        file_display_name = f"{file_path} ({file_name})"
        file_names.append(file_display_name)
        file_paths[file_display_name] = file_path
    
    selected_file = st.sidebar.selectbox(
        "选择数据文件:",
        options=file_names,
        format_func=lambda x: os.path.basename(file_paths[x])
    )
    
    if selected_file:
        selected_path = file_paths[selected_file]
        st.sidebar.info(f"当前选择: {selected_path}")
        
        # 读取选定的文件
        with st.spinner("正在加载数据..."):
            df = load_data_file(selected_path)
        
        if df is None or df.empty:
            st.error("无法加载选定的数据文件或文件为空")
            return
        
        # 显示数据基本信息
        st.subheader("数据概览")
        col1, col2, col3 = st.columns(3)
        col1.metric("总记录数", len(df))
        col2.metric("数据时间范围", f"{df['DATETIME'].min()} 至 {df['DATETIME'].max()}" if 'DATETIME' in df.columns else "N/A")
        col3.metric("可用气体数据", f"CO2_dry: {df['CO2_dry'].count()}, CH4_dry: {df['CH4_dry'].count()}" if 'CO2_dry' in df.columns and 'CH4_dry' in df.columns else "N/A")
        
        # 显示数据预览
        st.subheader("数据预览")
        st.dataframe(df.head(10))
        
        # 检查是否有CO2_dry和CH4_dry列
        if 'CO2_dry' in df.columns and 'CH4_dry' in df.columns:
            st.subheader("气体浓度数据可视化")
            
            # 创建时间序列图
            fig = go.Figure()
            
            # 添加CO2_dry数据
            if df['CO2_dry'].notna().any():
                fig.add_trace(go.Scatter(
                    x=df['DATETIME'] if 'DATETIME' in df.columns else df.index,
                    y=df['CO2_dry'],
                    mode='lines+markers',
                    name='CO2_dry (ppm)',
                    line=dict(color='blue', width=1),
                    marker=dict(size=3)
                ))
            
            # 添加CH4_dry数据
            if df['CH4_dry'].notna().any():
                fig.add_trace(go.Scatter(
                    x=df['DATETIME'] if 'DATETIME' in df.columns else df.index,
                    y=df['CH4_dry'],
                    mode='lines+markers',
                    name='CH4_dry (ppm)',
                    yaxis='y2',
                    line=dict(color='red', width=1),
                    marker=dict(size=3)
                ))
            
            # 更新布局
            fig.update_layout(
                title='CO2和CH4干基浓度时间序列',
                xaxis_title='时间',
                yaxis=dict(
                    title='CO2_dry (ppm)',
                    side='left'
                ),
                yaxis2=dict(
                    title='CH4_dry (ppm)',
                    side='right',
                    overlaying='y'
                ),
                hovermode='x unified',
                width=1000,
                height=600
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # 统计信息
            st.subheader("统计信息")
            col1, col2 = st.columns(2)
            
            if 'CO2_dry' in df.columns:
                with col1:
                    st.write("**CO2_dry 统计**")
                    co2_stats = df['CO2_dry'].describe()
                    st.write(co2_stats)
            
            if 'CH4_dry' in df.columns:
                with col2:
                    st.write("**CH4_dry 统计**")
                    ch4_stats = df['CH4_dry'].describe()
                    st.write(ch4_stats)
            
            # 如果有时间信息，创建散点图
            if 'DATETIME' in df.columns:
                st.subheader("CO2 vs CH4 散点图")
                fig_scatter = px.scatter(
                    df, 
                    x='CO2_dry', 
                    y='CH4_dry',
                    title='CO2_dry vs CH4_dry 散点图',
                    trendline="ols" if df['CO2_dry'].notna().any() and df['CH4_dry'].notna().any() else None
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
        
        else:
            st.warning("选定的文件中不包含CO2_dry和CH4_dry列")
            st.write("可用列:", list(df.columns))

if __name__ == "__main__":
    main()