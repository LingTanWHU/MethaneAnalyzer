import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, time, timedelta
import pytz
from typing import Optional, Tuple

class DataPlotter:
    """数据图表绘制器"""
    
    def create_plots(self, df: pd.DataFrame, std_df: pd.DataFrame, 
                    time_window: str, agg_method: str, 
                    co2_range: Optional[Tuple[float, float]] = None,
                    ch4_range: Optional[Tuple[float, float]] = None,
                    h2o_range: Optional[Tuple[float, float]] = None,
                    c2h6_range: Optional[Tuple[float, float]] = None,
                    data_source: str = 'picarro',
                    picarro_concentration_type: str = 'dry') -> go.Figure:
        """创建气体浓度子图，支持 Picarro 和 Pico 数据"""
        
        if data_source == 'picarro':
            # Picarro: CH4, CO2, H2O (修改顺序)
            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08
            )
            
            # 合并主数据和标准差数据
            plot_df = df.copy()
            if not std_df.empty and time_window != "原始 (无平均)":
                plot_df = plot_df.merge(std_df[['DATETIME_DISPLAY'] + ['CH4_dry', 'CO2_dry', 'H2O']], 
                                       on='DATETIME_DISPLAY', suffixes=('', '_std'), how='left')

            # 根据浓度类型选择列名
            if picarro_concentration_type == 'dry':
                ch4_col = 'CH4_dry'
                co2_col = 'CO2_dry'
            else:  # raw
                ch4_col = 'CH4'
                co2_col = 'CO2'
            
            # 检查列是否存在，如果不存在则使用干基浓度列
            if ch4_col not in plot_df.columns:
                ch4_col = 'CH4_dry' if 'CH4_dry' in plot_df.columns else None
            if co2_col not in plot_df.columns:
                co2_col = 'CO2_dry' if 'CO2_dry' in plot_df.columns else None
            
            # 绘制 CH4 子图
            if ch4_col and ch4_col in plot_df.columns:
                self._add_gas_trace(fig, plot_df, ch4_col, 'red', 1, time_window)

            # 绘制 CO2 子图
            if co2_col and co2_col in plot_df.columns:
                self._add_gas_trace(fig, plot_df, co2_col, 'blue', 2, time_window)

            # 绘制 H2O 子图
            if 'H2O' in plot_df.columns:
                self._add_gas_trace(fig, plot_df, 'H2O', 'green', 3, time_window)

            # 添加垂直线
            self._add_vertical_lines(fig, plot_df, 1)  # CH4 子图
            self._add_vertical_lines(fig, plot_df, 2)  # CO2 子图
            self._add_vertical_lines(fig, plot_df, 3)  # H2O 子图

            # 设置 Y 轴范围
            if ch4_col and ch4_col in plot_df.columns and plot_df[ch4_col].notna().any():
                if ch4_range is not None and ch4_range != (0.0, 10.0):
                    fig.update_yaxes(range=ch4_range, row=1, col=1)
                else:
                    ch4_min, ch4_max = float(plot_df[ch4_col].min()), float(plot_df[ch4_col].max())
                    margin = (ch4_max - ch4_min) * 0.05 if ch4_max != ch4_min else 0.1
                    fig.update_yaxes(range=[ch4_min - margin, ch4_max + margin], row=1, col=1)

            if co2_col and co2_col in plot_df.columns and plot_df[co2_col].notna().any():
                if co2_range is not None and co2_range != (0.0, 1000.0):
                    fig.update_yaxes(range=co2_range, row=2, col=1)
                else:
                    co2_min, co2_max = float(plot_df[co2_col].min()), float(plot_df[co2_col].max())
                    margin = (co2_max - co2_min) * 0.05 if co2_max != co2_min else 50
                    fig.update_yaxes(range=[co2_min - margin, co2_max + margin], row=2, col=1)

            if 'H2O' in plot_df.columns and plot_df['H2O'].notna().any():
                if h2o_range is not None and h2o_range != (0.0, 100.0):
                    fig.update_yaxes(range=h2o_range, row=3, col=1)
                else:
                    h2o_min, h2o_max = float(plot_df['H2O'].min()), float(plot_df['H2O'].max())
                    margin = (h2o_max - h2o_min) * 0.05 if h2o_max != h2o_min else 1.0
                    fig.update_yaxes(range=[h2o_min - margin, h2o_max + margin], row=3, col=1)
        
        elif data_source == 'pico':
            # Pico: CH4, C2H6, H2O (保持原顺序)
            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08
            )
            
            # 合并主数据和标准差数据
            plot_df = df.copy()
            if not std_df.empty and time_window != "原始 (无平均)":
                plot_df = plot_df.merge(std_df[['DATETIME_DISPLAY'] + ['CH4', 'C2H6', 'H2O']], 
                                       on='DATETIME_DISPLAY', suffixes=('', '_std'), how='left')

            # 绘制 CH4 子图
            if 'CH4' in plot_df.columns:
                self._add_gas_trace(fig, plot_df, 'CH4', 'red', 1, time_window)

            # 绘制 C2H6 子图
            if 'C2H6' in plot_df.columns:
                self._add_gas_trace(fig, plot_df, 'C2H6', 'orange', 2, time_window)

            # 绘制 H2O 子图
            if 'H2O' in plot_df.columns:
                self._add_gas_trace(fig, plot_df, 'H2O', 'green', 3, time_window)

            # 添加垂直线
            self._add_vertical_lines(fig, plot_df, 1)  # CH4 子图
            self._add_vertical_lines(fig, plot_df, 2)  # C2H6 子图
            self._add_vertical_lines(fig, plot_df, 3)  # H2O 子图

            # 设置 Y 轴范围
            if 'CH4' in plot_df.columns and plot_df['CH4'].notna().any():
                if ch4_range is not None and ch4_range != (0.0, 10.0):
                    fig.update_yaxes(range=ch4_range, row=1, col=1)
                else:
                    ch4_min, ch4_max = float(plot_df['CH4'].min()), float(plot_df['CH4'].max())
                    margin = (ch4_max - ch4_min) * 0.05 if ch4_max != ch4_min else 0.1
                    fig.update_yaxes(range=[ch4_min - margin, ch4_max + margin], row=1, col=1)

            if 'C2H6' in plot_df.columns and plot_df['C2H6'].notna().any():
                if c2h6_range is not None and c2h6_range != (0.0, 1000.0):
                    fig.update_yaxes(range=c2h6_range, row=2, col=1)
                else:
                    c2h6_min, c2h6_max = float(plot_df['C2H6'].min()), float(plot_df['C2H6'].max())
                    margin = (c2h6_max - c2h6_min) * 0.05 if c2h6_max != c2h6_min else 10.0
                    fig.update_yaxes(range=[c2h6_min - margin, c2h6_max + margin], row=2, col=1)

            if 'H2O' in plot_df.columns and plot_df['H2O'].notna().any():
                if h2o_range is not None and h2o_range != (0.0, 100.0):
                    fig.update_yaxes(range=h2o_range, row=3, col=1)
                else:
                    h2o_min, h2o_max = float(plot_df['H2O'].min()), float(plot_df['H2O'].max())
                    margin = (h2o_max - h2o_min) * 0.05 if h2o_max != h2o_min else 1.0
                    fig.update_yaxes(range=[h2o_min - margin, h2o_max + margin], row=3, col=1)
        
        # 更新布局
        fig.update_layout(
            title=f'{data_source.upper()} 气体浓度时间序列 (时间窗口: {time_window.replace("原始 (无平均)", "无")}, 聚合方法: {"平均值" if agg_method == "mean" else "中位数"})',
            height=1000,
            width=1000,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )

        # 设置 Y 轴标题
        if data_source == 'picarro':
            if picarro_concentration_type == 'dry':
                ch4_title = "CH4_dry (ppm)" if 'CH4_dry' in df.columns else "CH4 (ppm)"
                co2_title = "CO2_dry (ppm)" if 'CO2_dry' in df.columns else "CO2 (ppm)"
            else:  # raw
                ch4_title = "CH4 (ppm)" if 'CH4' in df.columns else "CH4_dry (ppm)"
                co2_title = "CO2 (ppm)" if 'CO2' in df.columns else "CO2_dry (ppm)"
            
            fig.update_yaxes(title_text=ch4_title, row=1, col=1)  # CH4 在第一行
            fig.update_yaxes(title_text=co2_title, row=2, col=1)  # CO2 在第二行
            fig.update_yaxes(title_text="H2O (ppm)", row=3, col=1)
        else:  # pico
            fig.update_yaxes(title_text="CH4 (ppm)", row=1, col=1)
            fig.update_yaxes(title_text="C2H6 (ppb)", row=2, col=1)
            fig.update_yaxes(title_text="H2O (ppm)", row=3, col=1)
        
        fig.update_xaxes(title_text="时间", row=3, col=1)

        return fig
    
    def _add_vertical_lines(self, fig: go.Figure, df: pd.DataFrame, row: int):
        """添加每天的垂直线 (0点为实线, 6点、12点、18点为虚线)"""
        if 'DATETIME_DISPLAY' not in df.columns or df.empty:
            return
        
        # 获取时间范围
        start_time = df['DATETIME_DISPLAY'].min()
        end_time = df['DATETIME_DISPLAY'].max()
        
        if pd.isna(start_time) or pd.isna(end_time):
            return
        
        # 生成垂直线的时间点
        zero_lines = []  # 0点的实线
        other_lines = []  # 6点、12点、18点的虚线
        
        # 从开始日期的0点开始
        current_date = start_time.date()
        end_date = end_time.date()
        
        while current_date <= end_date:
            # 检查0点
            zero_time = pd.Timestamp(datetime.combine(current_date, time(0, 0)))
            # 如果原数据有时区信息，为新创建的时间也添加相同的时区
            if start_time.tzinfo is not None:
                zero_time = zero_time.tz_localize(start_time.tzinfo)
            
            if start_time <= zero_time <= end_time:
                zero_lines.append(zero_time)
            
            # 检查6点、12点、18点
            for hour in [6, 12, 18]:
                other_time = pd.Timestamp(datetime.combine(current_date, time(hour, 0)))
                # 如果原数据有时区信息，为新创建的时间也添加相同的时区
                if start_time.tzinfo is not None:
                    other_time = other_time.tz_localize(start_time.tzinfo)
                
                if start_time <= other_time <= end_time:
                    other_lines.append(other_time)
            
            current_date += timedelta(days=1)
        
        # 添加0点的实线
        for line_time in zero_lines:
            fig.add_vline(
                x=line_time,
                line_dash="solid",  # 实线
                line_color="gray",
                line_width=2,  # 稍微粗一些
                opacity=0.8,
                row=row
            )
        
        # 添加6点、12点、18点的虚线
        for line_time in other_lines:
            fig.add_vline(
                x=line_time,
                line_dash="dash",  # 虚线
                line_color="gray",
                line_width=1,
                opacity=0.5,
                row=row
            )
    
    def _add_gas_trace(self, fig: go.Figure, df: pd.DataFrame, gas_col: str, 
                      color: str, row: int, time_window: str):
        """为指定气体添加迹线（主曲线 + 不确定度阴影）"""
        if df[gas_col].notna().any():
            # 主曲线
            fig.add_trace(go.Scatter(
                x=df['DATETIME_DISPLAY'],
                y=df[gas_col],
                mode='lines+markers',
                name=f'{gas_col}',
                line=dict(color=color, width=1),
                marker=dict(size=3),
            ), row=row, col=1)

            # 如果有标准差数据且不是原始数据，添加阴影区域
            if time_window != "原始 (无平均)" and f'{gas_col}_std' in df.columns:
                upper = df[gas_col] + df[f'{gas_col}_std']
                lower = df[gas_col] - df[f'{gas_col}_std']

                # 识别连续段（非 NaN 区间）
                valid_mask = df[gas_col].notna() & df[f'{gas_col}_std'].notna()

                # 找出连续段的起始和结束索引
                segments = []
                start_idx = None
                for i in range(len(valid_mask)):
                    if valid_mask.iloc[i] and start_idx is None:
                        start_idx = i
                    elif not valid_mask.iloc[i] and start_idx is not None:
                        segments.append((start_idx, i))
                        start_idx = None
                if start_idx is not None:
                    segments.append((start_idx, len(valid_mask)))

                # 对每个连续段绘制阴影
                for start, end in segments:
                    segment_df = df.iloc[start:end].copy()
                    upper_segment = upper.iloc[start:end]
                    lower_segment = lower.iloc[start:end]

                    # 先画下边界（无填充）
                    fig.add_trace(go.Scatter(
                        x=segment_df['DATETIME_DISPLAY'],
                        y=lower_segment,
                        mode='lines',
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo='skip'
                    ), row=row, col=1)

                    # 再画上边界（带填充）
                    fig.add_trace(go.Scatter(
                        x=segment_df['DATETIME_DISPLAY'],
                        y=upper_segment,
                        mode='lines',
                        line=dict(width=0),
                        fill='tonexty',
                        fillcolor=f'rgba({self._get_rgba_color(color)}, 0.2)',
                        name=f'{gas_col} ± σ',
                        showlegend=(start == 0),
                        hoverinfo='skip'
                    ), row=row, col=1)
    
    def _get_rgba_color(self, color: str) -> str:
        """将颜色名转换为RGBA值"""
        color_map = {
            'blue': '0,0,255',
            'red': '255,0,0',
            'green': '0,255,0',
            'orange': '255,165,0'  # 添加橙色用于 C2H6
        }
        return color_map.get(color, '0,0,0')