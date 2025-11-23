import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Optional, Tuple

class DataPlotter:
    """数据图表绘制器"""
    
    def create_plots(self, df: pd.DataFrame, std_df: pd.DataFrame, 
                    time_window: str, agg_method: str, 
                    co2_range: Optional[Tuple[float, float]] = None,
                    ch4_range: Optional[Tuple[float, float]] = None,
                    h2o_range: Optional[Tuple[float, float]] = None) -> go.Figure:
        """创建CO2、CH4和H2O的子图"""
        fig = make_subplots(
            rows=3, cols=1,  # 改为3行
            # subplot_titles=("CO₂ 干基浓度 (ppm)", "CH₄ 干基浓度 (ppm)", "H₂O 浓度 (ppm)"),  # 添加标题
            shared_xaxes=True,
            vertical_spacing=0.08
        )

        # 准备数据：保留 NaN，用于识别断开段
        plot_df = df.copy()
        if not std_df.empty and time_window != "原始 (无平均)":
            # 合并标准差数据 - 添加 H2O
            plot_df = plot_df.merge(std_df[['DATETIME_DISPLAY'] + ['CO2_dry', 'CH4_dry', 'H2O']], 
                                   on='DATETIME_DISPLAY', suffixes=('', '_std'), how='left')

        # 绘制 CO2 子图
        self._add_gas_trace(fig, plot_df, 'CO2_dry', 'blue', 1, time_window)

        # 绘制 CH4 子图
        self._add_gas_trace(fig, plot_df, 'CH4_dry', 'red', 2, time_window)

        # 绘制 H2O 子图
        self._add_gas_trace(fig, plot_df, 'H2O', 'green', 3, time_window)  # 添加 H2O

        # 更新布局
        fig.update_layout(
            title=f'气体浓度时间序列 (时间窗口: {time_window.replace("原始 (无平均)", "无")}, 聚合方法: {"平均值" if agg_method == "mean" else "中位数"})',
            height=1000,  # 增加高度
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
        fig.update_yaxes(title_text="CO2_dry (ppm)", row=1, col=1)
        fig.update_yaxes(title_text="CH4_dry (ppm)", row=2, col=1)
        fig.update_yaxes(title_text="H2O (ppm)", row=3, col=1)  # 添加 H2O Y轴标题
        fig.update_xaxes(title_text="时间", row=3, col=1)

        # 根据数据自动设置 Y 轴范围，或使用用户指定的范围
        if 'CO2_dry' in plot_df.columns and plot_df['CO2_dry'].notna().any():
            if co2_range is not None and co2_range != (0.0, 1000.0):
                fig.update_yaxes(range=co2_range, row=1, col=1)
            else:
                co2_min, co2_max = float(plot_df['CO2_dry'].min()), float(plot_df['CO2_dry'].max())
                margin = (co2_max - co2_min) * 0.05 if co2_max != co2_min else 50
                fig.update_yaxes(range=[co2_min - margin, co2_max + margin], row=1, col=1)

        if 'CH4_dry' in plot_df.columns and plot_df['CH4_dry'].notna().any():
            if ch4_range is not None and ch4_range != (0.0, 10.0):
                fig.update_yaxes(range=ch4_range, row=2, col=1)
            else:
                ch4_min, ch4_max = float(plot_df['CH4_dry'].min()), float(plot_df['CH4_dry'].max())
                margin = (ch4_max - ch4_min) * 0.05 if ch4_max != ch4_min else 0.1
                fig.update_yaxes(range=[ch4_min - margin, ch4_max + margin], row=2, col=1)

        # 添加 H2O Y轴范围设置
        if 'H2O' in plot_df.columns and plot_df['H2O'].notna().any():
            if h2o_range is not None and h2o_range != (0.0, 100.0):
                fig.update_yaxes(range=h2o_range, row=3, col=1)
            else:
                h2o_min, h2o_max = float(plot_df['H2O'].min()), float(plot_df['H2O'].max())
                margin = (h2o_max - h2o_min) * 0.05 if h2o_max != h2o_min else 1.0
                fig.update_yaxes(range=[h2o_min - margin, h2o_max + margin], row=3, col=1)

        return fig
    
    def _add_gas_trace(self, fig: go.Figure, df: pd.DataFrame, gas_col: str, 
                      color: str, row: int, time_window: str):
        """为指定气体添加迹线（主曲线 + 不确定度阴影）"""
        if df[gas_col].notna().any():
            # 主曲线
            fig.add_trace(go.Scatter(
                x=df['DATETIME_DISPLAY'],
                y=df[gas_col],
                mode='lines+markers',
                name=f'{gas_col} (ppm)',
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
            'green': '0,255,0'  # 添加绿色用于 H2O
        }
        return color_map.get(color, '0,0,0')