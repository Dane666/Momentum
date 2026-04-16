# -*- coding: utf-8 -*-
"""
回测可视化模块
使用 Plotly 生成交互式回测分析图表
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict
import logging

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    go = None  # 占位符，防止类型注解报错
    make_subplots = None
    px = None

logger = logging.getLogger('momentum')


class BacktestVisualizer:
    """
    回测结果可视化器
    
    使用示例:
    ```python
    from momentum.backtest import BacktestVisualizer
    
    viz = BacktestVisualizer(session_id='abc123')
    viz.plot_all()  # 生成所有图表
    viz.plot_equity_curve()  # 单独生成净值曲线
    ```
    """

    def __init__(self, session_id: str):
        """
        初始化可视化器
        
        Args:
            session_id: 回测会话 ID
        """
        if not PLOTLY_AVAILABLE:
            raise ImportError("请先安装 plotly: pip install plotly")
        
        from ..data import (
            get_backtest_sessions,
            get_session_trades,
            get_session_equity_curve,
            get_trade_statistics
        )
        
        self.session_id = session_id
        
        # 加载数据
        sessions = get_backtest_sessions(limit=100)
        self.session_info = sessions[sessions['session_id'] == session_id]
        
        if self.session_info.empty:
            raise ValueError(f"未找到会话: {session_id}")
        
        self.trades = get_session_trades(session_id)
        self.equity_curve = get_session_equity_curve(session_id)
        self.stats = get_trade_statistics(session_id)
        
        # 提取会话参数
        row = self.session_info.iloc[0]
        self.backtest_days = row.get('backtest_days', 0)
        self.hold_period = row.get('hold_period', 0)
        self.total_return = row.get('total_return', 0) or 0
        self.annual_return = row.get('annual_return', 0) or 0
        self.sharpe_ratio = row.get('sharpe_ratio', 0) or 0
        self.max_drawdown = row.get('max_drawdown', 0) or 0
        self.win_rate = row.get('win_rate', 0) or 0
        
        # 配色方案
        self.colors = {
            'primary': '#1f77b4',      # 蓝色 - 策略净值
            'secondary': '#ff7f0e',    # 橙色 - 基准
            'positive': '#2ca02c',     # 绿色 - 盈利
            'negative': '#d62728',     # 红色 - 亏损
            'neutral': '#7f7f7f',      # 灰色
            'background': '#fafafa',
            'grid': '#e0e0e0'
        }
        
        logger.info(f"[Visualizer] 加载会话 {session_id}: {len(self.trades)} 笔交易")

    def plot_equity_curve(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        绘制净值曲线（含基准对比和回撤）
        
        Args:
            show: 是否显示图表
            save_path: 保存路径（HTML 文件）
            
        Returns:
            Plotly Figure 对象
        """
        if self.equity_curve.empty:
            logger.warning("无净值数据")
            return None
        
        df = self.equity_curve.copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 创建双 Y 轴图表
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.7, 0.3],
            subplot_titles=('策略净值 vs 基准', '回撤曲线')
        )
        
        # 1. 策略净值
        fig.add_trace(
            go.Scatter(
                x=df['trade_date'],
                y=df['nav'],
                mode='lines',
                name='策略净值',
                line=dict(color=self.colors['primary'], width=2),
                hovertemplate='日期: %{x}<br>净值: %{y:.4f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 2. 基准净值
        if 'benchmark_nav' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['trade_date'],
                    y=df['benchmark_nav'],
                    mode='lines',
                    name='沪深300基准',
                    line=dict(color=self.colors['secondary'], width=1.5, dash='dash'),
                    hovertemplate='日期: %{x}<br>基准: %{y:.4f}<extra></extra>'
                ),
                row=1, col=1
            )
        
        # 3. 回撤曲线
        if 'drawdown' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['trade_date'],
                    y=-df['drawdown'],  # 取负值，回撤向下显示
                    mode='lines',
                    name='回撤',
                    fill='tozeroy',
                    line=dict(color=self.colors['negative'], width=1),
                    fillcolor='rgba(214, 39, 40, 0.3)',
                    hovertemplate='日期: %{x}<br>回撤: %{y:.2f}%<extra></extra>'
                ),
                row=2, col=1
            )
        
        # 布局设置
        fig.update_layout(
            title=dict(
                text=f'📈 回测结果 | 收益: {self.total_return:.2f}% | 年化: {self.annual_return:.2f}% | '
                     f'夏普: {self.sharpe_ratio:.2f} | 最大回撤: {self.max_drawdown:.2f}%',
                font=dict(size=16)
            ),
            height=600,
            showlegend=True,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            ),
            hovermode='x unified',
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        # 坐标轴设置
        fig.update_xaxes(showgrid=True, gridcolor=self.colors['grid'])
        fig.update_yaxes(showgrid=True, gridcolor=self.colors['grid'])
        fig.update_yaxes(title_text='净值', row=1, col=1)
        fig.update_yaxes(title_text='回撤 %', row=2, col=1)
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 图表已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def plot_trade_analysis(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        绘制交易分析图表（收益分布、板块分析、退出原因）
        
        Args:
            show: 是否显示图表
            save_path: 保存路径
            
        Returns:
            Plotly Figure 对象
        """
        if self.trades.empty:
            logger.warning("无交易数据")
            return None
        
        # 只分析卖出交易（有盈亏信息）
        sells = self.trades[self.trades['trade_type'] == 'SELL'].copy()
        if sells.empty:
            logger.warning("无卖出交易数据")
            return None
        
        sells['pnl_pct'] = sells['pnl_pct'].fillna(0)
        
        # 创建 2x2 子图
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                '收益率分布', '按退出原因统计',
                '按板块收益', '单笔收益散点图'
            ),
            specs=[
                [{'type': 'histogram'}, {'type': 'pie'}],
                [{'type': 'bar'}, {'type': 'scatter'}]
            ]
        )
        
        # 1. 收益率分布直方图
        colors = [self.colors['positive'] if x > 0 else self.colors['negative'] 
                  for x in sells['pnl_pct']]
        
        fig.add_trace(
            go.Histogram(
                x=sells['pnl_pct'],
                nbinsx=30,
                name='收益分布',
                marker_color=self.colors['primary'],
                opacity=0.7,
                hovertemplate='收益区间: %{x:.1f}%<br>交易数: %{y}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 添加均值线
        mean_ret = sells['pnl_pct'].mean()
        fig.add_vline(
            x=mean_ret, 
            line_dash='dash', 
            line_color=self.colors['negative'] if mean_ret < 0 else self.colors['positive'],
            annotation_text=f'均值: {mean_ret:.2f}%',
            row=1, col=1
        )
        
        # 2. 退出原因饼图
        exit_stats = sells['exit_reason'].value_counts()
        fig.add_trace(
            go.Pie(
                labels=exit_stats.index,
                values=exit_stats.values,
                name='退出原因',
                hole=0.4,
                marker_colors=px.colors.qualitative.Set2,
                textinfo='label+percent',
                hovertemplate='%{label}<br>数量: %{value}<br>占比: %{percent}<extra></extra>'
            ),
            row=1, col=2
        )
        
        # 3. 板块收益柱状图
        if 'sector' in sells.columns and sells['sector'].notna().any():
            sector_stats = sells.groupby('sector')['pnl_pct'].agg(['mean', 'count'])
            sector_stats = sector_stats[sector_stats['count'] >= 2].sort_values('mean', ascending=True)
            
            if not sector_stats.empty:
                bar_colors = [self.colors['positive'] if x > 0 else self.colors['negative'] 
                              for x in sector_stats['mean']]
                
                fig.add_trace(
                    go.Bar(
                        y=sector_stats.index,
                        x=sector_stats['mean'],
                        orientation='h',
                        name='板块收益',
                        marker_color=bar_colors,
                        text=[f'{x:.1f}%' for x in sector_stats['mean']],
                        textposition='outside',
                        hovertemplate='板块: %{y}<br>平均收益: %{x:.2f}%<extra></extra>'
                    ),
                    row=2, col=1
                )
        
        # 4. 单笔收益散点图（按时间）
        sells['trade_date'] = pd.to_datetime(sells['trade_date'])
        scatter_colors = [self.colors['positive'] if x > 0 else self.colors['negative'] 
                          for x in sells['pnl_pct']]
        
        fig.add_trace(
            go.Scatter(
                x=sells['trade_date'],
                y=sells['pnl_pct'],
                mode='markers',
                name='单笔收益',
                marker=dict(
                    color=scatter_colors,
                    size=8,
                    opacity=0.6
                ),
                hovertemplate='日期: %{x}<br>代码: %{customdata[0]}<br>收益: %{y:.2f}%<extra></extra>',
                customdata=sells[['code']].values
            ),
            row=2, col=2
        )
        
        # 添加零线 (使用 shape 代替 add_hline 避免子图问题)
        fig.add_shape(
            type='line',
            x0=sells['trade_date'].min(),
            x1=sells['trade_date'].max(),
            y0=0, y1=0,
            line=dict(color='gray', dash='dash', width=1),
            row=2, col=2
        )
        
        # 布局
        fig.update_layout(
            title=dict(
                text=f'📊 交易分析 | 总交易: {len(sells)} 笔 | 胜率: {self.win_rate:.1f}%',
                font=dict(size=16)
            ),
            height=700,
            showlegend=False,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        fig.update_xaxes(title_text='收益率 %', row=1, col=1)
        fig.update_yaxes(title_text='交易数量', row=1, col=1)
        fig.update_xaxes(title_text='平均收益 %', row=2, col=1)
        fig.update_xaxes(title_text='日期', row=2, col=2)
        fig.update_yaxes(title_text='收益率 %', row=2, col=2)
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 图表已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def plot_monthly_returns(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        绘制月度收益热力图
        
        Args:
            show: 是否显示图表
            save_path: 保存路径
            
        Returns:
            Plotly Figure 对象
        """
        if self.equity_curve.empty:
            logger.warning("无净值数据")
            return None
        
        df = self.equity_curve.copy()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.set_index('trade_date')
        
        # 计算月度收益 (使用 'ME' 代替废弃的 'M')
        monthly_nav = df['nav'].resample('ME').last()
        monthly_returns = monthly_nav.pct_change() * 100
        monthly_returns = monthly_returns.dropna()
        
        if monthly_returns.empty:
            logger.warning("月度数据不足")
            return None
        
        # 构建热力图数据
        monthly_df = pd.DataFrame({
            'year': monthly_returns.index.year,
            'month': monthly_returns.index.month,
            'return': monthly_returns.values
        })
        
        # 透视表
        pivot = monthly_df.pivot(index='year', columns='month', values='return')
        pivot.columns = ['1月', '2月', '3月', '4月', '5月', '6月', 
                         '7月', '8月', '9月', '10月', '11月', '12月'][:len(pivot.columns)]
        
        # 创建热力图
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index.astype(str),
            colorscale=[
                [0, self.colors['negative']],
                [0.5, 'white'],
                [1, self.colors['positive']]
            ],
            zmid=0,
            text=[[f'{v:.1f}%' if pd.notna(v) else '' for v in row] for row in pivot.values],
            texttemplate='%{text}',
            textfont=dict(size=12),
            hovertemplate='%{y}年 %{x}<br>收益: %{z:.2f}%<extra></extra>',
            colorbar=dict(title='收益率 %')
        ))
        
        fig.update_layout(
            title=dict(
                text='📅 月度收益热力图',
                font=dict(size=16)
            ),
            height=400,
            xaxis_title='月份',
            yaxis_title='年份',
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 图表已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def plot_trade_timeline(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        绘制交易时间线（买卖点标记）
        
        Args:
            show: 是否显示图表
            save_path: 保存路径
            
        Returns:
            Plotly Figure 对象
        """
        if self.trades.empty or self.equity_curve.empty:
            logger.warning("无数据")
            return None
        
        df_eq = self.equity_curve.copy()
        df_eq['trade_date'] = pd.to_datetime(df_eq['trade_date'])
        
        buys = self.trades[self.trades['trade_type'] == 'BUY'].copy()
        sells = self.trades[self.trades['trade_type'] == 'SELL'].copy()
        buys['trade_date'] = pd.to_datetime(buys['trade_date'])
        sells['trade_date'] = pd.to_datetime(sells['trade_date'])
        
        fig = go.Figure()
        
        # 净值曲线
        fig.add_trace(
            go.Scatter(
                x=df_eq['trade_date'],
                y=df_eq['nav'],
                mode='lines',
                name='策略净值',
                line=dict(color=self.colors['primary'], width=2)
            )
        )
        
        # 买入点 - 合并同一天的交易
        buy_dates = buys.groupby('trade_date').size().reset_index(name='count')
        buy_nav = []
        for d in buy_dates['trade_date']:
            nav_val = df_eq[df_eq['trade_date'] == d]['nav']
            buy_nav.append(nav_val.values[0] if len(nav_val) > 0 else 1.0)
        
        fig.add_trace(
            go.Scatter(
                x=buy_dates['trade_date'],
                y=buy_nav,
                mode='markers',
                name='买入',
                marker=dict(
                    symbol='triangle-up',
                    size=12,
                    color=self.colors['positive']
                ),
                hovertemplate='买入日期: %{x}<br>净值: %{y:.4f}<br>交易数: %{customdata}<extra></extra>',
                customdata=buy_dates['count']
            )
        )
        
        # 卖出点
        sell_dates = sells.groupby('trade_date').agg({
            'pnl_pct': 'mean',
            'code': 'count'
        }).reset_index()
        sell_dates.columns = ['trade_date', 'avg_pnl', 'count']
        
        sell_nav = []
        for d in sell_dates['trade_date']:
            nav_val = df_eq[df_eq['trade_date'] == d]['nav']
            sell_nav.append(nav_val.values[0] if len(nav_val) > 0 else 1.0)
        
        # 根据盈亏着色
        sell_colors = [self.colors['positive'] if p > 0 else self.colors['negative'] 
                       for p in sell_dates['avg_pnl']]
        
        fig.add_trace(
            go.Scatter(
                x=sell_dates['trade_date'],
                y=sell_nav,
                mode='markers',
                name='卖出',
                marker=dict(
                    symbol='triangle-down',
                    size=12,
                    color=sell_colors
                ),
                hovertemplate='卖出日期: %{x}<br>净值: %{y:.4f}<br>'
                              '平均收益: %{customdata[0]:.2f}%<br>交易数: %{customdata[1]}<extra></extra>',
                customdata=sell_dates[['avg_pnl', 'count']].values
            )
        )
        
        fig.update_layout(
            title=dict(
                text='🎯 交易时间线',
                font=dict(size=16)
            ),
            height=500,
            showlegend=True,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            ),
            hovermode='x unified',
            xaxis_title='日期',
            yaxis_title='净值',
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        fig.update_xaxes(showgrid=True, gridcolor=self.colors['grid'])
        fig.update_yaxes(showgrid=True, gridcolor=self.colors['grid'])
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 图表已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def plot_trade_log(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        生成详细交易日志表格（含买卖原因、止损设置）
        
        用于模仿回测操作手法，展示每笔交易的：
        - 买入/卖出时间
        - 买卖原因
        - 止损价格
        - 持仓收益
        
        Args:
            show: 是否显示图表
            save_path: 保存路径
            
        Returns:
            Plotly Figure 对象
        """
        from ..data.trade_reason import TradeReasonAnalyzer
        
        if self.trades.empty:
            logger.warning("无交易数据")
            return None
        
        buys = self.trades[self.trades['trade_type'] == 'BUY'].copy()
        sells = self.trades[self.trades['trade_type'] == 'SELL'].copy()
        
        if sells.empty:
            logger.warning("无卖出交易数据")
            return None
        
        analyzer = TradeReasonAnalyzer()
        
        # 构建交易日志数据
        trade_logs = []
        
        for _, sell in sells.iterrows():
            code = sell['code']
            name = sell['name']
            
            # 找对应买入
            buy_records = buys[(buys['code'] == code) & (buys['trade_date'] <= sell['trade_date'])]
            if buy_records.empty:
                continue
            buy = buy_records.iloc[-1]
            
            # 提取因子数据
            buy_price = buy['price']
            sell_price = sell['price']
            shares = buy.get('shares', 100) or 100
            alpha_score = buy.get('alpha_score', 0) or 0
            mom_5 = buy.get('mom_5', 0) or 0
            sharpe = buy.get('sharpe', 0) or 0
            rsi = buy.get('rsi', 0) or 0
            atr = buy.get('atr', 0) or 0
            ma20 = buy_price * 0.95  # 估算 (站上MA20买入)
            exit_reason = sell.get('exit_reason', 'Time_Exit')
            hold_days = sell.get('hold_days', 3) or 3
            pnl_pct = sell.get('pnl_pct', 0) or 0
            action_label = buy.get('action_label', '') or ''
            position_value = buy.get('position_value', buy_price * shares) or (buy_price * shares)
            account_balance = sell.get('account_balance', 0) or 0
            
            # 计算实际盈亏金额
            pnl_amount = (sell_price - buy_price) * shares
            
            # 计算止损价
            atr_stop = ma20 - 1.5 * atr if atr > 0 else ma20 * 0.95
            stop_pct = ((atr_stop / buy_price) - 1) * 100
            
            # 简化买入原因 (使用标签)
            if action_label:
                buy_reason_str = action_label
            else:
                buy_reasons = []
                if mom_5 > 0:
                    buy_reasons.append(f"动量+{mom_5*100:.1f}%")
                if sharpe > 0.8:
                    buy_reasons.append(f"夏普{sharpe:.2f}")
                if alpha_score > 0:
                    buy_reasons.append(f"Alpha{alpha_score:.2f}")
                buy_reason_str = "、".join(buy_reasons) if buy_reasons else "综合评分入选"
            
            # 生成卖出原因摘要
            if exit_reason == 'ATR_Stop':
                sell_reason_str = f"ATR止损 (低于{atr_stop:.2f})"
            elif exit_reason == 'MA5_Exit':
                sell_reason_str = "跌破MA5趋势线"
            elif exit_reason == 'Time_Exit':
                sell_reason_str = f"持满{hold_days}天到期"
            else:
                sell_reason_str = exit_reason
            
            # 收益状态
            pnl_status = "🟢 盈利" if pnl_pct > 0 else "🔴 亏损"
            
            trade_logs.append({
                '序号': len(trade_logs) + 1,
                '代码': code,
                '名称': name,
                '操作标签': action_label if action_label else '📊 回测买入',
                '买入日期': str(buy['trade_date'])[:10],
                '买入价': f"¥{buy_price:.2f}",
                '股数': f"{shares}股",
                '仓位金额': f"¥{position_value:.0f}",
                '卖出日期': str(sell['trade_date'])[:10],
                '卖出价': f"¥{sell_price:.2f}",
                '卖出原因': sell_reason_str,
                '持仓天数': f"{hold_days}天",
                '收益率': f"{pnl_pct:+.2f}%",
                '盈亏金额': f"¥{pnl_amount:+.0f}",
                '账户余额': f"¥{account_balance:.0f}",
                '状态': pnl_status
            })
        
        if not trade_logs:
            logger.warning("无法生成交易日志")
            return None
        
        df_log = pd.DataFrame(trade_logs)
        
        # 创建表格图
        fig = go.Figure()
        
        # 设置表格颜色
        header_color = '#4472C4'
        row_colors = []
        for log in trade_logs:
            if '盈利' in log['状态']:
                row_colors.append('#E2EFDA')  # 浅绿
            else:
                row_colors.append('#FCE4D6')  # 浅红
        
        fig.add_trace(
            go.Table(
                header=dict(
                    values=[f"<b>{col}</b>" for col in df_log.columns],
                    fill_color=header_color,
                    font=dict(color='white', size=11),
                    align='center',
                    height=35
                ),
                cells=dict(
                    values=[df_log[col] for col in df_log.columns],
                    fill_color=[row_colors],
                    font=dict(size=10),
                    align=['center', 'center', 'left', 'left', 'center', 'right', 
                           'center', 'right', 'center', 'right', 'left', 'center', 
                           'right', 'right', 'right', 'center'],
                    height=28
                ),
                columnwidth=[35, 60, 70, 90, 75, 70, 55, 80, 75, 70, 100, 55, 60, 70, 85, 55]
            )
        )
        
        # 添加统计摘要
        win_count = len([l for l in trade_logs if '盈利' in l['状态']])
        loss_count = len(trade_logs) - win_count
        avg_pnl = sum(float(l['收益率'].replace('%', '').replace('+', '')) for l in trade_logs) / len(trade_logs)
        total_pnl = sum(float(l['盈亏金额'].replace('¥', '').replace('+', '').replace(',', '')) for l in trade_logs)
        
        # 获取最终账户余额
        final_balance = float(trade_logs[-1]['账户余额'].replace('¥', '').replace(',', '')) if trade_logs else 0
        
        fig.update_layout(
            title=dict(
                text=f'📋 交易操作日志 | 总计 {len(trade_logs)} 笔 | '
                     f'盈利 {win_count} 笔 | 亏损 {loss_count} 笔 | '
                     f'平均收益 {avg_pnl:+.2f}% | 总盈亏 ¥{total_pnl:+,.0f} | '
                     f'账户余额 ¥{final_balance:,.0f}',
                font=dict(size=14)
            ),
            height=max(500, 120 + len(trade_logs) * 32),
            margin=dict(l=10, r=10, t=70, b=20),
            paper_bgcolor='white'
        )
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 交易日志已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def generate_actionable_report(self, save_path: Optional[str] = None) -> str:
        """
        生成可操作的交易报告（纯文本格式）
        
        用于打印或复制，方便模仿回测操作
        
        Args:
            save_path: 保存路径（txt文件）
            
        Returns:
            报告文本
        """
        from ..data.trade_reason import TradeReasonAnalyzer
        
        if self.trades.empty:
            return "无交易数据"
        
        buys = self.trades[self.trades['trade_type'] == 'BUY'].copy()
        sells = self.trades[self.trades['trade_type'] == 'SELL'].copy()
        
        analyzer = TradeReasonAnalyzer()
        
        report_lines = [
            "=" * 80,
            f"📋 可操作交易清单 - Momentum 动量策略",
            f"回测会话: {self.session_id[:12]}",
            f"回测天数: {self.backtest_days} 天 | 持仓周期: {self.hold_period} 天",
            f"总收益: {self.total_return:.2f}% | 胜率: {self.win_rate:.1f}%",
            "=" * 80,
            "",
            "【操作手法说明】",
            "1. 买入时机: 尾盘14:50左右，确认收盘价站上MA20",
            "2. 止损设置: 开盘后立即设置条件单，日内低于止损价即触发",
            "3. 止盈观察: 每日收盘检查是否跌破MA5，是则次日开盘卖出",
            "4. 时间纪律: 无论盈亏，持满N天必须离场",
            "",
            "-" * 80,
            "【每日操作明细】",
            "-" * 80,
        ]
        
        # 按日期组织交易
        all_dates = set()
        for _, row in buys.iterrows():
            all_dates.add(str(row['trade_date'])[:10])
        for _, row in sells.iterrows():
            all_dates.add(str(row['trade_date'])[:10])
        
        for date in sorted(all_dates):
            day_buys = buys[buys['trade_date'].astype(str).str[:10] == date]
            day_sells = sells[sells['trade_date'].astype(str).str[:10] == date]
            
            if day_buys.empty and day_sells.empty:
                continue
            
            report_lines.append(f"\n📅 {date}")
            report_lines.append("-" * 40)
            
            # 卖出操作 (优先，清仓再买入)
            if not day_sells.empty:
                report_lines.append("  【卖出】(建议时间: 09:30-10:00)")
                for _, sell in day_sells.iterrows():
                    exit_reason = sell.get('exit_reason', 'Time_Exit')
                    pnl = sell.get('pnl_pct', 0) or 0
                    hold = sell.get('hold_days', 0) or 0
                    
                    if exit_reason == 'ATR_Stop':
                        action = "🛑 止损卖出"
                        timing = "开盘价/止损触发价"
                    elif exit_reason == 'MA5_Exit':
                        action = "📉 趋势止盈"
                        timing = "开盘价卖出"
                    else:
                        action = "⏰ 到期卖出"
                        timing = "开盘价卖出"
                    
                    emoji = "🟢" if pnl > 0 else "🔴"
                    report_lines.append(
                        f"    {emoji} {sell['code']} {sell['name']}"
                    )
                    report_lines.append(
                        f"       {action} | {timing} | 持仓{hold}天 | 收益{pnl:+.2f}%"
                    )
            
            # 买入操作
            if not day_buys.empty:
                report_lines.append("  【买入】(建议时间: 14:50 尾盘)")
                for _, buy in day_buys.iterrows():
                    alpha = buy.get('alpha_score', 0) or 0
                    mom_5 = buy.get('mom_5', 0) or 0
                    sharpe = buy.get('sharpe', 0) or 0
                    atr = buy.get('atr', 0) or 0
                    price = buy['price']
                    
                    # 计算止损
                    ma20_est = price * 0.95
                    stop_price = ma20_est - 1.5 * atr if atr > 0 else price * 0.93
                    stop_pct = ((stop_price / price) - 1) * 100
                    
                    report_lines.append(
                        f"    🔵 {buy['code']} {buy['name']}"
                    )
                    report_lines.append(
                        f"       买入价: ¥{price:.2f}"
                    )
                    report_lines.append(
                        f"       买入理由: Alpha{alpha:.2f} | 动量{mom_5*100:+.1f}% | 夏普{sharpe:.2f}"
                    )
                    report_lines.append(
                        f"       ⚠️ 设置止损: ¥{stop_price:.2f} ({stop_pct:.1f}%)"
                    )
        
        # 汇总统计
        report_lines.extend([
            "",
            "=" * 80,
            "【策略汇总】",
            "=" * 80,
        ])
        
        if self.stats:
            exit_stats = self.stats.get('exit_stats', {})
            report_lines.append("\n退出原因统计:")
            for reason, count in exit_stats.items():
                if reason == 'ATR_Stop':
                    desc = "ATR止损 (波动率止损)"
                elif reason == 'MA5_Exit':
                    desc = "MA5止盈 (趋势走弱)"
                else:
                    desc = "时间到期 (持仓期满)"
                report_lines.append(f"  • {desc}: {count}笔")
        
        report_lines.extend([
            "",
            "【关键提醒】",
            f"  • 每笔交易必须设置止损单！",
            f"  • 止损公式: MA20 - 1.5×ATR",
            f"  • 持仓周期: {self.hold_period} 天",
            f"  • 单日最多持仓: 5 只",
            "",
            "=" * 80,
        ])
        
        report_text = "\n".join(report_lines)
        
        if save_path:
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"[Visualizer] 操作报告已保存: {save_path}")
        
        return report_text

    def plot_summary_dashboard(self, show: bool = True, save_path: Optional[str] = None) -> "go.Figure":
        """
        绘制综合仪表板（一页展示所有关键指标）
        
        Args:
            show: 是否显示图表
            save_path: 保存路径
            
        Returns:
            Plotly Figure 对象
        """
        # 创建复杂布局
        fig = make_subplots(
            rows=3, cols=3,
            specs=[
                [{'colspan': 2, 'rowspan': 1}, None, {'type': 'indicator'}],
                [{'colspan': 2, 'rowspan': 1}, None, {'type': 'indicator'}],
                [{'type': 'pie'}, {'type': 'bar'}, {'type': 'indicator'}]
            ],
            subplot_titles=('净值曲线', '', '回撤', '', '退出原因', '收益分布', ''),
            vertical_spacing=0.12,
            horizontal_spacing=0.08
        )
        
        # 1. 净值曲线 (row=1, col=1)
        if not self.equity_curve.empty:
            df = self.equity_curve.copy()
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            fig.add_trace(
                go.Scatter(
                    x=df['trade_date'],
                    y=df['nav'],
                    mode='lines',
                    name='净值',
                    line=dict(color=self.colors['primary'], width=2),
                    showlegend=False
                ),
                row=1, col=1
            )
        
        # 2. 回撤曲线 (row=2, col=1)
        if not self.equity_curve.empty and 'drawdown' in self.equity_curve.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['trade_date'],
                    y=-df['drawdown'],
                    mode='lines',
                    name='回撤',
                    fill='tozeroy',
                    line=dict(color=self.colors['negative'], width=1),
                    fillcolor='rgba(214, 39, 40, 0.3)',
                    showlegend=False
                ),
                row=2, col=1
            )
        
        # 3. 指标卡片 - 总收益
        fig.add_trace(
            go.Indicator(
                mode='number+delta',
                value=self.total_return,
                number={'suffix': '%', 'font': {'size': 36}},
                delta={'reference': 0, 'relative': False},
                title={'text': '总收益', 'font': {'size': 14}},
                domain={'row': 0, 'column': 2}
            ),
            row=1, col=3
        )
        
        # 4. 指标卡片 - 夏普比率
        fig.add_trace(
            go.Indicator(
                mode='number',
                value=self.sharpe_ratio,
                number={'font': {'size': 36}},
                title={'text': '夏普比率', 'font': {'size': 14}},
                domain={'row': 1, 'column': 2}
            ),
            row=2, col=3
        )
        
        # 5. 指标卡片 - 胜率
        fig.add_trace(
            go.Indicator(
                mode='gauge+number',
                value=self.win_rate,
                number={'suffix': '%'},
                title={'text': '胜率'},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': self.colors['primary']},
                    'steps': [
                        {'range': [0, 40], 'color': 'rgba(214, 39, 40, 0.3)'},
                        {'range': [40, 60], 'color': 'rgba(255, 165, 0, 0.3)'},
                        {'range': [60, 100], 'color': 'rgba(44, 160, 44, 0.3)'}
                    ],
                    'threshold': {
                        'line': {'color': 'red', 'width': 2},
                        'thickness': 0.75,
                        'value': 50
                    }
                }
            ),
            row=3, col=3
        )
        
        # 6. 退出原因饼图 (row=3, col=1)
        if not self.trades.empty:
            sells = self.trades[self.trades['trade_type'] == 'SELL']
            if not sells.empty:
                exit_stats = sells['exit_reason'].value_counts()
                fig.add_trace(
                    go.Pie(
                        labels=exit_stats.index,
                        values=exit_stats.values,
                        hole=0.4,
                        marker_colors=px.colors.qualitative.Set2,
                        textinfo='percent',
                        showlegend=False
                    ),
                    row=3, col=1
                )
        
        # 7. 收益分布柱状图 (row=3, col=2)
        if not self.trades.empty:
            sells = self.trades[self.trades['trade_type'] == 'SELL'].copy()
            if not sells.empty:
                sells['pnl_pct'] = sells['pnl_pct'].fillna(0)
                
                # 分组统计
                bins = [-100, -5, -2, 0, 2, 5, 100]
                labels = ['<-5%', '-5~-2%', '-2~0%', '0~2%', '2~5%', '>5%']
                sells['ret_group'] = pd.cut(sells['pnl_pct'], bins=bins, labels=labels)
                group_counts = sells['ret_group'].value_counts().reindex(labels).fillna(0)
                
                bar_colors = [self.colors['negative']]*3 + [self.colors['positive']]*3
                
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=group_counts.values,
                        marker_color=bar_colors,
                        showlegend=False
                    ),
                    row=3, col=2
                )
        
        # 布局
        fig.update_layout(
            title=dict(
                text=f'📊 回测仪表板 | 会话: {self.session_id[:8]} | '
                     f'回测{self.backtest_days}天 | 持仓{self.hold_period}天',
                font=dict(size=18)
            ),
            height=800,
            showlegend=False,
            plot_bgcolor=self.colors['background'],
            paper_bgcolor='white'
        )
        
        if save_path:
            fig.write_html(save_path)
            logger.info(f"[Visualizer] 仪表板已保存: {save_path}")
        
        if show:
            fig.show()
        
        return fig

    def plot_all(self, save_dir: Optional[str] = None, show: bool = True):
        """
        生成所有图表
        
        Args:
            save_dir: 保存目录（可选）
            show: 是否显示图表
        """
        import os
        
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            prefix = os.path.join(save_dir, f'backtest_{self.session_id[:8]}')
        else:
            prefix = None
        
        print(f"\n{'='*60}")
        print(f"📈 生成回测可视化报告")
        print(f"会话ID: {self.session_id}")
        print(f"{'='*60}\n")
        
        # 1. 净值曲线
        print("1/5 生成净值曲线...")
        self.plot_equity_curve(
            show=show, 
            save_path=f'{prefix}_equity.html' if prefix else None
        )
        
        # 2. 交易分析
        print("2/5 生成交易分析...")
        self.plot_trade_analysis(
            show=show,
            save_path=f'{prefix}_trades.html' if prefix else None
        )
        
        # 3. 月度收益
        print("3/5 生成月度收益热力图...")
        self.plot_monthly_returns(
            show=show,
            save_path=f'{prefix}_monthly.html' if prefix else None
        )
        
        # 4. 交易时间线
        print("4/5 生成交易时间线...")
        self.plot_trade_timeline(
            show=show,
            save_path=f'{prefix}_timeline.html' if prefix else None
        )
        
        # 5. 综合仪表板
        print("5/7 生成综合仪表板...")
        self.plot_summary_dashboard(
            show=show,
            save_path=f'{prefix}_dashboard.html' if prefix else None
        )
        
        # 6. 交易操作日志表格
        print("6/7 生成交易操作日志...")
        self.plot_trade_log(
            show=show,
            save_path=f'{prefix}_trade_log.html' if prefix else None
        )
        
        # 7. 可操作文本报告
        print("7/7 生成可操作文本报告...")
        report = self.generate_actionable_report(
            save_path=f'{prefix}_操作指南.txt' if prefix else None
        )
        if not prefix:
            print("\n" + report)
        
        print(f"\n✅ 所有图表生成完成!")
        if prefix:
            print(f"📁 保存位置: {save_dir}")
            print(f"📋 操作指南: {prefix}_操作指南.txt")


def visualize_latest_backtest(show: bool = True, save_dir: Optional[str] = None):
    """
    可视化最近一次回测结果
    
    Args:
        show: 是否显示图表
        save_dir: 保存目录
    """
    from ..data import get_backtest_sessions
    
    sessions = get_backtest_sessions(limit=1)
    if sessions.empty:
        print("暂无回测记录")
        return
    
    session_id = sessions.iloc[0]['session_id']
    viz = BacktestVisualizer(session_id)
    viz.plot_all(save_dir=save_dir, show=show)
    return viz


def visualize_session(session_id: str, show: bool = True, save_dir: Optional[str] = None):
    """
    可视化指定回测会话
    
    Args:
        session_id: 会话 ID
        show: 是否显示图表
        save_dir: 保存目录
    """
    viz = BacktestVisualizer(session_id)
    viz.plot_all(save_dir=save_dir, show=show)
    return viz
