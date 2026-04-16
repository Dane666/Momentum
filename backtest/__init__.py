# -*- coding: utf-8 -*-
"""
Momentum Backtest Module - 回测模块

支持功能:
- 参数敏感性分析
- 交易记录持久化 (SQLite)
- 可视化数据查询
- Plotly 交互式图表
- 滑动窗口稳定性分析 (v2)
- 收益集中度检测 (v2)
"""

from .simulator import MomentumBacktester, run_sensitivity_analysis

# 导入参数优化器
from .param_optimizer import (
    ParamOptimizer,
    run_param_optimization,
    DEFAULT_PARAM_GRID,
    FAST_PARAM_GRID,
)

# 导入稳定性分析模块 (v2)
from .stability import (
    StabilityAnalyzer,
    detect_lookahead_bias,
    run_stability_check,
)

# 导入交易记录相关函数
from ..data import (
    BacktestTradeRecorder,
    get_backtest_sessions,
    get_session_trades,
    get_session_equity_curve,
    get_session_positions,
    get_trade_statistics,
    delete_session,
)

# 导入可视化模块
from .visualizer import (
    BacktestVisualizer,
    visualize_latest_backtest,
    visualize_session,
)

__all__ = [
    'MomentumBacktester', 
    'run_sensitivity_analysis',
    # 参数优化
    'ParamOptimizer',
    'run_param_optimization',
    'DEFAULT_PARAM_GRID',
    'FAST_PARAM_GRID',
    # 交易记录
    'BacktestTradeRecorder',
    'get_backtest_sessions',
    'get_session_trades',
    'get_session_equity_curve',
    'get_session_positions',
    'get_trade_statistics',
    'delete_session',
    # 可视化
    'BacktestVisualizer',
    'visualize_latest_backtest',
    'visualize_session',
]
