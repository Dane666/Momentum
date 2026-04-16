# -*- coding: utf-8 -*-
"""
Momentum CLI Module - 命令行接口模块

将命令行相关功能按职责分离:
- monitor: 持仓诊断相关命令
- backtest: 回测相关命令
- analysis: 分析报告相关命令
"""

from .monitor import (
    run_portfolio_monitor,
    run_market_scan,
    run_full_workflow,
    run_etf_scan,
    run_full_workflow_with_ollama,
)

from .backtest_cmd import (
    run_backtest,
    show_backtest_history,
    show_session_detail,
    run_visualize,
)

from .analysis import (
    run_trade_analysis,
    show_strategy_rules,
)

from .grid_cmd import (
    run_grid_screening,
    print_grid_trading_guide,
)

__all__ = [
    # 持仓诊断
    'run_portfolio_monitor',
    'run_market_scan', 
    'run_full_workflow',
    'run_etf_scan',
    'run_full_workflow_with_ollama',
    # 回测
    'run_backtest',
    'show_backtest_history',
    'show_session_detail',
    'run_visualize',
    # 分析
    'run_trade_analysis',
    'show_strategy_rules',
    # 网格交易
    'run_grid_screening',
    'print_grid_trading_guide',
]
