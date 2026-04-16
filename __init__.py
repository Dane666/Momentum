# -*- coding: utf-8 -*-
"""
Momentum 量化策略包
Qlib-Pro v16 工程化重构

目录结构:
    momentum/
    ├── __init__.py          # 包入口
    ├── config.py            # 配置管理
    ├── core/                # 核心引擎
    │   ├── __init__.py
    │   └── engine.py        # MomentumEngine
    ├── factors/             # 因子模块
    │   ├── __init__.py
    │   ├── technical.py     # 技术因子
    │   ├── sentiment.py     # 情绪因子
    │   └── market.py        # 市场因子
    ├── data/                # 数据层
    │   ├── __init__.py
    │   ├── db.py            # 数据库操作
    │   ├── cache.py         # K线缓存
    │   └── fetcher.py       # API封装
    ├── backtest/            # 回测模块
    │   ├── __init__.py
    │   └── simulator.py     # 回测模拟器
    ├── notify/              # 通知模块
    │   ├── __init__.py
    │   └── feishu.py        # 飞书通知
    └── main.py              # 主入口

使用示例:
    from momentum.core import MomentumEngine
    from momentum.backtest import MomentumBacktester

    # 实盘选股
    engine = MomentumEngine(holdings=['000001', '600000'])
    engine.run_all_market_scan_pro()

    # 回测
    backtester = MomentumBacktester(backtest_days=250, hold_period=3)
    backtester.run_backtest()
"""

from . import config

__version__ = '16.1.0'
__author__ = 'Quantitative Team'

__all__ = ['config']
