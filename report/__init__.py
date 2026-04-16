# -*- coding: utf-8 -*-
"""
Report 模块 - 统一报告生成

职责:
- 选股报告生成
- 持仓诊断报告生成
- ETF诊断报告生成
- 回测报告生成

从 engine.py 提取报告相关逻辑，遵循单一职责原则。
"""

from .formatter import ReportFormatter, pad_str, format_pct, format_price, format_amount
from .scan_report import ScanReportGenerator
from .portfolio_report import PortfolioReportGenerator

__all__ = [
    'ReportFormatter',
    'pad_str',
    'format_pct',
    'format_price',
    'format_amount',
    'ScanReportGenerator',
    'PortfolioReportGenerator',
]
