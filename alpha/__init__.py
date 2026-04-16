# -*- coding: utf-8 -*-
"""
Momentum Alpha Module - Alpha模型层

职责:
- Alpha因子合成与标准化
- 股票池过滤逻辑
- 行业中性化处理

主要组件:
- AlphaModel: Alpha 因子模型类
- industry_neutralization(): 行业中性化便捷函数
- compute_alpha_score(): Alpha 得分计算便捷函数
- ALPHA_WEIGHTS: Alpha 因子权重配置

使用示例:
    >>> from momentum.alpha import industry_neutralization
    >>> df = industry_neutralization(df, market_total_amount=1.5e12)
"""

from .alpha_model import (
    AlphaModel,
    industry_neutralization,
    compute_alpha_score,
    ALPHA_WEIGHTS,
    FACTOR_COLS_TODAY,
    FACTOR_COLS_YESTERDAY,
)

__all__ = [
    'AlphaModel',
    'industry_neutralization',
    'compute_alpha_score',
    'ALPHA_WEIGHTS',
    'FACTOR_COLS_TODAY',
    'FACTOR_COLS_YESTERDAY',
]
