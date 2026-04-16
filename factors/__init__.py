# -*- coding: utf-8 -*-
"""
Momentum Factors Module - 因子层
- 技术因子
- 情绪因子
- 市场宽度
- 量化因子 (庄股识别)
"""

from .technical import (
    compute_technical_snapshot,
    compute_dual_day_factors,
    get_style_group,
    compute_rsi,
    compute_atr,
)
from .sentiment import (
    get_limit_up_streak,
    get_connect_sentiment,
    get_position_multiplier,
)
from .market import (
    get_market_breadth_pro,
    get_hot_sectors,
)
from .quant_factors import (
    QuantFactors,
    calc_momentum_quality,
    calc_ivol,
    calc_amihud_illiquidity,
    calc_overnight_intraday,
)

__all__ = [
    # 技术因子
    'compute_technical_snapshot',
    'compute_dual_day_factors',
    'get_style_group',
    'compute_rsi',
    'compute_atr',
    # 情绪因子
    'get_limit_up_streak',
    'get_connect_sentiment',
    'get_position_multiplier',
    # 市场因子
    'get_market_breadth_pro',
    'get_hot_sectors',
    # 量化因子 (庄股识别)
    'QuantFactors',
    'calc_momentum_quality',
    'calc_ivol',
    'calc_amihud_illiquidity',
    'calc_overnight_intraday',
]
