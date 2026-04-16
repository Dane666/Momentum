# -*- coding: utf-8 -*-
"""
技术因子模块
- 动量因子
- 波动率因子
- RSI/ATR等技术指标
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional
import logging

logger = logging.getLogger('momentum')


def get_style_group(mkt_cap: float) -> str:
    """
    根据市值进行风格分组

    Args:
        mkt_cap: 总市值

    Returns:
        'LargeCap' 或 'SmallCap'
    """
    # 以 200亿 作为大小盘分界线
    return 'LargeCap' if mkt_cap > 20000000000 else 'SmallCap'


def compute_rsi(prices: pd.Series, period: int = 14) -> float:
    """
    计算 RSI 指标

    Args:
        prices: 收盘价序列
        period: 计算周期

    Returns:
        RSI 值 (0-100)
    """
    delta = prices.diff()
    gain = delta.clip(lower=0).tail(period).mean()
    loss = (-delta.clip(upper=0)).tail(period).mean()

    if loss == 0:
        return 100.0

    rs = gain / loss
    return 100 - (100 / (1 + rs))


def compute_atr(df: pd.DataFrame, period: int = 20) -> float:
    """
    计算 ATR (Average True Range)

    Args:
        df: 包含 high, low, close 的 DataFrame
        period: 计算周期

    Returns:
        ATR 值
    """
    high = df['high']
    low = df['low']
    close = df['close'].shift(1)

    tr1 = high - low
    tr2 = (high - close).abs()
    tr3 = (low - close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.tail(period).mean()


def compute_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    计算 ADX (Average Directional Index)

    Args:
        df: 包含 high, low, close 的 DataFrame
        period: 计算周期

    Returns:
        ADX 值 (0-100)
    """
    if len(df) < period * 2:
        return 0.0

    high = df['high']
    low = df['low']
    close = df['close']

    # 1. 计算 TR
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 2. 计算 DM
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    # 3. 平滑处理 (Wilder's Smoothing)
    # 初始值用简单平均
    tr_smooth = tr.rolling(window=period).sum()
    plus_dm_smooth = pd.Series(plus_dm, index=df.index).rolling(window=period).sum()
    minus_dm_smooth = pd.Series(minus_dm, index=df.index).rolling(window=period).sum()

    # 4. 计算 DI
    plus_di = 100 * (plus_dm_smooth / tr_smooth)
    minus_di = 100 * (minus_dm_smooth / tr_smooth)

    # 5. 计算 DX
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))

    # 6. 计算 ADX (DX 的移动平均)
    adx = dx.rolling(window=period).mean()

    return adx.iloc[-1]


def compute_technical_snapshot(df: pd.DataFrame) -> dict:
    """
    计算技术因子快照

    Args:
        df: K线数据 DataFrame (需要 close, volume, turnover_rate 等字段)

    Returns:
        dict: 包含各技术因子的字典
    """
    if len(df) < 35:
        return None

    try:
        df = df.copy()
        df['ret'] = df['close'].pct_change()

        # 波动率
        v20 = df['ret'].tail(20).std() + 1e-9

        # 5日动量
        mom_5 = (df['close'].iloc[-1] / df['close'].iloc[-5]) - 1

        # 20日动量 (波动率调整)
        mom_20_raw = (df['close'].iloc[-1] / df['close'].iloc[-20]) - 1
        mom_20 = mom_20_raw * (0.02 / v20)

        # 夏普比率
        sharpe = (df['ret'].tail(20).mean() / v20) * np.sqrt(252)

        # 量比
        avg_vol_5 = df['volume'].tail(6).iloc[:-1].mean()
        vol_ratio = df['volume'].iloc[-1] / (avg_vol_5 + 1e-9)

        # 换手率
        turnover = df['turnover_rate'].iloc[-1] if 'turnover_rate' in df.columns else 0

        # 换手率调整系数
        turnover_mult = 1.0
        if 12 < turnover < 18:
            turnover_mult = 1.15  # 充分换手奖励
        elif turnover < 3 and df['ret'].iloc[-1] > 0.05:
            turnover_mult = 0.6  # 缩量拉升惩罚

        # RSI
        rsi = compute_rsi(df['close'])

        # ATR
        atr = compute_atr(df)

        # MA
        ma5 = df['close'].rolling(5).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]

        # 乖离率
        bias_20 = (df['close'].iloc[-1] / ma20) - 1

        return {
            'mom_5': mom_5,
            'mom_20': mom_20 * turnover_mult,
            'sharpe': sharpe,
            'vol_ratio': vol_ratio,
            'turnover': turnover,
            'rsi': rsi,
            'atr': atr,
            'ma5': ma5,
            'ma20': ma20,
            'bias_20': bias_20,
            'close': df['close'].iloc[-1],
            'change_pct': df['ret'].iloc[-1] * 100,
        }

    except Exception as e:
        logger.debug(f"[Technical] 计算失败: {e}")
        return None


def compute_dual_day_factors(df: pd.DataFrame) -> Tuple[Optional[dict], Optional[dict]]:
    """
    计算今日和昨日的技术因子 (用于趋势判断)

    Args:
        df: K线数据 DataFrame

    Returns:
        (today_factors, yesterday_factors)
    """
    if len(df) < 36:
        return None, None

    today = compute_technical_snapshot(df)
    yesterday = compute_technical_snapshot(df.iloc[:-1])

    return today, yesterday
