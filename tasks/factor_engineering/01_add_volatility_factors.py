# -*- coding: utf-8 -*-
"""
因子模块 01: 波动率类因子
=======================
输入: DataFrame (索引 trade_date, 列 open/high/low/close/volume/turnover_ratio)
输出: 因子 Series (与输入同索引)

因子列表:
- hist_vol_20 / hist_vol_60 : 历史波动率(年化)
- hist_vol_ratio           : 短/长窗口波动率比(波动放大)
- atr_pct_14               : ATR 归一化(占收盘价比)
- boll_width_20            : 布林带宽度 (upper-lower)/mid
- boll_pctb_20             : %B = (close-lower)/(upper-lower)
"""
import numpy as np
import pandas as pd


def hist_vol(df: pd.DataFrame, window: int = 20, annualize: bool = True) -> pd.Series:
    """历史波动率: 日收益率滚动标准差。"""
    ret = df['close'].pct_change()
    vol = ret.rolling(window).std()
    if annualize:
        vol = vol * np.sqrt(252)
    return vol


def hist_vol_ratio(df: pd.DataFrame, short: int = 20, long: int = 60) -> pd.Series:
    """短窗口波动 / 长窗口波动, 捕捉波动放大。"""
    s = hist_vol(df, short, annualize=False)
    l = hist_vol(df, long, annualize=False)
    return s / (l + 1e-9)


def atr_pct(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """ATR 归一化: ATR / close, 消除价格量纲。"""
    high, low, close = df['high'], df['low'], df['close']
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low),
         (high - prev_close).abs(),
         (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window).mean()
    return atr / (close + 1e-9)


def boll_width(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.Series:
    """布林带宽度: (upper-lower)/mid。"""
    mid = df['close'].rolling(window).mean()
    std = df['close'].rolling(window).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return (upper - lower) / (mid + 1e-9)


def boll_pctb(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.Series:
    """%B 位置: (close-lower)/(upper-lower), 0=下轨, 1=上轨。"""
    mid = df['close'].rolling(window).mean()
    std = df['close'].rolling(window).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return (df['close'] - lower) / (upper - lower + 1e-9)


def all_volatility_factors(df: pd.DataFrame) -> pd.DataFrame:
    """一次性计算全部波动率因子, 返回宽表(索引同 df)。"""
    out = pd.DataFrame(index=df.index)
    out['hist_vol_20'] = hist_vol(df, 20)
    out['hist_vol_60'] = hist_vol(df, 60)
    out['hist_vol_ratio'] = hist_vol_ratio(df, 20, 60)
    out['atr_pct_14'] = atr_pct(df, 14)
    out['boll_width_20'] = boll_width(df, 20)
    out['boll_pctb_20'] = boll_pctb(df, 20)
    return out
