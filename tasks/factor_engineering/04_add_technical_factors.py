# -*- coding: utf-8 -*-
"""
因子模块 04: 改进版技术指标因子
=======================
在现有 compute_rsi(简单均值) 基础上改用 Wilder's 平滑; 新增 MACD 柱斜率、
KDJ(K/D/J)、长周期乖离率。

因子列表:
- rsi_14_imp      : Wilder's RSI(14) (改进版)
- macd_hist_slope : MACD 柱一阶差分(柱变化加速度)
- kdj_k / kdj_d / kdj_j : KDJ 三线与 J 线
- bias_60         : 60日乖离率 close/MA60 - 1
"""
import numpy as np
import pandas as pd


def rsi_ewm(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """Wilder's RSI (EWM alpha=1/window), 改进 compute_rsi 的简单均值。"""
    delta = df['close'].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta.clip(upper=0.0))
    avg_gain = gain.ewm(alpha=1.0 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / window, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100.0 - 100.0 / (1.0 + rs)


def macd_hist_slope(df: pd.DataFrame, fast: int = 12, slow: int = 26,
                    signal: int = 9) -> pd.Series:
    """MACD 柱(DIF-DEA)的一阶差分, 表征动能加速度。"""
    ema_f = df['close'].ewm(span=fast, adjust=False).mean()
    ema_s = df['close'].ewm(span=slow, adjust=False).mean()
    dif = ema_f - ema_s
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = dif - dea
    return hist.diff()


def kdj(df: pd.DataFrame, window: int = 9, k_smooth: int = 3,
        d_smooth: int = 3):
    """KDJ: 返回 (K, D, J) 三个 Series。"""
    low_n = df['low'].rolling(window).min()
    high_n = df['high'].rolling(window).max()
    rsv = (df['close'] - low_n) / (high_n - low_n + 1e-9) * 100.0
    k = rsv.ewm(alpha=1.0 / k_smooth, adjust=False).mean()
    d = k.ewm(alpha=1.0 / d_smooth, adjust=False).mean()
    j = 3.0 * k - 2.0 * d
    return k, d, j


def bias_n(df: pd.DataFrame, window: int = 60) -> pd.Series:
    """N日乖离率: close / MA(N) - 1。"""
    ma = df['close'].rolling(window).mean()
    return df['close'] / (ma + 1e-9) - 1.0


def all_technical_factors(df: pd.DataFrame) -> pd.DataFrame:
    """一次性计算全部改进技术指标因子。"""
    out = pd.DataFrame(index=df.index)
    out['rsi_14_imp'] = rsi_ewm(df, 14)
    out['macd_hist_slope'] = macd_hist_slope(df)
    k, d, j = kdj(df)
    out['kdj_k'] = k
    out['kdj_d'] = d
    out['kdj_j'] = j
    out['bias_60'] = bias_n(df, 60)
    return out
