# -*- coding: utf-8 -*-
"""
因子模块 03: 资金流向类因子
=======================
输入: OHLCV + turnover_ratio, 另有 amount(成交金额) 可用。

因子列表:
- mfi_14        : 资金流量指标 Money Flow Index (14)
- obv           : 能量潮 On-Balance Volume (归一化: 对数值变化)
- obv_ma_ratio  : OBV / MA20(OBV) - 1 (资金趋势)
- cmf_20        : Chaikin Money Flow (20)
- main_flow_ratio : 主力资金方向近似 ((收-低)-(高-收))/(高-低) 加权量比
"""
import numpy as np
import pandas as pd


def mfi(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """资金流量指标: 典型价加权成交量的正负流向比。"""
    tp = (df['high'] + df['low'] + df['close']) / 3.0
    rmf = tp * df['volume']
    up = tp > tp.shift(1)
    down = tp < tp.shift(1)
    pos = rmf.where(up, 0.0).rolling(window).sum()
    neg = rmf.where(down, 0.0).rolling(window).sum()
    ratio = pos / (neg + 1e-9)
    return 100.0 - 100.0 / (1.0 + ratio)


def obv(df: pd.DataFrame) -> pd.Series:
    """能量潮 OBV (累积)。返回原始累积值, 量纲大, 建议配合 obv_ma_ratio 使用。"""
    direction = np.sign(df['close'].diff().fillna(0.0))
    return (direction * df['volume']).cumsum()


def obv_ma_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """OBV 相对其均线的偏离, 表征资金趋势强弱。"""
    o = obv(df)
    ma = o.rolling(window).mean()
    return o / (ma + 1e-9) - 1.0


def cmf(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """Chaikin Money Flow: 资金流量累积 / 成交量累积。"""
    hl = (df['high'] - df['low']) + 1e-9
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / hl
    mfv = mfm * df['volume']
    return mfv.rolling(window).sum() / (df['volume'].rolling(window).sum() + 1e-9)


def main_flow_ratio(df: pd.DataFrame, window: int = 5) -> pd.Series:
    """主力资金方向近似: 实体重心偏向买方(收近高)为正, 加权成交量。"""
    hl = (df['high'] - df['low']) + 1e-9
    direction = ((df['close'] - df['low']) - (df['high'] - df['close'])) / hl
    mf = (direction * df['volume']).rolling(window).mean()
    tot = df['volume'].rolling(window).mean()
    return mf / (tot + 1e-9)


def all_money_flow_factors(df: pd.DataFrame) -> pd.DataFrame:
    """一次性计算全部资金流向因子。"""
    out = pd.DataFrame(index=df.index)
    out['mfi_14'] = mfi(df, 14)
    out['obv'] = obv(df)
    out['obv_ma_ratio'] = obv_ma_ratio(df, 20)
    out['cmf_20'] = cmf(df, 20)
    out['main_flow_ratio'] = main_flow_ratio(df, 5)
    return out
