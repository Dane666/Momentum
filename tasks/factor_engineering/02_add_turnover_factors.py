# -*- coding: utf-8 -*-
"""
因子模块 02: 换手率类因子
=======================
依赖 kline_cache.turnover_ratio (注意: 现有 factors/technical.py 误用 turnover_rate 字段,
本模块使用正确的 turnover_ratio)。

因子列表:
- turn_chg_5     : 当日换手 / 5日均换手 - 1 (换手突变)
- turn_zscore_20 : 换手率 20日 z-score (横向偏离)
- turn_ma_ratio  : 5日换手均 / 60日换手均 (放量换手)
- turn_rank_60   : 当日换手在 60日内的分位排名 (0-1)
"""
import numpy as np
import pandas as pd


def turn_chg(df: pd.DataFrame, window: int = 5) -> pd.Series:
    """换手率突变: 当日 / 短期均值 - 1。"""
    tr = df['turnover_ratio']
    ma = tr.rolling(window).mean()
    return tr / (ma + 1e-9) - 1.0


def turn_zscore(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """换手率横向 z-score。"""
    tr = df['turnover_ratio']
    m = tr.rolling(window).mean()
    s = tr.rolling(window).std()
    return (tr - m) / (s + 1e-9)


def turn_ma_ratio(df: pd.DataFrame, short: int = 5, long: int = 60) -> pd.Series:
    """短/长窗口换手均值比, 捕捉持续放量。"""
    return df['turnover_ratio'].rolling(short).mean() / (
        df['turnover_ratio'].rolling(long).mean() + 1e-9
    )


def turn_rank(df: pd.DataFrame, window: int = 60) -> pd.Series:
    """当日换手在滚动窗口内的分位排名。"""
    tr = df['turnover_ratio']

    def _rank(x: np.ndarray) -> float:
        if len(x) < 2:
            return np.nan
        return float((x[-1] > x[:-1]).mean())

    return tr.rolling(window, min_periods=2).apply(_rank, raw=True)


def all_turnover_factors(df: pd.DataFrame) -> pd.DataFrame:
    """一次性计算全部换手率因子。"""
    out = pd.DataFrame(index=df.index)
    out['turn_chg_5'] = turn_chg(df, 5)
    out['turn_zscore_20'] = turn_zscore(df, 20)
    out['turn_ma_ratio'] = turn_ma_ratio(df, 5, 60)
    out['turn_rank_60'] = turn_rank(df, 60)
    return out
