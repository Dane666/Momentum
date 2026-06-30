# -*- coding: utf-8 -*-
"""mootdx 数据源 — TCP 通达信, 零鉴权不封IP (K线/指数/股票列表)"""
import pandas as pd; import logging
from typing import Optional
logger = logging.getLogger('momentum')
_q = None

def _get():
    global _q
    if _q is None:
        from mootdx.quotes import Quotes
        _q = Quotes.factory(market='std')
    return _q

def fetch_kline_mootdx(code: str, start_date: str = None, count: int = 500) -> Optional[pd.DataFrame]:
    try:
        df = _get().bars(symbol=code, category=4, start=0, count=count)
        if df is None or df.empty: return None
        df = df.reset_index()
        if 'datetime' in df.columns: df = df.drop(columns=['datetime'])
        df = df.rename(columns={'index':'trade_date','vol':'volume'})
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        df['turnover_ratio'] = 0.0
        return df[['trade_date','open','close','high','low','volume','amount','turnover_ratio']]
    except Exception as e:
        logger.debug(f"mootdx kline {code}: {e}")
        return None

def fetch_index_mootdx(code: str = '000001', count: int = 800) -> Optional[pd.DataFrame]:
    try:
        df = _get().bars(symbol=code, category=4, start=0, count=count)
        if df is None or df.empty: return None
        df = df.reset_index()
        if 'datetime' in df.columns: df = df.drop(columns=['datetime'])
        df = df.rename(columns={'index':'trade_date'})
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        return df[['trade_date','open','close','high','low']]
    except Exception as e:
        logger.warning(f"mootdx index {code}: {e}")
        return None
