# -*- coding: utf-8 -*-
"""30分钟 ATR 动态退出: 追踪止损 + 时间风控 + 乖离脉冲"""
import pandas as pd; import numpy as np
from typing import Tuple

ATR_P=14; ATR_M=2.2; MIN_H=2; TIME_B=12; TIME_TH=0.018; BIAS_P=5; BIAS_TH=0.045
SLP_C=0.15; MAX_SLP=0.005

def _get_atr_m():
    try:
        from .. import config as cfg
        return getattr(cfg,'ATR_MULTIPLIER_30M',2.2)
    except: return 2.2

def calc_atr(df,p=ATR_P):
    h,l,c=df['high'],df['low'],df['close']
    tr=pd.concat([h-l,(h-c.shift(1)).abs(),(l-c.shift(1)).abs()],axis=1).max(axis=1)
    return tr.rolling(p).mean()

def simulate_atr_exit(entry_price,df,entry_idx,hold_period=30,slippage=0.008,use_daily=True
    )->Tuple[float,str,int,str]:
    df=df.copy(); df['atr']=calc_atr(df)
    min_h=max(1,MIN_H//2) if use_daily else MIN_H
    t_bars=max(2,TIME_B//6) if use_daily else TIME_B
    highest=entry_price; ph=df['high'].iloc[entry_idx]
    for i in range(1,min(hold_period+1,len(df)-entry_idx)):
        ci=entry_idx+i;bh=df['high'].iloc[ci];bl=df['low'].iloc[ci]
        bc=df['close'].iloc[ci];ba=df['atr'].iloc[ci]
        if bh>highest: highest=bh
        ed=df['trade_date'].iloc[ci]; ed=str(ed)[:10] if hasattr(ed,'strftime') else str(ed)[:10]
        if i<min_h: ph=bh; continue
        # ATR trailing
        if pd.notna(ba) and ba>0:
            s=highest-(_get_atr_m()*ba)
            if bc<s:
                sl=min(ba*SLP_C,bc*MAX_SLP); ret=(bc-sl)/entry_price-1-slippage
                return ret,"ATR_Trail",i,ed
        # Time stop
        pnl=bc/entry_price-1
        if i>=t_bars and pnl<TIME_TH: return pnl-slippage,"Time_Stop",i,ed
        # Bias take-profit
        if i>=BIAS_P:
            m5=df['close'].iloc[ci-BIAS_P+1:ci+1].mean()
            b5=(bc/m5-1) if m5>0 else 0
            if b5>BIAS_TH and bc<ph: return pnl-slippage,"Bias_TP",i,ed
        ph=bh
    ei=min(entry_idx+hold_period,len(df)-1);ed=df['trade_date'].iloc[ei]
    ed=str(ed)[:10] if hasattr(ed,'strftime') else str(ed)[:10]
    return (df['close'].iloc[ei]/entry_price-1-slippage),"Time_Expire",ei-entry_idx,ed


def simulate_hybrid_exit(entry_price,df,entry_idx,hold_period=30,slippage=0.008,use_daily=True,
    atr_mult=3.5)->Tuple[float,str,int,str]:
    """
    混合制退出: 原止盈(固定+10%) + MA5/MA20 + ATR滚动止损(3.5×) + 5天到期
    """
    from .. import config as cfg
    tp=entry_price*getattr(cfg,'TAKE_PROFIT_PCT',0.10)+entry_price  # +10% fixed
    bias_limit=getattr(cfg,'BIAS_PROFIT_LIMIT',0.20)
    rsi_danger=getattr(cfg,'RSI_DANGER_ZONE',80.0)

    df=df.copy(); df['atr']=calc_atr(df)
    df['ma5']=df['close'].rolling(5).mean()
    df['ma20']=df['close'].rolling(20).mean()

    highest=entry_price
    for i in range(1,min(hold_period+1,len(df)-entry_idx)):
        ci=entry_idx+i
        bh=df['high'].iloc[ci];bl=df['low'].iloc[ci]
        bc=df['close'].iloc[ci];ba=df['atr'].iloc[ci]
        ma5=df['ma5'].iloc[ci];ma20=df['ma20'].iloc[ci]
        if bh>highest: highest=bh
        ed=df['trade_date'].iloc[ci];ed=str(ed)[:10] if hasattr(ed,'strftime') else str(ed)[:10]

        # ① 固定止盈+10%
        if bh>=tp:
            return (tp/entry_price-1-slippage),"TP_Fixed",i,ed
        # ② ATR 滚动止损
        if pd.notna(ba) and ba>0:
            atr_stop=highest-(atr_mult*ba)
            if bc<atr_stop:
                return (bc/entry_price-1-slippage),"ATR_Stop",i,ed
        # ③ MA5 趋势跟随
        if pd.notna(ma5) and bc<ma5:
            return (bc/entry_price-1-slippage),"MA5_Exit",i,ed
        # ④ 乖离率止盈
        if pd.notna(ma20) and ma20>0:
            bias=(bc/ma20)-1
            if bias>=bias_limit:
                return (bc/entry_price-1-slippage),"Bias_Exit",i,ed
        # ⑤ RSI超买
        rsi_val=_calc_rsi(df['close'].iloc[:ci+1])
        if rsi_val>=rsi_danger:
            return (bc/entry_price-1-slippage),"RSI_Exit",i,ed
        # ⑥ MA20破位
        if pd.notna(ma20) and bc<ma20:
            return (bc/entry_price-1-slippage),"MA20_Exit",i,ed

    ei=min(entry_idx+hold_period,len(df)-1);ed=df['trade_date'].iloc[ei]
    ed=str(ed)[:10] if hasattr(ed,'strftime') else str(ed)[:10]
    return (df['close'].iloc[ei]/entry_price-1-slippage),"Time_Expire",ei-entry_idx,ed


def _calc_rsi(prices,p=14):
    if len(prices)<p+1: return 50.0
    d=prices.diff();g=d.clip(lower=0);l=(-d).clip(lower=0)
    ag=g.rolling(p).mean().iloc[-1];al=l.rolling(p).mean().iloc[-1]
    return 100-100/(1+ag/al) if al>0 else 100.0
