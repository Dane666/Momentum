# -*- coding: utf-8 -*-
"""30分钟 ATR 动态退出: 追踪止损 + 时间风控 + 乖离脉冲"""
import pandas as pd; import numpy as np
from typing import Tuple

ATR_P=14; ATR_M=2.2; MIN_H=2; TIME_B=12; TIME_TH=0.018; BIAS_P=5; BIAS_TH=0.045
SLP_C=0.15; MAX_SLP=0.005

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
            s=highest-(ATR_M*ba)
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
