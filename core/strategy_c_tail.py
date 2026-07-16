# -*- coding: utf-8 -*-
"""
C 尾盘偷袭板策略 — gap_h2_lu0_trapFalse 生产版
=============================================
对标 opt_study/harness_c_enhanced.py gap_h2_lu0_trapFalse.
核心: 热门行业资金流入 Top-K → 尾盘偷袭板龙头(涨幅>5%/收盘≈最高/开盘未涨停/未封板)
     → MA60 开门闸口 → 持2天 → 隔天高开止盈/到期
"""
from __future__ import annotations
import logging
from collections import defaultdict
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger('momentum.c_tail')


def _get_sector(code: str) -> str:
    try:
        from ..data import load_or_fetch_kline, fetch_kline_from_api
        df = load_or_fetch_kline(str(code), fetch_kline_from_api)
        if df is not None and not df.empty:
            for col in ['sw', 'industry', 'sector']:
                if col in df.columns:
                    val = df[col].dropna()
                    if not val.empty:
                        return str(val.iloc[-1])
    except Exception:
        pass
    p = code[:2]
    if p == '60': return '上海主板'
    elif p == '68': return '科创板'
    elif p == '00': return '深圳主板'
    elif p == '30': return '创业板'
    return '其它'


def fetch_market_data() -> Optional[pd.DataFrame]:
    """全A实时行情, 复用项目数据管道(efinance → Sina 降级)."""
    try:
        from ..data import fetch_realtime_quotes
        df = fetch_realtime_quotes(fs='沪深A股')
        if df is not None and not df.empty:
            for c in ['涨跌幅', '成交额', '最新价']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            for c in ['最高', '最高价', '今开', '昨收', '开盘价']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            return df
    except Exception as e:
        logger.warning(f"实时行情: {e}")
    return None


def calc_sector_heat(df: pd.DataFrame, code_sector: Dict[str, str],
                     min_stocks: int = 4) -> Dict[str, float]:
    df = df.copy()
    df['code'] = df['股票代码'].astype(str)
    df = df[df['code'].str.startswith(('60', '00', '30', '68'))]
    df = df[~df['股票名称'].astype(str).str.contains('ST|退', na=False)]
    df['change'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
    df['amt'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)
    df['net_flow'] = df['amt'] * df['change'] / 100
    heat, cnt = defaultdict(float), defaultdict(int)
    for _, r in df.iterrows():
        sec = code_sector.get(r['code'], '其它')
        heat[sec] += r['net_flow']
        cnt[sec] += 1
    return {s: h for s, h in heat.items() if cnt.get(s, 0) >= min_stocks}


def pick_tailspike(df: pd.DataFrame, hot_codes: List[str]) -> List[Dict]:
    """
    条件(对标 pick_tailspike, lu_min=0, intraday_min=0.05):
      ret>5%  &  close≈high(≥0.99)  &  开盘未涨停(op/prev<lim-3%)
      &  盘中碰过涨停价(high≥limit_price*0.995)  &  未封板
    """
    df = df.copy()
    df['code'] = df['股票代码'].astype(str)
    df = df[df['code'].str.startswith(('60', '00', '30', '68'))]
    df = df[~df['股票名称'].astype(str).str.contains('ST|退', na=False)]
    df['change'] = pd.to_numeric(df['涨跌幅'], errors='coerce').fillna(0)
    df['price'] = pd.to_numeric(df['最新价'], errors='coerce').fillna(0)
    df['amt'] = pd.to_numeric(df['成交额'], errors='coerce').fillna(0)

    col_hi = next((c for c in ['最高', '最高价'] if c in df.columns), None)
    col_op = next((c for c in ['今开', '开盘价'] if c in df.columns), None)
    col_pre = next((c for c in ['昨收', '昨日收盘'] if c in df.columns), None)
    df['high'] = pd.to_numeric(df[col_hi], errors='coerce').fillna(0) if col_hi else df['price']
    df['open'] = pd.to_numeric(df[col_op], errors='coerce').fillna(0) if col_op else df['price']
    df['prev'] = pd.to_numeric(df[col_pre], errors='coerce').fillna(0) if col_pre else df['price']

    if hot_codes:
        df = df[df['code'].isin(set(hot_codes))]

    df = df[df['change'] > 5.0]
    df['_is_lu'] = False
    for idx, row in df.iterrows():
        c = str(row['code'])
        lim = 19.9 if c.startswith(('30', '688')) else 9.9
        df.at[idx, '_is_lu'] = row['change'] >= lim
    df = df[~df['_is_lu']]

    candidates = []
    for _, r in df.iterrows():
        code = str(r['code']); ret = r['change'] / 100
        price = r['price']; hi = max(r['high'], price)
        op = r['open'] if r['open'] > 0 else price
        prev = r['prev'] if r['prev'] > 0 else (price / (1 + ret) if ret > -0.99 else price)
        lim = 0.20 if code.startswith(('30', '688')) else 0.10
        opv = (op / prev) - 1
        limit_price = prev * (1 + lim)
        if not (ret > 0.05): continue
        if not (price >= hi * 0.99): continue
        if opv >= lim - 0.03: continue
        if hi < limit_price * 0.995: continue
        candidates.append({
            'code': code, 'name': str(r.get('股票名称', code)),
            'price': round(price, 2), 'change_pct': round(r['change'], 2),
            'amount': round(r['amt'] / 1e8, 2),
        })
    candidates.sort(key=lambda x: x['change_pct'], reverse=True)
    return candidates


def run_c_scan(top_k: int = 8, top_n: int = 3) -> Tuple[List[Dict], List, str, bool]:
    is_bull, ma60_msg, ma60_detail = True, '', {}
    try:
        from ..factors.market import calc_ma60_gate_open
        is_bull, ma60_msg, ma60_detail = calc_ma60_gate_open('000001')
    except Exception as e:
        logger.warning(f"MA60: {e}")

    df_all = fetch_market_data()
    if df_all is None or df_all.empty:
        return [], [], '数据获取失败', is_bull

    df_all['code_str'] = df_all['股票代码'].astype(str)
    df_all['_amt'] = pd.to_numeric(df_all['成交额'], errors='coerce').fillna(0)
    # Only look up sector for top-200 by turnover; fallback to board prefix for rest
    top200 = df_all.nlargest(200, '_amt')['code_str'].tolist()
    code_sector = {}
    for c in top200:
        s = _get_sector(c)
        if s and s != '其它': code_sector[c] = s
    for _, r in df_all.iterrows():
        c = str(r['股票代码'])
        if c not in code_sector:
            p = c[:2]
            if p == '60': code_sector[c] = '上海主板'
            elif p == '68': code_sector[c] = '科创板'
            elif p == '00': code_sector[c] = '深圳主板'
            elif p == '30': code_sector[c] = '创业板'
            else: code_sector[c] = '其它'

    hot = calc_sector_heat(df_all, code_sector, 4)
    ranked = sorted(hot.items(), key=lambda x: -x[1])[:top_k]
    hot_names = [s for s, _ in ranked]

    hot_stock_set = set()
    for _, r in df_all.iterrows():
        c = str(r['股票代码'])
        if code_sector.get(c, '其它') in hot_names:
            hot_stock_set.add(c)

    leaders = []
    if is_bull and hot_stock_set:
        leaders = pick_tailspike(df_all, list(hot_stock_set))[:top_n]

    lines = ['🎯 C·尾盘偷袭板 | 14:44']
    if is_bull:
        d = ma60_detail.get('diff_pct', 0) if ma60_detail else 0
        lines.append(f'✅ MA60: 站上 (+{d:.1f}%)')
    else:
        lines.append('⚠️ MA60: 跌破 — 空仓')
    lines.append('─' * 40)
    if ranked:
        lines.append(f'🔥 热门行业 Top{len(ranked)}:')
        for s, h in ranked[:8]:
            lines.append(f'  {s}: {h/1e4:+.0f}万')
    if leaders:
        lines.append(f'\n🚀 偷袭板 ({len(leaders)}只):')
        for l in leaders:
            lines.append(f'  {l["code"]} {l["name"]}: ¥{l["price"]:.2f} '
                         f'({l["change_pct"]:+.1f}%) | 成交{l["amount"]:.1f}亿')
    elif is_bull:
        lines.append('\n📋 无符合偷袭板条件的标的')
    return leaders, ranked, '\n'.join(lines), is_bull
