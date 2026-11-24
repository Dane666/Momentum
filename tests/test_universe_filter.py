# -*- coding: utf-8 -*-
"""测试候选池质量过滤(2026-11-24): 主板过滤 + 涨停/一字板可买入性过滤。

背景: 用户交易权限仅覆盖沪深主板(60/00), 创业板/科创板选出也无法买入;
且 20% 涨跌停与模型假设(10%)不符。选股时点若为涨停/一字板则无法以合理价买入。
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
for _p in [str(ROOT), str(ROOT / 'tasks' / 'model_inference')]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _universe import filter_mainboard, filter_st, MAINBOARD_PREFIXES


def test_mainboard_prefixes():
    assert MAINBOARD_PREFIXES == ('60', '00')


def test_filter_mainboard_keeps_only_mainboard():
    pan = pd.DataFrame({'code': ['600000', '000001', '002230', '601398',
                                 '300001', '688001', '830001']})
    out = filter_mainboard(pan)
    codes = out['code'].tolist()
    assert '600000' in codes
    assert '000001' in codes
    assert '002230' in codes
    assert '601398' in codes
    # 创业板/科创板/北交所被剔除
    assert '300001' not in codes
    assert '688001' not in codes
    assert '830001' not in codes


def test_filter_mainboard_tolerates_exchange_suffix():
    # 代码带交易所后缀(如 603272.SH)也应正确归一
    pan = pd.DataFrame({'code': ['603272.SH', '300750.SZ', 'sh600036']})
    out = filter_mainboard(pan)
    codes = out['code'].tolist()
    assert '603272.SH' in codes       # 60 主板保留
    assert 'sh600036' in codes        # 60 主板保留
    assert '300750.SZ' not in codes   # 创业板剔除


def test_filter_mainboard_no_code_column_passthrough():
    pan = pd.DataFrame({'foo': [1, 2, 3]})
    out = filter_mainboard(pan)
    assert len(out) == 3   # 无 code 列时不破坏


# ---- 涨停/一字板可买入性过滤(内联 02 的 _flag_untradable 逻辑, 便于单测) ----
def _flag_untradable(ctx, pan):
    need = set(pan['code'].unique())
    ctxmap = {}
    for c, g in ctx.items():
        if c not in need or len(g) == 0:
            continue
        d = g[['open', 'high', 'close']].copy()
        d['prev'] = d['close'].shift(1)
        ctxmap[c] = d
    flags = []
    for idx in pan.index:
        c = pan.at[idx, 'code']
        td = pan.at[idx, 'trade_date']
        d = ctxmap.get(c)
        if d is None or td not in d.index:
            flags.append(False)
            continue
        row = d.loc[td]
        pre = row['prev']
        if pre is None or not np.isfinite(pre) or pre <= 0:
            flags.append(False)
            continue
        close = float(row['close']); high = float(row['high'])
        pct = (close - pre) / pre
        thr = 0.195 if str(c).startswith(('30', '68')) else 0.095
        is_limit = (pct >= thr) and abs(close - high) < 1e-6
        flags.append(bool(is_limit))
    return pd.Series(flags, index=pan.index, dtype=bool)


def _ctx(codes_series):
    dates = pd.to_datetime(['2026-11-20', '2026-11-21'])
    ctx = {}
    for c, rows in codes_series.items():
        ctx[c] = pd.DataFrame(rows, index=dates)
    return ctx


def test_untradable_flags_mainboard_limitup():
    # 主板: 一字涨停(open=high=close=prev*1.1) 应被标记为不可买入
    ctx = _ctx({
        '600001': {'open': [10.0, 11.0], 'high': [10.2, 11.0], 'close': [10.0, 11.0]},
        '600003': {'open': [10.0, 10.4], 'high': [10.2, 10.6], 'close': [10.0, 10.5]},
    })
    d1 = pd.to_datetime('2026-11-21')
    pan = pd.DataFrame({'code': ['600001', '600003'], 'trade_date': [d1, d1]})
    flags = _flag_untradable(ctx, pan)
    assert bool(flags.iloc[0]) is True    # 一字涨停不可买
    assert bool(flags.iloc[1]) is False   # 涨5%正常可买


def test_untradable_flags_growth_board_20pct():
    # 创业板 20% 涨停: 涨19.9%收最高应被标记(阈值 0.195 自适应)
    ctx = _ctx({
        '300001': {'open': [10.0, 11.99], 'high': [10.2, 11.99], 'close': [10.0, 11.99]},
        '300002': {'open': [10.0, 10.5], 'high': [10.2, 10.6], 'close': [10.0, 10.5]},
    })
    d1 = pd.to_datetime('2026-11-21')
    pan = pd.DataFrame({'code': ['300001', '300002'], 'trade_date': [d1, d1]})
    flags = _flag_untradable(ctx, pan)
    assert bool(flags.iloc[0]) is True    # 创业板19.9%涨停不可买
    assert bool(flags.iloc[1]) is False   # 涨5%可买


def test_untradable_missing_ctx_treated_tradable():
    # ctx 缺该股票时保守判定为可买(False), 不误杀
    ctx = {}
    d1 = pd.to_datetime('2026-11-21')
    pan = pd.DataFrame({'code': ['600999'], 'trade_date': [d1]})
    flags = _flag_untradable(ctx, pan)
    assert bool(flags.iloc[0]) is False
