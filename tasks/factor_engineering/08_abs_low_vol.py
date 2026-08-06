# -*- coding: utf-8 -*-
"""
补充: 全市场横截面 绝对低波动 增量回测 (a-修正)
================================================
修正 07 的方法学局限(信号稀疏导致同日横截面排序无效):
改测"绝对低波动"——信号股须同时是全市场当日 vol60 低分位(前 33%),
验证低波动因子在信号股上的真实有效性。

复用与 07 相同的 harness 数据链。输出 output/abs_low_vol_report.md + csv。
"""
import sys, importlib.util
from pathlib import Path

ROOT = Path('.').resolve()
sys.path.insert(0, str(ROOT / 'tools'))
sys.path.insert(0, str(ROOT / 'opt_study'))
sys.path.insert(0, str(ROOT.parent))
try:
    import momentum  # noqa
except ImportError:
    s = importlib.util.spec_from_file_location(
        'momentum', ROOT / '__init__.py', submodule_search_locations=[str(ROOT)])
    m = importlib.util.module_from_spec(s)
    sys.modules['momentum'] = m
    s.loader.exec_module(m)

import volume_price_scan as VPS
import volume_price_strategy as VS
import pandas as pd
import numpy as np

OUT = ROOT / 'tasks' / 'factor_engineering' / 'output'
OUT.mkdir(parents=True, exist_ok=True)
HORIZONS = (1, 3, 5, 10, 20)
ABS_THRESH = 0.33   # 绝对低波动: vol60 全市场分位 <= 0.33


def hist_vol_series(g, win=60):
    pct = g['close'].pct_change()
    s = pct.rolling(win).std()
    s.index = [str(t)[:10] for t in s.index]
    return s


def fwd_ret(g, date, N):
    idx = g.index
    try:
        i = idx.get_loc(pd.Timestamp(date))
    except KeyError:
        return np.nan
    if i + N >= len(g):
        return np.nan
    c0 = g['close'].iloc[i]
    c1 = g['close'].iloc[i + N]
    if c0 <= 0 or pd.isna(c0) or pd.isna(c1):
        return np.nan
    return c1 / c0 - 1.0


def main():
    print('[1/4] 加载数据链 ...')
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    ctx = {c: g for c, g in ctx.items() if len(g) >= 120}
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)

    print('[2/4] vol60 全市场面板 + 信号事件 ...')
    vol_map = {c: hist_vol_series(g, 60) for c, g in ctx.items()}
    # 全市场 vol60 面板: date_str -> {code: vol}
    panel = {}
    for code, v in vol_map.items():
        for d, val in v.items():
            panel.setdefault(d, {})[code] = val

    inv = VS.build_inv(ctx, cal, names, hot_at, regime,
                       min_history=120, use_theme_resonance=True, bull_only=True)
    events = []
    for kind, d in (('breakout', inv['breakout']), ('pullback', inv['pullback'])):
        for date, codes in d.items():
            for code in codes:
                events.append((date, code, kind))

    print(f'     信号事件={len(events)}; 计算 vol 分位 + 前向收益 ...')
    rows = []
    for date, code, kind in events:
        g = ctx.get(code)
        if g is None:
            continue
        gv = panel.get(date)
        vol = vol_map.get(code)
        raw_vol = vol.get(date, np.nan) if (vol is not None and date in vol.index) else np.nan
        # 全市场分位 (低=好)
        vp = np.nan
        if gv is not None and not pd.isna(raw_vol):
            vals = [x for x in gv.values() if not pd.isna(x)]
            if vals:
                vp = sum(1 for x in vals if x <= raw_vol) / len(vals)
        rec = {'date': date, 'code': code, 'kind': kind,
               'vol60': raw_vol, 'vol_pct': vp}
        for N in HORIZONS:
            rec[f'r{N}'] = fwd_ret(g, date, N)
        rows.append(rec)
    sig = pd.DataFrame(rows)
    sig.to_csv(OUT / 'abs_low_vol_backtest.csv', index=False)

    base = sig
    abs_lv = sig[sig['vol_pct'] <= ABS_THRESH]

    def stb(df):
        out = {}
        for N in HORIZONS:
            s = df[f'r{N}'].dropna()
            out[N] = (float(s.mean()), float((s > 0).mean()), int(len(s))) if len(s) else (np.nan, np.nan, 0)
        return out

    def tbl(df, label):
        rows = []
        for N in HORIZONS:
            m, w, n = stb(df)[N]
            rows.append({'group': label, 'horizon': N,
                         'mean_ret%': round(m * 100, 3) if n else None,
                         'win_rate%': round(w * 100, 2) if n else None, 'n': n})
        return rows

    cmp = pd.DataFrame(tbl(base, '基线-全部信号')
                       + tbl(abs_lv, f'绝对低波动(vol分位<= {ABS_THRESH:.0%})'))

    # 按 vol_pct 十分位看梯度(全市场分位越低越好?)
    sig2 = sig.dropna(subset=['vol_pct']).copy()
    sig2['dec'] = pd.cut(sig2['vol_pct'], bins=[-0.01, 0.1, 0.2, 0.33, 0.5, 0.7, 1.01],
                         labels=['<=0.1', '0.1-0.2', '0.2-0.33', '0.33-0.5', '0.5-0.7', '>0.7'])
    dec_rows = []
    for dec in ['<=0.1', '0.1-0.2', '0.2-0.33', '0.33-0.5', '0.5-0.7', '>0.7']:
        dd = sig2[sig2.dec == dec]
        for N in HORIZONS:
            s = dd[f'r{N}'].dropna()
            dec_rows.append({'vol_pct_band': dec, 'horizon': N,
                             'mean_ret%': round(s.mean() * 100, 3) if len(s) else None,
                             'win_rate%': round((s > 0).mean() * 100, 2) if len(s) else None,
                             'n': int(len(s))})
    dec_df = pd.DataFrame(dec_rows)

    # 报告
    bm, bw, bn = stb(base)[20]
    am, aw, an = stb(abs_lv)[20]
    lines = ['# 全市场横截面 绝对低波动 增量回测 (修正版)\n']
    lines.append(f'- 数据: 2024-01-02~2026-08-04 | 股票 {len(ctx)} | 信号 {len(sig)}')
    lines.append(f'- 绝对低波动: 信号股 vol60 全市场分位 <= {ABS_THRESH:.0%} (越低越偏好)\n')
    lines.append('## 一、基线 vs 绝对低波动\n')
    lines.append(cmp.to_markdown(index=False))
    lines.append('\n## 二、vol 全市场分位梯度 (越低波动越好?)\n')
    lines.append(dec_df.to_markdown(index=False))
    lines.append('\n**解读**: 若 vol 分位越低(<=0.1)收益/胜率越高 => 低波动因子在信号股上绝对有效。\n')
    lines.append('## 三、结论(20日)\n')
    lines.append(f'- 基线: {bm*100:.2f}% / 胜率 {bw*100:.1f}% / n={bn}')
    lines.append(f'- 绝对低波动: {am*100:.2f}% / 胜率 {aw*100:.1f}% / n={an}')
    dret = (am - bm) * 100; dwin = (aw - bw) * 100
    lines.append(f'- **差异: 收益 {dret:+.2f}pp / 胜率 {dwin:+.1f}pp (样本 {bn}->{an})**')
    if an >= 30:
        if dwin > 0 and dret > 0:
            v = '✅ 绝对低波动有效'
        elif dwin > 0:
            v = '⚠️ 胜率提升但收益未提升'
        else:
            v = '❌ 绝对低波动未带来提升'
    else:
        v = '⚠️ 绝对低波动样本过少, 结论不稳'
    lines.append(f'\n**判定: {v}**')
    (OUT / 'abs_low_vol_report.md').write_text('\n'.join(lines), encoding='utf-8')
    print(f'-> {OUT/"abs_low_vol_report.md"}')
    print(f'基线: {bm*100:.2f}%/{bw*100:.1f}% n={bn}')
    print(f'绝对低波动: {am*100:.2f}%/{aw*100:.1f}% n={an}')
    print(f'差异: 收益 {dret:+.2f}pp 胜率 {dwin:+.1f}pp | {v}')


if __name__ == '__main__':
    main()
