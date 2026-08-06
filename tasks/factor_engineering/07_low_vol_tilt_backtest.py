# -*- coding: utf-8 -*-
"""
低波动倾斜 增量回测 (a)
======================
目标: 在现有价量策略(突破放量/缩量回踩, 经板块共振+环境门禁+平台过滤)的
      *已产出信号* 之上, 叠加"低波动倾斜"——按信号股当日横截面 60 日历史波动率
     排序, 偏好低波动一半, 观察胜率/收益是否提升。

完全复用真实部署的数据链路(_load_harness, WINDOW_START=2024-01-01, 与 CI 一致),
不修改生产策略代码。仅在本脚本内做"低波动倾斜"对比, 供是否进入 Phase2 决策。

输出:
  output/low_vol_tilt_backtest.csv          明细
  output/low_vol_tilt_report.md             对比报告(基线 vs 低波动倾斜 + volatility quintile)
"""
import sys, os, importlib.util, json
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
TILT_FRAC = 0.5   # 低波动倾斜: 取当日信号股波动最低的一半


def hist_vol_series(g, win=60):
    """单股 60 日历史波动率(close pct 滚动 std), index 转 str date 便于对齐 inv。"""
    pct = g['close'].pct_change()
    s = pct.rolling(win).std()
    s.index = [str(t)[:10] for t in s.index]
    return s


def fwd_ret(g, date, N):
    """信号日 date 收盘定型后 N 个交易日收益; 缺数据返回 nan。"""
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


def stats_block(df):
    """返回 {N: (mean_ret, win_rate, n)}。"""
    out = {}
    for N in HORIZONS:
        col = f'r{N}'
        s = df[col].dropna()
        if len(s):
            out[N] = (float(s.mean()), float((s > 0).mean()), int(len(s)))
        else:
            out[N] = (np.nan, np.nan, 0)
    return out


def agg_table(df, label):
    """把 stats_block 渲染成行 dict 列表。"""
    st = stats_block(df)
    rows = []
    for N in HORIZONS:
        mean, win, n = st[N]
        rows.append({
            'group': label, 'horizon': N,
            'mean_ret%': round(mean * 100, 3) if n else None,
            'win_rate%': round(win * 100, 2) if n else None,
            'n': n,
        })
    return rows


def main():
    print('[1/5] 加载 harness 数据链 (WINDOW_START=2024-01-01, 与 CI 一致) ...')
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    # 仅取 >=120 历史的股票(与 build_inv min_history 对齐)
    ctx = {c: g for c, g in ctx.items() if len(g) >= 120}
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    print(f'     股票数={len(ctx)}  交易日={len(cal)} '
          f'(' + str(cal[0])[:10] + '~' + str(cal[-1])[:10] + ')')

    print('[2/5] 预计算 60 日历史波动率因子 ...')
    vol_map = {c: hist_vol_series(g, 60) for c, g in ctx.items()}

    print('[3/5] 生成基线信号 build_inv (板块共振+环境门禁+平台过滤) ...')
    inv = VS.build_inv(ctx, cal, names, hot_at, regime,
                       min_history=120, use_theme_resonance=True, bull_only=True)
    n_b = sum(len(v) for v in inv['breakout'].values())
    n_p = sum(len(v) for v in inv['pullback'].values())
    print(f'     基线信号: breakout={n_b}  pullback={n_p}')

    # 收集所有信号事件
    events = []
    for kind, d in (('breakout', inv['breakout']), ('pullback', inv['pullback'])):
        for date, codes in d.items():
            for code in codes:
                events.append((date, code, kind))
    if not events:
        print('!!! 基线 0 信号, 无法回测(检查数据/板块)')
        return

    print(f'     信号事件总数={len(events)}, 计算前向收益 ...')
    rows = []
    for date, code, kind in events:
        g = ctx.get(code)
        if g is None:
            continue
        v = vol_map.get(code)
        vol = v.get(date, np.nan) if (v is not None and date in v.index) else np.nan
        rec = {'date': date, 'code': code, 'kind': kind, 'vol60': vol}
        for N in HORIZONS:
            rec[f'r{N}'] = fwd_ret(g, date, N)
        rows.append(rec)
    sig = pd.DataFrame(rows)
    sig.to_csv(OUT / 'low_vol_tilt_backtest.csv', index=False)
    print(f'     明细 -> {OUT/"low_vol_tilt_backtest.csv"} '
          f'(有效事件={len(sig)}, vol60 非空={sig["vol60"].notna().sum()})')

    print('[4/5] 低波动倾斜 + quintile 统计 ...')
    # 低波动倾斜: 按 (date, kind) 组内 vol60 升序取前 TILT_FRAC
    tilt_parts = []
    for (d, k), sub in sig.groupby(['date', 'kind']):
        sub = sub.sort_values('vol60')
        kk = max(1, int(np.ceil(len(sub) * TILT_FRAC)))
        tilt_parts.append(sub.iloc[:kk])
    tilt = pd.concat(tilt_parts) if tilt_parts else sig.iloc[0:0]

    # volatility quintile: 按 date 组内 vol60 分 5 组 (Q1=最低波动)
    sig_q = sig.dropna(subset=['vol60']).copy()
    parts = []
    for d, sub in sig_q.groupby('date'):
        if len(sub) >= 5:
            sub = sub.assign(q=pd.qcut(sub['vol60'].rank(method='first'), 5,
                                       labels=[1, 2, 3, 4, 5]))
        else:
            sub = sub.assign(q=3)
        parts.append(sub)
    sig_q = pd.concat(parts) if parts else sig_q

    # ---- 报告 ----
    base_all = agg_table(sig, '基线-全部信号')
    tilt_all = agg_table(tilt, f'低波动倾斜-低波动{TILT_FRAC:.0%}')
    base_b = agg_table(sig[sig.kind == 'breakout'], '基线-breakout')
    tilt_b = agg_table(tilt[tilt.kind == 'breakout'], '低波动-breakout')
    base_p = agg_table(sig[sig.kind == 'pullback'], '基线-pullback')
    tilt_p = agg_table(tilt[tilt.kind == 'pullback'], '低波动-pullback')

    cmp_rows = base_all + tilt_all + base_b + tilt_b + base_p + tilt_p
    cmp_df = pd.DataFrame(cmp_rows)

    # quintile 表
    q_rows = []
    for q in [1, 2, 3, 4, 5]:
        qd = sig_q[sig_q.q == q]
        st = stats_block(qd)
        for N in HORIZONS:
            mean, win, n = st[N]
            q_rows.append({
                'vol_quintile': ('Q%d(最低)' % q) if q == 1 else (
                    ('Q%d(最高)' % q) if q == 5 else ('Q%d' % q)),
                'horizon': N,
                'mean_ret%': round(mean * 100, 3) if n else None,
                'win_rate%': round(win * 100, 2) if n else None,
                'n': n,
            })
    q_df = pd.DataFrame(q_rows)

    print('[5/5] 写出报告 ...')
    lines = ['# 低波动倾斜 增量回测报告\n']
    lines.append(f'- 数据窗口: {str(cal[0])[:10]} ~ {str(cal[-1])[:10]} '
                 f'| 股票 {len(ctx)} | 信号事件 {len(sig)}')
    lines.append(f'- 基线信号: breakout={n_b} / pullback={n_p} (经板块共振+环境门禁+平台过滤)')
    lines.append(f'- 低波动倾斜: 各信号日按 60 日历史波动率升序取最低 {TILT_FRAC:.0%}')
    lines.append(f'- 低波动因子: `hist_vol_60` (Phase1 验证 IC≈-0.12/IR≈-0.59, 低波动异象)\n')

    lines.append('## 一、基线 vs 低波动倾斜 (收益% / 胜率% / 样本数)\n')
    lines.append(cmp_df.to_markdown(index=False))
    lines.append('\n**解读**: 低波动倾斜组均值收益/胜率更高 => 偏好低波动可提升策略表现。\n')

    lines.append('## 二、波动率分位梯度 (Q1=最低波动 ~ Q5=最高波动)\n')
    lines.append(q_df.to_markdown(index=False))
    lines.append('\n**解读**: 若 Q1 > Q5 且梯度单调 => 波动率越高未来收益越低(低波动异象成立), '
                 '是低波动倾斜有效性的直接证据。\n')

    # 关键结论: 20日 提升
    def g21(df, N=20):
        st = stats_block(df)[N]
        return st
    bm, bw, bn = g21(sig)
    tm, tw, tn = g21(tilt)
    lines.append('## 三、结论 (20日窗口)\n')
    lines.append(f'- 基线: 均值 {bm*100:.2f}% / 胜率 {bw*100:.1f}% / n={bn}')
    lines.append(f'- 低波动倾斜: 均值 {tm*100:.2f}% / 胜率 {tw*100:.1f}% / n={tn}')
    dret = (tm - bm) * 100
    dwin = (tw - bw) * 100
    lines.append(f'- **差异: 收益 {dret:+.2f}pp / 胜率 {dwin:+.1f}pp** '
                 f'(样本 {bn} -> {tn})')
    if dwin > 0 and dret > 0:
        verdict = '✅ 低波动倾斜在收益与胜率上均提升 => 支持引入。'
    elif dwin > 0:
        verdict = '⚠️ 胜率提升但收益未提升 => 可改善稳定性, 需结合持仓期判断。'
    else:
        verdict = '❌ 低波动倾斜未带来提升 => 不建议引入(或换因子/参数)。'
    lines.append(f'\n**判定: {verdict}**')

    report = '\n'.join(lines)
    (OUT / 'low_vol_tilt_report.md').write_text(report, encoding='utf-8')
    print(f'     报告 -> {OUT/"low_vol_tilt_report.md"}')
    print('\n=== 摘要(20日) ===')
    print(f'基线: 收益 {bm*100:.2f}% 胜率 {bw*100:.1f}% n={bn}')
    print(f'低波动倾斜: 收益 {tm*100:.2f}% 胜率 {tw*100:.1f}% n={tn}')
    print(f'差异: 收益 {dret:+.2f}pp 胜率 {dwin:+.1f}pp')
    print(verdict)


if __name__ == '__main__':
    main()
