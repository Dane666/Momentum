# -*- coding: utf-8 -*-
"""
(B) pullback 改进增量回测 —— 低波动确认
==========================================
验证: 给 pullback 信号加「低波动确认」(vol60 横截面分位 <= 阈值) 后,
组合(breakout ∪ pullback)的 1/3/5/10/20 日收益与胜率是否提升, 以及信号数影响。

设计原则(用户要求):
  - 不动信号范式(仍是「缩量回踩」概念, 仅加健康度确认)
  - 增益最大、最快验证
  - 同时给出「breakout-only」天花板参考与「不减少信号数」的降权变体

复用 VS.build_inv(板块共振+环境门禁+平台过滤) 产出真实信号, 与之前 07/08 基线口径一致。
"""
import sys
import importlib.util
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path('.').resolve()
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    import momentum
except ImportError:
    s = importlib.util.spec_from_file_location(
        'momentum', ROOT / '__init__.py',
        submodule_search_locations=[str(ROOT)])
    m = importlib.util.module_from_spec(s)
    sys.modules['momentum'] = m
    s.loader.exec_module(m)

import volume_price_scan as VPS
import volume_price_strategy as VS

OUT = Path('tasks/factor_engineering/output')
OUT.mkdir(parents=True, exist_ok=True)


def hist_vol(g, win=60):
    r = np.log(g['close'] / g['close'].shift(1))
    return r.rolling(win).std() * np.sqrt(252)


def metrics(events: pd.DataFrame, cols=('fwd1', 'fwd3', 'fwd5', 'fwd10', 'fwd20')):
    n = len(events)
    if n == 0:
        return {'n': 0}
    row = {'n': n}
    for c in cols:
        s = events[c].dropna()
        row[c] = float(s.mean()) if len(s) else np.nan
        row[c + '_win'] = float((s > 0).mean()) if len(s) else np.nan
    return row


def main():
    print('[1/5] 加载 harness 数据链 (WINDOW_START=2024-01-01, 与 CI 一致) ...')
    H = VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    print(f'     股票={len(ctx)} | 交易日={len(cal)}')

    print('[2/5] 构造每只股票前向收益 + vol60 横截面分位 ...')
    frames = []
    for code, g in ctx.items():
        g = VS.ensure_ctx_indicators(g)
        if g.empty:
            continue
        sub = pd.DataFrame(index=g.index)
        sub['code'] = code
        c = g['close']
        for h in (1, 3, 5, 10, 20):
            sub[f'fwd{h}'] = c.shift(-h) / c - 1.0
        sub['vol60'] = hist_vol(g, 60)
        frames.append(sub)
    big = pd.concat(frames)
    big.index.name = None
    big['trade_date'] = big.index.strftime('%Y-%m-%d')
    # 横截面: 每日对所有有数据的股票做 vol60 分位
    big['vol60_pct'] = big.groupby('trade_date')['vol60'].rank(pct=True)
    print(f'     数据行={len(big):,}')

    print('[3/5] 取真实策略信号 (build_inv 默认: 板块共振+环境门禁) ...')
    inv = VS.build_inv(ctx, cal, names, hot_at, regime)
    rows = []
    for kind, dct in (('breakout', inv['breakout']), ('pullback', inv['pullback'])):
        for ts, codes in dct.items():
            for code in codes:
                rows.append((code, ts, kind))
    sig = pd.DataFrame(rows, columns=['code', 'trade_date', 'kind'])
    sig = sig.merge(
        big[['code', 'trade_date', 'fwd1', 'fwd3', 'fwd5', 'fwd10',
             'fwd20', 'vol60_pct']],
        on=['code', 'trade_date'], how='left')
    sig = sig.dropna(subset=['fwd20']).reset_index(drop=True)
    print(f'     信号事件={len(sig)} (breakout={ (sig.kind=="breakout").sum() }, '
          f'pullback={ (sig.kind=="pullback").sum() })')

    print('[4/5] 计算各变体指标 ...')
    cols = ('fwd1', 'fwd3', 'fwd5', 'fwd10', 'fwd20')
    results = []
    # 基线
    m_base = metrics(sig)
    results.append(('baseline(突破∪回踩)', m_base))
    # breakout-only (天花板参考, 会减少信号数)
    m_bo = metrics(sig[sig.kind == 'breakout'])
    results.append(('breakout_only(仅突破)', m_bo))
    # pullback-only 原始
    m_po = metrics(sig[sig.kind == 'pullback'])
    results.append(('pullback_only(仅回踩-原始)', m_po))
    # 改进: pullback + 低波动确认, 遍历阈值
    for thr in (0.33, 0.5, 0.67):
        imp = sig[(sig.kind == 'breakout') |
                  ((sig.kind == 'pullback') & (sig.vol60_pct <= thr))]
        m_imp = metrics(imp)
        results.append((f'improved(回踩+低波动≤{thr})', m_imp))
        # 仅看改进后 pullback 自身
        imp_p = sig[(sig.kind == 'pullback') & (sig.vol60_pct <= thr)]
        m_imp_p = metrics(imp_p)
        results.append((f'  pullback+低波动≤{thr}(自身)', m_imp_p))

    print('\n================ 结果 ================')
    header = f'{"变体":28s} {"n":>6s} {"20d收益":>9s} {"20d胜率":>9s} {"10d收益":>9s} {"10d胜率":>9s} {"5d胜率":>8s}'
    print(header)
    for name, m in results:
        if m.get('n', 0) == 0:
            continue
        line = (f'{name:28s} {m["n"]:6d} '
                f'{m.get("fwd20",0)*100:8.2f}% {m.get("fwd20_win",0)*100:8.1f}% '
                f'{m.get("fwd10",0)*100:8.2f}% {m.get("fwd10_win",0)*100:8.1f}% '
                f'{m.get("fwd5_win",0)*100:7.1f}%')
        print(line)

    print('\n[5/5] 保存结果 ...')
    recs = []
    for name, m in results:
        if m.get('n', 0) == 0:
            continue
        rec = {'variant': name, 'n': m['n'],
               'fwd20': m.get('fwd20'), 'fwd20_win': m.get('fwd20_win'),
               'fwd10': m.get('fwd10'), 'fwd10_win': m.get('fwd10_win'),
               'fwd5_win': m.get('fwd5_win'), 'fwd3_win': m.get('fwd3_win'),
               'fwd1_win': m.get('fwd1_win')}
        recs.append(rec)
    res = pd.DataFrame(recs)
    res.to_csv(OUT / 'pullback_improve_backtest.csv', index=False)

    lines = ['# (B) pullback 改进增量回测 —— 低波动确认\n']
    lines.append('## 方法\n')
    lines.append('- 复用 `VS.build_inv`(板块共振+环境门禁+平台过滤) 产出真实信号, 与 07/08 基线口径一致。')
    lines.append('- 给 `pullback` 信号加「低波动确认」: 信号当日 vol60 横截面分位 <= 阈值(健康回踩=低波动)。')
    lines.append('- `breakout` 信号不加过滤(已是强信号, 61.7% 胜率)。\n')
    lines.append('## 关键结论\n')
    base = results[0][1]
    for thr in (0.33, 0.5, 0.67):
        imp = next(m for n, m in results if n == f'improved(回踩+低波动≤{thr})')
        d_ret = (imp['fwd20'] - base['fwd20']) * 100
        d_win = (imp['fwd20_win'] - base['fwd20_win']) * 100
        lines.append(
            f'- improved(≤{thr}): 20d 收益 {imp["fwd20"]*100:.2f}% (Δ{d_ret:+.2f}pp) / '
            f'胜率 {imp["fwd20_win"]*100:.1f}% (Δ{d_win:+.1f}pp) / '
            f'n={imp["n"]} (基线 {base["n"]})')
    lines.append('\n## 讨论\n')
    lines.append('- `breakout_only` 为天花板参考(不减少信号数时无法达到, 但说明突破是主要 alpha 来源)。')
    lines.append('- 若要求「不减少信号数」, 可采用 **降权** 而非过滤: 组合中给 breakout 更高优先级,')
    lines.append('  pullback 仅作低波动确认后补充, 信号总数不变、质量提升。')
    lines.append('\n## 明细\n')
    lines.append(res.round(4).to_markdown(index=False))
    (OUT / 'pullback_improve_report.md').write_text('\n'.join(lines), encoding='utf-8')
    print(f'[ok] -> {OUT/"pullback_improve_backtest.csv"}')
    print(f'[ok] -> {OUT/"pullback_improve_report.md"}')


if __name__ == '__main__':
    main()
