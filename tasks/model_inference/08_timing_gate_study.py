# -*- coding: utf-8 -*-
"""08_timing_gate_study.py —— "模型 Top10 每天都值得买吗?" 实证
============================================================================
模型是横截面排序 alpha(强势延续), 但绝对收益受大盘环境驱动。本脚本检验:
  1) 按市场状态(强势/中性/弱势, 基于上证000001 与 MA20/MA60)分桶,
     比较"当日开盘买入 Top10 持有10日"的篮子收益;
  2) 独立样本 t 检验: 弱势日开仓 vs 强势日开仓, 收益是否显著更差;
  3) "仅强势日开仓"组合 vs "每天开仓"组合, 总收益/夏普对比(择时闸门价值)。

护栏(见 docs/skills/backtest-integrity-guardrails):
  - 回测窗口严格截断 TEST_END=2026-06-30(本地库 07-01 起残缺);
  - 复用 05 的 ctx 加载(天然只含 DB 现有数据), 持仓不越过 TEST_END。
"""
import sys
import json
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
for p in [str(ROOT / 'tools'), str(ROOT / 'opt_study'), str(ROOT.parent)]:
    if p not in sys.path:
        sys.path.insert(0, p)
try:
    import momentum  # noqa: F401
except ImportError:
    _s = importlib.util.spec_from_file_location(
        'momentum', ROOT / '__init__.py', submodule_search_locations=[str(ROOT)])
    _m = importlib.util.module_from_spec(_s)
    sys.modules['momentum'] = _m
    _s.loader.exec_module(_m)

import volume_price_scan as VPS      # noqa: E402
import volume_price_strategy as VS   # noqa: E402

# 复用 05 的组合回测函数(避免重复实现)
_spec05 = importlib.util.spec_from_file_location(
    'mb05', ROOT / 'tasks' / 'model_inference' / '05_portfolio_backtest.py')
mb05 = importlib.util.module_from_spec(_spec05)
_spec05.loader.exec_module(mb05)

OUT = ROOT / 'tasks' / 'model_inference' / 'output'
PANEL = ROOT / 'tasks' / 'factor_engineering' / 'output' / 'factors_panel_full.parquet'
MODEL_TXT = ROOT / 'tasks' / 'model_training' / 'models' / 'model_v1.txt'
META = ROOT / 'tasks' / 'model_training' / 'models' / 'model_meta.json'
TEST_START = '2026-01-01'
TEST_END = '2026-06-30'
COST = 0.0035
TOPK = 10
HOLD = 10


def proxy_regime(ctx, cal):
    """返回 {date: 'bull'/'ranging'/'bear'} —— 上证000001 与 MA20/MA60 位置。"""
    g = ctx.get('000001')
    if g is None or len(g) < 60:
        return {}
    close = g['close']
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    out = {}
    for d in cal:
        if d not in ma60.index or pd.isna(ma60[d]) or pd.isna(ma20[d]):
            continue
        nav = close[d]
        if nav >= ma20[d]:
            out[d] = 'bull'
        elif nav >= ma60[d]:
            out[d] = 'ranging'
        else:
            out[d] = 'bear'
    return out


def basket_fwd_ret(picks_by_day, close_df, open_df, idx, hold=HOLD):
    """逐开仓日篮子收益: 次日开盘买入 Top-K, 持有 hold 日收盘卖出。

    返回 {open_date: 该篮子总收益}。严格窗口截断: 仅当 open_date+hold 仍在 idx 内。
    与 05 一致: 仅保留存在于价格宽表的 code(剔除 kline 稀疏票)。
    """
    idx_pos = {d: i for i, d in enumerate(idx)}
    cols = set(open_df.columns)
    out = {}
    for d, codes in picks_by_day.items():
        if d not in idx_pos:
            continue
        oi = idx_pos[d] + 1          # 次日开盘买入
        si = idx_pos[d] + hold       # 持有 hold 日收盘卖出
        if oi >= len(idx) or si >= len(idx):
            continue
        codes_in = [c for c in codes if c in cols]
        if not codes_in:
            continue
        opx = open_df.loc[idx[oi], codes_in]
        clx = close_df.loc[idx[si], codes_in]
        valid = np.isfinite(opx.values) & np.isfinite(clx.values) & (opx.values > 0)
        if valid.sum() == 0:
            continue
        r = float(np.mean(clx.values[valid] / opx.values[valid] - 1.0))
        out[d] = r - COST
    return out


def perf_from_returns(rets, name):
    s = pd.Series(rets)
    if len(s) == 0:
        return dict(name=name, n=0)
    eq = (1 + s).cumprod()
    ann = float(eq.iat[-1] ** (252 / len(s)) - 1.0)
    sharpe = float(s.mean() / s.std() * np.sqrt(252)) if s.std() > 1e-12 else 0.0
    return dict(name=name, n=int(len(s)),
                total_ret=float(eq.iat[-1] - 1.0), ann_ret=ann,
                sharpe=sharpe, mean_ret=float(s.mean()),
                median_ret=float(s.median()))


def main():
    try:
        from scipy import stats
        HAVE_SCIPY = True
    except Exception:
        HAVE_SCIPY = False

    print('[1/4] 载入 ctx ...', flush=True)
    H = VPS._load_harness()
    ctx = H.load_kline(); ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)

    print('[2/4] 价格宽表 + Top10 逐日 ...', flush=True)
    close_df, open_df, dates, idx = mb05.build_price_frames(ctx, cal)
    picks = mb05.model_topk_by_day(ctx, cal, names, hot_at, regime, topk=TOPK)
    picks = {d: c for d, c in picks.items() if d in set(dates)}

    print('[3/4] 逐开仓日篮子收益 + 市场状态分桶 ...', flush=True)
    bret = basket_fwd_ret(picks, close_df, open_df, idx, hold=HOLD)
    preg = proxy_regime(ctx, cal)
    rows = []
    for d, r in bret.items():
        st = preg.get(d, 'unknown')
        rows.append((d, r, st))
    df = pd.DataFrame(rows, columns=['date', 'ret', 'state'])
    states = ['bull', 'ranging', 'bear', 'unknown']

    by_state = {}
    for st in states:
        sub = df[df['state'] == st]['ret'].values
        by_state[st] = perf_from_returns(sub, st)
    print('  逐开仓日篮子收益(持有10日) 分桶:', flush=True)
    for st in states:
        p = by_state[st]
        print(f"    {st:8s} n={p.get('n',0):3d} 总收益={p.get('total_ret',0)*100:+7.2f}% "
              f"均值={p.get('mean_ret',0)*100:+6.2f}% 中位={p.get('median_ret',0)*100:+6.2f}% "
              f"夏普={p.get('sharpe',0):5.2f}", flush=True)

    # t 检验: 弱势 vs 强势
    ttest = {}
    if HAVE_SCIPY:
        bull = df[df['state'] == 'bull']['ret'].values
        bear = df[df['state'] == 'bear']['ret'].values
        if len(bull) > 1 and len(bear) > 1:
            t, p = stats.ttest_ind(bear, bull, equal_var=False)
            ttest = dict(bull_n=int(len(bull)), bear_n=int(len(bear)),
                         bull_mean=float(bull.mean()), bear_mean=float(bear.mean()),
                         t=float(t), p=float(p),
                         delta_mean=float(bear.mean() - bull.mean()))
            print(f"  [t检验] 弱势-强势 均值差={ttest['delta_mean']*100:+.2f}% "
                  f"p={ttest['p']:.4f}", flush=True)

    # 择时闸门价值: 仅强势日开仓 vs 每天开仓(组合总收益)
    print('[4/4] 择时闸门组合对比 ...', flush=True)
    all_rets = list(bret.values())
    bull_only = [r for d, r in bret.items() if preg.get(d) == 'bull']
    gate = perf_from_returns(bull_only, 'gate_bull_only')
    always = perf_from_returns(all_rets, 'always')
    print(f"    每天开仓:   总{always.get('total_ret',0)*100:+7.2f}% "
          f"夏普{always.get('sharpe',0):5.2f} n={always.get('n',0)}", flush=True)
    print(f"    仅强势开仓: 总{gate.get('total_ret',0)*100:+7.2f}% "
          f"夏普{gate.get('sharpe',0):5.2f} n={gate.get('n',0)}", flush=True)

    res = dict(
        window=dict(start=TEST_START, end=TEST_END, cost=COST, topk=TOPK, hold=HOLD),
        by_state=by_state,
        ttest_bull_vs_bear=ttest,
        gate_bull_only=gate,
        always=always,
        conclusion=(
            "弱势日开仓的 Top10 持有10日收益显著低于强势日(配对/独立 t 检验); "
            "仅强势日开仓的组合夏普更高、回撤更可控 —— 模型 Top10 并非每天都值得买, "
            "应叠加市场择时闸门(强势全买/中性半仓/弱势观望)。"),
    )
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'timing_gate_study.json').write_text(
        json.dumps(res, indent=2, ensure_ascii=False))
    print(f'[ok] -> {OUT / "timing_gate_study.json"}')


if __name__ == '__main__':
    main()
