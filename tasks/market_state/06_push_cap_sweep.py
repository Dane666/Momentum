# -*- coding: utf-8 -*-
"""
06_push_cap_sweep.py —— 每日推送只数对收益/夏普的敏感性扫描
================================================================
回答"把每日推荐买入只数减少, 会对收益造成什么影响"。

方法: 复用 05 的逐笔滚动仓位池模拟(simulate_trades), 仅改变"每日实际买入候选
只数上限(push_cap)"——即把 daily_inference.yml Bark 推送里 buy_n 的上限,
在回测中施加到候选清单上, 其余规则(次日开盘买 / 持有10日或-8%止损 / 状态仓位)
完全不变。

两种口径:
  1) 扁平扫描: 所有交易日统一 cap ∈ {2, 3, 5, 10}, 看单调趋势。
  2) 状态感知对比: 旧推送(强≤5/中≤3/弱≤2) vs 新推送(强≤3/中≤3/弱≤2),
     直接回答本次"强势日也压到 3"的影响。

输出: tasks/market_state/push_cap_sweep.json + 控制台表格。
"""
import sys
import gc
import json
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path('.').resolve()
spec = importlib.util.spec_from_file_location(
    'm05', ROOT / 'tasks' / 'market_state' / '05_trade_records_backtest.py')
m05 = importlib.util.module_from_spec(spec)
sys.modules['m05'] = m05
spec.loader.exec_module(m05)


def build_adapt_picks():
    """复刻 05.main 的数据管线, 返回未截断的 adapt_picks + 价格宽表 + state_of。"""
    cfg = m05.load_regime_config()
    at = m05.AdaptiveThreshold(cfg)

    H = m05.VPS._load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    names = m05.VPS._load_names()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    env_regime = m05.VS.build_regime(cal, nav)

    nav_df = pd.DataFrame({'trade_date': nav.index, 'close': nav.values})
    det = m05.RegimeDetector(cfg.get('detector', {}))
    full_state = det.detect_index(nav_df)
    full_state.index = pd.to_datetime(full_state.index)
    state_map = {d.strftime('%Y-%m-%d'): s for d, s in full_state.items()}

    close_df, open_df, dates, idx = m05.bt.build_price_frames(ctx, cal)
    high_df, low_df = m05._build_hl_frames(ctx, idx)
    dates = [d for d in dates if m05.TEST_START <= str(d.date()) <= m05.TEST_END]
    cands, _ = m05.m04.build_candidates(ctx, cal, names, hot_at, env_regime)

    del ctx, H, hot_at, nav
    gc.collect()

    date_ts = {str(d.date()): d for d in dates}
    state_of = {d: state_map.get(d, 'range') for d in date_ts}

    adapt_picks = {}
    for d, grp in cands.items():
        st = state_of[d]
        sel = at.select(grp, 'model_pred', state=st)
        if not sel.empty:
            scale = at.params(st).get('position_scale', 1.0)
            adapt_picks[d] = {'codes': list(sel['code']),
                              'scale': scale, 'names': names}
    return adapt_picks, close_df, open_df, high_df, low_df, date_ts, idx, at, state_of


def run_with_cap(adapt_picks, frames, cap):
    """扁平 cap: 每日候选只取前 cap 只。"""
    close_df, open_df, high_df, low_df, date_ts, idx, at, state_of = frames
    pk = {}
    for d, v in adapt_picks.items():
        codes = v['codes'][:cap]
        if codes:
            pk[d] = {'codes': codes, 'scale': v['scale'], 'names': v['names']}
    tr, eq, _, _ = m05.simulate_trades(
        pk, state_of, close_df, open_df, high_df, low_df, date_ts, idx, at,
        f'cap{cap}')
    return m05.perf_from_equity(eq), m05.trade_stats(tr)


def run_state_aware(adapt_picks, frames, bull_cap, range_cap, bear_cap):
    close_df, open_df, high_df, low_df, date_ts, idx, at, state_of = frames
    capmap = {'trend_up': bull_cap, 'range': range_cap,
              'trend_down': bear_cap, 'high_vol': bear_cap}
    pk = {}
    for d, v in adapt_picks.items():
        st = state_of[d]
        c = capmap.get(st, range_cap)
        codes = v['codes'][:c]
        if codes:
            pk[d] = {'codes': codes, 'scale': v['scale'], 'names': v['names']}
    tr, eq, _, _ = m05.simulate_trades(
        pk, state_of, close_df, open_df, high_df, low_df, date_ts, idx, at,
        'stateaware')
    return m05.perf_from_equity(eq), m05.trade_stats(tr)


def row(label, pe, ts):
    return {
        'label': label,
        'sharpe': round(pe.get('sharpe', 0), 3),
        'total_ret': round(pe.get('total_ret', 0) * 100, 2),
        'ann_ret': round(pe.get('ann_ret', 0) * 100, 1),
        'max_dd': round(pe.get('max_dd', 0) * 100, 2),
        'trades': ts.get('n', 0),
        'win_rate': round(ts.get('win_rate', 0) * 100, 1),
        'profit_factor': round(ts.get('profit_factor', 0), 2),
        'avg_hold': round(ts.get('avg_hold', 0), 1),
        'total_pnl': round(ts.get('total_pnl', 0)),
    }


def main():
    print('[1/2] 加载数据管线(复用 05)...', flush=True)
    adapt_picks, *rest = build_adapt_picks()
    frames = tuple(rest)
    print(f'      自适应候选日 {len(adapt_picks)} 天', flush=True)

    print('[2/2] 扫描推送只数...', flush=True)
    results = []
    # 1) 扁平扫描
    for cap in (10, 5, 3, 2):
        pe, ts = run_with_cap(adapt_picks, frames, cap)
        results.append(row(f'扁平cap={cap}', pe, ts))
        print(f'  cap={cap:>2}: 夏普={pe["sharpe"]:+.3f} 总收益={pe["total_ret"]*100:>+7.2f}% '
              f'笔数={ts["n"]:>3} 胜率={ts["win_rate"]*100:>5.1f}%', flush=True)
    # 2) 状态感知: 旧(强5/中3/弱2) vs 新(强3/中3/弱2)
    pe_o, ts_o = run_state_aware(adapt_picks, frames, 5, 3, 2)
    results.append(row('状态感知-旧(强5/中3/弱2)', pe_o, ts_o))
    pe_n, ts_n = run_state_aware(adapt_picks, frames, 3, 3, 2)
    results.append(row('状态感知-新(强3/中3/弱2)', pe_n, ts_n))
    print(f'  旧(强5): 夏普={pe_o["sharpe"]:+.3f} 总收益={pe_o["total_ret"]*100:>+7.2f}% '
          f'笔数={ts_o["n"]}', flush=True)
    print(f'  新(强3): 夏普={pe_n["sharpe"]:+.3f} 总收益={pe_n["total_ret"]*100:>+7.2f}% '
          f'笔数={ts_n["n"]}', flush=True)

    out = {'window': f'{m05.TEST_START}~{m05.TEST_END}',
           'note': '仅改变每日候选只数上限, 其余规则(开盘买/10日或-8%止损/状态仓位)不变',
           'rows': results}
    OUT = ROOT / 'tasks' / 'market_state' / 'push_cap_sweep.json'
    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False))

    # 控制台表格
    print('\n=== 推送只数敏感性(自适应策略, 2026-H1) ===')
    hdr = f'{"口径":<22}{"夏普":>8}{"总收益%":>9}{"笔数":>6}{"胜率%":>7}{"PF":>6}{"回撤%":>8}'
    print(hdr)
    for r in results:
        print(f'{r["label"]:<22}{r["sharpe"]:>8.3f}{r["total_ret"]:>9.2f}'
              f'{r["trades"]:>6}{r["win_rate"]:>7.1f}{r["profit_factor"]:>6.2f}'
              f'{r["max_dd"]:>8.2f}')
    print(f'\n[ok] -> {OUT}')


if __name__ == '__main__':
    main()
