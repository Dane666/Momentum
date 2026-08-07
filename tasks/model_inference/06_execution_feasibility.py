# -*- coding: utf-8 -*-
"""
06_execution_feasibility.py —— 回测成交价的实盘可执行性检验
==========================================================
回答用户的两个问题:
  1. 模型每天盘后跑, 买点卖点到底是什么(时间轴上何时下单、下什么单)?
  2. 回测里的买入价格, 实盘一定能买到吗?

时间轴前提(决定了买点的物理上限):
  T日 15:00 收盘 -> T日盘后跑模型(数据齐全) -> 最早 T+1 才能下单。
  => 回测中的 close_same(信号日收盘价买入) 在实盘 **不可得**, 只能作为理想上界参考。
  => 现实可执行的只有 open_next(T+1 开盘) 与 dip_next(T+1 挂限价单回踩).

本脚本量化五件事(全部基于 test 段真实 K 线, 2026-01-01~2026-06-30):
  A. 买入端漏斗 : 原始信号 -> 信号日涨停 -> 次日一字板 -> 次日停牌/无数据 -> 实际可成交
  B. 次日开盘缺口: gap = open(T+1)/close(T) - 1 的分布
                   -> "次日开盘买" 相对 "信号日收盘" 要多付多少
  C. 回踩限价单  : 各 dip_buf 的成交率, 以及未成交样本的机会成本
                   (未成交的是不是恰好是最强的票 -> 限价单是否在系统性错过上涨)
  D. 止损跳空    : 理论止损价 vs 实际可成交价的滑点(跳空低开直接击穿止损)
  E. 卖出端      : 到期卖出日跌停(卖不出)的比例

输出: tasks/model_inference/output/execution_feasibility.json
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

OUT = ROOT / 'tasks' / 'model_inference' / 'output'

# 复用 04 的信号生成与工具(文件名以数字开头, 需 importlib 载入)
_spec = importlib.util.spec_from_file_location(
    'bt04', Path(__file__).resolve().parent / '04_backtest_entry_exit.py')
BT = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(BT)

COST = BT.COST
DIP_BUFS = [0.01, 0.02, 0.03, 0.05]
SL = 0.08
HOLD_N = 10


def limit_ratio(code):
    return 0.20 if str(code).startswith(('30', '68')) else 0.10


def pct_chg(g, i):
    if i <= 0:
        return np.nan
    prev = g['close'].iat[i - 1]
    return (g['close'].iat[i] / prev - 1.0) if prev > 0 else np.nan


def is_limit_up(g, i, code):
    p = pct_chg(g, i)
    return bool(np.isfinite(p) and p >= limit_ratio(code) * 0.95)


def is_limit_down(g, i, code):
    p = pct_chg(g, i)
    return bool(np.isfinite(p) and p <= -limit_ratio(code) * 0.95)


def is_one_word(g, i, code):
    return bool(g['high'].iat[i] == g['low'].iat[i] and is_limit_up(g, i, code))


def q(a, name):
    """分位数摘要。"""
    a = np.asarray([x for x in a if np.isfinite(x)], dtype=float)
    if len(a) == 0:
        return {'name': name, 'n': 0}
    return {
        'name': name, 'n': int(len(a)),
        'mean': float(a.mean()), 'median': float(np.median(a)),
        'p10': float(np.percentile(a, 10)), 'p25': float(np.percentile(a, 25)),
        'p75': float(np.percentile(a, 75)), 'p90': float(np.percentile(a, 90)),
        'min': float(a.min()), 'max': float(a.max()),
    }


def main():
    print('[1/3] 载入 ctx ...', flush=True)
    ctx, cal, names, hot_at, regime = BT.load_ctx()
    print(f'      ctx={len(ctx)} 只', flush=True)

    print('[2/3] 生成模型 Top10 信号 ...', flush=True)
    msigs, _pan = BT.model_signals(ctx, cal, names, hot_at, regime, topk=10)
    print(f'      信号 {len(msigs)} 笔', flush=True)

    print('[3/3] 执行可行性统计 ...', flush=True)

    # ---------------- A. 买入端漏斗
    funnel = dict(total=0, no_data=0, sig_limit_up=0, next_one_word=0,
                  next_no_data=0, tradable=0)
    gaps = []                       # B. 次日开盘缺口
    dip_fill = {b: dict(filled=0, unfilled=0,
                        filled_ret=[], unfilled_ret_if_open=[])
                for b in DIP_BUFS}
    sl_slip = []                    # D. 止损滑点
    sl_hit = 0
    sl_gap_break = 0                # 跳空直接击穿止损的笔数
    sell_limit_down = 0             # E. 到期日跌停卖不出
    sell_total = 0

    for code, td in msigs:
        funnel['total'] += 1
        g = ctx.get(code)
        if g is None or td not in g.index:
            funnel['no_data'] += 1
            continue
        i = g.index.get_loc(td)
        if isinstance(i, slice) or i <= 0 or i + 1 >= len(g):
            funnel['no_data'] += 1
            continue

        # 信号日涨停 -> 次日大概率高开/一字, 视为不可买入
        if is_limit_up(g, i, code):
            funnel['sig_limit_up'] += 1
            continue
        j = i + 1
        if not np.isfinite(g['open'].iat[j]) or g['open'].iat[j] <= 0:
            funnel['next_no_data'] += 1
            continue
        if is_one_word(g, j, code):
            funnel['next_one_word'] += 1
            continue

        funnel['tradable'] += 1
        sig_close = float(g['close'].iat[i])
        open_next = float(g['open'].iat[j])
        gaps.append(open_next / sig_close - 1.0)

        # ---------------- C. 回踩限价单成交率 + 机会成本
        end = min(len(g) - 1, j + HOLD_N)
        exit_close = float(g['close'].iat[end])
        for b in DIP_BUFS:
            target = sig_close * (1 - b)
            if float(g['low'].iat[j]) <= target:
                buy = min(target, open_next)      # 低开则按开盘价成交(更优)
                dip_fill[b]['filled'] += 1
                dip_fill[b]['filled_ret'].append(exit_close / buy - 1.0 - COST)
            else:
                dip_fill[b]['unfilled'] += 1
                # 未成交样本: 若改用次日开盘买, 本可拿到的收益 = 机会成本
                dip_fill[b]['unfilled_ret_if_open'].append(
                    exit_close / open_next - 1.0 - COST)

        # ---------------- D. 止损跳空滑点(以 open_next 买入为基准)
        buy = open_next
        stop_px = buy * (1 - SL)
        for k in range(j + 1, end + 1):
            if float(g['low'].iat[k]) <= stop_px:
                sl_hit += 1
                op = float(g['open'].iat[k])
                # 若当日开盘已在止损价之下 -> 只能按开盘价成交(跳空击穿)
                real = min(op, stop_px) if op < stop_px else stop_px
                if op < stop_px:
                    sl_gap_break += 1
                sl_slip.append(real / stop_px - 1.0)
                break

        # ---------------- E. 到期卖出日跌停 -> 卖不出
        sell_total += 1
        if is_limit_down(g, end, code):
            sell_limit_down += 1

    # ---------------- 汇总
    tot = max(1, funnel['total'])
    res = {
        'window': f'{BT.TEST_START}~{BT.TEST_END}',
        'cost_roundtrip': COST,
        'hold_n': HOLD_N,
        'A_buy_funnel': {
            **funnel,
            'pct_sig_limit_up': funnel['sig_limit_up'] / tot,
            'pct_next_one_word': funnel['next_one_word'] / tot,
            'pct_tradable': funnel['tradable'] / tot,
        },
        'B_open_gap': q(gaps, 'open_next/close_sig-1'),
        'B_open_gap_tail': {
            'pct_gap_gt_3': float(np.mean([x > 0.03 for x in gaps])) if gaps else 0.0,
            'pct_gap_gt_5': float(np.mean([x > 0.05 for x in gaps])) if gaps else 0.0,
            'pct_gap_lt_0': float(np.mean([x < 0 for x in gaps])) if gaps else 0.0,
        },
        'C_dip_limit_order': {},
        'D_stoploss_slippage': {
            'sl': SL,
            'hit_n': sl_hit,
            'gap_break_n': sl_gap_break,
            'gap_break_rate': (sl_gap_break / sl_hit) if sl_hit else 0.0,
            'slippage': q(sl_slip, 'real/stop-1'),
        },
        'E_sell_limit_down': {
            'total': sell_total,
            'blocked': sell_limit_down,
            'rate': (sell_limit_down / sell_total) if sell_total else 0.0,
        },
    }
    for b in DIP_BUFS:
        d = dip_fill[b]
        n = d['filled'] + d['unfilled']
        fr = np.array(d['filled_ret'], dtype=float)
        ur = np.array(d['unfilled_ret_if_open'], dtype=float)
        res['C_dip_limit_order'][f'buf_{b}'] = {
            'fill_rate': (d['filled'] / n) if n else 0.0,
            'filled_n': d['filled'], 'unfilled_n': d['unfilled'],
            'filled_avg_ret': float(fr.mean()) if len(fr) else 0.0,
            'filled_win_rate': float((fr > 0).mean()) if len(fr) else 0.0,
            # 机会成本: 未成交那批若用开盘价买本可拿到的平均收益
            'missed_avg_ret_if_open': float(ur.mean()) if len(ur) else 0.0,
            'missed_win_rate_if_open': float((ur > 0).mean()) if len(ur) else 0.0,
        }

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'execution_feasibility.json').write_text(
        json.dumps(res, ensure_ascii=False, indent=2), encoding='utf-8')

    # ---------------- 打印
    A = res['A_buy_funnel']
    print('\n===== A. 买入端漏斗 =====')
    print(f"  原始信号        {A['total']}")
    print(f"  信号日涨停剔除  {A['sig_limit_up']}  ({A['pct_sig_limit_up']:.1%})")
    print(f"  次日一字板剔除  {A['next_one_word']}  ({A['pct_next_one_word']:.1%})")
    print(f"  数据缺失剔除    {A['no_data'] + A['next_no_data']}")
    print(f"  => 实际可成交   {A['tradable']}  ({A['pct_tradable']:.1%})")

    B = res['B_open_gap']
    print('\n===== B. 次日开盘缺口(相对信号日收盘) =====')
    if B.get('n'):
        print(f"  n={B['n']}  均值 {B['mean']:+.2%}  中位 {B['median']:+.2%}")
        print(f"  P10 {B['p10']:+.2%} | P25 {B['p25']:+.2%} | "
              f"P75 {B['p75']:+.2%} | P90 {B['p90']:+.2%}")
        t = res['B_open_gap_tail']
        print(f"  高开>3% 占比 {t['pct_gap_gt_3']:.1%} | "
              f"高开>5% {t['pct_gap_gt_5']:.1%} | 低开 {t['pct_gap_lt_0']:.1%}")

    print('\n===== C. 回踩限价单(挂 收盘*(1-buf)) =====')
    print(f"  {'buf':>6} {'成交率':>8} {'成交均收益':>11} {'成交胜率':>9} "
          f"{'未成交若开盘买':>14} {'未成交胜率':>11}")
    for b in DIP_BUFS:
        c = res['C_dip_limit_order'][f'buf_{b}']
        print(f"  {b:>6.2f} {c['fill_rate']:>8.1%} {c['filled_avg_ret']:>11.2%} "
              f"{c['filled_win_rate']:>9.1%} {c['missed_avg_ret_if_open']:>14.2%} "
              f"{c['missed_win_rate_if_open']:>11.1%}")

    D = res['D_stoploss_slippage']
    print(f"\n===== D. 止损跳空滑点(sl={SL:.0%}) =====")
    print(f"  触发止损 {D['hit_n']} 笔, 其中跳空击穿 {D['gap_break_n']} 笔 "
          f"({D['gap_break_rate']:.1%})")
    if D['slippage'].get('n'):
        s = D['slippage']
        print(f"  实际成交价 vs 理论止损价: 均值 {s['mean']:+.2%}  "
              f"中位 {s['median']:+.2%}  最差 {s['min']:+.2%}")

    E = res['E_sell_limit_down']
    print(f"\n===== E. 到期卖出跌停(卖不出) =====")
    print(f"  {E['blocked']}/{E['total']}  ({E['rate']:.2%})")

    print(f"\n[OK] -> {OUT / 'execution_feasibility.json'}")


if __name__ == '__main__':
    main()
