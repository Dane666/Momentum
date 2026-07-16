# -*- coding: utf-8 -*-
"""
动量策略(多因子) × 大盘闸口周期对比

问题: 动量策略如果也改用 MA60 闸口(站上才做, 跌破空仓), 收益/胜率/夏普/回撤
      相对原 MA20 闸口(生产代码当前用)或无择时, 是否会更高?

方法: 不修改任何原文件. 复用 harness.py 的动量选股核心(基准原策略 + 质量叠加),
      仅把"择时闸口"参数化, 对比 4 种闸口:
        none       : 始终在场(无择时)
        ma20_close : 当日收盘站上 MA20  ( = 原生产 R 变体 / A方案原闸口)
        ma60_close : 当日收盘站上 MA60
        ma60_open  : 开盘前用 T-1 收盘站上 MA60[T-1]  (龙头策略证明最优的时点)

选股/持有/退出/滑点口径与 harness.py 完全一致, 保证干净对比.
"""

from __future__ import annotations
import os, sys, json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# 复用 harness.py (其顶层已设置 sys.path 与 import momentum)
import harness as H

THIS = Path(__file__).resolve()
PROJ = THIS.parent

HOLDS = [5, 3]
SHIFTS = [0, 20, 40, 60]


def make_gate(mkt_nav_s, mkt_ma20_s, mkt_ma60_s, calendar, mode):
    """返回 date(Timestamp) -> bool 的闸口. 仅在'两者有效且跌破'时 False, 否则 True.

    none      : 全 True
    ma20_close: nav[T] > ma20[T]
    ma60_close: nav[T] > ma60[T]
    ma60_open : nav[T-1] > ma60[T-1]
    """
    if mode == "none":
        return {t: True for t in calendar}
    gate = {}
    idx = {t: i for i, t in enumerate(calendar)}
    for t in calendar:
        if mode == "ma20_close":
            nv = mkt_nav_s.get(t); m = mkt_ma20_s.get(t)
        elif mode == "ma60_close":
            nv = mkt_nav_s.get(t); m = mkt_ma60_s.get(t)
        elif mode == "ma60_open":
            i = idx[t]; p = calendar[i - 1] if i - 1 >= 0 else None
            if p is None:
                gate[t] = True; continue
            nv = mkt_nav_s.get(p); m = mkt_ma60_s.get(p)
        else:
            gate[t] = True; continue
        # 与 harness 原 R 变体一致: 仅当两者有效且跌破才 False
        gate[t] = not (nv is not None and m is not None and nv <= m)
    return gate


def run_variant_gate(variant, hold_period, window_shift, calendar, day_cache_getter, gate):
    """复制 harness.run_variant, 但把择时换成 gate dict 查询."""
    n = len(calendar)
    need = H.cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
    if n < need:
        return None
    if window_shift > 0:
        end_off = hold_period + window_shift
        start_off = H.cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
        test_dates = calendar[-start_off:-end_off]
    else:
        test_dates = calendar[-(H.cfg.BACKTEST_DAYS_DEFAULT + hold_period):-hold_period]

    rebalance_dates = test_dates[::hold_period]
    equity = [1.0]; daily = []; trade_count = 0; win_count = 0
    dates_out = []; eligible_sum = 0; eligible_n = 0

    for t_date in rebalance_dates:
        tt = pd.Timestamp(t_date)
        allow = gate.get(tt, True)
        if not allow:
            equity.append(equity[-1]); daily.append(0.0); dates_out.append(str(t_date)[:10]); continue

        day_results = day_cache_getter(t_date, hold_period)
        if not day_results:
            equity.append(equity[-1]); daily.append(0.0); dates_out.append(str(t_date)[:10]); continue

        if variant["type"] == "baseline":
            df_scored = H.score_baseline(day_results)
        elif variant["type"] == "overlay":
            df_scored = H.score_overlay(day_results, variant.get("overlay", {}))
        else:
            df_scored = H.score_optimized(day_results, variant["weights"])

        picks, eligible = H.select_picks(df_scored, variant)
        eligible_sum += eligible; eligible_n += 1
        if picks:
            p = pd.DataFrame(picks)
            period_ret = p["fwd_ret"].mean()
            wins = int((p["fwd_ret"] > 0).sum())
            trade_count += len(p); win_count += wins
            equity.append(equity[-1] * (1 + period_ret)); daily.append(period_ret)
        else:
            equity.append(equity[-1]); daily.append(0.0)
        dates_out.append(str(t_date)[:10])

    m = H.compute_metrics(equity, daily, trade_count, win_count, hold_period, dates_out)
    if m is not None:
        m["avg_eligible"] = round(eligible_sum / eligible_n, 1) if eligible_n else 0.0
        m["empty_ratio"] = round(1 - trade_count / len(rebalance_dates) * 0, 4)  # placeholder
    return m


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/4] 构建大盘代理 (nav/MA20/MA60) ...", flush=True)
    mkt_nav_s, mkt_ma20_s = H.build_market_proxy(data_cache, calendar)
    mkt_ma60_s = mkt_nav_s.rolling(60).mean()

    exit_engine = H.ExitRuleEngine(adaptive=getattr(H.cfg, "USE_ADAPTIVE_EXIT", True))
    min_amount = H.cfg.MIN_AMOUNT

    day_cache = {}
    def get_daily_top(t_date, top_n):
        tt = pd.Timestamp(t_date).normalize()
        amts = []
        for code, g in data_cache.items():
            dd = g[g["trade_date"] == tt]
            if not dd.empty:
                a = dd["amount"].iloc[0]
                if pd.notna(a) and a > 0:
                    amts.append((code, a))
        amts.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in amts[:top_n]]

    def day_cache_getter(t_date, hold_period):
        key = (str(t_date)[:10], hold_period)
        if key in day_cache:
            return day_cache[key]
        top = get_daily_top(t_date, H.cfg.POOL_SIZE)
        results = []
        for code in top:
            g = data_cache.get(code)
            if g is None:
                continue
            rec = H.simulate_day(code, code, sector_map.get(code, "其它"), g,
                                 t_date, hold_period, exit_engine, min_amount)
            if rec:
                results.append(rec)
        day_cache[key] = results
        return results

    print("[3/4] 选股版本 x 4 闸口 x 多窗口回测 ...", flush=True)
    # 选股版本: 基准原策略 + 质量叠加(与 harness VARIANTS 一致)
    OVERLAY_W = {"trend_r2": 0.30, "sortino_mom": 0.20, "low_vol": 0.15, "near_high": 0.10}
    SELECTIONS = [
        {"name": "基准(原动量策略)", "type": "baseline"},
        {"name": "Q:质量叠加", "type": "overlay", "overlay": OVERLAY_W},
    ]
    GATE_MODES = ["none", "ma20_close", "ma60_close", "ma60_open"]
    GATE_LABEL = {
        "none": "无择时(始终在场)",
        "ma20_close": "MA20收盘(原生产闸口)",
        "ma60_close": "MA60收盘",
        "ma60_open": "MA60开盘前(T-1)",
    }

    all_results = defaultdict(lambda: defaultdict(dict))
    # all_results[sel_name][gate_mode][config_key] = metrics

    total = len(SELECTIONS) * len(GATE_MODES) * len(HOLDS) * len(SHIFTS)
    done = 0
    for sel in SELECTIONS:
        for mode in GATE_MODES:
            gate = make_gate(mkt_nav_s, mkt_ma20_s, mkt_ma60_s, calendar, mode)
            n_empty = sum(1 for t in calendar if not gate.get(t, True))
            for hp in HOLDS:
                for shift in SHIFTS:
                    m = run_variant_gate(sel, hp, shift, calendar, day_cache_getter, gate)
                    done += 1
                    ck = f"hold{hp}_shift{shift}"
                    if m:
                        all_results[sel["name"]][mode][ck] = {
                            k: m[k] for k in ("profit_pct", "annual_ret", "sharpe",
                                              "win_rate", "max_dd", "trade_count",
                                              "final_nav", "avg_eligible")}
                        line = (f"收益={m['profit_pct']:7.2f}% 胜率={m['win_rate']:5.1f}% "
                                f"夏普={m['sharpe']:.2f} 回撤={m['max_dd']:5.1f}%")
                    else:
                        line = "(数据不足)"
                    print(f"      [{done}/{total}] {sel['name'][:14]:14s} {mode:14s} {ck:14s} {line}",
                          flush=True)
            print(f"      >> {sel['name']} / {mode}({GATE_LABEL[mode]}) "
                  f"空仓信号日={n_empty} ({n_empty/len(calendar)*100:.1f}%)", flush=True)

    print("[4/4] 汇总 ...", flush=True)
    summary = {}
    for sel_name, gates in all_results.items():
        summary[sel_name] = {}
        for mode, cfgs in gates.items():
            rows = list(cfgs.values())
            if not rows:
                continue
            summary[sel_name][mode] = {
                "avg_profit": round(float(np.mean([r["profit_pct"] for r in rows])), 2),
                "avg_annual": round(float(np.mean([r["annual_ret"] for r in rows])), 2),
                "avg_sharpe": round(float(np.mean([r["sharpe"] for r in rows])), 3),
                "avg_win_rate": round(float(np.mean([r["win_rate"] for r in rows])), 2),
                "avg_max_dd": round(float(np.mean([r["max_dd"] for r in rows])), 2),
                "avg_trades": round(float(np.mean([r["trade_count"] for r in rows])), 1),
                "n_configs": len(rows),
                "main": cfgs.get("hold5_shift0", {}),
                "main_hold3": cfgs.get("hold3_shift0", {}),
                "label": GATE_LABEL[mode],
            }

    out = {
        "meta": {
            "backtest_days": H.cfg.BACKTEST_DAYS_DEFAULT,
            "pool_size": H.cfg.POOL_SIZE,
            "max_picks": H.cfg.MAX_TOTAL_PICKS,
            "slippage": H.cfg.SLIPPAGE,
            "holds": HOLDS, "shifts": SHIFTS,
            "universe": len(data_cache),
            "calendar_start": str(calendar[0])[:10],
            "calendar_end": str(calendar[-1])[:10],
        },
        "summary": summary,
        "detail": {s: {m: c for m, c in g.items()} for s, g in all_results.items()},
    }
    out_path = PROJ / "c_momentum_ma60_results.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成 -> {out_path}", flush=True)

    # 控制台摘要
    for sel_name, gates in summary.items():
        print("\n" + "=" * 80)
        print(f"选股: {sel_name}")
        print(f"{'闸口':<22}{'均收益%':>9}{'均胜率%':>9}{'均夏普':>8}{'均回撤%':>9}{'均交易':>7}")
        print("-" * 80)
        for mode, s in gates.items():
            print(f"{s['label']:<22}{s['avg_profit']:>9.2f}{s['avg_win_rate']:>9.2f}"
                  f"{s['avg_sharpe']:>8.2f}{s['avg_max_dd']:>9.2f}{s['avg_trades']:>7.1f}")
        print("=" * 80)


if __name__ == "__main__":
    main()
