# -*- coding: utf-8 -*-
"""
C 方案(热门行业龙头, 无择时) —— 真实账户级回测 + 止损优化
============================================================
修正原 naive 模型的两个问题:
  1) 显式建模"仓位槽约束": N 个独立槽, 满仓才能买, 卖出(到期/止损)才释放槽位
     —— 即"上一只没卖出就没钱买下一只"。
  2) 加入固定止损, 扫描最优止损点。

模型(事件驱动, 日级):
  - 账户分 N 个等权子账户(槽), 每个子账户独立运作、互不影响。
  - 子账户 k 在每个"信号日"(每 hold 天一次)买入当日热门行业龙头中第 k 名
    (top-N 内按当日涨幅排名), 满仓投入该子账户当前现金。
  - 持仓期间每日检查退出:
      * 盘中 low <= 入场价×(1-止损%) → 以止损价(再减卖滑点) 止损离场 (reason=stop)
      * 持有满 hold 天           → 以当日收盘(减卖滑点) 离场 (reason=time)
  - 离场后该槽变空, 下一信号日再买入; 槽被占用期间绝不买入。

入场价 = 信号日收盘×(1+买滑点)  (14:45 近似)
止损价 = 入场价×(1-止损%) , 保守以该价成交(再减卖滑点)

严格复用原框架口径: load_universe / build_sector_heat(资金净流入) / compute_metrics 口径。

输出: compare3_stop_results.json
"""
from __future__ import annotations
import sys, json
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TESTS = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(TESTS))

import harness as H
from momentum import config as cfg
from harness_sector import build_sector_heat, fwd_ret_at, slice_test_dates
from harness_compare3 import build_day_returns, topn_leaders

SLIP = cfg.SLIPPAGE
WINDOW_SHIFTS = [0, 20, 40, 60]
TOP_K = 8
HOLDS = [3, 5]
NS = [1, 2, 3]
STOP_SWEEP = [0.0, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10]


def build_price_lookup(data_cache):
    """返回 (price_lookup, date_idx, date_list)
       price_lookup[code] = {date: (open,high,low,close)}
       date_idx[code]      = {date: 行索引}
       date_list[code]     = [有序交易日]
       用于按个股自身交易日数持有(与原 C 的 fwd_ret_at 同口径)。
    """
    price_lookup, date_idx, date_list = {}, {}, {}
    for code, g in data_cache.items():
        g = g.sort_values("trade_date")
        d = {}
        dates = g["trade_date"].tolist()
        for idx, r in enumerate(g.itertuples(index=False)):
            d[r.trade_date] = (r.open, r.high, r.low, r.close)
        price_lookup[code] = d
        date_idx[code] = {dt: idx for idx, dt in enumerate(dates)}
        date_list[code] = dates
    return price_lookup, date_idx, date_list


def simulate_account_c(N, hold, stop_pct, calendar, price_lookup, date_idx,
                        date_list, hot_by_date, day_ret_map, sector_map, reb_dates):
    """事件驱动模拟。返回 (equity_curve, trades, lo_idx, hi_idx)
    reb_dates: 入场日列表(由 slice_test_dates + [::hold] 得到, 与原 C 对齐)
    退出: 持有满 hold 个"个股交易日"(与原 C fwd_ret_at 同口径) 或盘中触及止损。
    """
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [1.0], [], 0, 0
    lo = min(signal_set)
    hi = min(max(signal_set) + hold, n - 1)

    subs = [{"cash": 1.0 / N, "pos": None} for _ in range(N)]
    equity_curve = []
    trades = []  # (ret, reason)

    for i in range(n):
        t = calendar[i]
        # 1) 退出检查
        for k in range(N):
            s = subs[k]
            pos = s["pos"]
            if not pos:
                continue
            pl = price_lookup.get(pos["code"])
            if pl is None or t not in pl:
                # 缺失日: 用上一日收盘价盯市, 不触发退出/止损
                continue
            o, h, low, close = pl[t]
            pos["last_close"] = close
            exit_fill = None
            reason = None
            if stop_pct > 0:
                stop_price = pos["entry_fill"] * (1.0 - stop_pct)
                if low <= stop_price:
                    exit_fill = stop_price * (1.0 - SLIP)   # 保守: 止损价再减卖滑点
                    reason = "stop"
            if reason is None and t >= pos["exit_date"]:
                exit_fill = close * (1.0 - SLIP)
                reason = "time"
            if reason is not None:
                proceeds = pos["shares"] * exit_fill
                s["cash"] += proceeds
                ret = exit_fill / pos["entry_fill"] - 1.0
                trades.append((ret, reason))
                s["pos"] = None
        # 2) 盯市(缺失日沿用 last_close, 避免净值虚假归零)
        eq = 0.0
        for k in range(N):
            s = subs[k]
            if s["pos"] is not None:
                last = s["pos"].get("last_close")
                if last is None:
                    pl = price_lookup.get(s["pos"]["code"])
                    last = pl[t][3] if (pl is not None and t in pl) else 0.0
                eq += s["cash"] + s["pos"]["shares"] * last
            else:
                eq += s["cash"]
        # 3) 记录净值(测试窗口起才计入, 避免前期空仓摊薄年化)
        if i >= lo:
            equity_curve.append(eq)
        # 4) 信号日买入
        if i in signal_set:
            leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            for k in range(N):
                if subs[k]["pos"] is None and k < len(leaders):
                    code = leaders[k]
                    pl = price_lookup.get(code)
                    if pl is None or t not in pl:
                        continue
                    # 计算个股"往后 hold 个交易日"的退出日
                    di = date_idx.get(code, {})
                    dl = date_list.get(code, [])
                    ii = di.get(t)
                    exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                    if exit_date is None:
                        continue
                    o, h, low, close = pl[t]
                    entry_fill = close * (1.0 + SLIP)
                    cost = subs[k]["cash"]
                    if cost <= 0:
                        continue
                    shares = cost / entry_fill
                    subs[k]["cash"] -= cost
                    subs[k]["pos"] = {"code": code, "entry_i": i,
                                      "entry_fill": entry_fill, "shares": shares,
                                      "last_close": close, "exit_date": exit_date}
    return equity_curve, trades, lo, hi


def metrics_from_equity(equity_curve, trades, hold):
    eq = np.array(equity_curve, dtype=float)
    daily = np.diff(eq) / eq[:-1]
    daily = daily[~np.isnan(daily)]
    n = len(eq)
    final = eq[-1]
    profit = (final - 1.0) * 100.0
    # 年化
    years = n / 252.0
    annual = (final ** (1.0 / years) - 1.0) * 100.0 if final > 0 and years > 0 else -100.0
    # 夏普
    if len(daily) > 1 and daily.std() > 0:
        sharpe = daily.mean() / daily.std() * np.sqrt(252.0)
    else:
        sharpe = 0.0
    # 最大回撤
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    max_dd = abs(dd.min()) * 100.0 if len(dd) else 0.0
    # 交易统计
    if trades:
        rets = [x[0] for x in trades]
        wins = sum(1 for r in rets if r > 0)
        win_rate = wins / len(rets) * 100.0
        stop_cnt = sum(1 for _, rs in trades if rs == "stop")
    else:
        win_rate = 0.0
        stop_cnt = 0
    return {
        "profit_pct": round(float(profit), 2),
        "annual_ret": round(float(annual), 2),
        "sharpe": round(float(sharpe), 3),
        "win_rate": round(float(win_rate), 2),
        "max_dd": round(float(max_dd), 2),
        "trade_count": len(trades),
        "stop_count": stop_cnt,
        "final_nav": round(float(final), 4),
    }


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/4] 行业热度(资金净流入) + 龙头池 + 价格查找 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    print("      完成", flush=True)

    print("[3/4] 扫描 止损点 × N × hold × 窗口 ...", flush=True)
    results = []   # 每个 (N, hold, stop) 的 8 窗口均值
    detail = defaultdict(dict)  # (N,hold,stop) -> {cfg: metrics}
    for N in NS:
        for hold in HOLDS:
            for stop in STOP_SWEEP:
                cfgs = {}
                for shift in WINDOW_SHIFTS:
                    test_dates = slice_test_dates(calendar, hold, shift)
                    if not test_dates:
                        continue
                    reb_dates = test_dates[::hold]   # 与原 C 对齐: 每 hold 天 rebalance
                    eq, trades, lo, hi = simulate_account_c(
                        N, hold, stop, calendar, price_lookup, date_idx, date_list,
                        hot_by_date, day_ret_map, sector_map, reb_dates)
                    if len(eq) < 30:
                        continue
                    m = metrics_from_equity(eq, trades, hold)
                    ck = f"hold{hold}_shift{shift}"
                    cfgs[ck] = m
                if not cfgs:
                    continue
                rows = list(cfgs.values())
                avg = {
                    "N": N, "hold": hold, "stop": stop,
                    "avg_profit": round(float(np.mean([r["profit_pct"] for r in rows])), 2),
                    "avg_annual": round(float(np.mean([r["annual_ret"] for r in rows])), 2),
                    "avg_sharpe": round(float(np.mean([r["sharpe"] for r in rows])), 3),
                    "avg_win_rate": round(float(np.mean([r["win_rate"] for r in rows])), 2),
                    "avg_max_dd": round(float(np.mean([r["max_dd"] for r in rows])), 2),
                    "avg_trades": round(float(np.mean([r["trade_count"] for r in rows])), 1),
                    "avg_stop_rate": round(float(np.mean([r["stop_count"] / r["trade_count"] for r in rows if r["trade_count"] > 0])) * 100, 1) if any(r["trade_count"] > 0 for r in rows) else 0.0,
                    "main": cfgs.get("hold5_shift0", cfgs.get("hold3_shift0", {})),
                }
                results.append(avg)
                detail[(N, hold, stop)] = cfgs
                tag = "无止损" if stop == 0 else f"止损{int(stop*100)}%"
                print(f"      N={N} hold={hold} {tag:<8} 收益={avg['avg_profit']:>8.2f}% "
                      f"胜率={avg['avg_win_rate']:>5.2f}% 夏普={avg['avg_sharpe']:.2f} "
                      f"回撤={avg['avg_max_dd']:>5.2f}% 交易={avg['avg_trades']:.1f} "
                      f"止损占比={avg['avg_stop_rate']:.1f}%", flush=True)

    # 优选: 每个 (N,hold) 取夏普最高者(夏普综合收益/风险; 同夏普取收益高)
    best = {}
    for N in NS:
        for hold in HOLDS:
            cands = [r for r in results if r["N"] == N and r["hold"] == hold]
            if not cands:
                continue
            cands.sort(key=lambda r: (r["avg_sharpe"], r["avg_profit"]), reverse=True)
            best[(N, hold)] = cands[0]

    out = {
        "meta": {
            "universe": len(data_cache),
            "calendar_start": str(calendar[0])[:10],
            "calendar_end": str(calendar[-1])[:10],
            "slippage": SLIP, "top_k": TOP_K, "window_shifts": WINDOW_SHIFTS,
            "stop_sweep": STOP_SWEEP,
            "note": "C方案真实账户级回测: N个独立仓位槽(满仓才买, 卖出才释放), "
                    "信号日(每hold天)买热门行业龙头第k名, 持有hold天或止损离场。",
        },
        "results": results,
        "best": {f"N{N}_hold{h}": best[(N, h)] for (N, h) in best},
    }
    (HERE / "compare3_stop_results.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> compare3_stop_results.json", flush=True)

    # 打印优选摘要
    print("\n=== 各 (N,hold) 最优止损点 ===", flush=True)
    for (N, h), b in best.items():
        tag = "无止损" if b["stop"] == 0 else f"止损{int(b['stop']*100)}%"
        print(f"  N={N} hold={h}: 最优={tag}  收益={b['avg_profit']:.2f}% "
              f"胜率={b['avg_win_rate']:.2f}% 夏普={b['avg_sharpe']:.2f} "
              f"回撤={b['avg_max_dd']:.2f}% 交易={b['avg_trades']:.1f}")


if __name__ == "__main__":
    main()
