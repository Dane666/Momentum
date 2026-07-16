# -*- coding: utf-8 -*-
"""
离线回测: 单股 + 行业择时 + 热门行业龙头
========================================
只读复用原策略(不修改任何原文件), 在 opt_study/harness.py 基础上扩展:
  Part 1: 单股(MAX_PICKS=1) + 仅选热门行业, 对比
          "首日成为热门行业" vs "持续 N 天热门行业" 的胜率/收益, 扫描最优参数
  Part 2: 14:45 直接买入热门行业龙头(当日涨幅最强), 测 +1/+2/+3 天收益, 与原动量策略(top1)对比

行业热度口径(修订 v2, 2026-07-15):
  按"当日资金流入"排名 —— 对板块内每只股票用主力资金净流入近似
      net_inflow = amount × (2×close − high − low) / (high − low)
  汇总得到板块资金净流入, top-K 板块 = 热门行业; 连续热门天数 = consec。
  '其它'板块不参与排名。龙头定义(涨幅最强)不变。

用法:
    python harness_sector.py            # 运行 Part1 + Part2, 输出 sector_results.json + 报告
"""
from __future__ import annotations
import sys, json, sqlite3, warnings
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

warnings.simplefilter("ignore")

HERE = Path(__file__).resolve().parent          # opt_study
TESTS = HERE.parent                              # tests
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(TESTS))

import harness as H                              # 复用 load_universe / simulate_day / score_baseline / select_picks / compute_metrics / build_market_proxy
from momentum import config as cfg
from momentum.risk import ExitRuleEngine

DB_PATH = H.DB_PATH
KLINE_START = H.KLINE_START
MIN_BARS = H.MIN_BARS

HOLD_PERIODS = [5, 3]
WINDOW_SHIFTS = [0, 20, 40, 60]
SLIP = cfg.SLIPPAGE


# =====================================================================
# 行业热度
# =====================================================================
def stock_net_inflow(g):
    """逐股主力资金净流入近似(同花顺/东方财富通用口径):
        net = amount × (2×close − high − low) / (high − low)
        high==low(一字板/无波动)时记为 0。返回与 g 对齐的 Series。"""
    high = g["high"].astype(float).values
    low = g["low"].astype(float).values
    close = g["close"].astype(float).values
    amount = g["amount"].astype(float).values
    denom = high - low
    ratio = np.where(denom > 0, (2.0 * close - high - low) / denom, 0.0)
    return pd.Series(amount * ratio, index=g.index)


def build_sector_heat(data_cache, sector_map, calendar, top_k, min_members=4):
    """返回 (hot_by_date, consec_by_date, leader_by_date)
       hot_by_date[d]   = set(热门行业, 按当日板块资金净流入 top-K)
       consec_by_date[d]= dict(行业->连续热门天数)
       leader_by_date[d]= 当日热门行业内涨幅最强的股票代码(或 None)

       修订 v2: 排名依据改为 板块主力资金净流入(逐股汇总)。
    """
    # 1) 逐日收集 (行业, 当日资金净流入, 当日收益率, 代码)
    date_members = defaultdict(list)
    for code, g in data_cache.items():
        g = g.sort_values("trade_date").reset_index(drop=True)
        sec = sector_map.get(code, "其它")
        if sec == "其它":
            continue
        ni = stock_net_inflow(g)
        rets = g["close"].pct_change()
        dates = g["trade_date"].tolist()
        for i in range(1, len(g)):
            r = rets.iloc[i]
            if pd.notna(r):
                date_members[dates[i]].append((sec, float(ni.iloc[i]), float(r), code))

    hot_by_date, consec_by_date, leader_by_date = {}, {}, {}
    consec = {}
    for d in calendar:
        members = date_members.get(d, [])
        if not members:
            hot_by_date[d] = set()
            consec_by_date[d] = dict(consec)
            leader_by_date[d] = None
            continue
        by_sec = defaultdict(float)
        for sec, inflow, r, code in members:
            by_sec[sec] += inflow          # 板块资金净流入 = 成员汇总
        # 过滤成员数过少板块
        sec_count = defaultdict(int)
        for sec, *_ in members:
            sec_count[sec] += 1
        sec_inflow = {s: v for s, v in by_sec.items() if sec_count[s] >= min_members}
        if not sec_inflow:
            hot_by_date[d] = set()
            consec_by_date[d] = dict(consec)
            leader_by_date[d] = None
            continue
        ranked = sorted(sec_inflow.items(), key=lambda x: -x[1])
        hot = set(s for s, _ in ranked[:top_k])
        for s in hot:
            consec[s] = consec.get(s, 0) + 1
        for s in list(consec.keys()):
            if s not in hot:
                consec[s] = 0
        hot_by_date[d] = hot
        consec_by_date[d] = dict(consec)
        # 龙头: 热门行业内当日涨幅最强
        best, bestret = None, -1e9
        for sec, inflow, r, code in members:
            if sec in hot and r > bestret:
                bestret, best = r, code
        leader_by_date[d] = best
    return hot_by_date, consec_by_date, leader_by_date


# =====================================================================
# 选股: 单股 + 行业规则
# =====================================================================
def picks_for(day_results, hot_set, consec_map, min_consec):
    """min_consec=None -> 不限行业(对照); 否则要求 sector in hot_set 且 consec>=min_consec
       返回 picks(list), 已用原策略严苛过滤器 + alpha 排序取 top1"""
    if min_consec is None:
        cands = day_results
    else:
        cands = [r for r in day_results
                 if (r["sector"] in hot_set) and (consec_map.get(r["sector"], 0) >= min_consec)]
    if not cands:
        return []
    df = H.score_baseline(cands)
    picks, _ = H.select_picks(df, {"max_picks": 1, "max_sector": 1})
    return picks


def slice_test_dates(calendar, hold_period, window_shift):
    n = len(calendar)
    need = cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
    if n < need:
        return []
    if window_shift > 0:
        end_off = hold_period + window_shift
        start_off = cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
        return calendar[-start_off:-end_off]
    return calendar[-(cfg.BACKTEST_DAYS_DEFAULT + hold_period):-hold_period]


# =====================================================================
# Part 1: 单股 + 行业择时 扫描
# =====================================================================
def run_part1(data_cache, day_cache_getter, calendar, heat, top_ks, holds, consec_list):
    """heat: dict top_k -> (hot_by_date, consec_by_date, leader_by_date)"""
    results = []  # 每行一个 (top_k, hold, min_consec_label) 的窗口均值
    detail = {}
    for top_k in top_ks:
        hot_by_date, consec_by_date, _ = heat[top_k]
        for hold in holds:
            for mc in consec_list:
                label = "不限(对照)" if mc is None else f"持续≥{mc}天"
                cfgs = {}
                for shift in WINDOW_SHIFTS:
                    test_dates = slice_test_dates(calendar, hold, shift)
                    if not test_dates:
                        continue
                    reb = test_dates[::hold]
                    equity = [1.0]; daily = []; trade_count = 0; win_count = 0
                    dates_out = []
                    for t in reb:
                        recs = day_cache_getter(t, hold)
                        if not recs:
                            equity.append(equity[-1]); daily.append(0.0); dates_out.append(str(t)[:10]); continue
                        picks = picks_for(recs, hot_by_date[t], consec_by_date[t], mc)
                        if picks:
                            pr = float(np.mean([p["fwd_ret"] for p in picks]))
                            trade_count += len(picks)
                            win_count += int((np.array([p["fwd_ret"] for p in picks]) > 0).sum())
                            equity.append(equity[-1] * (1 + pr))
                            daily.append(pr)
                        else:
                            equity.append(equity[-1]); daily.append(0.0)
                        dates_out.append(str(t)[:10])
                    m = H.compute_metrics(equity, daily, trade_count, win_count, hold, dates_out)
                    if m:
                        ck = f"hold{hold}_shift{shift}"
                        cfgs[ck] = {k: m[k] for k in
                                    ("profit_pct", "annual_ret", "sharpe", "win_rate", "max_dd", "trade_count", "final_nav")}
                if cfgs:
                    rows = list(cfgs.values())
                    avg = {
                        "top_k": top_k, "hold": hold, "min_consec": mc, "label": label,
                        "avg_profit": round(float(np.mean([r["profit_pct"] for r in rows])), 2),
                        "avg_annual": round(float(np.mean([r["annual_ret"] for r in rows])), 2),
                        "avg_sharpe": round(float(np.mean([r["sharpe"] for r in rows])), 3),
                        "avg_win_rate": round(float(np.mean([r["win_rate"] for r in rows])), 2),
                        "avg_max_dd": round(float(np.mean([r["max_dd"] for r in rows])), 2),
                        "avg_trades": round(float(np.mean([r["trade_count"] for r in rows])), 1),
                        "n_configs": len(rows),
                        "main": cfgs.get("hold5_shift0", {}),
                    }
                    results.append(avg)
                    key = f"T{top_k}_H{hold}_{'ctrl' if mc is None else 'c'+str(mc)}"
                    detail[key] = {"top_k": top_k, "hold": hold, "label": label,
                                   "equity": cfgs.get("hold5_shift0", {}).get("equity", []),
                                   "dates": cfgs.get("hold5_shift0", {}).get("dates", [])}
    return results, detail


# =====================================================================
# Part 2: 热门行业龙头 vs 动量策略(top1) +1/+2/+3 天
# =====================================================================
def fwd_ret_at(data_cache, code, t_date, h, slip=SLIP):
    g = data_cache.get(code)
    if g is None:
        return None
    idx = g.index[g["trade_date"] == pd.Timestamp(t_date).normalize()]
    if len(idx) == 0:
        return None
    i = int(idx[0])
    if i + h >= len(g):
        return None
    buy = g["close"].iloc[i]
    sell = g["close"].iloc[i + h]
    if not (pd.notna(buy) and pd.notna(sell) and buy > 0):
        return None
    return sell / buy - 1 - 2 * slip


def run_part2(data_cache, day_cache_getter, calendar, heat, top_k=8):
    hot_by_date, consec_by_date, leader_by_date = heat[top_k]
    leader_rets = {1: [], 2: [], 3: []}
    mom_rets = {1: [], 2: [], 3: []}
    mom_rets5 = []
    for shift in WINDOW_SHIFTS:
        test_dates = slice_test_dates(calendar, 5, shift)
        if not test_dates:
            continue
        for t in test_dates[::5]:
            # 动量策略 top1
            recs = day_cache_getter(t, 5)
            if recs:
                df = H.score_baseline(recs)
                picks, _ = H.select_picks(df, {"max_picks": 1, "max_sector": 1})
                if picks:
                    code = picks[0]["code"]
                    for h in (1, 2, 3):
                        r = fwd_ret_at(data_cache, code, t, h)
                        if r is not None:
                            mom_rets[h].append(r)
                    mom_rets5.append(float(picks[0]["fwd_ret"]))
            # 龙头
            lc = leader_by_date.get(t)
            if lc:
                for h in (1, 2, 3):
                    r = fwd_ret_at(data_cache, lc, t, h)
                    if r is not None:
                        leader_rets[h].append(r)
    return leader_rets, mom_rets, mom_rets5


def stat(rets):
    if not rets:
        return {"n": 0, "mean_ret": 0.0, "win_rate": 0.0}
    a = np.array(rets)
    return {"n": len(a), "mean_ret": round(float(a.mean()) * 100, 2),
            "win_rate": round(float((a > 0).mean()) * 100, 2)}


# =====================================================================
# 主流程
# =====================================================================
def main():
    print("[1/5] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/5] 构建市场择时代理 ...", flush=True)
    mkt_nav_s, mkt_ma20_s = H.build_market_proxy(data_cache, calendar)
    mkt_nav = mkt_nav_s.to_dict(); mkt_ma20 = mkt_ma20_s.to_dict()

    exit_engine = ExitRuleEngine(adaptive=getattr(cfg, "USE_ADAPTIVE_EXIT", True))
    min_amount = cfg.MIN_AMOUNT

    # day_cache(记忆化)
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
        top = get_daily_top(t_date, cfg.POOL_SIZE)
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

    print("[3/5] 构建行业热度(多 top-K) ...", flush=True)
    top_ks = [5, 8, 10]
    heat = {}
    for tk in top_ks:
        heat[tk] = build_sector_heat(data_cache, sector_map, calendar, tk)
        print(f"      top_k={tk} 完成", flush=True)

    print("[4/5] Part1: 单股+行业择时扫描 ...", flush=True)
    consec_list = [None, 1, 2, 3, 4, 5]
    part1, detail = run_part1(data_cache, day_cache_getter, calendar, heat, top_ks, HOLD_PERIODS, consec_list)

    # 找最优(按胜率, 次收益)
    ranked = sorted(part1, key=lambda x: (x["avg_win_rate"], x["avg_profit"]), reverse=True)
    best = ranked[0]
    print(f"      Part1 最优: top_k={best['top_k']} hold={best['hold']} {best['label']} "
          f"胜率={best['avg_win_rate']}% 收益={best['avg_profit']}%", flush=True)

    print("[5/5] Part2: 热门行业龙头 vs 动量策略 ...", flush=True)
    leader_rets, mom_rets, mom_rets5 = run_part2(data_cache, day_cache_getter, calendar, heat, top_k=8)
    part2 = {
        "leader": {h: stat(leader_rets[h]) for h in (1, 2, 3)},
        "momentum_top1": {h: stat(mom_rets[h]) for h in (1, 2, 3)},
        "momentum_top1_5d": stat(mom_rets5),
    }

    out = {
        "meta": {
            "universe": len(data_cache),
            "calendar_start": str(calendar[0])[:10],
            "calendar_end": str(calendar[-1])[:10],
            "slippage": SLIP, "top_ks": top_ks,
            "consec_list": [("ctrl" if c is None else c) for c in consec_list],
            "hold_periods": HOLD_PERIODS, "window_shifts": WINDOW_SHIFTS,
            "leader_top_k": 8,
        },
        "best_part1": best,
        "part1": part1,
        "part2": part2,
        "detail": detail,
    }
    out_path = HERE / "sector_results.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成 -> {out_path}", flush=True)

    # 控制台摘要
    print("\n===== Part1 行业择时(窗口均值) 按胜率排序 =====")
    print(f"{'top_k':>5}{'hold':>4}{'规则':>12}{'均收益%':>9}{'均胜率%':>9}{'均夏普':>8}{'均回撤%':>9}{'均交易':>7}")
    for r in ranked:
        print(f"{r['top_k']:>5}{r['hold']:>4}{r['label']:>12}{r['avg_profit']:>9.2f}"
              f"{r['avg_win_rate']:>9.2f}{r['avg_sharpe']:>8.2f}{r['avg_max_dd']:>9.2f}{r['avg_trades']:>7.1f}")
    print("\n===== Part2 龙头 vs 动量(top1) +H 天 =====")
    for h in (1, 2, 3):
        L, M = part2["leader"][h], part2["momentum_top1"][h]
        print(f"  +{h}天: 龙头 收益={L['mean_ret']:.2f}% 胜率={L['win_rate']:.1f}% (n={L['n']})"
              f" | 动量 收益={M['mean_ret']:.2f}% 胜率={M['win_rate']:.1f}% (n={M['n']})")
    M5 = part2["momentum_top1_5d"]
    print(f"  动量5天(原退出): 收益={M5['mean_ret']:.2f}% 胜率={M5['win_rate']:.1f}% (n={M5['n']})")


if __name__ == "__main__":
    main()
