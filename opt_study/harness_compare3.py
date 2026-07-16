# -*- coding: utf-8 -*-
"""
三个策略的回测对比 (1/2/3 只股, 算胜率与收益)
================================================
严格复用原策略框架 (harness.py): load_universe / simulate_day / score_baseline /
select_picks / compute_metrics / build_market_proxy; 热门行业用 harness_sector 的
"板块主力资金净流入" 口径 (v2)。

策略定义:
  A = 多因子框架 + "仅限热门行业"闸口 + 择时开关(破MA20空仓)
      -> 用 day_cache_getter(原框架全口径: ≥2亿成交额 + 套牢盘≤0.10 + sharpe>1.0
         + 自适应止盈止损), 仅保留热门行业候选, 再叠加市场择时闸口。
  B = 热门行业龙头 + 择时(破MA20空仓/减仓)
      -> 选热门行业内当日涨幅前N的龙头, 买&持有; 市场破MA20则该期空仓。
  C = 热门行业龙头 (无择时)
      -> 同上但不加择时闸口。

口径差异(如实标注):
  A 用原框架退出引擎(simulate_day); B/C 用简单买&持有(close[t+H]/close[t]-1-2*slip),
  且 B/C 不施加原框架的流动性/套牢盘过滤 —— 这正反映"龙头跟随"是另一套哲学。

输出: compare3_results.json
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
from momentum.risk import ExitRuleEngine

SLIP = cfg.SLIPPAGE
WINDOW_SHIFTS = [0, 20, 40, 60]
TOP_K = 8
STRAT_NAMES = {
    "A": "多因子+热门闸口+择时",
    "B": "龙头+择时(破MA20空仓)",
    "C": "龙头(无择时)",
}


def build_day_returns(data_cache, sector_map):
    """date -> [(code, day_ret), ...] 仅非'其它'板块。用于龙头按涨幅选取。"""
    out = defaultdict(list)
    for code, g in data_cache.items():
        g = g.sort_values("trade_date").reset_index(drop=True)
        sec = sector_map.get(code, "其它")
        if sec == "其它":
            continue
        rets = g["close"].pct_change()
        ds = g["trade_date"].tolist()
        for i in range(1, len(g)):
            r = rets.iloc[i]
            if pd.notna(r):
                out[ds[i]].append((code, float(r)))
    return out


def topn_leaders(day_ret_map, sector_map, hot_by_date, t, N):
    members = day_ret_map.get(t, [])
    hot = hot_by_date.get(t, set())
    cands = [(c, r) for c, r in members if sector_map.get(c, "其它") in hot]
    cands.sort(key=lambda x: -x[1])
    return [c for c, _ in cands[:N]]


def run_strategy(strategy, N, hold, shift, calendar, day_cache_getter,
                 mkt_nav, mkt_ma20, hot_by_date, day_ret_map, sector_map, data_cache):
    test_dates = slice_test_dates(calendar, hold, shift)
    if not test_dates:
        return None
    reb = test_dates[::hold]
    equity = [1.0]; daily = []; tc = 0; wc = 0; dates_out = []

    use_regime = (strategy in ("A", "B"))

    for t in reb:
        traded = False; pr = 0.0
        # 择时闸口
        if use_regime:
            nav_t = mkt_nav.get(pd.Timestamp(t), np.nan)
            ma_t = mkt_ma20.get(pd.Timestamp(t), np.nan)
            if pd.notna(nav_t) and pd.notna(ma_t) and nav_t < ma_t:
                # 空头: 该期空仓
                equity.append(equity[-1]); daily.append(0.0); dates_out.append(str(t)[:10]); continue

        if strategy == "A":
            recs = day_cache_getter(t, hold)
            recs = [r for r in recs if r["sector"] in hot_by_date.get(t, set())]
            if recs:
                df = H.score_baseline(recs)
                picks, _ = H.select_picks(df, {"max_picks": N, "max_sector": N})
                if picks:
                    p = pd.DataFrame(picks)
                    pr = float(p["fwd_ret"].mean())
                    wc += int((p["fwd_ret"] > 0).sum())
                    tc += len(p)
                    traded = True
        else:  # B / C 龙头
            leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            rets = []
            for code in leaders:
                r = fwd_ret_at(data_cache, code, t, hold)
                if r is not None:
                    rets.append(r)
            if rets:
                pr = float(np.mean(rets))
                wc += int((np.array(rets) > 0).sum())
                tc += len(rets)
                traded = True

        if traded:
            equity.append(equity[-1] * (1 + pr)); daily.append(pr)
        else:
            equity.append(equity[-1]); daily.append(0.0)
        dates_out.append(str(t)[:10])

    return H.compute_metrics(equity, daily, tc, wc, hold, dates_out)


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

    print("[3/5] 构建行业热度(资金净流入) + 龙头候选池 ...", flush=True)
    hot_by_date, consec_by_date, leader_by_date = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    print(f"      热门行业构建完成; 龙头候选池日期数={len(day_ret_map)}", flush=True)

    print("[4/5] 运行 三策略 × N(1/2/3) × 窗口 ...", flush=True)
    results = []
    for strategy in ["A", "B", "C"]:
        for N in [1, 2, 3]:
            holds = [5] if strategy == "A" else [5, 3]
            for hold in holds:
                cfgs = {}
                for shift in WINDOW_SHIFTS:
                    m = run_strategy(strategy, N, hold, shift, calendar, day_cache_getter,
                                     mkt_nav, mkt_ma20, hot_by_date, day_ret_map, sector_map, data_cache)
                    if m:
                        ck = f"hold{hold}_shift{shift}"
                        cfgs[ck] = {k: m[k] for k in
                                    ("profit_pct", "annual_ret", "sharpe", "win_rate", "max_dd", "trade_count", "final_nav")}
                if cfgs:
                    rows = list(cfgs.values())
                    avg = {
                        "strategy": strategy, "name": STRAT_NAMES[strategy], "N": N, "hold": hold,
                        "avg_profit": round(float(np.mean([r["profit_pct"] for r in rows])), 2),
                        "avg_annual": round(float(np.mean([r["annual_ret"] for r in rows])), 2),
                        "avg_sharpe": round(float(np.mean([r["sharpe"] for r in rows])), 3),
                        "avg_win_rate": round(float(np.mean([r["win_rate"] for r in rows])), 2),
                        "avg_max_dd": round(float(np.mean([r["max_dd"] for r in rows])), 2),
                        "avg_trades": round(float(np.mean([r["trade_count"] for r in rows])), 1),
                        "main": cfgs.get("hold5_shift0", {}),
                    }
                    results.append(avg)
                    print(f"      [{strategy}] N={N} hold={hold} 收益={avg['avg_profit']:>7.2f}% "
                          f"胜率={avg['avg_win_rate']:>5.2f}% 夏普={avg['avg_sharpe']:.2f} "
                          f"回撤={avg['avg_max_dd']:>5.2f}% 交易={avg['avg_trades']:.1f}", flush=True)

    out = {
        "meta": {
            "universe": len(data_cache),
            "calendar_start": str(calendar[0])[:10],
            "calendar_end": str(calendar[-1])[:10],
            "slippage": SLIP, "top_k": TOP_K, "window_shifts": WINDOW_SHIFTS,
            "note": "A=多因子框架(≥2亿+套牢盘≤0.10+sharpe>1.0+自适应退出)+热门行业闸口+择时; "
                    "B/C=热门行业龙头(当日涨幅前N)买&持有,无原框架过滤。",
        },
        "results": results,
    }
    (HERE / "compare3_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> compare3_results.json", flush=True)


if __name__ == "__main__":
    main()
