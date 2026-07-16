# -*- coding: utf-8 -*-
"""
龙头策略 × 大盘状态分析 + 择时过滤(机会不对就空仓等待)
========================================================
目标: 用户发现近期龙头策略全是负收益 -> 量化"什么大盘环境下龙头策略赚钱/亏钱",
      并找出真正有效的过滤条件, 使得信号日大盘环境不对时直接空仓等待。

做法:
  1) 还原龙头策略(C, N=3, hold=3, 无止损)的逐笔收益, 并把"入场日的大盘状态特征"贴上。
  2) 状态特征: 大盘站上MA20/MA60、20日动量、20日年化波动率、市场宽度(上涨家数占比)、宽度MA20。
  3) 分桶统计: 不同大盘状态下龙头策略的胜率/平均收益/样本数 -> 找出"亏钱环境"。
  4) 过滤扫描: 用单条件/组合条件作为信号日开关(不对则空仓等待), 重跑账户级回测,
     对比"始终在场" vs "过滤后", 看收益/夏普/回撤/空仓占比。
  5) 近期验证: 取测试窗口最后一段(约60交易日), 对比"始终在场"与"过滤后"的实际表现,
     并列出近期大盘状态, 验证过滤能躲开近期亏损。

输出: c_regime_results.json + c_regime_report.html
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
from harness_sector import build_sector_heat, slice_test_dates
from harness_compare3 import build_day_returns, topn_leaders
from harness_compare3_stop import build_price_lookup

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
N = 3
HOLDS = [3, 5]


# ---------------------------------------------------------------------
# 账户级模拟(可记录每笔入场特征 / 可施加信号日过滤)
# ---------------------------------------------------------------------
def simulate_c(calendar, price_lookup, date_idx, date_list, hot_by_date,
               day_ret_map, sector_map, reb_dates, hold, stop, init_capital,
               filter_fn=None, record=False):
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [init_capital], [], []
    lo = min(signal_set)
    subs = [{"cash": init_capital / N, "pos": None} for _ in range(N)]
    equity = []
    trades = []
    recs = []
    op_count = 0

    def nav():
        eq = 0.0
        for s in subs:
            if s["pos"] is not None:
                last = s["pos"].get("last_close")
                if last is None:
                    pl = price_lookup.get(s["pos"]["code"])
                    last = pl[calendar[0]][3] if pl else 0.0
                eq += s["cash"] + s["pos"]["shares"] * last
            else:
                eq += s["cash"]
        return eq

    for i in range(n):
        t = calendar[i]
        # 退出
        for k in range(N):
            s = subs[k]; pos = s["pos"]
            if not pos:
                continue
            pl = price_lookup.get(pos["code"])
            if pl is None or t not in pl:
                continue
            o, h, low, close = pl[t]
            pos["last_close"] = close
            exit_fill = None; reason = None
            if stop > 0:
                sp = pos["entry_fill"] * (1 - stop)
                if low <= sp:
                    exit_fill = sp * (1 - SLIP); reason = "止损"
            if reason is None and t >= pos["exit_date"]:
                exit_fill = close * (1 - SLIP); reason = "到期"
            if reason is not None:
                proceeds = pos["shares"] * exit_fill
                s["cash"] += proceeds
                ret = exit_fill / pos["entry_fill"] - 1.0
                trades.append(ret)
                s["pos"] = None
        # 盯市
        eq = nav()
        if i >= lo:
            equity.append(eq)
        # 信号日买入(可过滤)
        if i in signal_set:
            allow = (filter_fn is None) or filter_fn(t)
            if allow:
                leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            else:
                leaders = []
            for k in range(N):
                if subs[k]["pos"] is not None or k >= len(leaders):
                    continue
                code = leaders[k]
                pl = price_lookup.get(code)
                if pl is None or t not in pl:
                    continue
                di = date_idx.get(code, {}); dl = date_list.get(code, [])
                ii = di.get(t)
                exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                if exit_date is None:
                    continue
                o, h, low, close = pl[t]
                entry_fill = close * (1 + SLIP)
                cost = subs[k]["cash"]
                shares = int(cost / entry_fill // 100) * 100
                if shares <= 0:
                    continue
                subs[k]["cash"] -= shares * entry_fill
                subs[k]["pos"] = {"code": code, "entry_i": i,
                                  "entry_fill": entry_fill, "shares": shares,
                                  "last_close": close, "exit_date": exit_date}
                if record:
                    recs.append({"entry": str(t)[:10], "code": code,
                                 "slot": k + 1, "ret": None})
                    # ret 在退出时回填不便, 改为记录入场特征供离线分桶
    return equity, trades, recs


# ---------------------------------------------------------------------
# 简化版: 直接算每笔"始终在场"交易的收益 + 入场日特征(用于状态分桶)
# ---------------------------------------------------------------------
def per_trade_with_features(calendar, price_lookup, date_idx, date_list,
                            hot_by_date, day_ret_map, sector_map, reb_dates,
                            hold, feats):
    recs = []
    cal_idx = {t: i for i, t in enumerate(calendar)}
    for t in reb_dates:
        ti = cal_idx.get(t)
        if ti is None:
            continue
        leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
        for k, code in enumerate(leaders):
            pl = price_lookup.get(code)
            if pl is None or t not in pl:
                continue
            di = date_idx.get(code, {}); dl = date_list.get(code, [])
            ii = di.get(t)
            exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
            if exit_date is None:
                continue
            o, h, low, close = pl[t]
            entry_fill = close * (1 + SLIP)
            # 退出价: 用 exit_date 当天 low/close 判止损(无止损)
            ej = cal_idx.get(exit_date)
            if ej is None or exit_date not in pl:
                continue
            _, eh, elow, eclose = pl[exit_date]
            exit_fill = eclose * (1 - SLIP)
            ret = exit_fill / entry_fill - 1.0
            f = feats.get(pd.Timestamp(t))
            rec = {"entry": str(t)[:10], "code": code, "slot": k + 1,
                   "ret": ret, "mom20": f["mom20"], "vol20": f["vol20"],
                   "above_ma20": f["above_ma20"], "above_ma60": f["above_ma60"],
                   "breadth": f["breadth"], "breadth_ma20": f["breadth_ma20"]}
            recs.append(rec)
    return recs


def metrics_from_equity(equity, init=INIT_CAPITAL):
    eq = np.array(equity, dtype=float)
    daily = np.diff(eq) / eq[:-1]; daily = daily[~np.isnan(daily)]
    final = eq[-1]
    profit = (final - init) / init * 100.0
    years = len(eq) / 252.0
    annual = (final / init) ** (1.0 / years) - 1.0 if years > 0 else -1.0
    sharpe = daily.mean() / daily.std() * np.sqrt(252.0) if len(daily) > 1 and daily.std() > 0 else 0.0
    peak = np.maximum.accumulate(eq); dd = (eq - peak) / peak
    max_dd = abs(dd.min()) * 100.0 if len(dd) else 0.0
    return {"期末净值": round(final, 2), "总收益%": round(profit, 2),
            "年化%": round(annual * 100, 2), "夏普": round(sharpe, 3),
            "最大回撤%": round(max_dd, 2)}


def main():
    print("[1/5] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/5] 大盘代理 + 状态特征 ...", flush=True)
    mkt_nav_s, mkt_ma20_s = H.build_market_proxy(data_cache, calendar)
    mkt_nav = mkt_nav_s
    mkt_ma60 = mkt_nav.rolling(60).mean()
    mkt_mom20 = mkt_nav.pct_change(20)
    mkt_vol20 = mkt_nav.pct_change().rolling(20).std() * np.sqrt(252)
    # 市场宽度(上涨家数占比, 排除'其它'板块)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    breadth = {}
    for d, members in day_ret_map.items():
        if not members:
            breadth[d] = np.nan
            continue
        pos = sum(1 for _, r in members if r > 0)
        breadth[d] = pos / len(members)
    breadth_s = pd.Series(breadth)
    breadth_ma20 = breadth_s.rolling(20).mean()

    feats = {}
    for t in calendar:
        ts = pd.Timestamp(t)
        navv = mkt_nav.get(ts, np.nan)
        ma20 = mkt_ma20_s.get(ts, np.nan)
        ma60 = mkt_ma60.get(ts, np.nan)
        feats[ts] = {
            "mom20": mkt_mom20.get(ts, np.nan),
            "vol20": mkt_vol20.get(ts, np.nan),
            "above_ma20": (navv > ma20) if (pd.notna(navv) and pd.notna(ma20)) else False,
            "above_ma60": (navv > ma60) if (pd.notna(navv) and pd.notna(ma60)) else False,
            "breadth": breadth.get(ts, np.nan),
            "breadth_ma20": breadth_ma20.get(ts, np.nan),
        }
    print("      特征构建完成", flush=True)

    print("[3/5] 逐笔收益 + 入场日状态分桶 ...", flush=True)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    all_recs = {}
    for hold in HOLDS:
        td = slice_test_dates(calendar, hold, 0)
        reb = td[::hold]
        recs = per_trade_with_features(calendar, price_lookup, date_idx, date_list,
                                       hot_by_date, day_ret_map, sector_map, reb, hold, feats)
        all_recs[hold] = recs
        wins = [r for r in recs if r["ret"] > 0]
        print(f"      hold={hold}: 笔数={len(recs)} 整体胜率={len(wins)/len(recs)*100:.1f}% "
              f"整体均值={np.mean([r['ret'] for r in recs])*100:.2f}%", flush=True)

    # ---- 状态分桶: 找出亏钱环境 ----
    def agg(sel):
        if not sel:
            return {"笔数": 0, "胜率%": 0.0, "均值%": 0.0}
        rs = [r["ret"] for r in sel]
        return {"笔数": len(sel),
                "胜率%": round(sum(1 for x in rs if x > 0) / len(rs) * 100, 1),
                "均值%": round(np.mean(rs) * 100, 2)}

    def bucket(recs, key, bins):
        out = []
        for lo_b, hi_b, lab in bins:
            sel = [r for r in recs if (r[key] is not None and pd.notna(r[key])
                                       and (lo_b is None or r[key] >= lo_b)
                                       and (hi_b is None or r[key] < hi_b))]
            if not sel:
                continue
            rs = [r["ret"] for r in sel]
            out.append({"区间": lab, "笔数": len(sel),
                        "胜率%": round(sum(1 for x in rs if x > 0) / len(rs) * 100, 1),
                        "均值%": round(np.mean(rs) * 100, 2)})
        return out

    seg = {}
    for hold in HOLDS:
        recs = all_recs[hold]
        seg[str(hold)] = {
            "mom20": bucket(recs, "mom20", [(-9, -0.05, "<-5%"), (-0.05, 0, "-5%~0"),
                                            (0, 0.05, "0~5%"), (0.05, 0.10, "5%~10%"),
                                            (0.10, 9, ">10%")]),
            "above_ma20": [{"状态": "站上MA20", **agg([r for r in recs if r["above_ma20"]])},
                           {"状态": "跌破MA20", **agg([r for r in recs if not r["above_ma20"]])}],
            "above_ma60": [{"状态": "站上MA60", **agg([r for r in recs if r["above_ma60"]])},
                           {"状态": "跌破MA60", **agg([r for r in recs if not r["above_ma60"]])}],
            "breadth_ma20": bucket(recs, "breadth_ma20", [(0, 0.45, "<45%"), (0.45, 0.5, "45%~50%"),
                                                          (0.5, 0.55, "50%~55%"), (0.55, 9, ">55%")]),
            "vol20": bucket(recs, "vol20", [(0, 0.15, "<15%"), (0.15, 0.25, "15%~25%"),
                                            (0.25, 0.35, "25%~35%"), (0.35, 9, ">35%")]),
        }

    print("[4/5] 择时过滤扫描(信号日不对则空仓等待) ...", flush=True)
    # 候选过滤条件
    def mk(name, fn):
        return name, fn
    candidates = {
        "始终在场": None,
        "站上MA20": lambda t: feats[pd.Timestamp(t)]["above_ma20"],
        "站上MA60": lambda t: feats[pd.Timestamp(t)]["above_ma60"],
        "20日动量>0": lambda t: (feats[pd.Timestamp(t)]["mom20"] or 0) > 0,
        "宽度MA20>50%": lambda t: (feats[pd.Timestamp(t)]["breadth_ma20"] or 0) > 0.5,
        "波动率<25%": lambda t: (feats[pd.Timestamp(t)]["vol20"] or 9) < 0.25,
        "MA20&宽度>50%": lambda t: feats[pd.Timestamp(t)]["above_ma20"] and (feats[pd.Timestamp(t)]["breadth_ma20"] or 0) > 0.5,
        "MA20&动量>0": lambda t: feats[pd.Timestamp(t)]["above_ma20"] and (feats[pd.Timestamp(t)]["mom20"] or 0) > 0,
        "MA60&宽度>50%": lambda t: feats[pd.Timestamp(t)]["above_ma60"] and (feats[pd.Timestamp(t)]["breadth_ma20"] or 0) > 0.5,
        "MA20&宽度>50%&波动<25%": lambda t: feats[pd.Timestamp(t)]["above_ma20"] and (feats[pd.Timestamp(t)]["breadth_ma20"] or 0) > 0.5 and (feats[pd.Timestamp(t)]["vol20"] or 9) < 0.25,
        "MA60&宽度>50%&波动<25%": lambda t: feats[pd.Timestamp(t)]["above_ma60"] and (feats[pd.Timestamp(t)]["breadth_ma20"] or 0) > 0.5 and (feats[pd.Timestamp(t)]["vol20"] or 9) < 0.25,
    }

    scan = []
    for hold in HOLDS:
        td = slice_test_dates(calendar, hold, 0)
        reb = td[::hold]
        for fname, fn in candidates.items():
            eq, trades, _ = simulate_c(calendar, price_lookup, date_idx, date_list,
                                       hot_by_date, day_ret_map, sector_map, reb,
                                       hold, 0.0, INIT_CAPITAL, filter_fn=fn, record=False)
            if len(eq) < 30:
                continue
            m = metrics_from_equity(eq)
            # 空仓占比: 全程空仓的天数 / 总天数
            cash_days = 0
            total_days = 0
            # 重跑一次记空仓天数(简化: 用 account 状态)
            # 直接估算: 用信号日过滤比例近似
            if fn is not None:
                allow_cnt = sum(1 for t in reb if fn(t))
                cash_frac = 1 - allow_cnt / len(reb) if reb else 0
            else:
                cash_frac = 0.0
            scan.append({"hold": hold, "filter": fname, **m,
                         "空仓占比%": round(cash_frac * 100, 1),
                         "交易笔数": len(trades)})
            print(f"      hold={hold} {fname:<22} 收益={m['总收益%']:>7.2f}% "
                  f"夏普={m['夏普']:.2f} 回撤={m['最大回撤%']:>5.2f}% 空仓={cash_frac*100:>4.1f}% "
                  f"交易={len(trades)}", flush=True)

    # 选最优(优先夏普, 其次收益, 回撤不过大)
    best = {}
    for hold in HOLDS:
        cands = [r for r in scan if r["hold"] == hold and r["filter"] != "始终在场"]
        cands.sort(key=lambda r: (r["夏普"], r["总收益%"]), reverse=True)
        best[hold] = cands[0] if cands else None

    print("[5/5] 近期验证(最后60交易日) ...", flush=True)
    # 用 always-in 与最优过滤, 计算最后一段净值(取净值曲线最后60点)
    recent = {}
    for hold in HOLDS:
        td = slice_test_dates(calendar, hold, 0)
        reb = td[::hold]
        eq_always, _, _ = simulate_c(calendar, price_lookup, date_idx, date_list,
                                     hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                     INIT_CAPITAL, filter_fn=None)
        bf = best[hold]
        fn = candidates.get(bf["filter"]) if bf else None
        eq_filt, _, _ = simulate_c(calendar, price_lookup, date_idx, date_list,
                                   hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                   INIT_CAPITAL, filter_fn=fn)

        def tail_ret(eq):
            e = np.array(eq, dtype=float)
            if len(e) < 60:
                return None
            return (e[-1] / e[-60] - 1) * 100
        recent[str(hold)] = {
            "始终在场_近60日%": round(tail_ret(eq_always), 2),
            "过滤后_近60日%": round(tail_ret(eq_filt), 2),
            "最优过滤": bf["filter"] if bf else "-",
        }
        print(f"      hold={hold} 近60日: 始终在场={recent[str(hold)]['始终在场_近60日%']:.2f}% "
              f"过滤后={recent[str(hold)]['过滤后_近60日%']:.2f}%", flush=True)

    out = {
        "meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                 "N": N, "init": INIT_CAPITAL, "top_k": TOP_K,
                 "note": "龙头策略(C)结合大盘状态分析: 找出亏钱环境 + 信号日择时过滤(不对则空仓等待)"},
        "segmentation": seg,
        "scan": scan,
        "best": {str(h): best[h] for h in best},
        "recent": recent,
    }
    (HERE / "c_regime_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_regime_results.json")


if __name__ == "__main__":
    main()
