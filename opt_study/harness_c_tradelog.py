# -*- coding: utf-8 -*-
"""
C 方案真实逐笔操作记录生成 (10 万本金, 实盘口径)
===============================================
严格复用 harness_compare3_stop.simulate_account_c 的账户模型, 仅做两件事:
  1) 把归一化净值(起点 1.0)换成真实 100,000 元起点, 三个独立仓位槽各 100000/N。
  2) 每次 BUY/SELL 落一条操作记录; 并加入 A 股真实约束: 买入股数向下取整到 100 股。
     (原回测无整手限制, 这里补上以贴近实盘, 引申出少量零股现金滞留)

退出逻辑(无止损): 仅"持有满 hold 个个股交易日" -> 以当日收盘(减卖滑点)离场。
买入逻辑: 每 hold 个交易日为一个信号日, 槽位为空才买入当日热门行业龙头中第 k 名。
          龙头 = 热门行业内当日涨幅前 N(对同一热门行业无分散约束 -> 可能同一行业, 也可能跨行业)。

输出:
  c_tradelog_N3_hold3.csv / c_tradelog_N3_hold5.csv  (全部逐笔)
  c_tradelog_report.html  (汇总 + 前 40 笔样例)
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
LOT = 100  # A股整手


def simulate_with_log(N, hold, stop_pct, calendar, price_lookup, date_idx,
                      date_list, hot_by_date, day_ret_map, sector_map,
                      reb_dates, init_capital):
    """返回 (operations, equity_curve, trades, final_summary)
    operations: list[dict] 逐笔记录
    """
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [], [init_capital], [], {}
    lo = min(signal_set)
    hi = min(max(signal_set) + hold, n - 1)

    subs = [{"cash": init_capital / N, "pos": None} for _ in range(N)]
    equity_curve = []
    trades = []
    ops = []
    op_seq = 0

    def nav_now():
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
        dstr = str(t)[:10]
        # 1) 退出检查
        for k in range(N):
            s = subs[k]
            pos = s["pos"]
            if not pos:
                continue
            pl = price_lookup.get(pos["code"])
            if pl is None or t not in pl:
                continue  # 缺失日: 沿用 last_close, 不退出
            o, h, low, close = pl[t]
            pos["last_close"] = close
            exit_fill = None
            reason = None
            if stop_pct > 0:
                stop_price = pos["entry_fill"] * (1.0 - stop_pct)
                if low <= stop_price:
                    exit_fill = stop_price * (1.0 - SLIP)
                    reason = "止损"
            if reason is None and t >= pos["exit_date"]:
                exit_fill = close * (1.0 - SLIP)
                reason = "到期"
            if reason is not None:
                proceeds = pos["shares"] * exit_fill
                s["cash"] += proceeds
                ret = exit_fill / pos["entry_fill"] - 1.0
                pnl = proceeds - pos["shares"] * pos["entry_fill"]
                trades.append((ret, reason))
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "卖出",
                    "槽位": k + 1, "代码": pos["code"],
                    "行业": sector_map.get(pos["code"], "其它"),
                    "成交价": round(exit_fill, 3),
                    "股数": pos["shares"],
                    "成交额": round(proceeds, 2),
                    "滑动成本": round(proceeds * SLIP, 2),
                    "实现收益%": round(ret * 100, 2),
                    "实现盈亏元": round(pnl, 2),
                    "槽位现金": round(s["cash"], 2),
                    "账户净值": round(nav_now(), 2),
                    "备注": reason,
                })
                s["pos"] = None
        # 2) 盯市(缺失日沿用 last_close)
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
        if i >= lo:
            equity_curve.append(eq)
        # 3) 信号日买入
        if i in signal_set:
            leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            for k in range(N):
                if subs[k]["pos"] is not None or k >= len(leaders):
                    continue
                code = leaders[k]
                pl = price_lookup.get(code)
                if pl is None or t not in pl:
                    continue
                di = date_idx.get(code, {})
                dl = date_list.get(code, [])
                ii = di.get(t)
                exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                if exit_date is None:
                    continue
                o, h, low, close = pl[t]
                entry_fill = close * (1.0 + SLIP)
                cost = subs[k]["cash"]
                shares = int(cost / entry_fill // LOT) * LOT
                if shares <= 0:
                    continue
                actual_cost = shares * entry_fill
                subs[k]["cash"] -= actual_cost
                subs[k]["pos"] = {"code": code, "entry_i": i,
                                  "entry_fill": entry_fill, "shares": shares,
                                  "last_close": close, "exit_date": exit_date}
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "买入",
                    "槽位": k + 1, "代码": code,
                    "行业": sector_map.get(code, "其它"),
                    "成交价": round(entry_fill, 3),
                    "股数": shares,
                    "成交额": round(actual_cost, 2),
                    "滑动成本": round(actual_cost * SLIP, 2),
                    "实现收益%": "",
                    "实现盈亏元": "",
                    "槽位现金": round(subs[k]["cash"], 2),
                    "账户净值": round(nav_now(), 2),
                    "备注": f"持有至{str(exit_date)[:10]}到期",
                })
    return ops, equity_curve, trades, {}


def metrics_from_equity(equity_curve, trades, hold):
    eq = np.array(equity_curve, dtype=float)
    daily = np.diff(eq) / eq[:-1]
    daily = daily[~np.isnan(daily)]
    final = eq[-1]
    profit = (final - INIT_CAPITAL) / INIT_CAPITAL * 100.0
    years = len(eq) / 252.0
    annual = (final / INIT_CAPITAL) ** (1.0 / years) - 1.0 if years > 0 else -1.0
    sharpe = daily.mean() / daily.std() * np.sqrt(252.0) if len(daily) > 1 and daily.std() > 0 else 0.0
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    max_dd = abs(dd.min()) * 100.0 if len(dd) else 0.0
    rets = [x[0] for x in trades]
    wins = sum(1 for r in rets if r > 0)
    win_rate = wins / len(rets) * 100.0 if rets else 0.0
    return {
        "期末净值": round(float(final), 2),
        "总收益%": round(float(profit), 2),
        "年化%": round(float(annual * 100), 2),
        "夏普": round(float(sharpe), 3),
        "胜率%": round(float(win_rate), 2),
        "最大回撤%": round(float(max_dd), 2),
        "交易笔数": len(trades),
    }


def main():
    print("[1/3] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/3] 行业热度(资金净流入) + 龙头池 + 价格查找 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)

    # 行业集中度统计(回答"是否同一行业"): 看每次信号日 top3 的行业分布
    test_dates = slice_test_dates(calendar, 3, 0)
    reb = test_dates[::3]
    same_sector_days = 0
    multi_sector_days = 0
    for t in reb:
        leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, 3)
        secs = [sector_map.get(c, "其它") for c in leaders]
        secs = [s for s in secs if s != "其它"]
        if len(set(secs)) <= 1:
            same_sector_days += 1
        else:
            multi_sector_days += 1
    print(f"      信号日行业分布: 三只同行业={same_sector_days}天, 跨行业={multi_sector_days}天", flush=True)

    print("[3/3] 生成逐笔操作记录 (N=3, 无止损, hold=3 与 hold=5) ...", flush=True)
    summary = {}
    for hold in [3, 5]:
        td = slice_test_dates(calendar, hold, 0)
        if not td:
            continue
        reb_dates = td[::hold]
        ops, eq, trades, _ = simulate_with_log(
            3, hold, 0.0, calendar, price_lookup, date_idx, date_list,
            hot_by_date, day_ret_map, sector_map, reb_dates, INIT_CAPITAL)
        m = metrics_from_equity(eq, trades, hold)
        summary[f"hold{hold}"] = m
        df = pd.DataFrame(ops)
        csv_path = HERE / f"c_tradelog_N3_hold{hold}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"      hold={hold}: {csv_path.name}  操作记录={len(ops)}条 "
              f"期末净值={m['期末净值']:.0f} 总收益={m['总收益%']:.2f}% "
              f"胜率={m['胜率%']:.2f}% 交易={m['交易笔数']}", flush=True)
        # 存 HTML 用的样例/汇总
        if hold == 3:
            df.to_pickle(HERE / "_tradelog_hold3.pkl")

    # 行业分布写入
    summary["_行业分布"] = {"三只同行业天数": same_sector_days, "跨行业天数": multi_sector_days}

    (HERE / "c_tradelog_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_tradelog_summary.json")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
