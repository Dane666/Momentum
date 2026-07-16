# -*- coding: utf-8 -*-
"""
龙头策略"涨停买不进"现实约束分析
================================
问题: 龙头策略每日14:45选股, 选的是当日涨幅最强的热门行业龙头, 尾盘这些票
      往往已涨停封死, 散户根本排不进。原回测(harness_c_ma60)直接用收盘价成交,
      未检查涨停板限制 -> 收益被高估。

本文做两件事:
  1) 量化: 选出的龙头里, 涨停封板(硬买不进) / 接近涨停(基本买不进) 占多少。
  2) 修正回测: 三种买入处理对比
       - baseline : 无视涨停, 收盘直接买 (原回测口径)
       - skip     : 涨停封板的龙头该槽跳过不买(现金保留, 不顺延)
       - next     : 涨停封板龙头顺延到下一个"非封板"候选(取前N+4里前N个非封板)
     都挂 MA60 开门闸口(落地最优版); 另跑 baseline 无闸口作锚。

涨停判定(日线近似):
  板块: 30/688 开头=双创 20%涨停; 其余(60/00)=主板 10%。ST(5%)样本极少忽略。
  封板(硬买不进): 当日涨幅 >= 涨停线-0.1% 且 收盘≈最高(封死)
  接近涨停(基本买不进): 当日涨幅 >= 涨停线-0.6%
  *说明: 用"收盘涨停封板"代理14:45状态——收盘封板说明尾盘已封, 14:45基本买不进。
         这是日线数据下最合理的近似, 非分钟级精确。
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
from harness_c_ma60 import build_ma60_gate, metrics_from_equity

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
LOT = 100
N = 3
HOLD = 3


def pct_limit(code):
    c = str(code)
    if c.startswith(("30", "688")):
        return 0.20
    return 0.10


def is_sealed(code, day_ret, pl_t):
    """收盘涨停封板(硬买不进)。"""
    if pl_t is None or day_ret is None:
        return False
    lim = pct_limit(code)
    o, h, low, close = pl_t
    pct_ok = day_ret >= (lim - 0.001)
    sealed = pct_ok and (h > 0 and abs(close - h) <= h * 0.0015)
    return sealed


def is_near(code, day_ret):
    """接近涨停(封单大, 基本买不进)。"""
    if day_ret is None:
        return False
    lim = pct_limit(code)
    return day_ret >= (lim - 0.006)


def build_dr_by_t(day_ret_map):
    return {t: {c: r for c, r in day_ret_map.get(t, [])} for t in day_ret_map}


def simulate_fix(N, hold, calendar, price_lookup, date_idx, date_list,
                 hot_by_date, day_ret_map, sector_map, reb_dates, init_capital,
                 gate, buy_mode, dr_by_t):
    """buy_mode: 'baseline' | 'skip' | 'next'。返回 (ops, equity, trades, sdt, sdo, sealed_skips)。"""
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [], [init_capital], [], 0, 0, 0
    lo = min(signal_set)
    subs = [{"cash": init_capital / N, "pos": None} for _ in range(N)]
    equity = []; trades = []; ops = []; op_seq = 0
    sdt = 0; sdo = 0; sealed_skips = 0

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
        t = calendar[i]; ts = pd.Timestamp(t); dstr = str(t)[:10]
        # 1) 退出
        for k in range(N):
            s = subs[k]; pos = s["pos"]
            if not pos:
                continue
            pl = price_lookup.get(pos["code"])
            if pl is None or t not in pl:
                continue
            o, h, low, close = pl[t]; pos["last_close"] = close
            exit_fill = None; reason = None
            if t >= pos["exit_date"]:
                exit_fill = close * (1 - SLIP); reason = "到期"
            if reason is not None:
                proceeds = pos["shares"] * exit_fill
                s["cash"] += proceeds
                ret = exit_fill / pos["entry_fill"] - 1.0
                trades.append(ret); op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "卖出", "槽位": k + 1,
                    "代码": pos["code"], "行业": sector_map.get(pos["code"], "其它"),
                    "成交价": round(exit_fill, 3), "股数": pos["shares"],
                    "成交额": round(proceeds, 2), "滑动成本": round(proceeds * SLIP, 2),
                    "实现收益%": round(ret * 100, 2),
                    "实现盈亏元": round(proceeds - pos["shares"] * pos["entry_fill"], 2),
                    "槽位现金": round(s["cash"], 2), "账户净值": round(nav(), 2),
                    "大盘MA60": ("站上" if (gate.get(ts, True) if gate is not None else True) else "跌破"),
                    "备注": reason,
                })
                s["pos"] = None
        # 2) 盯市
        eq = nav()
        if i >= lo:
            equity.append(eq)
        # 3) 信号日
        if i in signal_set:
            sdt += 1
            allow = (gate is None) or gate.get(ts, True)
            if not allow:
                sdo += 1
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "空仓等待", "槽位": "-",
                    "代码": "", "行业": "", "成交价": "", "股数": "",
                    "成交额": "", "滑动成本": "", "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[0]["cash"], 2), "账户净值": round(eq, 2),
                    "大盘MA60": "跌破", "备注": "MA60空头, 本周期不建仓, 现金等待",
                })
                continue
            # 选股
            if buy_mode == "baseline":
                chosen = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            elif buy_mode == "skip":
                chosen = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            else:  # next
                cands = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N + 4)
                chosen = [c for c in cands
                          if not is_sealed(c, dr_by_t.get(t, {}).get(c),
                                          price_lookup.get(c, {}).get(t))][:N]
            for k in range(N):
                if subs[k]["pos"] is not None or k >= len(chosen):
                    continue
                code = chosen[k]
                pl = price_lookup.get(code)
                if pl is None or t not in pl:
                    continue
                # skip 模式: 封板则跳过该槽(不顺延, 现金保留)
                if buy_mode == "skip" and is_sealed(code, dr_by_t.get(t, {}).get(code), pl[t]):
                    sealed_skips += 1
                    op_seq += 1
                    ops.append({
                        "序号": op_seq, "日期": dstr, "动作": "涨停跳过", "槽位": k + 1,
                        "代码": code, "行业": sector_map.get(code, "其它"),
                        "成交价": round(pl[t][3], 3), "股数": 0,
                        "成交额": 0, "滑动成本": 0, "实现收益%": "", "实现盈亏元": "",
                        "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                        "大盘MA60": "站上", "备注": "涨停封板, 14:45买不进, 本槽空置",
                    })
                    continue
                di = date_idx.get(code, {}); dl = date_list.get(code, [])
                ii = di.get(t)
                exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                if exit_date is None:
                    continue
                o, h, low, close = pl[t]
                entry_fill = close * (1 + SLIP)
                cost = subs[k]["cash"]
                shares = int(cost / entry_fill // LOT) * LOT
                if shares <= 0:
                    continue
                subs[k]["cash"] -= shares * entry_fill
                subs[k]["pos"] = {"code": code, "entry_i": i, "entry_fill": entry_fill,
                                  "shares": shares, "last_close": close, "exit_date": exit_date}
                op_seq += 1
                note = f"持有至{str(exit_date)[:10]}到期"
                if buy_mode == "next":
                    note += " [顺延非封板候选]"
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "买入", "槽位": k + 1,
                    "代码": code, "行业": sector_map.get(code, "其它"),
                    "成交价": round(entry_fill, 3), "股数": shares,
                    "成交额": round(shares * entry_fill, 2), "滑动成本": round(shares * entry_fill * SLIP, 2),
                    "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                    "大盘MA60": "站上", "备注": note,
                })
    return ops, equity, trades, sdt, sdo, sealed_skips


def stats(reb_dates, hot_by_date, day_ret_map, sector_map, price_lookup, dr_by_t, N):
    total = 0; sealed = 0; near = 0; top1_sealed = 0
    day_first_sealed = 0; day_any_sealed = 0; day_all_sealed = 0; days = 0
    for t in reb_dates:
        leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
        days += 1
        if not leaders:
            continue
        flags = []
        for idx, code in enumerate(leaders):
            total += 1
            dr = dr_by_t.get(t, {}).get(code)
            pl = price_lookup.get(code, {}).get(t)
            s = is_sealed(code, dr, pl); nr = is_near(code, dr)
            if s:
                sealed += 1
            if nr:
                near += 1
            if idx == 0 and s:
                top1_sealed += 1
            flags.append(s)
        if any(flags):
            day_any_sealed += 1
        if flags and flags[0]:
            day_first_sealed += 1
        if flags and all(flags):
            day_all_sealed += 1
    return {
        "信号日数": days,
        "Top3龙头样本数": total,
        "封板数": sealed, "封板占比%": round(sealed / total * 100, 1) if total else 0,
        "接近涨停数": near, "接近涨停占比%": round(near / total * 100, 1) if total else 0,
        "第1名封板数": top1_sealed,
        "首选买不进日占比%": round(day_first_sealed / days * 100, 1) if days else 0,
        "当天有封板龙头日占比%": round(day_any_sealed / days * 100, 1) if days else 0,
        "当天Top3全封板日占比%": round(day_all_sealed / days * 100, 1) if days else 0,
    }


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]} 股票={len(data_cache)}", flush=True)

    print("[2/4] 行业热度 + 龙头池 + 价格 + MA60闸门 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    gate, mkt_nav_s, ma60_s = build_ma60_gate(data_cache, calendar)
    dr_by_t = build_dr_by_t(day_ret_map)

    reb_all = slice_test_dates(calendar, HOLD, 0)[::HOLD]
    reb_gate = [d for d in reb_all if gate.get(pd.Timestamp(d), True)]
    print(f"      信号日: 始终在场={len(reb_all)}  MA60开门={len(reb_gate)}", flush=True)

    print("[3/4] 涨停买不进量化统计 ...", flush=True)
    st_all = stats(reb_all, hot_by_date, day_ret_map, sector_map, price_lookup, dr_by_t, N)
    st_gate = stats(reb_gate, hot_by_date, day_ret_map, sector_map, price_lookup, dr_by_t, N)
    print(f"      始终在场: Top3封板率={st_all['封板占比%']}%  首选买不进日={st_all['首选买不进日占比%']}%", flush=True)
    print(f"      MA60开门: Top3封板率={st_gate['封板占比%']}%  首选买不进日={st_gate['首选买不进日占比%']}%", flush=True)

    print("[4/4] 修正回测(baseline / skip / next, 含MA60开门 vs 无闸口锚) ...", flush=True)
    out = {"meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                    "N": N, "init": INIT_CAPITAL, "hold": HOLD, "top_k": TOP_K,
                    "规则": "MA60开门闸口 + 龙头策略; 涨停封板(收盘≈最高且达涨停线)视为14:45买不进"},
            "stats": {"始终在场": st_all, "MA60开门": st_gate},
            "backtest": {}}

    def run_and_record(tag, reb, g, mode):
        ops, eq, tr, sdt, sdo, skips = simulate_fix(
            N, HOLD, calendar, price_lookup, date_idx, date_list,
            hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, g, mode, dr_by_t)
        m = metrics_from_equity(eq)
        m["交易笔数"] = len(tr)
        m["涨停跳过槽次"] = skips
        m["信号日总数"] = sdt
        m["空仓信号日"] = sdo
        out["backtest"][tag] = m
        print(f"      {tag:18s} 期末¥{m['期末净值']:,.0f} 收益{m['总收益%']:+.1f}% "
              f"胜率- 夏普{m['夏普']} 回撤{m['最大回撤%']}% 跳过{skips}槽", flush=True)
        return ops, m

    # 无闸口锚
    run_and_record("无闸口_baseline", reb_all, None, "baseline")
    # MA60开门三版本
    ops_next, _ = run_and_record("MA60开门_next", reb_gate, gate, "next")
    ops_skip, _ = run_and_record("MA60开门_skip", reb_gate, gate, "skip")
    run_and_record("MA60开门_baseline", reb_gate, gate, "baseline")

    # 写 CSV: next(推荐现实版) + skip(看跳过哪些)
    df_next = pd.DataFrame(ops_next)
    df_next.to_csv(HERE / "c_tradelog_N3_hold3_ma60_limitup_next.csv", index=False, encoding="utf-8-sig")
    df_skip = pd.DataFrame(ops_skip)
    df_skip.to_csv(HERE / "c_tradelog_N3_hold3_ma60_limitup_skip.csv", index=False, encoding="utf-8-sig")

    (HERE / "c_limitup_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_limitup_results.json / CSV")


if __name__ == "__main__":
    main()
