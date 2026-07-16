# -*- coding: utf-8 -*-
"""
四种"可成交龙头策略"场景 × 基准动量策略 对比回测
=================================================
背景: 上一轮发现"龙头策略(追当日涨幅最强)95%候选涨停封板、买不进, 收益虚假"。
本任务回测用户提出的4种可成交场景, 它们都是买"没封板的强势股/分歧日", 规避买不进陷阱:

  0) 基准动量(同框架): 热门行业内按 5日动量(mom5)降序选 Top-N。作为对比基准。
  1) 场景A 首裂/炸板低吸(全天观察): 当日曾触涨停但炸开(未封死)仍收涨, 或早盘低开震荡企稳;
      且近3日曾涨停/近5日涨超15%(强势股); 板块仍是热门。
  2) 场景B 回踩均线低吸(二波): 收盘价回踩5日或10日均线附近(±3~5%), 缩量(量<5日均量*0.85),
      且近期曾涨停(拉过板的龙头); 板块热门。
  3) 场景C 尾盘偷袭板/潜伏板: 尾盘拉起收在高位(close≈high且涨幅>5%), 开盘不在涨停附近(非早盘板),
      且曾摸涨停但未封死(试探); 博弈隔天冲高溢价。
  4) 场景D 断板反包: 前1~2日曾涨停(是龙头), 今日断板(未涨停)且回调不深(-6%~+3%), 缩量,
      尾盘企稳(收盘在当日上半区); 赌隔天板块修复反包。

所有策略用完全一致的统一账户级回测引擎(买入=收盘*(1+SLIP); 若买入日涨停封板则跳过防御;
持 hold 天到期卖出; 可选 MA60开门闸口)。统计"买入日涨停封板率"以证明可成交性。

日线近似口径限制(已在报告标注):
  - 涨停判定 = 当日涨幅≥涨停线-0.1% 且 收盘≈最高(封死); 主板10%/双创20%。
  - "尾盘拉起""炸板""企稳"等盘中行为用 开/高/低/收 + 量能 近似, 非分钟级精确。
  - 炸板/试探 = 当日最高曾摸涨停价但未封死。
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
from harness_compare3_stop import build_price_lookup
from harness_c_ma60 import build_ma60_gate, metrics_from_equity

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
LOT = 100
N = 3


def islu(v):
    return v is True or v == 1.0


def getv(s, T):
    if s is None:
        return np.nan
    try:
        x = s.get(T, np.nan)
    except Exception:
        x = np.nan
    return x


def build_ctx(data_cache, sector_map, calendar):
    """预建 per-code 量价/均线/涨停序列 + 热门行业候选。"""
    S = {}
    for code, g in data_cache.items():
        g = g.sort_values("trade_date").reset_index(drop=True)
        idx = pd.to_datetime(g["trade_date"]).dt.normalize()
        close = pd.Series(g["close"].values, index=idx)
        vol = pd.Series(g["volume"].values, index=idx)
        op = pd.Series(g["open"].values, index=idx)
        hi = pd.Series(g["high"].values, index=idx)
        lo = pd.Series(g["low"].values, index=idx)
        prev = close.shift(1)
        ret = close.pct_change()
        lim = 0.20 if str(code).startswith(("30", "688")) else 0.10
        limit_price = prev * (1 + lim)
        lu = (ret >= (lim - 0.001)) & (close >= hi * (1 - 0.0015)) & (hi > 0)
        lu = lu.astype(object)
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        vma5 = vol.rolling(5).mean()
        mom5 = close / close.shift(5) - 1
        S[code] = {
            "close": close, "vol": vol, "open": op, "high": hi, "low": lo,
            "prev": prev, "ret": ret, "limit_price": limit_price, "lu": lu,
            "ma5": ma5, "ma10": ma10, "vma5": vma5, "mom5": mom5,
            "lu1": lu.shift(1), "lu2": lu.shift(2), "lu3": lu.shift(3),
            "open_to_prev": op / prev - 1,
            "intraday": (close - op) / op,
            "lower_pos": (close - lo) / (hi - lo + 1e-9),
            "limit": lim,
        }
    # 行业 -> 代码
    sector_to_codes = defaultdict(list)
    for code, sec in sector_map.items():
        if sec != "其它":
            sector_to_codes[sec].append(code)
    return S, sector_to_codes


# ---------------- 选股函数 ----------------
def pick_momentum(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    scored = []
    for code in cands:
        m5 = getv(S[code]["mom5"], T)
        if pd.notna(m5):
            scored.append((m5, code))
    scored.sort(reverse=True)
    return [c for _, c in scored[:N]]


def pick_break_open(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); hi = getv(s["high"], T); cl = getv(s["close"], T)
        lp = getv(s["limit_price"], T); opv = getv(s["open_to_prev"], T)
        lu = getv(s["lu"], T)
        lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T); lu3 = getv(s["lu3"], T)
        m5 = getv(s["mom5"], T); prev = getv(s["prev"], T)
        lo = getv(s["low"], T)
        if not pd.notna(ret) or not pd.notna(hi) or not pd.notna(lp):
            continue
        # 炸板: 曾触涨停价 且 未封死 且 仍收涨
        broke = (hi >= lp * (1 - 0.005)) and (not islu(lu)) and (ret > 0)
        # 低开震荡: 低开≥1% 且 未大跌 且 振幅适中
        amp = (hi - lo) / prev if pd.notna(prev) and prev > 0 else 0
        lowopen = (opv <= -0.01) and (ret > -0.02) and (amp < 0.09)
        if not (broke or lowopen):
            continue
        # 近期强势(是龙头/强势股)
        if not (islu(lu1) or islu(lu2) or islu(lu3) or (pd.notna(m5) and m5 > 0.15)):
            continue
        out.append((m5 if pd.notna(m5) else 0.0, code))
    out.sort(reverse=True)
    return [c for _, c in out[:N]]


def pick_pullback(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        cl = getv(s["close"], T); ma5v = getv(s["ma5"], T); ma10v = getv(s["ma10"], T)
        vol = getv(s["vol"], T); vma5v = getv(s["vma5"], T)
        lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T); lu3 = getv(s["lu3"], T)
        m5 = getv(s["mom5"], T)
        if not (pd.notna(cl) and pd.notna(ma5v) and pd.notna(ma10v)):
            continue
        # 回踩均线
        d5 = abs(cl / ma5v - 1); d10 = abs(cl / ma10v - 1)
        near = (d5 < 0.035) or (d10 < 0.05)
        if not near:
            continue
        # 缩量
        if not (pd.notna(vol) and pd.notna(vma5v) and vma5v > 0 and vol < vma5v * 0.85):
            continue
        # 近期曾涨停(拉过板的龙头)
        if not (islu(lu1) or islu(lu2) or islu(lu3) or (pd.notna(m5) and m5 > 0.15)):
            continue
        out.append((min(d5, d10), code))
    out.sort()  # 越贴近均线越好
    return [c for _, c in out[:N]]


def pick_tailspike(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); cl = getv(s["close"], T); hi = getv(s["high"], T)
        opv = getv(s["open_to_prev"], T); lp = getv(s["limit_price"], T)
        lu = getv(s["lu"], T); intraday = getv(s["intraday"], T)
        lim = s["limit"]
        if not (pd.notna(ret) and pd.notna(cl) and pd.notna(hi) and pd.notna(lp)):
            continue
        # 尾盘拉起收高位 且 开盘不在涨停附近(非早盘板) 且 曾摸涨停未封死(试探)
        cond = (ret > 0.05) and (cl >= hi * (1 - 0.01)) and (opv < lim - 0.03) \
               and (hi >= lp * (1 - 0.005)) and (not islu(lu))
        if not cond:
            continue
        out.append((intraday if pd.notna(intraday) else ret, code))
    out.sort(reverse=True)
    return [c for _, c in out[:N]]


def pick_breakrev(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); vol = getv(s["vol"], T); vma5v = getv(s["vma5"], T)
        lu = getv(s["lu"], T); lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T)
        lpos = getv(s["lower_pos"], T)
        if not (pd.notna(ret) and pd.notna(vol) and pd.notna(vma5v) and pd.notna(lpos)):
            continue
        # 前几天是龙头(涨停) + 今日断板(未涨停) + 回调不深 + 缩量 + 尾盘企稳
        if not (islu(lu1) or islu(lu2)):
            continue
        if islu(lu):
            continue
        if not (-0.06 < ret < 0.03):
            continue
        if not (vma5v > 0 and vol < vma5v * 0.85):
            continue
        if lpos <= 0.5:
            continue
        out.append((vol / vma5v, code))  # 越缩量越好
    out.sort()
    return [c for _, c in out[:N]]


SEL_FNS = {
    "动量基准": pick_momentum,
    "A炸板低吸": pick_break_open,
    "B回踩均线": pick_pullback,
    "C尾盘偷袭板": pick_tailspike,
    "D断板反包": pick_breakrev,
}


def simulate(sel_name, N, hold, calendar, price_lookup, date_idx, date_list,
             S, sector_map, hot_codes, reb_dates, init_capital, gate):
    sel_fn = SEL_FNS[sel_name]
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [], [init_capital], [], 0, 0, 0
    lo = min(signal_set)
    subs = [{"cash": init_capital / N, "pos": None} for _ in range(N)]
    equity = []; trades = []; ops = []; op_seq = 0
    sdt = 0; sdo = 0; sealed_skips = 0; total_buys = 0

    def nav():
        eq = 0.0
        for s in subs:
            if s["pos"] is not None:
                last = s["pos"].get("last_close")
                if last is None:
                    pl = price_lookup.get(s["pos"]["code"]); last = pl[calendar[0]][3] if pl else 0.0
                eq += s["cash"] + s["pos"]["shares"] * last
            else:
                eq += s["cash"]
        return eq

    for i in range(n):
        t = calendar[i]; ts = pd.Timestamp(t); dstr = str(t)[:10]
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
        eq = nav()
        if i >= lo:
            equity.append(eq)
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
            chosen = sel_fn(t, N, None, S, hot_codes)
            for k in range(N):
                if subs[k]["pos"] is not None or k >= len(chosen):
                    continue
                code = chosen[k]
                pl = price_lookup.get(code)
                if pl is None or t not in pl:
                    continue
                # 涨停封板防御(这些策略本选非封板, 个别封板仍跳过)
                if islu(getv(S[code]["lu"], t)):
                    sealed_skips += 1; total_buys += 1
                    op_seq += 1
                    ops.append({
                        "序号": op_seq, "日期": dstr, "动作": "涨停跳过", "槽位": k + 1,
                        "代码": code, "行业": sector_map.get(code, "其它"),
                        "成交价": round(pl[t][3], 3), "股数": 0,
                        "成交额": 0, "滑动成本": 0, "实现收益%": "", "实现盈亏元": "",
                        "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                        "大盘MA60": "站上", "备注": "买入日涨停封板, 14:45买不进, 本槽空置",
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
                total_buys += 1
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "买入", "槽位": k + 1,
                    "代码": code, "行业": sector_map.get(code, "其它"),
                    "成交价": round(entry_fill, 3), "股数": shares,
                    "成交额": round(shares * entry_fill, 2), "滑动成本": round(shares * entry_fill * SLIP, 2),
                    "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                    "大盘MA60": "站上", "备注": f"持有至{str(exit_date)[:10]}到期",
                })
    return ops, equity, trades, sdt, sdo, sealed_skips, total_buys


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]} 股票={len(data_cache)}", flush=True)

    print("[2/4] 行业热度 + 价格 + MA60闸门 + 预建序列 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    gate, _, _ = build_ma60_gate(data_cache, calendar)
    S, sector_to_codes = build_ctx(data_cache, sector_map, calendar)

    # 预建每信号日的"热门行业内候选"
    all_dates = list(calendar)
    hot_codes = {}
    print("      预建热门行业候选 ...", flush=True)
    for t in all_dates:
        hs = hot_by_date.get(t, set())
        if not hs:
            hot_codes[t] = []
            continue
        codes = []
        for sec in hs:
            codes.extend(c for c in sector_to_codes.get(sec, []) if c in S)
        hot_codes[t] = codes

    reb_all = slice_test_dates(calendar, 3, 0)[::3]
    reb_gate = [d for d in reb_all if gate.get(pd.Timestamp(d), True)]
    print(f"      信号日: 始终在场={len(reb_all)}  MA60开门={len(reb_gate)}", flush=True)

    print("[3/4] 4场景 + 基准动量 统一回测 ...", flush=True)
    out = {"meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                    "N": N, "init": INIT_CAPITAL, "hold_options": [2, 3],
                    "规则": "热门行业内按场景条件选股; 买入=收盘*(1+SLIP); 涨停封板防御跳过; 持hold天到期; 可选MA60开门闸口"},
            "strategies": {}, "buy_seal_rate": {}}

    for sel in SEL_FNS:
        out["strategies"][sel] = {}
        out["buy_seal_rate"][sel] = {}
        for gate_on, g, reb in [("无闸口", None, reb_all), ("MA60开门", gate, reb_gate)]:
            out["strategies"][sel][gate_on] = {}
            out["buy_seal_rate"][sel][gate_on] = {}
            for hold in [2, 3]:
                ops, eq, tr, sdt, sdo, skips, buys = simulate(
                    sel, N, hold, calendar, price_lookup, date_idx, date_list,
                    S, sector_map, hot_codes, reb, INIT_CAPITAL, g)
                m = metrics_from_equity(eq)
                m["交易笔数"] = len(tr)
                m["信号日总数"] = sdt
                m["空仓信号日"] = sdo
                m["涨停跳过槽次"] = skips
                m["买入日涨停封板率%"] = round(skips / buys * 100, 1) if buys else 0.0
                out["strategies"][sel][gate_on][f"持{hold}天"] = m
                out["buy_seal_rate"][sel][gate_on][f"持{hold}天"] = m["买入日涨停封板率%"]
                print(f"      {sel:8s} {gate_on:6s} 持{hold} 期末¥{m['期末净值']:,.0f} "
                      f"收益{m['总收益%']:+.1f}% 夏普{m['夏普']} 回撤{m['最大回撤%']}% "
                      f"封板率{m['买入日涨停封板率%']}% 笔数{len(tr)}", flush=True)
            # 写主口径 CSV (MA60开门 持3天)
            if gate_on == "MA60开门":
                ops3, eq3, tr3, _, _, _, _ = simulate(
                    sel, N, 3, calendar, price_lookup, date_idx, date_list,
                    S, sector_map, hot_codes, reb_gate, INIT_CAPITAL, g)
                df = pd.DataFrame(ops3)
                safe = sel.replace("/", "_")
                df.to_csv(HERE / f"c_scen_{safe}_h3_ma60.csv", index=False, encoding="utf-8-sig")

    (HERE / "c_scen_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_scen_results.json / 各策略CSV")


if __name__ == "__main__":
    main()
