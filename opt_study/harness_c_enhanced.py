# -*- coding: utf-8 -*-
"""
C 尾盘偷袭板 增强版回测 —— 接回生产退出机制 + 用户4点优化
=========================================================
(1) 深耕C: 缩短持有至1~2天 + "隔天高开即走"止盈; 叠加板块强度/涨停家数过滤。
(2) 接回生产框架退出机制: C选股接入 ExitRuleEngine(自适应止盈止损)+套牢盘过滤。
(3) A/B/D 双重过滤(大盘多头+板块主升)重测。
(4) 强/弱市分段看 C 稳健性。

退出机制复刻自 risk/exit_rules.py + risk/adaptive_exit.py:
  - 固定模式: 止损5%/止盈10%/跌破MA5/乖离超20%/RSI>=80/跌破MA20
  - 自适应模式(本框架默认): 按 ATR%/RSI/乖离率/市场环境 动态调参, 浮盈>5%移动止损护利
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
MAX_TRAPPED = 0.10          # 套牢盘过滤器(生产核心alpha)


def islu(v):
    return v is True or v == 1.0


def getv(s, T):
    if s is None:
        return np.nan
    try:
        return s.get(T, np.nan)
    except Exception:
        return np.nan


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    r = close.pct_change().fillna(0.0)
    gain = r.clip(lower=0.0).rolling(period).mean()
    loss = (-r).clip(lower=0.0).rolling(period).mean()
    rs = gain / (loss + 1e-12)
    return 100 - 100 / (1 + rs)


def _atr(high, low, close, period=14):
    prev = close.shift(1)
    tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def build_ctx(data_cache, sector_map, calendar):
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
        lu = ((ret >= (lim - 0.001)) & (close >= hi * (1 - 0.0015)) & (hi > 0)).astype(object)
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        vma5 = vol.rolling(5).mean()
        mom5 = close / close.shift(5) - 1
        atr = _atr(hi, lo, close)
        rsi14 = _rsi(close)
        S[code] = {
            "close": close, "vol": vol, "open": op, "high": hi, "low": lo,
            "prev": prev, "ret": ret, "limit_price": limit_price, "lu": lu,
            "ma5": ma5, "ma10": ma10, "ma20": ma20, "vma5": vma5, "mom5": mom5,
            "atr": atr, "rsi14": rsi14,
            "lu1": lu.shift(1), "lu2": lu.shift(2), "lu3": lu.shift(3),
            "open_to_prev": op / prev - 1,
            "intraday": (close - op) / op,
            "lower_pos": (close - lo) / (hi - lo + 1e-9),
            "limit": lim,
        }
    sector_to_codes = defaultdict(list)
    for code, sec in sector_map.items():
        if sec != "其它":
            sector_to_codes[sec].append(code)
    return S, sector_to_codes


def trapped_at(S, code, date_idx, date_list, T, lookback=60):
    """复刻 harness._trapped_ratio: 近60日套牢盘比例(以入场日收盘 cp 为基准)。"""
    ii = date_idx.get(code, {}).get(T)
    if ii is None:
        return np.nan
    dl = date_list[code]
    lo = max(0, ii - lookback + 1)
    window = dl[lo:ii + 1]
    s = S[code]
    cp = getv(s["close"], T)
    if not pd.notna(cp):
        return np.nan
    trapped = 0.0
    total = 0.0
    for d in window:
        h = getv(s["high"], d); l = getv(s["low"], d); v = getv(s["vol"], d)
        if not (pd.notna(h) and pd.notna(l) and pd.notna(v) and v > 0):
            continue
        if h <= cp:
            pass  # 当日全区间低于 cp, 全程盈利, 不计套牢
        elif l >= cp:
            trapped += v  # 当日全区间高于 cp, 全部套牢
        else:
            trapped += v * (h - cp) / (h - l) if h > l else v
        total += v
    return trapped / total if total > 0 else 1.0


# ---------------- 板块涨停家数(预建) ----------------
def build_sector_lu(sector_to_codes, S, calendar):
    """返回 date -> {sector: 当日涨停家数} (仅热门板块常用, 但全算以防重测A/B/D)。"""
    out = {}
    # 预建每个 code 的 lu 序列已含在 S; 这里按日聚合
    # 为效率, 先建 code->sectors 反查
    code_sectors = {}
    for sec, codes in sector_to_codes.items():
        for c in codes:
            if c in S:
                code_sectors.setdefault(c, []).append(sec)
    for t in calendar:
        d = {}
        for c, secs in code_sectors.items():
            if islu(getv(S[c]["lu"], t)):
                for sec in secs:
                    d[sec] = d.get(sec, 0) + 1
        out[t] = d
    return out


# ---------------- 选股 ----------------
def pick_tailspike(T, N, ctx, S, hot_codes, sector_lu=None, lu_min=0, intraday_min=0.05):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); cl = getv(s["close"], T); hi = getv(s["high"], T)
        opv = getv(s["open_to_prev"], T); lp = getv(s["limit_price"], T)
        lu = getv(s["lu"], T); intraday = getv(s["intraday"], T); lim = s["limit"]
        if not (pd.notna(ret) and pd.notna(cl) and pd.notna(hi) and pd.notna(lp)):
            continue
        cond = (ret > intraday_min) and (cl >= hi * (1 - 0.01)) and (opv < lim - 0.03) \
               and (hi >= lp * (1 - 0.005)) and (not islu(lu))
        if not cond:
            continue
        # 板块涨停家数过滤
        if lu_min > 0 and sector_lu is not None:
            cnt = sector_lu.get(T, {}).get(ctx["sector_of"].get(code, ""), 0)
            if cnt < lu_min:
                continue
        out.append((intraday if pd.notna(intraday) else ret, code))
    out.sort(reverse=True)
    return [c for _, c in out[:N]]


def pick_with_filter(sel_name):
    """返回带双重过滤(大盘多头+板块主升)的选股wrapper用于 A/B/D。"""
    base_fns = {
        "A炸板低吸": None, "B回踩均线": None, "D断板反包": None,
    }
    return base_fns


# A/B/D 原选股(从 harness_scenarios 复制逻辑, 加重过滤)
def pick_break_open(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); hi = getv(s["high"], T); cl = getv(s["close"], T)
        lp = getv(s["limit_price"], T); opv = getv(s["open_to_prev"], T)
        lu = getv(s["lu"], T); lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T); lu3 = getv(s["lu3"], T)
        m5 = getv(s["mom5"], T); prev = getv(s["prev"], T); lo = getv(s["low"], T)
        if not pd.notna(ret) or not pd.notna(hi) or not pd.notna(lp):
            continue
        broke = (hi >= lp * (1 - 0.005)) and (not islu(lu)) and (ret > 0)
        amp = (hi - lo) / prev if pd.notna(prev) and prev > 0 else 0
        lowopen = (opv <= -0.01) and (ret > -0.02) and (amp < 0.09)
        if not (broke or lowopen):
            continue
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
        lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T); lu3 = getv(s["lu3"], T); m5 = getv(s["mom5"], T)
        if not (pd.notna(cl) and pd.notna(ma5v) and pd.notna(ma10v)):
            continue
        d5 = abs(cl / ma5v - 1); d10 = abs(cl / ma10v - 1)
        if not ((d5 < 0.035) or (d10 < 0.05)):
            continue
        if not (pd.notna(vol) and pd.notna(vma5v) and vma5v > 0 and vol < vma5v * 0.85):
            continue
        if not (islu(lu1) or islu(lu2) or islu(lu3) or (pd.notna(m5) and m5 > 0.15)):
            continue
        out.append((min(d5, d10), code))
    out.sort()
    return [c for _, c in out[:N]]


def pick_breakrev(T, N, ctx, S, hot_codes):
    cands = hot_codes.get(T, [])
    out = []
    for code in cands:
        s = S[code]
        ret = getv(s["ret"], T); vol = getv(s["vol"], T); vma5v = getv(s["vma5"], T)
        lu = getv(s["lu"], T); lu1 = getv(s["lu1"], T); lu2 = getv(s["lu2"], T); lpos = getv(s["lower_pos"], T)
        if not (pd.notna(ret) and pd.notna(vol) and pd.notna(vma5v) and pd.notna(lpos)):
            continue
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
        out.append((vol / vma5v, code))
    out.sort()
    return [c for _, c in out[:N]]


# ---------------- 自适应退出参数(复刻 adaptive_exit.get_adaptive_params) ----------------
def adaptive_params(atr_pct, rsi, bias, mkt_cond):
    stop_loss = 0.05; take_profit = 0.10; bias_limit = 0.20; rsi_exit = 80.0
    if atr_pct > 3.0:
        stop_loss, take_profit = 0.08, 0.08
    elif atr_pct > 2.0:
        stop_loss, take_profit = 0.06, 0.09
    elif atr_pct < 0.8:
        stop_loss, take_profit = 0.03, 0.06
    if rsi > 80:
        take_profit = min(take_profit, 0.06); rsi_exit = 82
    elif rsi > 70:
        take_profit = min(take_profit, 0.08); rsi_exit = 80
    elif rsi < 30:
        stop_loss = max(stop_loss, 0.07)
    if bias > 0.15:
        bias_limit = 0.18; take_profit = min(take_profit, 0.07)
    elif bias < -0.10:
        stop_loss = max(stop_loss, 0.07)
    if mkt_cond == "bullish":
        take_profit *= 1.3; stop_loss *= 1.1
    elif mkt_cond == "bearish":
        take_profit *= 0.7; stop_loss *= 0.8
    elif mkt_cond == "volatile":
        stop_loss = max(stop_loss, 0.06); take_profit = min(take_profit, 0.08)
    return stop_loss, take_profit, bias_limit, rsi_exit


# ---------------- 账户级模拟(支持多种退出) ----------------
def simulate(sel_fn, N, hold, exit_mode, gate, mkt_cond, calendar, price_lookup,
             date_idx, date_list, S, sector_map, hot_codes, reb_dates, sector_lu,
             lu_min=0, trapped_filter=False, gap_thresh=0.01, dual_filter=False):
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [], [INIT_CAPITAL], [], 0, 0, 0
    lo = min(signal_set)
    subs = [{"cash": INIT_CAPITAL / N, "pos": None} for _ in range(N)]
    equity = []; trades = []; ops = []; op_seq = 0
    sdt = 0; sdo = 0; sealed_skips = 0; total_buys = 0

    def nav():
        eq = 0.0
        for s in subs:
            if s["pos"] is not None:
                last = s["pos"].get("last_close")
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
            day_in = i - pos["entry_i"]            # 1-based 持仓第几天
            exit_fill = None; reason = None
            # 隔天高开即走(仅持仓第1天T+1检查)
            if exit_mode in ("gap", "gap_adp") and day_in == 1:
                if o >= pos["entry_fill"] * (1 + gap_thresh):
                    exit_fill = o * (1 - SLIP); reason = "隔天高开止盈"
            # 自适应退出(逐日)
            if exit_mode in ("adp", "gap_adp") and reason is None:
                sc = S[pos["code"]]
                atr = getv(sc["atr"], t); rsi = getv(sc["rsi14"], t)
                ma20v = getv(sc["ma20"], t); ma5v = getv(sc["ma5"], t)
                cp = getv(sc["close"], t)
                atr_pct = (atr / cp * 100) if (pd.notna(atr) and pd.notna(cp) and cp > 0) else 1.5
                rsi = rsi if pd.notna(rsi) else 50.0
                bias = (cp / ma20v - 1) if (pd.notna(ma20v) and ma20v > 0) else 0.0
                mc = mkt_cond.get(t, "normal")
                slp, tpp, blim, rsex = adaptive_params(atr_pct, rsi, bias, mc)
                # 浮盈护利
                pnl = (cp / pos["entry_fill"] - 1) if pos["entry_fill"] else 0
                if pnl > 0.05:
                    slp = max(0.0, -pnl + 0.02)
                profit_price = pos["entry_fill"] * (1 + tpp)
                stop_price = pos["entry_fill"] * (1 - slp)
                if h >= profit_price:
                    exit_fill = profit_price * (1 - SLIP); reason = f"自适应止盈{tpp:.0%}"
                elif low <= stop_price:
                    exit_fill = stop_price * (1 - SLIP); reason = f"自适应止损{slp:.0%}"
                elif rsi >= rsex:
                    exit_fill = close * (1 - SLIP); reason = f"RSI超买{rsi:.0f}"
                elif bias >= blim:
                    exit_fill = close * (1 - SLIP); reason = f"乖离{bias:.0%}"
                elif pd.notna(ma5v) and cp < ma5v:
                    exit_fill = close * (1 - SLIP); reason = "跌破MA5"
            # 到期
            if reason is None and day_in >= hold:
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
            # 双重过滤(大盘多头): 跌破MA60直接不建仓(与MA60闸口同源, 此处 extra)
            if not allow:
                sdo += 1
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "空仓等待", "槽位": "-",
                    "代码": "", "行业": "", "成交价": "", "股数": "",
                    "成交额": "", "滑动成本": "", "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[0]["cash"], 2), "账户净值": round(eq, 2),
                    "大盘MA60": "跌破", "备注": "MA60空头, 本周期不建仓",
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
                # 涨停封板防御
                if islu(getv(S[code]["lu"], t)):
                    sealed_skips += 1; total_buys += 1
                    op_seq += 1
                    ops.append({
                        "序号": op_seq, "日期": dstr, "动作": "涨停跳过", "槽位": k + 1,
                        "代码": code, "行业": sector_map.get(code, "其它"),
                        "成交价": round(pl[t][3], 3), "股数": 0,
                        "成交额": 0, "滑动成本": 0, "实现收益%": "", "实现盈亏元": "",
                        "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                        "大盘MA60": "站上", "备注": "买入日涨停封板, 14:45买不进",
                    })
                    continue
                # 套牢盘过滤器
                if trapped_filter:
                    tr = trapped_at(S, code, date_idx, date_list, t)
                    if pd.notna(tr) and tr > MAX_TRAPPED:
                        total_buys += 1
                        op_seq += 1
                        ops.append({
                            "序号": op_seq, "日期": dstr, "动作": "套牢盘跳过", "槽位": k + 1,
                            "代码": code, "行业": sector_map.get(code, "其它"),
                            "成交价": round(pl[t][3], 3), "股数": 0,
                            "成交额": 0, "滑动成本": 0, "实现收益%": "", "实现盈亏元": "",
                            "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                            "大盘MA60": "站上", "备注": f"套牢盘{tr:.0%}>10%, 过滤",
                        })
                        continue
                di = date_idx.get(code, {}); dl = date_list.get(code, [])
                ii = di.get(t)
                exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                # 自适应模式下若提前触发, 实际持有可能<hold, 但用 hold 作为最大窗口即可
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
                    "大盘MA60": "站上", "备注": f"持有至{str(exit_date)[:10]}/{exit_mode}",
                })
    return ops, equity, trades, sdt, sdo, sealed_skips, total_buys


def main():
    print("[1/5] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]} 股票={len(data_cache)}", flush=True)

    print("[2/5] 行业热度/价格/MA60/预建序列/板块涨停家数 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    gate, nv, ma60 = build_ma60_gate(data_cache, calendar)
    S, sector_to_codes = build_ctx(data_cache, sector_map, calendar)
    sector_lu = build_sector_lu(sector_to_codes, S, calendar)
    # code -> sector 反查(给板块涨停过滤用)
    code_sector = {}
    for sec, codes in sector_to_codes.items():
        for c in codes:
            code_sector[c] = sec
    # 热门行业内候选
    hot_codes = {}
    for t in calendar:
        hs = hot_by_date.get(t, set())
        codes = []
        for sec in hs:
            codes.extend(c for c in sector_to_codes.get(sec, []) if c in S)
        hot_codes[t] = codes
    ctx = {"sector_of": code_sector}

    # 大盘环境(强/弱/正常)供自适应参数
    mom5 = nv.pct_change(5)
    mkt_cond = {}
    for t in calendar:
        if t not in nv.index:
            mkt_cond[t] = "normal"; continue
        ab = getv(nv, t) > getv(ma60, t) if pd.notna(getv(ma60, t)) else True
        rising = getv(mom5, t) > 0 if pd.notna(getv(mom5, t)) else False
        if not pd.notna(getv(ma60, t)):
            mkt_cond[t] = "normal"
        elif ab and rising:
            mkt_cond[t] = "bullish"
        elif not ab:
            mkt_cond[t] = "bearish"
        else:
            mkt_cond[t] = "normal"

    reb_all = slice_test_dates(calendar, 3, 0)[::3]
    reb_gate = [d for d in reb_all if gate.get(pd.Timestamp(d), True)]
    print(f"      信号日: 始终在场={len(reb_all)} MA60开门={len(reb_gate)}", flush=True)

    out = {"meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                    "N": N, "init": INIT_CAPITAL,
                    "生产动量基准": "+32.4%(hold5) / +33.98%(hold3) 胜率~52% 夏普~1.3 回撤~12%"},
            "C_variants": {}, "ABnD_filtered": {}, "strong_weak": {}}

    # ---------- (1)(2) C 增强网格 ----------
    print("[3/5] C 增强网格(退出模式×持有×过滤) ...", flush=True)
    for exit_mode in ["hold", "gap", "adp", "gap_adp"]:
        for hold in [1, 2]:
            for lu_min, trapped in [(0, False), (2, True)]:
                key = f"{exit_mode}_h{hold}_lu{lu_min}_trap{trapped}"
                ops, eq, tr, sdt, sdo, skips, buys = simulate(
                    lambda T, N, c, S, hc: pick_tailspike(T, N, ctx, S, hc, sector_lu, lu_min),
                    N, hold, exit_mode, gate, mkt_cond, calendar, price_lookup,
                    date_idx, date_list, S, sector_map, hot_codes, reb_gate, sector_lu,
                    lu_min=lu_min, trapped_filter=trapped)
                m = metrics_from_equity(eq)
                m["交易笔数"] = len(tr); m["信号日"] = sdt; m["空仓"] = sdo
                m["涨停跳过"] = skips; m["封板率%"] = round(skips / buys * 100, 1) if buys else 0.0
                m["胜率%"] = round(sum(1 for r in tr if r > 0) / len(tr) * 100, 1) if tr else 0.0
                out["C_variants"][key] = m
                print(f"      C {key:28s} 收益{m['总收益%']:+.1f}% 夏普{m['夏普']} 回撤{m['最大回撤%']}% "
                      f"胜{m.get('胜率%','-')} 封板{m['封板率%']}%", flush=True)
                if exit_mode == "gap" and hold == 2 and lu_min == 2:
                    # 增强(过滤)版逐笔
                    df = pd.DataFrame(ops)
                    df.to_csv(HERE / "c_enh_Cbest_ma60.csv", index=False, encoding="utf-8-sig")
                if exit_mode == "gap" and hold == 2 and lu_min == 0:
                    # 最高收益版(无过滤)逐笔
                    df = pd.DataFrame(ops)
                    df.to_csv(HERE / "c_enh_Ctop_ma60.csv", index=False, encoding="utf-8-sig")

    # ---------- (3) A/B/D 双重过滤重测 ----------
    print("[4/5] A/B/D 双重过滤(大盘多头+板块主升)重测 ...", flush=True)
    abnd = {"A炸板低吸": pick_break_open, "B回踩均线": pick_pullback, "D断板反包": pick_breakrev}
    for name, fn in abnd.items():
        out["ABnD_filtered"][name] = {}
        for hold in [2, 3]:
            ops, eq, tr, sdt, sdo, skips, buys = simulate(
                fn, N, hold, "adp", gate, mkt_cond, calendar, price_lookup,
                date_idx, date_list, S, sector_map, hot_codes, reb_gate, sector_lu,
                lu_min=0, trapped_filter=True)
            m = metrics_from_equity(eq)
            m["交易笔数"] = len(tr); m["信号日"] = sdt; m["空仓"] = sdo
            m["涨停跳过"] = skips; m["封板率%"] = round(skips / buys * 100, 1) if buys else 0.0
            out["ABnD_filtered"][name][f"持{hold}天"] = m
            print(f"      {name} 持{hold} 收益{m['总收益%']:+.1f}% 夏普{m['夏普']} 回撤{m['最大回撤%']}%", flush=True)

    # ---------- (4) 强/弱市分段(关MA60闸口, 纯看 C 在牛/熊/震荡的内在稳健性) ----------
    print("[5/5] 强/弱市分段(C: 增强过滤版 / 最高收益版, 关闸口看内在稳健性) ...", flush=True)
    sw_configs = {
        "增强(过滤) gap_h2_lu2_trapTrue": (2, True, "gap"),
        "最高收益 gap_h2_lu0_trapFalse": (0, False, "gap"),
    }
    for vlabel, (lu_min, trapped, exit_mode) in sw_configs.items():
        out["strong_weak"][vlabel] = {}
        for label, cond in [("强市bullish", "bullish"), ("弱市bearish", "bearish"), ("震荡normal", "normal")]:
            dates = [d for d in reb_all if mkt_cond.get(pd.Timestamp(d), "normal") == cond]
            if not dates:
                out["strong_weak"][vlabel][label] = {"note": "无样本"}
                continue
            ops, eq, tr, sdt, sdo, skips, buys = simulate(
                lambda T, N, c, S, hc: pick_tailspike(T, N, ctx, S, hc, sector_lu, lu_min),
                N, 2, exit_mode, None, mkt_cond, calendar, price_lookup,
                date_idx, date_list, S, sector_map, hot_codes, dates, sector_lu,
                lu_min=lu_min, trapped_filter=trapped)
            m = metrics_from_equity(eq)
            m["交易笔数"] = len(tr); m["信号日"] = sdt
            m["胜率%"] = round(sum(1 for r in tr if r > 0) / len(tr) * 100, 1) if tr else 0.0
            m["封板率%"] = round(skips / buys * 100, 1) if buys else 0.0
            out["strong_weak"][vlabel][label] = m
            print(f"      {vlabel} {label} 信号日={sdt} 收益{m['总收益%']:+.1f}% 夏普{m['夏普']} "
                  f"回撤{m['最大回撤%']}% 胜{m['胜率%']}%", flush=True)

    (HERE / "c_enhanced_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_enhanced_results.json / c_enh_Cbest_ma60.csv")


if __name__ == "__main__":
    main()
