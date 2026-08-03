"""价量口诀策略·买点研究: 对比"机械次日买" vs "回踩支撑/均线低吸"。

背景: 原口诀核心是"盘中回踩支撑位/均线附近低吸", 而非追突破当天或盲买次日开盘。
本模块用日K(OHLC)近似几种买点, 验证"等回踩支撑买入"是否提升胜率/收益。

买点模式(entry modes):
  open        : T+1 开盘价买入(机械)
  close       : T+1 收盘价买入(机械)
  low         : T+1 最低价买入(理想化: 盘中捕捉到低点)
  dip_buf     : 仅当 T+1 最低价触及支撑位*(1+buf) 才买, 买入价=min(最低价, 支撑位)
                (真实"回踩支撑低吸": 不回踩就不买, 不追高)
  dip_openlow : 买入价=(T+1开盘+最低)/2 的盘中低吸近似(始终买, 但取偏低价)

支撑位定义(support):
  breakout(突破放量): 信号日之前 N 日 实体最高价 PRE_HHV (= 被突破的平台高点)
  pullback(缩量回踩): T+1 的 MA20 (回踩的均线支撑)

退出: 固定持有 hold 日 或 盘中触及 止损 stop (同 harness 口径, 无前视泄漏)。
"""
import os, sys
import importlib.util
import numpy as np
import pandas as pd

HERE = "opt_study"
ROOT = "."
sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location("h", HERE + "/harness_oversold_quality.py")
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)
H.DB = os.path.join(ROOT, "qlib_pro_v16.db")
H.ROOT = ROOT
H.WINDOW_START = "2024-07-01"
H.WINDOW_END = "2026-07-22"

import opt_study.volume_price_strategy as VS


def _support(g, sig_idx, exec_idx, kind, N=60):
    """返回支撑位 S (信号日 sig_idx, 执行日 exec_idx)。"""
    if kind == "breakout":
        pre = g.iloc[max(0, sig_idx - N):sig_idx]
        if pre.empty:
            return float(g.iloc[sig_idx]["close"])
        return float(max(max(c, o) for c, o in zip(pre["close"], pre["open"])))
    else:  # ma20
        ma = g["close"].rolling(20).mean()
        v = ma.iloc[exec_idx]
        return float(v) if not np.isnan(v) else float(g.iloc[exec_idx]["close"])


def simulate_custom(ctx, cal, inv, hold, stop, mode, support_kind, buf=0.0):
    """自定义 slot 回测, 支持多种买点。返回 (trades, equity_list)。"""
    N_SLOTS = H.N_SLOTS
    INIT = H.INIT_CAPITAL
    SLIP = H.SLIP
    slots = [None] * N_SLOTS
    capital = INIT
    eq = []
    last_exit = {}
    all_trades = []  # 平仓即记录, 避免最后收集时持仓已清空
    cal_set = set(str(t)[:10] for t in cal)

    def openmv():
        mv = 0.0
        for s in slots:
            if not s:
                continue
            g = ctx[s["code"]]
            px = g.loc[s["exit_t"], "close"] if s["exit_t"] in g.index else s["entry"]
            mv += px * s["shares"]
        return mv

    def record_exit(pos):
        all_trades.append(dict(code=pos["code"], entry=pos["entry"],
                               exit_px=pos["exit_px"],
                               ret=pos["exit_px"] / pos["entry"] - 1,
                               reason=pos["reason"]))

    for ti, t in enumerate(cal):
        ts = str(t)[:10]
        # 1) 处理当日到期平仓
        for si in range(N_SLOTS):
            pos = slots[si]
            if pos and pos["exit_t"] == t:
                capital += pos["shares"] * pos["exit_px"]
                record_exit(pos)
                last_exit[pos["code"]] = ti
                slots[si] = None
        # 2) 新信号建仓
        for code in inv.get(ts, []):
            if any(p and p["code"] == code for p in slots):
                continue
            if ti + 1 >= len(cal):
                continue
            t1 = cal[ti + 1]
            g = ctx[code]
            if t1 not in g.index:
                continue
            i1 = g.index.get_loc(t1)
            sig_idx = g.index.get_loc(t) if t in g.index else max(0, i1 - 1)
            O1 = float(g.loc[t1, "open"]); H1 = float(g.loc[t1, "high"])
            L1 = float(g.loc[t1, "low"]); C1 = float(g.loc[t1, "close"])
            S = _support(g, sig_idx, i1, support_kind)
            # 决定买点
            take = True
            if mode == "open":
                entry = O1
            elif mode == "close":
                entry = C1
            elif mode == "low":
                entry = L1
            elif mode == "dip_openlow":
                entry = (O1 + L1) / 2.0
            elif mode == "dip_buf":
                # 仅当盘中最低价触及支撑(1+buf)内才低吸
                if L1 <= S * (1 + buf):
                    entry = min(L1, S)
                else:
                    take = False
            else:
                take = False
            if not take:
                continue
            entry *= (1 + SLIP)
            # 计算退出(持有 hold 日 or 止损)
            exit_px = None; exit_t = None; reason = "持有到期"
            for k in range(1, hold + 1):
                if i1 + k >= len(g.index):
                    break
                tk = g.index[i1 + k]
                lk = float(g.loc[tk, "low"])
                if lk <= entry * (1 + stop):
                    exit_px = lk; exit_t = tk; reason = "止损"; break
            if exit_px is None:
                tend = g.index[min(i1 + hold, len(g.index) - 1)]
                exit_px = float(g.loc[tend, "close"]); exit_t = tend
            free = next((k for k in range(N_SLOTS) if slots[k] is None), None)
            if free is None:
                break
            free_slots = sum(1 for s in slots if s is None)
            shares = int((capital / free_slots) / entry / 100) * 100
            if shares <= 0:
                continue
            capital -= shares * entry
            slots[free] = dict(code=code, entry=entry, shares=shares,
                               exit_t=exit_t, exit_px=exit_px, reason=reason)
        eq.append(capital + openmv())
    # 最后仍有未平仓的(极少), 以末日收盘强平记录
    for si in range(N_SLOTS):
        pos = slots[si]
        if pos:
            g = ctx[pos["code"]]
            last = g.index[-1]
            pos["exit_px"] = float(g.loc[last, "close"])
            pos["exit_t"] = last
            pos["reason"] = "末日强平"
            record_exit(pos)
    trades = all_trades
    return trades, eq


def summarize(trades, eq):
    if not trades:
        return dict(n=0, winrate=0, avg_ret=0, total_ret=0, sharpe=0, maxdd=0)
    rets = [t["ret"] for t in trades]
    wins = [r for r in rets if r > 0]
    eqa = np.array(eq)
    total = eqa[-1] / eqa[0] - 1 if len(eqa) > 1 else 0
    daily = pd.Series(eqa).pct_change().dropna()
    sharpe = daily.mean() / daily.std() * np.sqrt(252) if daily.std() > 0 else 0
    peak = np.maximum.accumulate(eqa); dd = (eqa - peak) / peak
    return dict(n=len(trades), winrate=round(100 * len(wins) / len(trades), 1),
                avg_ret=round(100 * np.mean(rets), 2),
                total_ret=round(100 * total, 2),
                sharpe=round(float(sharpe), 3), maxdd=round(100 * dd.min(), 2))


def main():
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    fmap = H.load_fundamentals()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    inv = VS.build_inv(ctx, cal, {}, hot_at, regime, min_history=120,
                       use_theme_resonance=True, bull_only=True)
    print("信号日数:", {k: len(v) for k, v in inv.items()})

    modes = ["open", "close", "low", "dip_openlow", "dip_buf"]
    bufs = [0.0, 0.01, 0.02]
    holds = [10, 15, 20]
    stops = [-0.05, -0.08, -0.10]

    for key, sk in (("breakout", "breakout"), ("pullback", "ma20")):
        print(f"\n{'='*70}\n  信号类型: {key}  (支撑={sk})\n{'='*70}")
        best = None
        for hold in holds:
            for stop in stops:
                # 机械买点
                for mode in ("open", "close", "low", "dip_openlow"):
                    tr, eq = simulate_custom(ctx, cal, inv[key], hold, stop, mode, sk)
                    s = summarize(tr, eq)
                    tag = f"{mode:11s}"
                    print(f"  hold={hold} stop={stop:>5} {tag}: n={s['n']:>3} 胜率={s['winrate']:>5}% 收益={s['total_ret']:>7}% 夏普={s['sharpe']:>5} 回撤={s['maxdd']:>6}%")
                # 回踩支撑买点(不同 buf)
                for buf in bufs:
                    tr, eq = simulate_custom(ctx, cal, inv[key], hold, stop, "dip_buf", sk, buf)
                    s = summarize(tr, eq)
                    tag = f"dip_buf{buf:+.0%}"
                    print(f"  hold={hold} stop={stop:>5} {tag:11s}: n={s['n']:>3} 胜率={s['winrate']:>5}% 收益={s['total_ret']:>7}% 夏普={s['sharpe']:>5} 回撤={s['maxdd']:>6}%  (仅回踩才买)")
                if best is None or s["total_ret"] > best["total_ret"]:
                    best = dict(key=key, hold=hold, stop=stop, mode="dip_buf", buf=buf, **s)
        print(f"  >>> {key} 最优(回踩支撑口径):", best)


if __name__ == "__main__":
    main()
