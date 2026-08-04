# -*- coding: utf-8 -*-
"""价量口诀·卖点研究: 把回踩支撑策略的"卖点"锁定在压力位附近, 看是否提高收益。

背景: 已验证回踩低吸买点(dip_buf)翻正 —— 缩量回踩 hold20/止损-5% → 胜率44%/+47.3%。
但"固定持有N日"是钝化退出: 既可能过早卖飞, 也可能在压力位前磨蹭。
本脚本把卖点改为"价格触及压力位(前N日最高价)附近即卖出(取当日收盘)", 与固定持有基线对比。

压力位定义(pressure):
  pullback(缩量回踩): 信号日前 N 日最高价(回踩前那波反弹的高点 = 本轮反弹的天然压力)
  breakout(突破放量): 信号日前 N 日最高价 * 1.10(突破后的度量目标位)

退出优先级: 止损(stop) > 压力位卖出 > 持有到期(未达压力, 到 cap 强平)

固定买点: dip_buf buf=0.02 (与已验证口径一致), 仅变卖点, 隔离卖点效应。
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


def _pressure(g, sig_idx, kind='ma20', N=60):
    """压力位: 回踩前 N 日最高价(天然阻力); 突破则取 *1.10 度量目标。"""
    pre = g.iloc[max(0, sig_idx - N):sig_idx + 1]
    if pre.empty:
        return float(g.iloc[sig_idx]['close'])
    base = float(pre['high'].max())
    return base * 1.10 if kind == 'breakout' else base


def simulate_custom(ctx, cal, inv, hold, stop, mode, support_kind,
                    buf=0.0, exit_mode='hold', sell_buf=0.0, cap=None, pres_n=60):
    """自定义 slot 回测, 支持多种买点 + 两种卖点(hold / pressure)。

    exit_mode='hold'    : 固定持有 hold 日(基线)。
    exit_mode='pressure': 价格触及压力位*(1-sell_buf)即卖出(取当日收盘), 止损优先,
                          cap 日内未达则持有到期强平。
    返回 (trades, eq, reason_counts)。
    """
    N_SLOTS = H.N_SLOTS
    INIT = H.INIT_CAPITAL
    SLIP = H.SLIP
    slots = [None] * N_SLOTS
    capital = INIT
    eq = []
    last_exit = {}
    all_trades = []
    reason_counts = {}
    cap = cap or hold
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
        reason_counts[pos["reason"]] = reason_counts.get(pos["reason"], 0) + 1

    for ti, t in enumerate(cal):
        ts = str(t)[:10]
        for si in range(N_SLOTS):
            pos = slots[si]
            if pos and pos["exit_t"] == t:
                capital += pos["shares"] * pos["exit_px"]
                record_exit(pos)
                last_exit[pos["code"]] = ti
                slots[si] = None
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
            # 支撑位(买点用)
            if support_kind == "breakout":
                pre = g.iloc[max(0, sig_idx - 60):sig_idx]
                S = float(max(max(c, o) for c, o in zip(pre["close"], pre["open"]))) if not pre.empty else C1
            else:
                ma = g["close"].rolling(20).mean()
                S = float(ma.iloc[i1]) if not np.isnan(ma.iloc[i1]) else C1
            # 压力位(卖点用)
            P = _pressure(g, sig_idx, kind=support_kind, N=pres_n)
            take = True
            if mode == "dip_buf":
                if L1 <= S * (1 + buf):
                    entry = min(L1, S)
                else:
                    take = False
            else:
                take = False
            if not take:
                continue
            entry *= (1 + SLIP)
            exit_px = None; exit_t = None; reason = "持有到期"
            for k in range(1, cap + 1):
                if i1 + k >= len(g.index):
                    break
                tk = g.index[i1 + k]
                hk = float(g.loc[tk, "high"]); lk = float(g.loc[tk, "low"])
                if lk <= entry * (1 + stop):
                    exit_px = lk; exit_t = tk; reason = "止损"; break
                if (exit_mode == 'pressure' and P > entry * 1.005
                        and hk >= P * (1 - sell_buf)):
                    exit_px = float(g.loc[tk, "close"]); exit_t = tk
                    reason = "压力位卖出"; break
            if exit_px is None:
                tend = g.index[min(i1 + cap, len(g.index) - 1)]
                exit_px = float(g.loc[tend, "close"]); exit_t = tend
                reason = "持有到期" if exit_mode == 'hold' else "未达压力到期"
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
    for si in range(N_SLOTS):
        pos = slots[si]
        if pos:
            g = ctx[pos["code"]]
            last = g.index[-1]
            pos["exit_px"] = float(g.loc[last, "close"])
            pos["exit_t"] = last
            pos["reason"] = "末日强平"
            record_exit(pos)
    return all_trades, eq, reason_counts


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
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    inv = VS.build_inv(ctx, cal, {}, hot_at, regime, min_history=120,
                       use_theme_resonance=True, bull_only=True)
    print("信号日数:", {k: len(v) for k, v in inv.items()})

    # 固定买点: 缩量回踩 dip_buf buf=0.02, 止损-5% (与已验证口径一致)
    kind = "ma20"          # 缩量回踩
    key = "pullback"
    buf = 0.02
    stop = -0.05
    base_hold = 20

    print(f"\n{'='*78}\n  回踩支撑策略卖点研究 (买点固定 dip_buf buf={buf}, 止损{stop})\n{'='*78}")

    # 基线: 固定持有
    tr0, eq0, rc0 = simulate_custom(ctx, cal, inv[key], base_hold, stop,
                                    "dip_buf", kind, buf=buf, exit_mode='hold')
    s0 = summarize(tr0, eq0)
    print(f"\n[基线] 固定持有 {base_hold} 日:")
    print(f"  n={s0['n']} 胜率={s0['winrate']}% 收益={s0['total_ret']}% 夏普={s0['sharpe']} 回撤={s0['maxdd']}%")
    print(f"  退出分布: {rc0}")

    # 压力位卖出: 扫 sell_buf × cap
    rows = []
    print(f"\n[压力位卖出] 扫 sell_buf × 持有上限cap:")
    for sell_buf in (0.0, 0.01, 0.02, 0.03):
        for cap in (20, 30, 45):
            tr, eq, rc = simulate_custom(ctx, cal, inv[key], base_hold, stop,
                                         "dip_buf", kind, buf=buf,
                                         exit_mode='pressure', sell_buf=sell_buf, cap=cap)
            s = summarize(tr, eq)
            rows.append(dict(sell_buf=sell_buf, cap=cap, **s, reasons=rc))
            print(f"  sell_buf={sell_buf:>5} cap={cap:>2}: n={s['n']:>3} 胜率={s['winrate']:>5}% "
                  f"收益={s['total_ret']:>7}% 夏普={s['sharpe']:>5} 回撤={s['maxdd']:>6}%  分布={rc}")
            sys.stdout.flush()

    # 找最优压力位配置
    best = max(rows, key=lambda r: r['total_ret'])
    print(f"\n>>> 压力位最优(按收益): sell_buf={best['sell_buf']} cap={best['cap']} -> 收益{best['total_ret']}% (基线 {s0['total_ret']}%)")
    delta = best['total_ret'] - s0['total_ret']
    print(f">>> 收益提升: {delta:+.2f}%  | 胜率 {s0['winrate']}% -> {best['winrate']}%")

    # 输出 JSON
    out = dict(baseline=dict(hold=base_hold, stop=stop, **s0, reasons=rc0),
               pressure_best=best,
               pressure_grid=rows,
               conclusion=("压力位卖出提高收益" if delta > 0 else "压力位卖出未提高收益"))
    import json
    with open("opt_study/volume_price_exit_result.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n已写 opt_study/volume_price_exit_result.json")


if __name__ == "__main__":
    main()
