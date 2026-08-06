# -*- coding: utf-8 -*-
"""
价量口诀策略 · 选股参数网格搜索(零泄漏 in-sample -> OOS)
========================================================
目标: 把"突破放量 / 缩量回踩"两套信号的选股旋钮, 逐个挖出 OOS 最优值。

方法(严格防前视):
  1) 买卖点 EXIT 逻辑全部 FIXED 为已验证口径(隔离"选股"效应, 不改已验证的进出场):
       - 缩量回踩(pullback): 买=dip_buf(buf=0.02), 止损-5%, 卖=压力位(前60日高, sell_buf=0.02, cap=20)
       - 突破放量(breakout): 买=dip_buf(buf=0.02), 止损-8%, 卖=压力位(前高*1.10, sell_buf=0.02, cap=10)
     这些 EXIT 参数来自 volume_price_exit_study / forward_validation 已发表口径, 视为先验常数。
  2) 仅对 SELECTION 旋钮做网格(OFAT: 一次动一个旋钮, 其余固定在 BASE 已发布默认值)。
  3) 时间切分: IN_SAMPLE_END = 2026-01-31(与 forward_validation 一致)。
       - in-sample 选参: 每个旋钮各档在 ≤2026-01-31 上比 total_ret, 取最优档。
       - 组装 joint-best(各旋钮 in-sample 最优)后, 在 OOS(>2026-01-31) 严格零泄漏验证(真正泛化测试)。
       - 另报每个旋钮在 OOS 上的最优档(敏感度确认), 明确标注为 OOS 敏感度而非主结论。
  4) 信号索引 inv 用与 volume_price_strategy.build_inv 完全一致的判定重写(参数化), 复用
     simulate_custom 做组合层回测(含仓位/滑点)。

旋钮定义见 CAND 字典。BASE = 当前生产默认值。
"""
import os, sys, json, datetime as _dt
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
os.chdir(ROOT)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import numpy as np
import pandas as pd

import volume_price_exit_study as ES          # 复用 simulate_custom / summarize
import volume_price_strategy as VS            # 复用 _is_garbage / build_regime

# ---- 数据范围: 放开到 DB 实际末端 ----
ES.H.WINDOW_START = "2024-07-01"
ES.H.WINDOW_END = "2099-12-31"
ES.H.DB = os.environ.get("QLIB_DB") or os.path.join(ROOT, "qlib_pro_v16.db")

IN_SAMPLE_END = "2026-01-31"

# ============ 旋钮网格 ============
# 突破放量 旋钮
BRK = {
    "N_BREAK":      [20, 30, 60, 120],
    "VOL_RATIO":    [1.5, 2.0, 2.5],
    "SOLID_MIN_PCT":[2.0, 3.0, 4.0],
    "PLAT":         [(0.20, 0.40), (0.25, 0.40), (0.35, 0.60)],  # (amp_thr, runup_thr)
}
# 缩量回踩 旋钮
PUL = {
    "HAD_RAISE_PCT":[3.0, 4.0, 5.0],
    "HAD_RAISE_VOL":[1.2, 1.5, 2.0],
    "NEAR":         [(0.95, 1.05), (0.98, 1.03), (0.97, 1.04)],  # (lo, hi)
    "VOL_SHRINK":   [0.5, 0.6, 0.7],
}
# 共享旋钮(两类信号都吃板块共振)
SHARED = {
    "TOP_K_THEMES": [5, 8, 12],
}

# BASE = 当前生产默认值
BASE = {
    "breakout": dict(N_BREAK=60, VOL_RATIO=2.0, SOLID_MIN_PCT=3.0, PLAT=(0.25, 0.40), TOP_K_THEMES=8),
    "pullback": dict(HAD_RAISE_PCT=4.0, HAD_RAISE_VOL=1.5, NEAR=(0.98, 1.03), VOL_SHRINK=0.6, TOP_K_THEMES=8),
}

# 固定 EXIT(已验证, 不参加网格)
EXIT = {
    "breakout": dict(support_kind="breakout", buf=0.02, stop=-0.08, exit_mode="pressure", sell_buf=0.02, cap=10, pres_n=60),
    "pullback": dict(support_kind="ma20",     buf=0.02, stop=-0.05, exit_mode="pressure", sell_buf=0.02, cap=20, pres_n=60),
}

MIN_HISTORY = 120
PLAT_MIN = 40   # 平台窗口长度固定(只网格 amplitude/runup 阈值)


# ---------- 数据加载(一次) ----------
def load_once():
    print("加载K线...", flush=True)
    ctx = ES.H.load_kline()
    ctx = ES.H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    print(f"  标的数={len(ctx)} 交易日={len(cal)} 末端={str(cal[-1])[:10]}", flush=True)
    # 板块共振: 预计算 TOP_K=5/8/12 三种
    hot_by_k = {}
    for k in SHARED["TOP_K_THEMES"]:
        ES.H.TOP_K_THEMES = k
        hot = ES.H.build_hot_themes(ctx, cal)
        # 归一为 {date: set(codes)}
        hot_by_k[k] = {str(t)[:10]: hot.get(str(t)[:10], (set(), [], {}))[0] for t in cal}
    nav = ES.H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    regime_pass = {str(t)[:10]: (regime.get(str(t)[:10], "ranging") != "bear") for t in cal}
    # 名称(用于 ST/退 剔除)
    names = {}
    f = os.path.join(ROOT, "data", "stock_names.json")
    if os.path.exists(f):
        d = json.loads(open(f, encoding="utf-8").read())
        if isinstance(d, dict):
            names = {str(k): v for k, v in d.items()}
        elif isinstance(d, list):
            for it in d:
                if isinstance(it, dict) and "code" in it:
                    names[str(it["code"])] = it.get("name", it["code"])
    return ctx, cal, hot_by_k, regime_pass, names


def precompute_base(ctx):
    """每个标的预计算与旋钮无关的基数组(一次)。返回 P[code]={c,o,h,l,v,vma5,ma20,ma60,pct,idx}。"""
    P = {}
    for code, g in ctx.items():
        if g.empty:
            continue
        c = g["close"].to_numpy(dtype=float)
        o = g["open"].to_numpy(dtype=float)
        h = g["high"].to_numpy(dtype=float)
        l = g["low"].to_numpy(dtype=float)
        v = g["volume"].to_numpy(dtype=float)
        cs = pd.Series(c)
        vs = pd.Series(v)
        vma5 = vs.rolling(5).mean().to_numpy()       # 5日均量(必须是成交量, 非收盘价!)
        ma20 = cs.rolling(20).mean().to_numpy()
        ma60 = cs.rolling(60).mean().to_numpy()
        pct = np.empty_like(c); pct[0] = np.nan
        pct[1:] = c[1:] / c[:-1] - 1.0
        P[code] = dict(c=c, o=o, h=h, l=l, v=v, vma5=vma5, ma20=ma20, ma60=ma60, pct=pct,
                       idx=g.index)
    return P


def _roll(arr, w):
    return pd.Series(arr).rolling(w).max().to_numpy()


def sig_breakout(P, p):
    c, o, v = P["c"], P["o"], P["v"]
    vma5 = P["vma5"]
    N = p["N_BREAK"]
    mco = np.maximum(c, o)
    pre_hhv = _roll(mco, N)
    pre_hhv = np.concatenate([[np.nan], pre_hhv[:-1]])  # shift(1)
    breakout = c > pre_hhv
    vol_up = v > (np.nan_to_num(vma5, nan=0.0) * p["VOL_RATIO"])
    vol_up[np.isnan(vma5)] = False
    pct = P["pct"] * 100.0
    solid = (c > o) & (pct > p["SOLID_MIN_PCT"])
    sig = breakout & vol_up & solid
    # 平台过滤(窗口 PLAT_MIN=40)
    amp_thr, runup_thr = p["PLAT"]
    h = P["h"]; l = P["l"]
    hi40 = pd.Series(h).rolling(PLAT_MIN).max().to_numpy()
    lo40 = pd.Series(l).rolling(PLAT_MIN).min().to_numpy()
    n = len(c)
    start = np.full(n, np.nan)
    if n > PLAT_MIN:
        start[PLAT_MIN:] = c[:n - PLAT_MIN]
    with np.errstate(divide="ignore", invalid="ignore"):
        amp = (hi40 - lo40) / lo40
        run_up = hi40 / start - 1.0
    plat = (amp < amp_thr) & (run_up < runup_thr)
    plat[np.isnan(amp) | np.isnan(run_up)] = False
    return sig & plat


def sig_pullback(P, p):
    c, l, v = P["c"], P["l"], P["v"]
    ma20, ma60, vma5 = P["ma20"], P["ma60"], P["vma5"]
    trend = (ma20 > ma60) & (c > ma60)
    trend[np.isnan(ma20) | np.isnan(ma60)] = False
    pct = P["pct"] * 100.0
    had = ((pct > p["HAD_RAISE_PCT"]) & (v > np.nan_to_num(vma5, nan=0.0) * p["HAD_RAISE_VOL"]))
    had[np.isnan(vma5)] = False
    had = pd.Series(had).rolling(10).max().fillna(0).to_numpy() > 0
    lo, hi = p["NEAR"]
    near = (l >= ma20 * lo) & (l <= ma20 * hi) & (c >= ma20)
    near[np.isnan(ma20)] = False
    vol_shrink = v < (np.nan_to_num(vma5, nan=0.0) * p["VOL_SHRINK"])
    vol_shrink[np.isnan(vma5)] = False
    return trend & had & near & vol_shrink


def build_inv_fast(P, codes, kind, params, hot_by_k, regime_pass, names, cal):
    """完全等价于 VS.build_inv 的判定(use_theme_resonance=True, bull_only=True), 参数化。"""
    inv = defaultdict(list)
    top_k = params["TOP_K_THEMES"]
    for code in codes:
        Pc = P.get(code)
        if Pc is None:
            continue
        sig = sig_breakout(Pc, params) if kind == "breakout" else sig_pullback(Pc, params)
        if not sig.any():
            continue
        idx = Pc["idx"]
        name = names.get(code, code)
        # 名称垃圾(静态)
        s = str(name)
        name_bad = ("ST" in s) or ("退" in s) or s.startswith("*") or ("警示" in s)
        hotset = hot_by_k[top_k]
        for i in np.nonzero(sig)[0]:
            if i < MIN_HISTORY:
                continue
            ts = str(idx[i])[:10]
            close = Pc["c"][i]
            if name_bad or (close is not None and close < 1.5):
                continue
            if not regime_pass.get(ts, True):   # 熊市放弃
                continue
            if code not in hotset.get(ts, ()):
                continue
            inv[ts].append(code)
    return dict(inv)


def eval_combo(ctx, P, codes, cal, kind, params, hot_by_k, regime_pass, names,
               in_cal, oos_cal):
    inv = build_inv_fast(P, codes, kind, params, hot_by_k, regime_pass, names, cal)
    ex = EXIT[kind]
    # 入样
    tr_i, eq_i, _ = ES.simulate_custom(ctx, in_cal, inv, ex["cap"], ex["stop"],
                                       "dip_buf", ex["support_kind"], buf=ex["buf"],
                                       exit_mode=ex["exit_mode"], sell_buf=ex["sell_buf"],
                                       cap=ex["cap"], pres_n=ex["pres_n"])
    s_i = ES.summarize(tr_i, eq_i)
    # 样本外(零泄漏: 仅用 >IN_SAMPLE_END 的交易日)
    tr_o, eq_o, _ = ES.simulate_custom(ctx, oos_cal, inv, ex["cap"], ex["stop"],
                                       "dip_buf", ex["support_kind"], buf=ex["buf"],
                                       exit_mode=ex["exit_mode"], sell_buf=ex["sell_buf"],
                                       cap=ex["cap"], pres_n=ex["pres_n"])
    s_o = ES.summarize(tr_o, eq_o)
    return s_i, s_o


def grid_to_str(p):
    return ", ".join(f"{k}={v}" for k, v in p.items())


def main():
    ctx, cal, hot_by_k, regime_pass, names = load_once()
    P = precompute_base(ctx)
    codes = list(P.keys())
    in_cal = [t for t in cal if str(t)[:10] <= IN_SAMPLE_END]
    oos_cal = [t for t in cal if str(t)[:10] > IN_SAMPLE_END]
    print(f"入样交易日={len(in_cal)} 样本外交易日={len(oos_cal)}", flush=True)

    report = {"in_sample_end": IN_SAMPLE_END, "data_end": str(cal[-1])[:10],
              "kinds": {}}

    for kind, KNOBS in (("breakout", BRK), ("pullback", PUL)):
        print(f"\n{'='*70}\n  信号类型: {kind}\n{'='*70}", flush=True)
        base = dict(BASE[kind])
        # 记录每个旋钮的 in-sample 各档表现
        knob_results = {}
        for knob, levels in KNOBS.items():
            rows = []
            for lv in levels:
                p = dict(base)
                p[knob] = lv
                p["TOP_K_THEMES"] = base["TOP_K_THEMES"]
                s_i, s_o = eval_combo(ctx, P, codes, cal, kind, p, hot_by_k, regime_pass, names, in_cal, oos_cal)
                rows.append(dict(level=lv, n_ins=s_i["n"], ins_ret=s_i["total_ret"],
                                 ins_win=s_i["winrate"], oos_n=s_o["n"], oos_ret=s_o["total_ret"],
                                 oos_win=s_o["winrate"]))
                print(f"  {knob}={lv}: in n={s_i['n']:>3} 收益={s_i['total_ret']:>7}%  "
                      f"OOS n={s_o['n']:>3} 收益={s_o['total_ret']:>7}%  胜率={s_o['winrate']}%", flush=True)
            # in-sample 最优档(要求 n>=8 才算有效, 避免噪声档)
            valid = [r for r in rows if r["n_ins"] >= 8]
            best = max(valid, key=lambda r: r["ins_ret"]) if valid else max(rows, key=lambda r: r["n_ins"])
            knob_results[knob] = dict(levels=rows, best_level=best["level"],
                                      best_ins_ret=best["ins_ret"], best_ins_n=best["n_ins"])
            base[knob] = best["level"]   # 组装 joint-best
        # 共享旋钮 TOP_K
        for knob, levels in SHARED.items():
            rows = []
            for lv in levels:
                p = dict(base); p[knob] = lv
                s_i, s_o = eval_combo(ctx, P, codes, cal, kind, p, hot_by_k, regime_pass, names, in_cal, oos_cal)
                rows.append(dict(level=lv, n_ins=s_i["n"], ins_ret=s_i["total_ret"],
                                 ins_win=s_i["winrate"], oos_n=s_o["n"], oos_ret=s_o["total_ret"],
                                 oos_win=s_o["winrate"]))
                print(f"  {knob}={lv}: in n={s_i['n']:>3} 收益={s_i['total_ret']:>7}%  "
                      f"OOS n={s_o['n']:>3} 收益={s_o['total_ret']:>7}%  胜率={s_o['winrate']}%", flush=True)
            valid = [r for r in rows if r["n_ins"] >= 8]
            best = max(valid, key=lambda r: r["ins_ret"]) if valid else max(rows, key=lambda r: r["n_ins"])
            knob_results[knob] = dict(levels=rows, best_level=best["level"],
                                      best_ins_ret=best["ins_ret"], best_ins_n=best["n_ins"])
            base[knob] = best["level"]

        joint_best = dict(base)
        # ---- 主结论: joint-best 的 OOS 零泄漏验证 ----
        s_i, s_o = eval_combo(ctx, P, codes, cal, kind, joint_best, hot_by_k, regime_pass, names, in_cal, oos_cal)
        # BASE 的 OOS(对照)
        s_i_b, s_o_b = eval_combo(ctx, P, codes, cal, kind, dict(BASE[kind]), hot_by_k, regime_pass, names, in_cal, oos_cal)
        print(f"\n  >>> [{kind}] BASE OOS: n={s_o_b['n']} 收益={s_o_b['total_ret']}% 胜率={s_o_b['winrate']}%", flush=True)
        print(f"  >>> [{kind}] JOINT-BEST({grid_to_str(joint_best)}) OOS: n={s_o['n']} "
              f"收益={s_o['total_ret']}% 胜率={s_o['winrate']}%", flush=True)
        # ---- OOS 每旋钮敏感度(其余固定在 joint-best) ----
        oos_sens = {}
        all_knobs = {**KNOBS, **SHARED}
        for knob, levels in all_knobs.items():
            sens = []
            for lv in levels:
                p = dict(joint_best); p[knob] = lv
                _, s_o2 = eval_combo(ctx, P, codes, cal, kind, p, hot_by_k, regime_pass, names, in_cal, oos_cal)
                sens.append(dict(level=lv, oos_n=s_o2["n"], oos_ret=s_o2["total_ret"], oos_win=s_o2["winrate"]))
            valid = [r for r in sens if r["oos_n"] >= 5]
            oos_best = max(valid, key=lambda r: r["oos_ret"]) if valid else sens[0]
            oos_sens[knob] = dict(levels=sens, oos_best_level=oos_best["level"],
                                  oos_best_ret=oos_best["oos_ret"], oos_best_n=oos_best["oos_n"])

        report["kinds"][kind] = dict(
            base=dict(params=BASE[kind], oos=dict(n=s_o_b["n"], ret=s_o_b["total_ret"], win=s_o_b["winrate"])),
            joint_best=dict(params=joint_best,
                            in_sample=dict(n=s_i["n"], ret=s_i["total_ret"], win=s_i["winrate"]),
                            oos=dict(n=s_o["n"], ret=s_o["total_ret"], win=s_o["winrate"])),
            knob_in_sample=knob_results,
            knob_oos_sens=oos_sens,
        )

    # ---- 落盘 ----
    out = os.path.join(HERE, "volume_price_param_sweep_result.json")
    json.dump(report, open(out, "w"), ensure_ascii=False, indent=2,
              default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)
    print(f"\n完成. 结果: {out}")
    return report


if __name__ == "__main__":
    main()
