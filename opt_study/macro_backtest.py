# -*- coding: utf-8 -*-
"""
Macro Overwrite 接入三策略 —— 效果验证回测
=========================================
把 macro_overwrite.MacroOverwrite 作为「总闸」接入三策略既有开仓日闸口,
对比 baseline(无宏观总闸) vs +macro(接入总闸) 的指标变化, 验证其是否为净增益.

接入方式(均不改原始 harness, 与 regime_backtest 一致口径):
  低位绩优 : HOQ.simulate 的 regime_at 闸口复用 —— 把 macro.allow_map 作为 ma60 闸口,
             并以 rebuild_equity_from_scaled_trades 忠实应用软降仓.
  主动量   : 构造 ma20_forced, 在 macro 禁开日置 nav+eps(强制 nav<ma20 空仓);
             并以缩放后权益曲线应用软降仓.
  C-Tail   : simulate_c 的 filter_fn = macro.allow_fn(禁开日不放新仓).
             (C-Tail 软降仓在回测层不缩放, 原因: simulate_c 不暴露入场日;
              其日历软降仓在实时层通过「弱月降配 C-Tail 仓位」实现, 见报告说明.)

用法:
    python macro_backtest.py --quick                # 本地快速(缩短窗口)
    python macro_backtest.py --strategy all
    python macro_backtest.py --strategy low_quality --force-calendar  # 低质+强制日历(反事实)
"""
from __future__ import annotations
import sys, os, json, argparse
import numpy as np
import pandas as pd
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(ROOT))

# momentum 模块注册(跨目录名/平台一致, 同 regime_backtest / momentum-scan)
import importlib.util as _ilu
from pathlib import Path as _P
if 'momentum' not in sys.modules:
    _root_dir = _P(ROOT).resolve()
    if (_root_dir / '__init__.py').exists():
        _spec = _ilu.spec_from_file_location(
            'momentum', str(_root_dir / '__init__.py'),
            submodule_search_locations=[str(_root_dir)])
        _mm = _ilu.module_from_spec(_spec)
        sys.modules['momentum'] = _mm
        _spec.loader.exec_module(_mm)

from macro_overwrite import (MacroOverwrite, build_overlay_series, allow_fn,
                             scale_period_returns, rebuild_equity_from_scaled_trades)

STRAT_CN = {"low_quality": "低位绩优", "momentum": "主动量", "c_tail": "C-Tail"}


# ===========================================================================
# 指标辅助
# ===========================================================================
def _eq_metrics(equity: list, dates: list, trade_count=0, win_rate=0.0, hold_period=5):
    """从权益曲线 + 日期 复算 metrics(用于主动量/ C-Tail 软降仓后)."""
    arr = np.array(equity, dtype=float)
    if len(arr) < 2:
        return dict(n=trade_count, winrate=win_rate, avg_ret=0, sharpe=0,
                    profit_pct=0, maxdd=0)
    total_ret = (arr[-1] / arr[0] - 1) * 100
    daily = np.diff(arr) / arr[:-1]
    rstd = daily.std()
    rmean = daily.mean()
    ppy = 252 / hold_period
    sharpe = (rmean / rstd * np.sqrt(ppy)) if rstd > 0 else 0.0
    peak = np.maximum.accumulate(arr)
    mdd = np.max((peak - arr) / (peak + 1e-9)) * 100
    return dict(n=trade_count, winrate=round(win_rate, 2),
                avg_ret=round(float(daily.mean() * 100), 2),
                sharpe=round(float(sharpe), 3),
                profit_pct=round(float(total_ret), 2), maxdd=round(float(mdd), 2))


def _conv_c_tail(equity, trades):
    if not trades:
        return dict(n=0, winrate=0, avg_ret=0, sharpe=0, profit_pct=0, maxdd=0)
    arr = np.array(trades, dtype=float)
    n = int(len(arr))
    winrate = float((arr > 0).mean() * 100)
    avg_ret = float(arr.mean() * 100)
    eq = np.array(equity, dtype=float)
    total_ret = float((eq[-1] / eq[0] - 1) * 100) if len(eq) > 1 else 0.0
    daily = np.diff(eq) / eq[:-1]
    sharpe = float(daily.mean() / daily.std() * np.sqrt(252.0)) if (len(daily) > 1 and daily.std() > 0) else 0.0
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    maxdd = float(abs(dd.min()) * 100)
    return dict(n=n, winrate=round(winrate, 2), avg_ret=round(avg_ret, 2),
                sharpe=round(sharpe, 3), profit_pct=round(total_ret, 2),
                maxdd=round(maxdd, 2))


# ===========================================================================
# 策略① 低位绩优 (harness_oversold_quality)
# ===========================================================================
def run_low_quality(mo: MacroOverwrite, quick: bool, force_calendar: bool = False):
    import harness_oversold_quality as HOQ
    HOQ.DB = os.environ.get("MOMENTUM_DB_PATH") or os.path.join(ROOT, "qlib_pro_v16.db")
    HOQ.ROOT = ROOT
    if quick:
        HOQ.WINDOW_START = "2025-09-01"
    print("[低位绩优] 加载K线...", flush=True)
    ctx = HOQ.load_kline()
    cal = sorted({t for g in ctx.values() for t in g.index})
    cal_slice = [t for t in cal if HOQ.WINDOW_START <= str(t)[:10] <= HOQ.WINDOW_END]
    ctx = HOQ.build_ctx(ctx)
    fmap = HOQ.load_fundamentals()
    hot_at = HOQ.build_hot_themes(ctx, cal_slice)
    nav = HOQ.build_market_proxy(ctx, cal_slice)
    cfg = dict(mode="deep", dd=-0.18, gap=0.03, rsi_th=35,
               ma60_rising=False, vol_confirm=False, macd_rsi=False,
               hot_on=True, pe_pb_on=True, quality_on=True, theme_cap=1)
    stop, hold = -0.15, 20
    inv = HOQ.build_signal_index(ctx, cal_slice, cfg)
    cal_str = [str(t)[:10] for t in cal_slice]

    # baseline: 自然(无宏观总闸, 也无大盘择时)
    tr0, eq0 = HOQ.simulate(ctx, cal_slice, inv, hot_at, fmap, hold, "close", stop, cfg, 10)
    base = HOQ.metrics(tr0, eq0)

    # +macro: 用 regime_at(ma60) 闸口复用 macro.allow_map; 软降仓用权益重建
    cfg_m = dict(cfg)
    cfg_m["regime_on"] = True
    cfg_m["regime_ma"] = "ma60"
    strat = "low_quality"
    if force_calendar:
        # 反事实: 强制对低位绩优开启日历软护栏, 验证其影响
        mo2 = MacroOverwrite()
        mo2.cfg["per_strategy"]["low_quality"] = {"calendar": True, "stress": "normal"}
        ov = mo2.build_series(nav, strat)
    else:
        ov = mo.build_series(nav, strat)
    allow = mo.allow_map(ov)
    scale = mo.scale_map(ov)
    tr1, eq1 = HOQ.simulate(ctx, cal_slice, inv, hot_at, fmap, hold, "close", stop,
                            cfg_m, 10, {"ma60": allow})
    scaled_tr, scaled_eq = rebuild_equity_from_scaled_trades(
        tr1, ctx, cal_slice, scale, init_capital=HOQ.INIT_CAPITAL)
    macro = HOQ.metrics(scaled_tr, scaled_eq)

    n_block = sum(1 for k, d in ov.items() if not d.allow_new)
    n_soft = sum(1 for k, d in ov.items() if d.level == "soft")
    print(f"  baseline: n={base['n']} 胜率={base['winrate']} 总收益={base['total_ret']} "
          f"夏普={base['sharpe']} 回撤={base['maxdd']}", flush=True)
    print(f"  +macro  : n={macro['n']} 胜率={macro['winrate']} 总收益={macro['total_ret']} "
          f"夏普={macro['sharpe']} 回撤={macro['maxdd']}", flush=True)
    print(f"  总闸触发: 禁开日={n_block} 软降仓日={n_soft} (force_calendar={force_calendar})", flush=True)
    return dict(baseline=base, macro=macro,
                gate=dict(block_days=n_block, soft_days=n_soft, force_calendar=force_calendar))


# ===========================================================================
# 策略② 主动量 (harness)
# ===========================================================================
def _momentum_getter(data_cache, sector_map, calendar):
    import harness as H
    from momentum import config as cfg
    exit_engine = H.ExitRuleEngine(adaptive=getattr(cfg, "USE_ADAPTIVE_EXIT", True))
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

    def getter(t_date, hold_period):
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

    return getter


def run_momentum(mo: MacroOverwrite, quick: bool):
    import harness as H
    from momentum import config as cfg
    print("[主动量] 载入 universe...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    nav, ma20 = H.build_market_proxy(data_cache, calendar)
    hold = 5
    cfg.BACKTEST_DAYS_DEFAULT = len(calendar) - hold - 1
    day_cache_getter = _momentum_getter(data_cache, sector_map, calendar)
    nav_d, ma20_d = nav.to_dict(), ma20.to_dict()

    # baseline (自带择时 nav<MA20 空仓)
    m0 = H.run_variant({"name": "基线", "type": "baseline", "regime": True},
                       hold, 0, calendar, day_cache_getter, nav_d, ma20_d)
    base = _eq_metrics(m0["equity"], m0["dates"], m0["trade_count"], m0["win_rate"], hold)

    # +macro: ma20_forced 在禁开日强制 nav<ma20; 软降仓缩放权益曲线
    ov = mo.build_series(nav, "momentum")
    ma20_forced = ma20.copy()
    for t in nav.index:
        k = str(t)[:10]
        if not ov.get(k).allow_new:
            ma20_forced[t] = nav[t] + 1e-6
    m1 = H.run_variant({"name": "+macro", "type": "baseline", "regime": True},
                       hold, 0, calendar, day_cache_getter, nav_d, ma20_forced.to_dict())
    scale = mo.scale_map(ov)
    # 软降仓: 缩放每期收益(再平衡模型, 期收益独立复利)
    daily = np.diff(np.array(m1["equity"])) / np.array(m1["equity"])[:-1]
    sdaily = scale_period_returns(m1["dates"][1:], list(daily), scale)
    seq = [1.0]
    for r in sdaily:
        seq.append(seq[-1] * (1 + r))
    macro = _eq_metrics(seq, m1["dates"], m1["trade_count"], m1["win_rate"], hold)

    n_block = sum(1 for k, d in ov.items() if not d.allow_new)
    n_soft = sum(1 for k, d in ov.items() if d.level == "soft")
    print(f"  baseline: n={base['n']} 胜率={base['winrate']} 总收益={base['profit_pct']} "
          f"夏普={base['sharpe']} 回撤={base['maxdd']}", flush=True)
    print(f"  +macro  : n={macro['n']} 胜率={macro['winrate']} 总收益={macro['profit_pct']} "
          f"夏普={macro['sharpe']} 回撤={macro['maxdd']}", flush=True)
    print(f"  总闸触发: 禁开日={n_block} 软降仓日={n_soft}", flush=True)
    return dict(baseline=base, macro=macro,
                gate=dict(block_days=n_block, soft_days=n_soft))


# ===========================================================================
# 策略③ C-Tail (harness_c_regime.simulate_c)
# ===========================================================================
def run_c_tail(mo: MacroOverwrite, quick: bool):
    import harness as H
    import harness_c_regime as HC
    from harness_sector import build_sector_heat
    from harness_compare3 import build_day_returns
    from harness_compare3_stop import build_price_lookup
    print("[C-Tail] 载入 universe...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    nav, ma20 = H.build_market_proxy(data_cache, calendar)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, 8)
    day_ret_map = build_day_returns(data_cache, sector_map)
    hold = 3
    reb = [calendar[i] for i in range(0, len(calendar) - hold, hold)]

    # baseline (filter_fn=None)
    eq0, trades0, _ = HC.simulate_c(calendar, price_lookup, date_idx, date_list,
                                    hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                    100000.0, filter_fn=None)
    base = _conv_c_tail(eq0, trades0)
    # +macro: filter_fn = allow_fn(禁开日不放新仓)
    ov = mo.build_series(nav, "c_tail")
    fn = allow_fn(ov)
    eq1, trades1, _ = HC.simulate_c(calendar, price_lookup, date_idx, date_list,
                                    hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                    100000.0, filter_fn=fn)
    macro = _conv_c_tail(eq1, trades1)

    n_block = sum(1 for k, d in ov.items() if not d.allow_new)
    n_soft = sum(1 for k, d in ov.items() if d.level == "soft")
    print(f"  baseline: n={base['n']} 胜率={base['winrate']} 均收益={base['avg_ret']} "
          f"总收益={base['profit_pct']} 夏普={base['sharpe']} 回撤={base['maxdd']}", flush=True)
    print(f"  +macro  : n={macro['n']} 胜率={macro['winrate']} 均收益={macro['avg_ret']} "
          f"总收益={macro['profit_pct']} 夏普={macro['sharpe']} 回撤={macro['maxdd']}", flush=True)
    print(f"  总闸触发: 禁开日={n_block} 软降仓日={n_soft} "
          f"(C-Tail 软降仓在实时层实现, 回测仅含硬熔断禁开)", flush=True)
    return dict(baseline=base, macro=macro,
                gate=dict(block_days=n_block, soft_days=n_soft))


# ===========================================================================
# 汇总 / 报告
# ===========================================================================
def _delta(a: dict, b: dict):
    """b(+macro) 相对 a(baseline) 的增量(仅数值字段)."""
    out = {}
    for k in b:
        if isinstance(b[k], (int, float)) and isinstance(a.get(k), (int, float)):
            out[k] = round(b[k] - a[k], 3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="all",
                    choices=["all", "low_quality", "momentum", "c_tail"])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--force-calendar", action="store_true",
                    help="对低位绩优强制开启日历软护栏(反事实验证)")
    ap.add_argument("--out", default=HERE)
    args = ap.parse_args()

    mo = MacroOverwrite()
    res = {}
    if args.strategy in ("all", "low_quality"):
        try:
            res["low_quality"] = run_low_quality(mo, args.quick, args.force_calendar)
        except Exception as e:
            import traceback; traceback.print_exc()
    if args.strategy in ("all", "momentum"):
        try:
            res["momentum"] = run_momentum(mo, args.quick)
        except Exception as e:
            import traceback; traceback.print_exc()
    if args.strategy in ("all", "c_tail"):
        try:
            res["c_tail"] = run_c_tail(mo, args.quick)
        except Exception as e:
            import traceback; traceback.print_exc()

    # 汇总对比表
    print("\n===== Macro Overwrite 接入对比 (baseline → +macro) =====")
    hdr = f"{'策略':<10}{'指标':<10}{'baseline':>14}{'+macro':>14}{'Δ':>12}"
    print(hdr)
    for s, d in res.items():
        base, macro, gate = d["baseline"], d["macro"], d["gate"]
        print(f"-- {STRAT_CN[s]} (禁开{gate['block_days']}天/软降{gate['soft_days']}天) --")
        for k in ("n", "winrate", "avg_ret", "profit_pct", "sharpe", "maxdd"):
            bv = base.get(k, 0); mv = macro.get(k, 0)
            dv = round(mv - bv, 3) if isinstance(bv, (int, float)) else 0
            print(f"{'':<10}{k:<10}{str(bv):>14}{str(mv):>14}{str(dv):>12}")

    # 写 JSON
    os.makedirs(args.out, exist_ok=True)
    out_path = os.path.join(args.out, "macro_backtest_result.json")
    payload = dict(
        generated=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        config=mo.cfg,
        note=("baseline=无宏观总闸; +macro=接入 MacroOverwrite 总闸(应激熔断+日历软护栏). "
              "低位绩优/C-Tail 软降仓已忠实应用, 主动量软降仓缩放权益曲线; "
              "C-Tail 日历软降仓在实时层(弱月降配)实现, 回测仅含硬熔断禁开."),
        results=res,
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n已写出 -> {out_path}")


if __name__ == "__main__":
    main()
