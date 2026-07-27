# -*- coding: utf-8 -*-
"""
三策略 × 市场环境(牛/熊/震荡) 分桶回测 + 推荐矩阵
================================================
目标: 量化"超跌绩优 / 主动量 / C-Tail"三个策略分别在 牛市 / 熊市 / 震荡市 下的表现,
      产出"当前市场环境 → 推荐策略"的映射, 供 Bark 推送展示, 让使用者按大盘状态自选策略。

环境定义(合成等权全A净值 proxy, 与三策略回测 harness 完全一致口径):
    nav  = 等权全A净值(横截面日均收益累乘)
    ma20 = nav.rolling(20);  ma60 = nav.rolling(60)
    bull   = nav>=ma20 且 nav>=ma60          (多头/上升趋势)
    bear   = nav< ma20 且 nav< ma60          (空头/下跌趋势)
    ranging= 其余(ma20/ma60 交叉区, 震荡)

每个策略用【自身 universe】计算 proxy 与 regime 标签, 再分别跑 4 个场景:
    natural : 该策略真实行为(主动量/C-Tail 自带择时闸口, 弱市自然空仓)
    bull    : 强制仅在该环境开仓(看其在该环境的"真实边缘")
    bear    : 同上
    ranging : 同上
用同一套 harness 的 simulate/run_variant, 仅改变"开仓日闸口", 不改变策略信号本身。

用法:
    python regime_backtest.py --strategy all [--quick] [--out .]
    python regime_backtest.py --strategy low_quality   # 仅低位绩优(可本地验证)
    python regime_backtest.py --strategy momentum      # 仅主动量
    python regime_backtest.py --strategy c_tail        # 仅 C-Tail
"""
from __future__ import annotations
import sys, os, json, argparse
from collections import defaultdict
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(ROOT))   # tests/ 使 momentum 包可导入 (harness 依赖)

# CI 中 checkout 顶层目录名可能与包名大小写不一致(如 repo 名 "Momentum"),
# 导致 sys.path 自动发现 'momentum' 失败(harness 的 `from momentum import config` 报 ModuleNotFoundError).
# 这里显式把 ROOT(本身即 momentum 包根, 含 __init__.py) 注册为 'momentum' 模块,
# 与 momentum-scan.yml 的 Push 步骤一致, 保证跨平台/跨目录名可用.
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

MIN_N = 8          # 单环境样本量门槛(低于此置信度降为 low)
WIN_RATE_OK = 55.0 # 胜率达标线

REGIMES = ["bull", "bear", "ranging"]
REGIME_CN = {"bull": "牛市(多头)", "bear": "熊市(空头)", "ranging": "震荡市"}
STRAT_CN = {"low_quality": "低位绩优", "momentum": "主动量", "c_tail": "C-Tail"}


# ---------------------------------------------------------------------------
# 环境标签
# ---------------------------------------------------------------------------
def regime_labels(nav: pd.Series) -> dict:
    """返回 { 'YYYY-MM-DD': 'bull'|'bear'|'ranging'|'na' }"""
    ma20 = nav.rolling(20).mean()
    ma60 = nav.rolling(60).mean()
    lab = {}
    for t in nav.index:
        nv = nav.get(t, np.nan)
        m20 = ma20.get(t, np.nan)
        m60 = ma60.get(t, np.nan)
        if pd.isna(nv) or pd.isna(m20) or pd.isna(m60):
            lab[str(t)[:10]] = "na"
            continue
        a20 = nv >= m20
        a60 = nv >= m60
        if a20 and a60:
            lab[str(t)[:10]] = "bull"
        elif (not a20) and (not a60):
            lab[str(t)[:10]] = "bear"
        else:
            lab[str(t)[:10]] = "ranging"
    return lab


def empty_metrics():
    return dict(n=0, winrate=0, avg_ret=0, sharpe=0, profit_pct=0, max_dd=0)


# ---------------------------------------------------------------------------
# 策略① 低位绩优 (harness_oversold_quality)  —— 无择时, 全环境可交易
# ---------------------------------------------------------------------------
def run_low_quality(quick: bool):
    import harness_oversold_quality as HOQ
    import os
    # HOQ 顶部硬编码了本地 macOS 绝对路径 DB/ROOT, 在 CI(Linux) 上无效
    # (sqlite3 "unable to open database file"). 这里用可移植路径覆盖:
    # 优先 env MOMENTUM_DB_PATH, 否则 ROOT/qlib_pro_v16.db. 不改外部 harness 文件, 符合"旁路"约束.
    HOQ.DB = os.environ.get("MOMENTUM_DB_PATH") or os.path.join(ROOT, "qlib_pro_v16.db")
    HOQ.ROOT = ROOT
    if quick:
        HOQ.WINDOW_START = "2025-09-01"   # 缩短窗口, 本地快速验证
    print("[低位绩优] 加载K线...", flush=True)
    ctx = HOQ.load_kline()
    cal = sorted({t for g in ctx.values() for t in g.index})
    cal_slice = [t for t in cal if HOQ.WINDOW_START <= str(t)[:10] <= HOQ.WINDOW_END]
    ctx = HOQ.build_ctx(ctx)
    fmap = HOQ.load_fundamentals()
    hot_at = HOQ.build_hot_themes(ctx, cal_slice)
    print(f"  标的={len(ctx)} 交易日={len(cal_slice)}", flush=True)

    # 环境标签(用自身 proxy)
    nav = HOQ.build_market_proxy(ctx, cal_slice)
    label = regime_labels(nav)
    reg_counts = defaultdict(int)
    for v in label.values():
        reg_counts[v] += 1
    print(f"  环境分布: {dict(reg_counts)}", flush=True)

    # 发布最优组合: V2 + 单题材上限1 + 止损-15% (无市场择时, 因择时对超跌反弹有害)
    cfg = dict(mode="deep", dd=-0.18, gap=0.03, rsi_th=35,
               ma60_rising=False, vol_confirm=False, macd_rsi=False,
               hot_on=True, pe_pb_on=True, quality_on=True, theme_cap=1)
    stop, hold = -0.15, 20
    inv = HOQ.build_signal_index(ctx, cal_slice, cfg)
    cal_str = [str(t)[:10] for t in cal_slice]

    out = {}
    # natural
    tr, eq = HOQ.simulate(ctx, cal_slice, inv, hot_at, fmap, hold, "close", stop, cfg, 10)
    out["natural"] = HOQ.metrics(tr, eq)
    # forced per regime
    for R in REGIMES:
        allow = {d: (label.get(d) == R) for d in cal_str}
        cfg_r = dict(cfg)
        cfg_r["regime_on"] = True
        cfg_r["regime_ma"] = "ma60"
        tr, eq = HOQ.simulate(ctx, cal_slice, inv, hot_at, fmap, hold, "close", stop,
                              cfg_r, 10, {"ma60": allow})
        out[R] = HOQ.metrics(tr, eq)
        print(f"  [{R}] n={out[R]['n']} 胜率={out[R]['winrate']}% 均收益={out[R]['avg_ret']}% "
              f"总收益={out[R]['total_ret']}% 夏普={out[R]['sharpe']}", flush=True)
    return out


# ---------------------------------------------------------------------------
# 策略② 主动量 (harness)  —— 自带择时(nav<MA20 空仓)
# ---------------------------------------------------------------------------
def _momentum_getter(data_cache, sector_map, calendar):
    """复刻 harness.main 的 day_cache_getter (离线, 纯 DB, 无需网络)."""
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


def run_momentum(quick: bool):
    import harness as H
    from momentum import config as cfg
    print("[主动量] 载入 universe...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"  标的={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)
    nav, ma20 = H.build_market_proxy(data_cache, calendar)
    label = regime_labels(nav)
    reg_counts = defaultdict(int)
    for v in label.values():
        reg_counts[v] += 1
    print(f"  环境分布: {dict(reg_counts)}", flush=True)

    # 扩大回测窗口至全覆盖, 保证各环境样本充足 (原 BACKTEST_DAYS_DEFAULT=250 仅约1年)
    hold = 5
    cfg.BACKTEST_DAYS_DEFAULT = len(calendar) - hold - 1

    day_cache_getter = _momentum_getter(data_cache, sector_map, calendar)
    nav_d, ma20_d = nav.to_dict(), ma20.to_dict()

    out = {}
    # natural: 真实策略(自带择时, nav<MA20 空仓)
    m = H.run_variant({"name": "基准(原策略)", "type": "baseline", "regime": True},
                      hold, 0, calendar, day_cache_getter, nav_d, ma20_d)
    out["natural"] = _conv_momentum(m)
    print(f"  [natural] {out['natural']}", flush=True)
    # forced per regime: 构造 ma20_forced 使仅 R 日 nav>=ma20 (隔离该环境真实边缘)
    for R in REGIMES:
        ma20_forced = ma20.copy()
        for t in nav.index:
            k = str(t)[:10]
            if label.get(k) == R:
                ma20_forced[t] = nav[t] - 1e-6   # 允许开仓
            else:
                ma20_forced[t] = nav[t] + 1e-6   # 禁止
        m = H.run_variant({"name": "forced", "type": "baseline", "regime": True},
                          hold, 0, calendar, day_cache_getter, nav_d, ma20_forced.to_dict())
        out[R] = _conv_momentum(m)
        print(f"  [{R}] {out[R]}", flush=True)
    return out


def _conv_momentum(m):
    if m is None:
        return empty_metrics()
    n = m.get("trade_count", 0)
    return dict(n=n, winrate=m.get("win_rate", 0),
                avg_ret=round(m.get("profit_pct", 0) / max(n, 1), 2),
                sharpe=m.get("sharpe", 0), profit_pct=m.get("profit_pct", 0),
                max_dd=m.get("max_dd", 0))


def _conv_c_tail(equity, trades):
    """从 simulate_c 的净值曲线 + 每笔收益列表计算标准指标(与另两策略口径对齐)."""
    if not trades:
        return empty_metrics()
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
    max_dd = float(abs(dd.min()) * 100)
    return dict(n=n, winrate=round(winrate, 2), avg_ret=round(avg_ret, 2),
                sharpe=round(sharpe, 3), profit_pct=round(total_ret, 2),
                max_dd=round(max_dd, 2))


# ---------------------------------------------------------------------------
# 策略③ C-Tail (harness_c_regime.simulate_c)  —— 信号日 filter_fn 闸口
# ---------------------------------------------------------------------------
def run_c_tail(quick: bool):
    import harness as H
    import harness_c_regime as HC
    from harness_sector import build_sector_heat
    from harness_compare3 import build_day_returns
    from harness_compare3_stop import build_price_lookup
    print("[C-Tail] 载入 universe...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    nav, ma20 = H.build_market_proxy(data_cache, calendar)
    label = regime_labels(nav)
    reg_counts = defaultdict(int)
    for v in label.values():
        reg_counts[v] += 1
    print(f"  环境分布: {dict(reg_counts)}", flush=True)

    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, 8)
    day_ret_map = build_day_returns(data_cache, sector_map)
    hold = 3
    # 全历史 rebalance 日期(每 hold 天), 不受 BACKTEST_DAYS_DEFAULT 窗口限制
    reb = [calendar[i] for i in range(0, len(calendar) - hold, hold)]

    out = {}
    # natural (filter_fn=None)
    eq, trades, _ = HC.simulate_c(calendar, price_lookup, date_idx, date_list,
                                  hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                  100000.0, filter_fn=None)
    out["natural"] = _conv_c_tail(eq, trades)
    print(f"  [natural] {out['natural']}", flush=True)
    # forced per regime
    for R in REGIMES:
        fn = (lambda t, R=R: label.get(str(t)[:10]) == R)
        eq, trades, _ = HC.simulate_c(calendar, price_lookup, date_idx, date_list,
                                      hot_by_date, day_ret_map, sector_map, reb, hold, 0.0,
                                      100000.0, filter_fn=fn)
        out[R] = _conv_c_tail(eq, trades)
        print(f"  [{R}] {out[R]}", flush=True)
    return out


# ---------------------------------------------------------------------------
# 推荐推导
# ---------------------------------------------------------------------------
def build_recommendation(matrix: dict):
    """基于【各策略在对应环境强制开仓】的实测表现做推荐(真正的"操作窗口"):
       对每个环境 R, 比较三策略 forced[R] 指标, 选表现最优者。
       候选门槛: 样本 n>=MIN_N 且 胜率>0; 优先选 胜率>=WIN_RATE_OK 且 n>=15 的(高置信),
       否则在达标候选中选最优(中置信); 若均不达标则退而取样本最足者(低置信)。"""
    STRATS = ("low_quality", "momentum", "c_tail")
    rec = {}
    for R in REGIMES:
        good, any_cand = {}, {}
        for s in STRATS:
            m = matrix.get(s, {}).get(R)
            if not m or m.get("n", 0) < MIN_N:
                continue
            any_cand[s] = m
            if m.get("winrate", 0) >= WIN_RATE_OK:
                good[s] = m
        pool = good if good else any_cand
        if pool:
            # 综合排序: 均收益 > 夏普 > 胜率
            best = max(pool, key=lambda s: (pool[s]["avg_ret"], pool[s]["sharpe"], pool[s]["winrate"]))
            bm = pool[best]
            conf = "high" if (best in good and bm["n"] >= 15) else ("medium" if best in good else "low")
            rec[R] = dict(strategy=best, confidence=conf,
                          winrate=bm["winrate"], avg_ret=bm["avg_ret"], n=bm["n"], sharpe=bm["sharpe"],
                          reason=f"{REGIME_CN[R]}：{STRAT_CN[best]} 回测最优(胜率{bm['winrate']}%/"
                                 f"均收益{bm['avg_ret']}%, n={bm['n']})，建议优先采用")
        else:
            rec[R] = dict(strategy=None, confidence="low",
                          reason=f"{REGIME_CN[R]}：三策略分环境样本均不足(MIN_N={MIN_N})，"
                                 f"建议降低仓位或观望")
    return rec


def compute_current_regime():
    """用全A等权净值 proxy 计算最新可得交易日的 regime 标签 (离线, DB 口径)."""
    import harness as H
    data_cache, sector_map, calendar = H.load_universe()
    nav, _ = H.build_market_proxy(data_cache, calendar)
    lab = regime_labels(nav)
    for t in reversed(nav.index):
        k = str(t)[:10]
        if lab.get(k) in ("bull", "bear", "ranging"):
            return dict(date=k, regime=lab[k])
    return dict(date=None, regime="na")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="all",
                    choices=["all", "low_quality", "momentum", "c_tail"])
    ap.add_argument("--quick", action="store_true", help="缩短窗口, 本地快速验证")
    ap.add_argument("--out", default=HERE)
    args = ap.parse_args()

    matrix = {}
    if args.strategy in ("all", "low_quality"):
        try:
            matrix["low_quality"] = run_low_quality(args.quick)
        except Exception as e:
            import traceback
            print(f"[低位绩优] 运行失败: {e}", flush=True)
            traceback.print_exc()
    if args.strategy in ("all", "momentum"):
        try:
            matrix["momentum"] = run_momentum(args.quick)
        except Exception as e:
            import traceback
            print(f"[主动量] 运行失败: {e}", flush=True)
            traceback.print_exc()
    if args.strategy in ("all", "c_tail"):
        try:
            matrix["c_tail"] = run_c_tail(args.quick)
        except Exception as e:
            import traceback
            print(f"[C-Tail] 运行失败: {e}", flush=True)
            traceback.print_exc()

    rec = build_recommendation(matrix)
    cur = compute_current_regime()
    rec_for_cur = rec.get(cur["regime"], {})
    out = dict(
        generated=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        regimes_definition="bull=nav>=MA20且>=MA60; bear=nav<MA20且<MA60; ranging=交叉区",
        current_regime=cur,
        recommended_for_current=dict(
            regime=cur["regime"],
            regime_cn=REGIME_CN.get(cur["regime"], cur["regime"]),
            strategy=rec_for_cur.get("strategy"),
            strategy_cn=STRAT_CN.get(rec_for_cur.get("strategy")) if rec_for_cur.get("strategy") else None,
            confidence=rec_for_cur.get("confidence", "low"),
            reason=rec_for_cur.get("reason", ""),
        ),
        matrix=matrix,
        recommendation=rec,
    )
    os.makedirs(args.out, exist_ok=True)
    json_path = os.path.join(args.out, "regime_recommendation.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("\n===== 推荐矩阵 =====")
    print(json.dumps(rec, ensure_ascii=False, indent=2))
    print(f"\n已写出 -> {json_path}")
    # 同时打印 3x3 概览
    print("\n===== 3x3 概览 (胜率% / 均收益% / n) =====")
    hdr = f"{'策略':<10}" + "".join(f"{REGIME_CN[R]:>16}" for R in REGIMES) + f"{'natural':>16}"
    print(hdr)
    for s in ("low_quality", "momentum", "c_tail"):
        if s not in matrix:
            continue
        row = f"{STRAT_CN[s]:<10}"
        for R in REGIMES:
            m = matrix[s].get(R, empty_metrics())
            row += f"{str(m.get('winrate',0))+'/'+str(m.get('avg_ret',0))+'/'+str(m.get('n',0)):>16}"
        mn = matrix[s].get("natural", empty_metrics())
        row += f"{str(mn.get('winrate',0))+'/'+str(mn.get('avg_ret',0))+'/'+str(mn.get('n',0)):>16}"
        print(row)


if __name__ == "__main__":
    main()
