# -*- coding: utf-8 -*-
"""
离线回测对比框架 (只读复用原策略, 不修改任何原文件)

目标:
1. 忠实复刻 backtest/simulator.py 的因子计算 + 选股 + 退出 + 指标口径, 作为【基准】
2. 复用原版 momentum.alpha.AlphaModel (打分) 与 momentum.risk.ExitRuleEngine (退出),
   保证基准与线上策略完全一致
3. 在同一套数据/退出/口径下, 注入若干【优化变体】(新因子 + 权重重构 + 择时 + 加速度过滤),
   对比收益与胜率

数据: 直接读取 qlib_pro_v16.db 的 kline_cache / stock_sector_cache (离线, 不触发任何网络)

用法:
    python harness.py            # 运行全部变体 + 多窗口稳健性, 输出 results.json
"""

from __future__ import annotations

import os
import sys
import json
import sqlite3
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter("ignore")

# ---- 路径与离线环境 ----
os.environ["no_proxy"] = "*"
os.environ["NO_PROXY"] = "*"
THIS = Path(__file__).resolve()
PROJ = THIS.parent.parent            # .../tests/momentum
TESTS = PROJ.parent                  # .../tests
sys.path.insert(0, str(TESTS))

# ---- 复用原策略的干净模块 (确认不触发 adata / 网络) ----
from momentum import config as cfg               # 全部阈值参数
from momentum.alpha import AlphaModel            # 原版 Alpha 打分 (基准用)
from momentum.risk import ExitRuleEngine         # 原版退出引擎 (基准+优化共用)

DB_PATH = str(PROJ / "qlib_pro_v16.db")
KLINE_START = "2024-06-01"           # 载入起点, 覆盖足够历史
MIN_BARS = 100                        # 与 simulator 一致: len(df) > 100 才纳入


# =====================================================================
# 1. 数据层 (离线, 纯 DB)
# =====================================================================
def load_universe():
    """从 DB 载入全市场主板非科创/创业? -> 与 simulator 一致: 60/00 开头.
    返回 (data_cache, sector_map, calendar)
    """
    conn = sqlite3.connect(DB_PATH)
    # 载入 K 线 (只取 60/00 开头, 起始日期之后)
    q = (
        "SELECT code, trade_date, open, high, low, close, volume, amount, turnover_ratio "
        "FROM kline_cache WHERE trade_date >= ? "
        "AND (substr(code,1,2)='60' OR substr(code,1,2)='00')"
    )
    df = pd.read_sql_query(q, conn, params=[KLINE_START])
    # 板块
    sec = pd.read_sql_query("SELECT code, sector FROM stock_sector_cache", conn)
    conn.close()

    sector_map = dict(zip(sec["code"], sec["sector"]))

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
    for c in ["open", "high", "low", "close", "volume", "amount", "turnover_ratio"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"])

    data_cache = {}
    for code, g in df.groupby("code"):
        g = g.sort_values("trade_date").reset_index(drop=True)
        if len(g) <= MIN_BARS:
            continue
        g = g.rename(columns={"turnover_ratio": "turnover_rate"})
        g["vol"] = g["volume"]
        g["turnover_rate"] = g["turnover_rate"].fillna(0)
        data_cache[code] = g

    # 交易日历: 全体股票日期并集 (simulator 在指数缺失时也是这样降级)
    all_dates = sorted(set(df["trade_date"].tolist()))
    return data_cache, sector_map, all_dates


def build_market_proxy(data_cache, calendar):
    """构建等权市场指数代理 (用于择时): 每日横截面平均涨跌幅的累乘净值."""
    # 收集每只股票的日收益, 对齐到 calendar
    rets = pd.DataFrame(index=pd.DatetimeIndex(calendar))
    # 为效率, 只用成交额较大的一批股票近似市场 (取全体亦可, 这里用全体)
    daily_ret_sum = pd.Series(0.0, index=rets.index)
    daily_ret_cnt = pd.Series(0.0, index=rets.index)
    for code, g in data_cache.items():
        s = g.set_index("trade_date")["close"].reindex(rets.index)
        r = s.pct_change()
        mask = r.notna()
        daily_ret_sum[mask] += r[mask]
        daily_ret_cnt[mask] += 1
    mkt_ret = (daily_ret_sum / daily_ret_cnt.replace(0, np.nan)).fillna(0)
    mkt_nav = (1 + mkt_ret).cumprod()
    mkt_ma20 = mkt_nav.rolling(20).mean()
    return mkt_nav, mkt_ma20


# =====================================================================
# 2. 因子 + 前向收益 (忠实复刻 simulator._simulate_day_data 的离线路径)
#    注意: 该结果与"打分方式/选股过滤"无关, 可在所有变体间复用 -> 记忆化
# =====================================================================
def _calc_rsi(closes: pd.Series, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    ag = gain.rolling(period).mean().iloc[-1]
    al = loss.rolling(period).mean().iloc[-1]
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - (100 / (1 + rs))


def _trapped_ratio(df, lookback=60) -> float:
    df = df.tail(lookback)
    cp = float(df["close"].iloc[-1])
    total, trapped = 0.0, 0.0
    for h, l, v in zip(df["high"], df["low"], df["volume"]):
        h, l, v = float(h), float(l), float(v)
        if v <= 0 or h <= cp:
            continue
        if l >= cp:
            trapped += v
        else:
            trapped += v * (h - cp) / (h - l) if h > l else v
        total += v
    return trapped / total if total > 0 else 1.0


def _extra_factors(snap_df: pd.DataFrame) -> dict:
    """优化用的新因子 (全部可由 K 线缓存点对点计算, 无未来函数)."""
    close = snap_df["close"]
    ret = snap_df["ret"]
    out = {}

    # (1) 趋势质量 R^2: 对 log(close) 近20日线性回归的拟合优度 (越高=越顺滑的趋势)
    n = 20
    y = np.log(close.tail(n).values)
    x = np.arange(len(y))
    if len(y) >= 5 and np.std(y) > 0:
        xm, ym = x.mean(), y.mean()
        cov = ((x - xm) * (y - ym)).sum()
        vx = ((x - xm) ** 2).sum()
        slope = cov / (vx + 1e-12)
        yhat = ym + slope * (x - xm)
        ss_res = ((y - yhat) ** 2).sum()
        ss_tot = ((y - ym) ** 2).sum()
        r2 = 1 - ss_res / (ss_tot + 1e-12)
        out["trend_r2"] = float(max(0.0, r2)) * (1 if slope > 0 else -1)  # 上行趋势才加分
    else:
        out["trend_r2"] = 0.0

    # (2) 下行波动调整动量 (Sortino 式): 20日动量 / 下行波动
    r20 = ret.tail(20)
    downside = r20[r20 < 0]
    dstd = downside.std() if len(downside) > 2 else (r20.std() + 1e-9)
    mom20_raw = (close.iloc[-1] / close.iloc[-20]) - 1 if len(close) >= 20 else 0.0
    out["sortino_mom"] = float(mom20_raw / (dstd + 1e-9))

    # (3) 波动稳定性: 近20日收益波动的倒数 (低波动优先, 降低回撤/提高胜率)
    v20 = r20.std() + 1e-9
    out["low_vol"] = float(1.0 / (v20 * 100 + 1e-9))

    # (4) 量能健康度: 近5日均量 / 近20日均量 (温和放量, 过热则回落)
    vol = snap_df["volume"]
    v5 = vol.tail(5).mean()
    v20v = vol.tail(20).mean()
    ratio = v5 / (v20v + 1e-9)
    # 以 1.3 为理想, 越偏离越差 (钟形)
    out["vol_health"] = float(np.exp(-((ratio - 1.3) ** 2) / 0.5))

    # (5) 距20日高点回撤 (离新高越近越强, 但不追已冲高过多)
    hh = close.tail(20).max()
    out["near_high"] = float(close.iloc[-1] / (hh + 1e-9))
    return out


def simulate_day(code, name, sector, full_df, t_date, hold_period, exit_engine, min_amount):
    """复刻 simulator._simulate_day_data 离线路径 + 追加优化因子.
    style_group 恒为 SmallCap (与离线 mkt_cap=10e9 默认一致)."""
    target_t = pd.Timestamp(t_date).normalize()
    snap = full_df[full_df["trade_date"] <= target_t]
    if len(snap) < 35 or snap["trade_date"].iloc[-1] != target_t:
        return None
    snap = snap.copy()
    current_price = snap["close"].iloc[-1]
    current_amount = snap["amount"].iloc[-1]
    if not (pd.notna(current_amount) and current_amount >= min_amount):
        return None

    snap["ret"] = snap["close"].pct_change()

    def metrics(df_slice):
        if len(df_slice) < 20:
            return None
        cs = df_slice["close"]
        v20 = df_slice["ret"].tail(20).std() + 1e-9
        mom_5 = (cs.iloc[-1] / cs.iloc[-5]) - 1
        mom_20 = ((cs.iloc[-1] / cs.iloc[-20]) - 1) * (0.02 / v20)
        curr_to = df_slice["turnover_rate"].iloc[-1] if "turnover_rate" in df_slice else 0
        to_mult = 1.15 if 12 < curr_to < 18 else (0.6 if curr_to < 3 and df_slice["ret"].iloc[-1] > 0.05 else 1.0)
        sharpe = (df_slice["ret"].tail(20).mean() / v20) * np.sqrt(252)
        atr = (df_slice["high"] - df_slice["low"]).tail(20).mean()
        vr = df_slice["volume"].iloc[-1] / (df_slice["volume"].tail(6).iloc[:-1].mean() + 1e-9)
        return mom_5, mom_20 * to_mult, sharpe, vr, curr_to, atr

    mt = metrics(snap)
    my = metrics(snap.iloc[:-1])
    if mt is None or my is None:
        return None
    mom5_t, mom20_t, sh_t, vr_t, to_t, atr_t = mt
    mom5_y, mom20_y, sh_y, vr_y, to_y, atr_y = my

    ma5 = snap["close"].rolling(5).mean().iloc[-1]
    ma20 = snap["close"].rolling(20).mean().iloc[-1]
    bias_20 = (current_price / ma20) - 1 if ma20 > 0 else 0
    rsi = _calc_rsi(snap["close"])

    # 前向收益: 复用原版 ExitRuleEngine (adaptive 由 config 决定)
    t_idx = full_df.index[full_df["trade_date"] == target_t][0]
    fwd_ret, reason, hold_days, exit_date = exit_engine.simulate_exit(
        entry_price=current_price, df=full_df, entry_idx=int(t_idx),
        hold_period=hold_period, slippage=cfg.SLIPPAGE,
    )

    trapped = _trapped_ratio(snap, 60)
    rec = {
        "code": code, "name": name, "sector": sector, "style_group": "SmallCap",
        "mom_5_t": mom5_t, "mom_20_t": mom20_t, "sharpe_t": sh_t, "vr_t": vr_t, "turnover_t": to_t,
        "mom_5_y": mom5_y, "mom_20_y": mom20_y, "sharpe_y": sh_y, "vr_y": vr_y,
        "bias_20": bias_20, "rsi": rsi, "atr": atr_t,
        "nlp_score": cfg.NLP_SCORE_DEFAULT, "hk_bonus": 0.0,
        "chip_rate": 0.0, "big_order_t": 0.0, "big_order_y": 0.0,
        "trapped_ratio": trapped, "fwd_ret": fwd_ret, "exit_reason": reason,
        "close": current_price,
    }
    rec.update(_extra_factors(snap))
    return rec


# =====================================================================
# 3. 打分器
# =====================================================================
def score_baseline(day_results):
    """基准: 原版 AlphaModel (常规市 nlp_weight=0.3)."""
    df = pd.DataFrame(day_results)
    model = AlphaModel(market_total_amount=1.0e12, vol_surge_limit=cfg.VOL_SURGE_LIMIT)
    return model.neutralize_and_score(df)


def _zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / (s.std() + 1e-9)


def score_overlay(day_results, overlay_weights):
    """在原版 AlphaModel 打分基础上, 叠加质量因子做二次精修 (保留原版核心排序).
    overlay_weights: dict 新因子 -> 权重.
    """
    df = pd.DataFrame(day_results)
    model = AlphaModel(market_total_amount=1.0e12, vol_surge_limit=cfg.VOL_SURGE_LIMIT)
    df = model.neutralize_and_score(df)
    bonus = pd.Series(0.0, index=df.index)
    for key, w in overlay_weights.items():
        if key in df.columns and w != 0:
            bonus = bonus + w * _zscore(df[key].astype(float))
    df["alpha_score"] = df["alpha_score"] + bonus
    # 加速度: 原版 alpha_trend 已由 AlphaModel 计算
    return df


def score_optimized(day_results, weights):
    """优化打分: 横截面 z-score 加权 (全体 SmallCap 同组), 复用量比 Sigmoid 惩罚.
    weights: dict 因子->权重. 支持基础因子与新因子.
    """
    df = pd.DataFrame(day_results).copy()
    # 计算所有需要的 z 分
    fields = {
        "mom_5": "mom_5_t", "mom_20": "mom_20_t", "sharpe": "sharpe_t",
        "trend_r2": "trend_r2", "sortino_mom": "sortino_mom", "low_vol": "low_vol",
        "vol_health": "vol_health", "near_high": "near_high",
    }
    z = {}
    for key, col in fields.items():
        if col in df.columns:
            z[key] = _zscore(df[col].astype(float))
        else:
            z[key] = pd.Series(0.0, index=df.index)

    score = pd.Series(0.0, index=df.index)
    for key, w in weights.items():
        if w == 0:
            continue
        score = score + w * z.get(key, pd.Series(0.0, index=df.index))

    # 量比 Sigmoid 惩罚 (与原版一致)
    vr = df["vr_t"].astype(float)
    penalty = np.where(vr > cfg.VOL_SURGE_LIMIT, 1.0 / (1.0 + np.exp(2 * (vr - 4.5))), 1.0)
    df["alpha_score"] = score.values * penalty
    # alpha_trend 近似: 用 mom_5_t - mom_5_y 的方向 (用于加速度过滤)
    df["alpha_trend"] = _zscore(df["mom_5_t"].astype(float)) - _zscore(df["mom_5_y"].astype(float))
    return df


# =====================================================================
# 4. 选股 + 净值曲线
# =====================================================================
def select_picks(df_scored, variant):
    """选股过滤. 变体可覆盖 trapped/sharpe/max_picks 阈值, 并可加 acceleration 过滤.
    返回 (picks, eligible_count)."""
    df_sorted = df_scored.sort_values("alpha_score", ascending=False)
    picks, sector_counts = [], {}
    require_accel = variant.get("require_accel", False)
    max_picks = variant.get("max_picks", cfg.MAX_TOTAL_PICKS)
    min_sharpe = variant.get("min_sharpe", cfg.MIN_SHARPE)
    max_trapped = variant.get("max_trapped", getattr(cfg, "MAX_TRAPPED_RATIO", 0.10))
    max_sector = variant.get("max_sector", cfg.MAX_SECTOR_PICKS)

    eligible = 0
    for _, row in df_sorted.iterrows():
        if row["rsi"] > getattr(cfg, "RSI_DANGER_ZONE", 80.0):
            continue
        if row["sharpe_t"] <= min_sharpe:
            continue
        if getattr(cfg, "ENABLE_TRAPPED_FILTER", False):
            if row.get("trapped_ratio", 1.0) > max_trapped:
                continue
        if require_accel and row.get("alpha_trend", 0) <= 0:
            continue
        eligible += 1
        if len(picks) >= max_picks:
            continue
        s = row["sector"]
        if sector_counts.get(s, 0) < max_sector:
            picks.append(row)
            sector_counts[s] = sector_counts.get(s, 0) + 1
    return picks, eligible


def run_variant(variant, hold_period, window_shift, calendar, day_cache_getter,
                mkt_nav, mkt_ma20):
    """执行单个变体的回测, 返回指标 + 净值曲线."""
    n = len(calendar)
    need = cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
    if n < need:
        return None
    if window_shift > 0:
        end_off = hold_period + window_shift
        start_off = cfg.BACKTEST_DAYS_DEFAULT + hold_period + window_shift
        test_dates = calendar[-start_off:-end_off]
    else:
        test_dates = calendar[-(cfg.BACKTEST_DAYS_DEFAULT + hold_period):-hold_period]

    rebalance_dates = test_dates[::hold_period]
    equity = [1.0]
    daily = []
    trade_count = 0
    win_count = 0
    dates_out = []
    eligible_sum = 0
    eligible_n = 0

    use_regime = variant.get("regime", False)

    for t_date in rebalance_dates:
        # 择时: 市场弱 -> 空仓
        if use_regime:
            nav_t = mkt_nav.get(pd.Timestamp(t_date), np.nan)
            ma_t = mkt_ma20.get(pd.Timestamp(t_date), np.nan)
            if pd.notna(nav_t) and pd.notna(ma_t) and nav_t < ma_t:
                equity.append(equity[-1])
                daily.append(0.0)
                dates_out.append(str(t_date)[:10])
                continue

        day_results = day_cache_getter(t_date, hold_period)
        if not day_results:
            equity.append(equity[-1]); daily.append(0.0); dates_out.append(str(t_date)[:10]); continue

        if variant["type"] == "baseline":
            df_scored = score_baseline(day_results)
        elif variant["type"] == "overlay":
            df_scored = score_overlay(day_results, variant.get("overlay", {}))
        else:
            df_scored = score_optimized(day_results, variant["weights"])

        picks, eligible = select_picks(df_scored, variant)
        eligible_sum += eligible
        eligible_n += 1
        if picks:
            p = pd.DataFrame(picks)
            period_ret = p["fwd_ret"].mean()
            wins = int((p["fwd_ret"] > 0).sum())
            trade_count += len(p)
            win_count += wins
            equity.append(equity[-1] * (1 + period_ret))
            daily.append(period_ret)
        else:
            equity.append(equity[-1]); daily.append(0.0)
        dates_out.append(str(t_date)[:10])

    m = compute_metrics(equity, daily, trade_count, win_count, hold_period, dates_out)
    if m is not None:
        m["avg_eligible"] = round(eligible_sum / eligible_n, 1) if eligible_n else 0.0
    return m


def compute_metrics(curve, daily, trade_count, win_count, hold_period, dates_out):
    if len(curve) < 2:
        return None
    arr = np.array(curve)
    total_ret = (arr[-1] - 1) * 100
    peak = np.maximum.accumulate(arr)
    mdd = np.max((peak - arr) / (peak + 1e-9)) * 100
    dser = pd.Series(daily)
    rstd = dser.std()
    rmean = dser.mean()
    ppy = 252 / hold_period
    sharpe = (rmean / (rstd + 1e-9)) * np.sqrt(ppy) if rstd > 0 else 0.0
    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0
    annual = (arr[-1] ** (252 / cfg.BACKTEST_DAYS_DEFAULT) - 1) * 100
    return {
        "hold_period": hold_period,
        "profit_pct": round(total_ret, 2),
        "annual_ret": round(annual, 2),
        "sharpe": round(sharpe, 3),
        "win_rate": round(win_rate, 2),
        "max_dd": round(mdd, 2),
        "final_nav": round(float(arr[-1]), 4),
        "trade_count": trade_count,
        "equity": [round(float(x), 4) for x in curve],
        "dates": dates_out,
    }


# =====================================================================
# 5. 主流程
# =====================================================================
# 质量因子叠加权重 (在原版核心排序基础上做二次精修, 不替换核心因子)
OVERLAY_W = {"trend_r2": 0.30, "sortino_mom": 0.20, "low_vol": 0.15, "near_high": 0.10}

# 全部变体均【保留原策略的严苛过滤器】(trapped<=0.10, sharpe>1.0) —— 已验证该过滤器是核心 alpha
VARIANTS = [
    # 0. 原策略 (基准)
    {"name": "基准(原策略)", "type": "baseline"},
    # R. 仅市场择时 (市场净值<MA20 则空仓, 规避系统性下跌)
    {"name": "R:市场择时", "type": "baseline", "regime": True},
    # X. 仅动量加速过滤 (只买 alpha_trend>0, 动能仍在增强)
    {"name": "X:动量加速", "type": "baseline", "require_accel": True},
    # Q. 质量因子叠加 (原版打分 + 趋势质量/下行调整动量/低波/近新高 二次精修)
    {"name": "Q:质量叠加", "type": "overlay", "overlay": OVERLAY_W},
    # RXQ. 择时 + 加速 + 质量叠加  [推荐组合]
    {"name": "RXQ:择时+加速+质量", "type": "overlay", "overlay": OVERLAY_W,
     "regime": True, "require_accel": True},
    # RQ. 择时 + 质量叠加 (不加加速, 保留更多交易)
    {"name": "RQ:择时+质量", "type": "overlay", "overlay": OVERLAY_W, "regime": True},
]

HOLD_PERIODS = [5, 3]
WINDOW_SHIFTS = [0, 20, 40, 60]


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = load_universe()
    print(f"      股票数={len(data_cache)} 交易日={len(calendar)} "
          f"区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/4] 构建市场择时代理 ...", flush=True)
    mkt_nav_s, mkt_ma20_s = build_market_proxy(data_cache, calendar)
    mkt_nav = mkt_nav_s.to_dict()
    mkt_ma20 = mkt_ma20_s.to_dict()

    exit_engine = ExitRuleEngine(adaptive=getattr(cfg, "USE_ADAPTIVE_EXIT", True))
    min_amount = cfg.MIN_AMOUNT

    # 记忆化: (t_date, hold) -> day_results
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

    def day_cache_getter(t_date, hold_period):
        key = (str(t_date)[:10], hold_period)
        if key in day_cache:
            return day_cache[key]
        top = get_daily_top(t_date, cfg.POOL_SIZE)
        results = []
        for code in top:
            g = data_cache.get(code)
            if g is None:
                continue
            rec = simulate_day(code, code, sector_map.get(code, "其它"), g,
                               t_date, hold_period, exit_engine, min_amount)
            if rec:
                results.append(rec)
        day_cache[key] = results
        return results

    print("[3/4] 逐变体 x 多窗口回测 ...", flush=True)
    all_results = defaultdict(dict)   # variant_name -> {config_key: metrics}
    detail = defaultdict(dict)

    total = len(VARIANTS) * len(HOLD_PERIODS) * len(WINDOW_SHIFTS)
    done = 0
    for hp in HOLD_PERIODS:
        for shift in WINDOW_SHIFTS:
            for v in VARIANTS:
                m = run_variant(v, hp, shift, calendar, day_cache_getter, mkt_nav, mkt_ma20)
                done += 1
                ck = f"hold{hp}_shift{shift}"
                if m:
                    all_results[v["name"]][ck] = {k: m[k] for k in
                        ("profit_pct", "annual_ret", "sharpe", "win_rate", "max_dd", "trade_count", "final_nav", "avg_eligible")}
                    # 只保留主口径(hold5_shift0)的净值曲线用于绘图
                    if hp == 5 and shift == 0:
                        detail[v["name"]] = {"equity": m["equity"], "dates": m["dates"]}
                    line = f"收益={m['profit_pct']:7.2f}%  胜率={m['win_rate']:5.1f}%  夏普={m['sharpe']:.2f}"
                else:
                    line = "(数据不足)"
                print(f"      [{done}/{total}] {v['name']:16s} {ck:14s} {line}", flush=True)

    print("[4/4] 汇总输出 ...", flush=True)
    # 计算每个变体在所有窗口的均值 (稳健性)
    summary = {}
    for vname, cfgs in all_results.items():
        rows = list(cfgs.values())
        if not rows:
            continue
        summary[vname] = {
            "avg_profit": round(float(np.mean([r["profit_pct"] for r in rows])), 2),
            "avg_annual": round(float(np.mean([r["annual_ret"] for r in rows])), 2),
            "avg_sharpe": round(float(np.mean([r["sharpe"] for r in rows])), 3),
            "avg_win_rate": round(float(np.mean([r["win_rate"] for r in rows])), 2),
            "avg_max_dd": round(float(np.mean([r["max_dd"] for r in rows])), 2),
            "avg_trades": round(float(np.mean([r["trade_count"] for r in rows])), 1),
            "n_configs": len(rows),
            # 主口径
            "main": cfgs.get("hold5_shift0", {}),
        }

    out = {
        "meta": {
            "backtest_days": cfg.BACKTEST_DAYS_DEFAULT,
            "pool_size": cfg.POOL_SIZE,
            "max_picks": cfg.MAX_TOTAL_PICKS,
            "max_sector_picks": cfg.MAX_SECTOR_PICKS,
            "slippage": cfg.SLIPPAGE,
            "adaptive_exit": getattr(cfg, "USE_ADAPTIVE_EXIT", True),
            "hold_periods": HOLD_PERIODS,
            "window_shifts": WINDOW_SHIFTS,
            "universe": len(data_cache),
            "calendar_start": str(calendar[0])[:10],
            "calendar_end": str(calendar[-1])[:10],
        },
        "variants": [
            {"name": v["name"], "type": v["type"],
             "regime": v.get("regime", False), "require_accel": v.get("require_accel", False),
             "weights": v.get("weights", "ALPHA_WEIGHTS(原版)")}
            for v in VARIANTS
        ],
        "summary": summary,
        "detail_by_config": all_results,
        "equity_curves": detail,
    }
    out_path = THIS.parent / "results.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成 -> {out_path}", flush=True)

    # 控制台摘要
    print("\n" + "=" * 78)
    print(f"{'变体':<18}{'均收益%':>9}{'均年化%':>9}{'均夏普':>8}{'均胜率%':>9}{'均回撤%':>9}{'均交易':>7}")
    print("-" * 78)
    for vname, s in summary.items():
        print(f"{vname:<18}{s['avg_profit']:>9.2f}{s['avg_annual']:>9.2f}{s['avg_sharpe']:>8.2f}"
              f"{s['avg_win_rate']:>9.2f}{s['avg_max_dd']:>9.2f}{s['avg_trades']:>7.1f}")
    print("=" * 78)


if __name__ == "__main__":
    main()
