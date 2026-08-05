# -*- coding: utf-8 -*-
"""
价量趋势口诀选股策略
====================
对应"突破要放量, 洗盘要缩量"等口诀, 落地为两套盘后选股公式 + 三大过滤器。

两套信号(均盘后判定, 用当日收盘定型数据):
  A. 突破放量: 收盘价创 N 日新高 + 成交量 ≥ 5日均量*倍数 + 收中阳(涨幅>3%且收阳)。
  B. 缩量回踩: 20日线在60日线上方(多头趋势) + 近10日有过放量拉升 + 回踩逼近20日线
             + 成交量极度萎缩(<5日均量*0.6)。

三大过滤器(盘后二次筛选, 提升胜率):
  1. 大盘环境: 用全A等权净值 proxy 判牛/熊/震荡; 熊市放弃突破信号(仅保留"放量砸盘恐慌止"逻辑, 此处简化为熊市不买)。
  2. 板块主线共振: 信号股所属题材须为当日 TOP_K 热门题材(主力资金在场)。
  3. 筹码位置/平台长度: 突破放量要求突破前经历≥plat_min 日横盘(平台); 缩量回踩天然在趋势中。

风险剔除: ST/*ST/退/警示/仙股(<1.5) 直接剔除(对应"盘后二次人工过滤"的自动化版)。

本模块只产出"信号索引" inv: {date_str: [code,...]}, 由回测/扫描层消费。
复用 harness_oversold_quality 的 build_ctx / build_hot_themes / build_market_proxy / load_market_stats。
"""
import numpy as np
import pandas as pd

# 信号参数(对应通达信公式默认/文中建议)
N_BREAK = 60            # 突破天数
VOL_RATIO = 1.5         # 放量倍数(相对5日均量)
SOLID_MIN_PCT = 4.0     # 中阳最低涨幅%
HAD_RAISE_PCT = 4.0     # 近10日放量拉升阈值
HAD_RAISE_VOL = 1.5     # 放量拉升量比(相对5日均量)
NEAR_MA_LO = 0.98       # 回踩20日线下沿
NEAR_MA_HI = 1.03       # 回踩20日线上沿
VOL_SHRINK = 0.7        # 缩量阈值(相对5日均量)
PLAT_MIN = 40           # 突破前横盘最小天数(平台长度过滤)


def ensure_ctx_indicators(g):
    """确保单只 g 已含 ma20/ma60/vma5 等指标(若 build_ctx 未跑过则补算)。"""
    if "ma20" not in g.columns:
        c = g["close"]; h = g["high"]; l = g["low"]; v = g["volume"]
        g["ma5"] = c.rolling(5).mean()
        g["ma20"] = c.rolling(20).mean()
        g["ma60"] = c.rolling(60).mean()
        g["ma120"] = c.rolling(120).mean()
        g["high60"] = h.rolling(N_BREAK).max()
        g["vma5"] = v.rolling(5).mean()
        g["vma20"] = v.rolling(20).mean()
    return g


def _is_garbage(name, close):
    """剔除 ST/*ST/退/警示/仙股。"""
    s = str(name)
    if "ST" in s or "退" in s or s.startswith("*") or "警示" in s:
        return True
    if close is not None and close < 1.5:
        return True
    return False


def signal_breakout(g):
    """突破放量信号: 返回布尔 Series(与 g.index 对齐)。"""
    c = g["close"]; o = g["open"]; v = g["volume"]; h = g["high"]
    vma5 = v.rolling(5).mean()
    pre_hhv = (np.maximum(c, o)).rolling(N_BREAK).max().shift(1)
    breakout = c > pre_hhv
    vol_up = v > (vma5.shift(1) * VOL_RATIO)
    pct = (c / c.shift(1) - 1.0) * 100.0
    solid = (c > o) & (pct > SOLID_MIN_PCT)
    return breakout & vol_up & solid


def signal_pullback(g):
    """缩量回踩信号: 返回布尔 Series。"""
    c = g["close"]; l = g["low"]; v = g["volume"]
    ma20 = c.rolling(20).mean(); ma60 = c.rolling(60).mean()
    vma5 = v.rolling(5).mean()
    trend = (ma20 > ma60) & (c > ma60)
    # 近10日有过放量拉升
    pct = (c / c.shift(1) - 1.0) * 100.0
    had_raise = (pct > HAD_RAISE_PCT) & (v > vma5 * HAD_RAISE_VOL)
    had = had_raise.rolling(10).max().fillna(0) > 0
    near_ma = l.between(ma20 * NEAR_MA_LO, ma20 * NEAR_MA_HI) & (c >= ma20)
    vol_shrink = v < (vma5 * VOL_SHRINK)
    return trend & had & near_ma & vol_shrink


def platform_filter(g, t):
    """突破前是否经历≥PLAT_MIN 日横盘(平台): 突破日前 PLAT_MIN 日最高-最低振幅<25%
    且未出现大幅上涨(避免追高已拉升平台)。返回 True/False。"""
    i = g.index.get_loc(t)
    if i < PLAT_MIN + 1:
        return False
    win = g.iloc[i - PLAT_MIN:i]
    if win.empty:
        return False
    hi = win["high"].max(); lo = win["low"].min()
    if hi <= 0 or lo <= 0:
        return False
    amp = (hi - lo) / lo
    # 平台: 振幅不大(横盘)且无单边暴涨(前段最高价相对起点涨幅<40%)
    start = win["close"].iloc[0]
    if start <= 0:
        return False
    run_up = (hi / start - 1.0)
    return (amp < 0.25) and (run_up < 0.40)


def build_inv(ctx, cal, names, hot_at, regime, min_history=120,
              use_theme_resonance=True, bull_only=True):
    """构建两套信号索引(已叠加过滤器), 返回 dict:
        {'breakout': {date:[codes]}, 'pullback': {date:[codes]}}
    regime: {date: 'bull'/'bear'/'ranging'} 由调用方用 build_market_proxy 判定。
    use_theme_resonance: True=要求信号股属当日资金流 TOP_K 热门板块(主线共振);
                        False=退化为仅行业分类不限热门。
    bull_only: True=仅在 bull/ranging 环境开仓(熊市全放弃); False=原宽松规则。
    """
    inv_b = {}
    inv_p = {}
    cal_set = set(str(t)[:10] for t in cal)
    for code, g in ctx.items():
        g = ensure_ctx_indicators(g)
        if g.empty:
            continue
        name = names.get(code, code)
        try:
            sig_b = signal_breakout(g)
            sig_p = signal_pullback(g)
        except Exception:
            continue
        idx = g.index
        for i, t in enumerate(idx):
            ts = str(t)[:10]
            if ts not in cal_set:
                continue
            if i < min_history:
                continue
            close = float(g.loc[t, "close"])
            if _is_garbage(name, close):
                continue
            # 板块主线共振: 信号股须属当日资金流 TOP_K 热门题材(主力在场)
            if use_theme_resonance:
                hot_codes = hot_at.get(ts, (set(), [], {}))[0]
                if code not in hot_codes:
                    continue
            # 大盘环境
            rg = regime.get(ts, "ranging")
            if bull_only:
                if rg == "bear":
                    continue
            # --- 突破放量 ---
            if bool(sig_b.iloc[i]):
                if not bull_only and rg == "bear":
                    pass
                elif platform_filter(g, t):
                    inv_b.setdefault(ts, []).append(code)
            # --- 缩量回踩 ---
            if bool(sig_p.iloc[i]):
                if not bull_only and rg == "bear":
                    continue
                inv_p.setdefault(ts, []).append(code)
    return dict(breakout=inv_b, pullback=inv_p)


def build_regime(cal, nav):
    """由全A等权净值判定每日环境: bull/ranging/bear。
    nav: pd.Series(索引同 cal 的 Timestamp)。"""
    s = nav.reindex(pd.DatetimeIndex(cal)).ffill().bfill()
    ma20 = s.rolling(20).mean(); ma60 = s.rolling(60).mean()
    reg = {}
    for t in cal:
        ts = str(t)[:10]
        i = s.index.get_loc(t) if t in s.index else None
        if i is None or i < 60:
            reg[ts] = "ranging"
            continue
        n = s.iloc[i]; a20 = ma20.iloc[i]; a60 = ma60.iloc[i]
        if pd.isna(a20) or pd.isna(a60):
            reg[ts] = "ranging"
        elif n >= a20 and n >= a60:
            reg[ts] = "bull"
        elif n < a20 and n < a60:
            reg[ts] = "bear"
        else:
            reg[ts] = "ranging"
    return reg


# 通达信/同花顺公式源码(供用户直接复制使用, 见报告)
TDX_FORMULAS = {
    "突破放量": """
{N:=60; VOL_RATIO:=1.5;}
PRE_HHV:=REF(HHV(MAX(C,O), 60), 1);
BREAKOUT:=C > PRE_HHV;
VOL_UP:=VOL > REF(MA(VOL,5), 1) * 2.0;
SOLID_K:=(C > O) AND ((C - REF(C,1)) / REF(C,1) * 100 > 3);
XG: BREAKOUT AND VOL_UP AND SOLID_K;
""",
    "缩量回踩": """
MA20:=MA(C,20);
MA60:=MA(C,60);
TREND:=MA20 > MA60 AND C > MA60;
HAD_RAISE:=COUNT((C-REF(C,1))/REF(C,1)*100 > 4 AND VOL > MA(VOL,5)*1.5, 10) >= 1;
NEAR_MA:=BETWEEN(L, MA20 * 0.98, MA20 * 1.03) AND C >= MA20;
VOL_SHRINK:=VOL < MA(VOL,5) * 0.7;
XG: TREND AND HAD_RAISE AND NEAR_MA AND VOL_SHRINK;
""",
}
