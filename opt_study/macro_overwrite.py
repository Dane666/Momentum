# -*- coding: utf-8 -*-
"""
Macro Overwrite —— 宏观熔断 / 日历软护栏 独立模块
================================================

定位
----
三策略(低位绩优 / 主动量 / C-Tail)之上的「总闸」. 它 **不改变任何策略信号本身**,
只在【开仓日】层面做两层叠加裁决, 输出每个交易日的 :class:`OverlayDecision`:

  1) 日历软护栏 (CalendarGuard)
       历史弱月(4/6/12, 全样本 1995-2026 等权验证: 4月 t=-2.15 p≈0.03 最弱,
       6/12月亦弱) → 软降仓 (position_scale<1); 春季(1-3月)不降.
  2) 市场应激熔断 (StressBreaker)
       全A等权净值 nav 远离 MA60(系统性下跌) 时 → 软降仓 / 硬熔断(禁止新开仓).

设计原则(来自 calendar 效应长期验证结论, 见 seasonal_timing_validation_report.md)
------------------------------------------------------------------------------
  * 纯择时绝对收益被僵尸股等权基准放大、不可信 → 只做「软护栏」, 不做硬按月份满仓/空仓.
  * 因子倾斜(小盘 / 红利)因全库无市值 / 股息率数据 **不可测** → 本模块不实现,
    仅保留 per_strategy 接口位, 未来补数据后可扩展.
  * 低位绩优是 **逆势** 策略(熊市胜率100%, 回测最优), 系统性弱市恰恰是它的行情,
    故对其默认 **关闭日历软护栏**, 应激熔断也设为 crash_only(仅极端暴跌硬熔断),
    避免误伤其最佳交易窗口. 该默认可由 backtest 数据复盘修正.

接口约束
--------
  * 策略无关: 模块只消费 (date, nav, ma60) 这类「市场环境」输入, 绝不触碰任何策略内部.
  * 旁路设计: 模块不修改任何原始 harness; 三策略通过各自既有的「开仓日闸口」
    (HOQ.simulate 的 regime_at / 主动量 ma20_forced / C-Tail filter_fn) 接入,
    把 Macro Overwrite 的 allow_new 作为 **最高优先级总闸** AND 进去.

用法
----
    from macro_overwrite import MacroOverwrite, build_overlay_series, allow_fn
    mo = MacroOverwrite()                         # 默认配置
    ov = mo.build_series(nav)                     # nav: 等权全A净值 pd.Series
    # 接入三策略(见 macro_backtest.py):
    #   low_quality: HOQ.simulate(..., cfg_regime_on, {"ma60": {d: ov[d].allow_new}})
    #   momentum   : 构造 ma20_forced, 在 ov 禁开日置 nav+eps
    #   c_tail     : filter_fn = lambda t: ov[str(t)[:10]].allow_new
"""
from __future__ import annotations
import copy
import numpy as np
import pandas as pd
from typing import Dict, Optional


# ---------------------------------------------------------------------------
# 默认配置
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = {
    # 应激熔断(系统性风险保护). 阈值基于 nav 相对 MA60 的偏离(dev = nav/MA60 - 1).
    "stress": {
        "enabled": True,
        "hard_below_ma60": -0.10,   # dev < -10% → 硬熔断(禁止新开仓, 已有持仓照常退出)
        "soft_below_ma60": -0.04,   # -10% <= dev < -4% → 软降仓
        "soft_scale": 0.5,          # 软降仓比例(1.0=不降)
        "crash_hard_below_ma60": -0.15,  # crash_only 模式下的硬熔断阈值(更陡)
    },
    # 日历软护栏(历史弱月软降仓; 春季不降, 仅标注).
    "calendar": {
        "enabled": True,
        "soft_months": [4, 6, 12],  # 历史弱月 → 软降仓
        "soft_scale": 0.5,
        "bias_months": [1, 2, 3],   # 春季(仅标注, 不降仓)
    },
    # 各策略适用性. stress 取值: "normal"(软+硬) / "crash_only"(仅极端硬熔断) / False(关).
    # calendar: True/False.
    # 默认: 应激仅 crash_only(极端暴跌硬熔断, 非侵入保险); 日历软护栏全部关闭.
    #   依据 macro_backtest 实测(2024-2026): 4/6/12 月 blanket×0.5 软降仓对三策略均为净负
    #   (各策略弱月反而有正 edge, 降仓即放弃收益, 回撤未改善). 日历组件仍保留、可一行开启,
    #   但默认不做机械降仓(呼应日历效应验证"不做硬/机械按月份操作"的结论).
    "per_strategy": {
        "low_quality": {"calendar": False, "stress": "crash_only"},  # 逆势: 仅防极端暴跌
        "momentum":     {"calendar": False, "stress": "crash_only"},
        "c_tail":       {"calendar": False, "stress": "crash_only"},
    },
}

LEVEL_RANK = {"normal": 0, "soft": 1, "hard": 2}


class OverlayDecision:
    """单日宏观覆盖裁决."""

    __slots__ = ("level", "allow_new", "position_scale", "reason", "tags")

    def __init__(self, level: str, allow_new: bool, position_scale: float,
                 reason: str, tags: Optional[list] = None):
        self.level = level                    # 'normal' | 'soft' | 'hard'
        self.allow_new = allow_new            # 总闸是否允许新开仓
        self.position_scale = float(position_scale)  # 软降仓比例(1.0=不降, 0=空仓)
        self.reason = reason
        self.tags = tags or []

    def to_dict(self) -> dict:
        return dict(level=self.level, allow_new=self.allow_new,
                    position_scale=round(self.position_scale, 3),
                    reason=self.reason, tags=self.tags)

    def __repr__(self):
        return (f"Overlay({self.level}, allow={self.allow_new}, "
                f"scale={self.position_scale:.2f}, {self.reason})")


class MacroOverwrite:
    """宏观熔断 / 日历软护栏 总闸."""

    def __init__(self, config: Optional[dict] = None):
        self.cfg = copy.deepcopy(DEFAULT_CONFIG)
        if config:
            self._deep_update(self.cfg, config)

    @staticmethod
    def _deep_update(base: dict, over: dict):
        for k, v in over.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                MacroOverwrite._deep_update(base[k], v)
            else:
                base[k] = v

    # ------------------------------------------------------------------
    # 单日裁决
    # ------------------------------------------------------------------
    def decide(self, date, nav_val, ma60_val, strategy: Optional[str] = None) -> OverlayDecision:
        """对单个交易日做宏观覆盖裁决.

        Parameters
        ----------
        date      : 任意可被 str(date)[:10] 取 YYYY-MM-DD 的日期对象.
        nav_val   : 当日等权全A净值(或该策略 proxy nav).
        ma60_val  : 当日 MA60.
        strategy  : 可选, 用于读取 per_strategy 适用性(决定日历/应激是否启用).
        """
        month = int(str(date)[5:7])
        cfg = self.cfg

        # ---- 应激熔断 ----
        stress_level = "normal"
        stress_scale = 1.0
        sconf = cfg["stress"]
        # strategy=None → 广谱快照, 与部署默认(crash_only)保持一致, 避免"虚报硬熔断"
        s_mode = cfg["per_strategy"].get(strategy, {}).get("stress", "normal") if strategy else "crash_only"
        if sconf["enabled"] and s_mode:
            if nav_val is not None and ma60_val is not None and not pd.isna(nav_val) and not pd.isna(ma60_val):
                dev = nav_val / ma60_val - 1.0
                if s_mode == "crash_only":
                    if dev < sconf["crash_hard_below_ma60"]:
                        stress_level = "hard"
                else:  # normal
                    if dev < sconf["hard_below_ma60"]:
                        stress_level = "hard"
                    elif dev < sconf["soft_below_ma60"]:
                        stress_level = "soft"
                        stress_scale = sconf["soft_scale"]

        # ---- 日历软护栏 ----
        cal_level = "normal"
        cal_scale = 1.0
        cconf = cfg["calendar"]
        # strategy=None → 不标注日历(广谱快照)
        c_on = cfg["per_strategy"].get(strategy, {}).get("calendar", True) if strategy else False
        if cconf["enabled"] and c_on:
            if month in cconf["soft_months"]:
                cal_level = "soft"
                cal_scale = cconf["soft_scale"]

        # ---- 叠加(取更严重者) ----
        level = max(stress_level, cal_level, key=lambda x: LEVEL_RANK[x])

        if level == "hard":
            allow_new = False
            position_scale = 0.0
        elif level == "soft":
            allow_new = True
            # 取应激/日历两者中更保守的降仓比例
            position_scale = min(stress_scale, cal_scale)
        else:
            allow_new = True
            position_scale = 1.0

        # ---- 文案 ----
        reasons = []
        tags = []
        if stress_level == "hard":
            reasons.append(f"应激硬熔断(nav/MA60-1={_dev_str(nav_val, ma60_val)})")
            tags.append("stress_hard")
        elif stress_level == "soft":
            reasons.append(f"应激软降仓×{stress_scale}(nav/MA60-1={_dev_str(nav_val, ma60_val)})")
            tags.append("stress_soft")
        if cal_level == "soft":
            reasons.append(f"日历弱月({month}月)软降仓×{cal_scale}")
            tags.append(f"cal_{month}")
        if not reasons:
            reasons.append("正常")
        reason = "; ".join(reasons)

        return OverlayDecision(level, allow_new, position_scale, reason, tags)

    # ------------------------------------------------------------------
    # 序列构建(供回测 / 实时)
    # ------------------------------------------------------------------
    def build_series(self, nav: pd.Series, strategy: Optional[str] = None) -> Dict[str, OverlayDecision]:
        """给定等权全A净值 nav(pd.Series, 索引为交易日), 产出 {YYYY-MM-DD: OverlayDecision}."""
        ma60 = nav.rolling(60).mean()
        out: Dict[str, OverlayDecision] = {}
        for t in nav.index:
            k = str(t)[:10]
            nv = nav.get(t, np.nan)
            m60 = ma60.get(t, np.nan)
            if pd.isna(m60):
                out[k] = OverlayDecision("normal", True, 1.0, "MA60 预热期", [])
            else:
                out[k] = self.decide(t, nv, m60, strategy)
        return out

    # ------------------------------------------------------------------
    # 接入辅助
    # ------------------------------------------------------------------
    def allow_map(self, series: Dict[str, OverlayDecision]) -> Dict[str, bool]:
        """{date: allow_new} —— 直接喂给 HOQ.simulate 的 regime_at 或 C-Tail filter_fn."""
        return {d: s.allow_new for d, s in series.items()}

    def scale_map(self, series: Dict[str, OverlayDecision]) -> Dict[str, float]:
        """{date: position_scale} —— 软降仓比例, 供持仓规模缩放."""
        return {d: s.position_scale for d, s in series.items()}


def _dev_str(nav_val, ma60_val) -> str:
    if nav_val is None or ma60_val is None or pd.isna(nav_val) or pd.isna(ma60_val) or ma60_val == 0:
        return "n/a"
    return f"{(nav_val / ma60_val - 1) * 100:.1f}%"


# ---------------------------------------------------------------------------
# 便捷函数: 从 nav 直接产出 per-strategy overlay 序列
# ---------------------------------------------------------------------------
def build_overlay_series(nav: pd.Series, strategy: Optional[str] = None,
                         config: Optional[dict] = None) -> Dict[str, OverlayDecision]:
    mo = MacroOverwrite(config)
    return mo.build_series(nav, strategy)


def allow_fn(series: Dict[str, OverlayDecision]):
    """返回 filter_fn(t) -> bool, 供 C-Tail simulate_c 的 filter_fn 直接传入."""
    return lambda t: series.get(str(t)[:10], OverlayDecision("normal", True, 1.0, "", [])).allow_new


# ---------------------------------------------------------------------------
# 软降仓在「持仓规模」层面的忠实应用(不改 harness, 仅缩放已产出交易/期收益)
# ---------------------------------------------------------------------------
def scale_period_returns(dates: list, daily_rets: list, scale_map: Dict[str, float]) -> list:
    """对「再平衡型」策略(主动量 / C-Tail)逐期收益按开仓日 position_scale 缩放.

    这类策略每期收益相互独立地复利进净值, 直接缩放每期收益即可忠实反映降仓.
    """
    out = []
    for d, r in zip(dates, daily_rets):
        s = scale_map.get(str(d)[:10], 1.0)
        out.append(r * s)
    return out


def rebuild_equity_from_scaled_trades(trades: list, ctx: dict, cal_slice: list,
                                      scale_map: Dict[str, float], init_capital: float = 100_000.0,
                                      slip: float = 0.0):
    """对「槽位型」策略(低位绩优)忠实重建净值: 按开仓日 position_scale 缩放 shares,
    再逐日按收盘价市值重估(与 HOQ.simulate 内部记账口径一致).

    trades: HOQ.simulate 产出的交易列表(需含 code/buy_t/sell_t/shares/buy_px/sell_px).
    返回 (scaled_trades, eq_list).
    """
    cal_pos = {pd.Timestamp(t): i for i, t in enumerate(cal_slice)}

    scaled = []
    for tr in trades:
        s = scale_map.get(str(tr["buy_t"])[:10], 1.0)
        if s <= 0:
            continue  # 硬熔断日不开仓
        nsh = int(tr["shares"] * s)
        if nsh <= 0:
            continue
        ntr = dict(tr)
        ntr["shares"] = nsh
        ntr["pnl"] = nsh * (tr["sell_px"] - tr["buy_px"])
        scaled.append(ntr)

    # 逐日重放现金流 + 市值重估(口径同 HOQ.simulate)
    # 注意: trade 字典的 buy_px/sell_px 已由 HOQ 内含滑点, 此处不再重复加减滑点.
    cash = init_capital
    # 按买卖日索引组织
    buys = {}       # day_idx -> list of (code, shares, buy_px)
    sells = {}      # day_idx -> list of (code, shares, sell_px)
    for tr in scaled:
        bi = cal_pos.get(pd.Timestamp(tr["buy_t"]))
        si = cal_pos.get(pd.Timestamp(tr["sell_t"]))
        if bi is None or si is None:
            continue
        buys.setdefault(bi, []).append((tr["code"], tr["shares"], tr["buy_px"]))
        sells.setdefault(si, []).append((tr["code"], tr["shares"], tr["sell_px"]))
    eq = []
    for i, t in enumerate(cal_slice):
        # 卖出回款(清仓日: 头寸已不在市, 不计入市值重估)
        for code, sh, sp in sells.get(i, []):
            cash += sh * sp
        # 买入扣款
        for code, sh, bp in buys.get(i, []):
            cash -= sh * bp
        # 市值重估: 持仓区间为 [bi, si) —— 含买入日(按收盘价, 含滑点拖累), 不含清仓日
        mv = 0.0
        for tr in scaled:
            bi = cal_pos.get(pd.Timestamp(tr["buy_t"]))
            si = cal_pos.get(pd.Timestamp(tr["sell_t"]))
            if bi is None or si is None:
                continue
            if bi <= i < si:
                g = ctx.get(tr["code"])
                if g is not None and t in g.index:
                    mv += tr["shares"] * g.loc[t, "close"]
                else:
                    mv += tr["shares"] * tr["buy_px"]
        eq.append(cash + mv)
    return scaled, (eq if eq else [init_capital])


if __name__ == "__main__":
    # 自测: 用一段合成 nav 验证裁决
    import matplotlib
    idx = pd.date_range("2024-01-01", periods=300, freq="B")
    nav = pd.Series(np.linspace(1.0, 1.2, len(idx)), index=idx)
    mo = MacroOverwrite()
    ser = mo.build_series(nav, "momentum")
    print("样本裁决(前5):")
    for k in list(ser)[:5]:
        print(" ", k, ser[k])
    # 制造一段暴跌
    nav2 = nav.copy()
    nav2.iloc[-30:] = np.linspace(1.2, 0.95, 30)
    ser2 = mo.build_series(nav2, "momentum")
    hard = [k for k, v in ser2.items() if v.level == "hard"]
    soft = [k for k, v in ser2.items() if v.level == "soft"]
    print(f"暴跌段: hard={len(hard)} soft={len(soft)}")
    # 逆势策略默认关闭日历 + crash_only
    ser3 = mo.build_series(nav, "low_quality")
    cal_tag = [k for k, v in ser3.items() if any(t.startswith("cal_") for t in v.tags)]
    print(f"low_quality 日历弱月触发数={len(cal_tag)} (应=0, 因默认关闭)")
