# -*- coding: utf-8 -*-
"""价量口诀·盘后选股(做计划)扫描。

盘后(15:00 后)对最新交易日 T 的收盘定型数据跑信号, 选出"突破放量 / 缩量回踩"
预选池, 给出次日盘中"回踩支撑位买点参考价"与操作提示。

设计原则(对应原文"盘后选股做计划, 盘中验证做执行"):
  - 本脚本只做"盘后计划": 选出信号股 + 给出次日盘中支撑位买点, 不自动下单。
  - 真正买入 = 人工次日盘中按量比确认"回踩支撑位附近 + 抛压衰竭"后低吸
    (即回测中验证有效的 dip_buf 回踩低吸买点, 而非追突破当天/盲买次日开盘)。
  - 计划池登记到 tracking(status=PLAN): position_monitor 不监控 PLAN(避免误判止损),
    但 CI 通用 Bark 步骤仍会读取并推送; 本脚本也直接 Bark 一条结构化计划池消息。

回测依据(entry_study): 机械次日买(开/收) 胜率24-35%且亏损; 改"次日盘中回踩支撑低吸"
(dip_buf) 后翻正 —— 突破放量 hold10/止损-8% → 胜率60%/+27.8%; 缩量回踩 hold20/止损-5%
→ 胜率44%/+47.3%。故计划池买点锚定各自支撑位。
"""
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

import importlib.util
import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "opt_study"))

logger = logging.getLogger("vol_price_scan")

# 策略模块(纯信号计算, 不读 DB)
import volume_price_strategy as VS  # noqa: E402


def _load_harness():
    p = ROOT / "opt_study" / "harness_oversold_quality.py"
    spec = importlib.util.spec_from_file_location("harness_oversold_quality", p)
    H = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(H)
    H.DB = os.path.join(str(ROOT), "qlib_pro_v16.db")
    H.ROOT = str(ROOT)
    # 放开到 DB 实际覆盖范围(否则 harness 默认 WINDOW_END=2026-07-15 会截断最新盘后数据)
    H.WINDOW_START = "2024-01-01"
    H.WINDOW_END = "2099-12-31"
    return H


def _load_names():
    names = {}
    try:
        f = ROOT / "data" / "stock_names.json"
        if f.exists():
            d = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(d, dict):
                names = {str(k): v for k, v in d.items()}
            elif isinstance(d, list):
                for it in d:
                    if isinstance(it, dict) and "code" in it:
                        names[str(it["code"])] = it.get("name", it["code"])
    except Exception:
        pass
    return names


def support_breakout(g, i, N=60):
    """突破放量支撑位 = 信号日之前 N 日实体最高价(被突破的平台高点)。"""
    pre = g.iloc[max(0, i - N):i]
    if pre.empty:
        return float(g.iloc[i]["close"])
    return float(max(max(c, o) for c, o in zip(pre["close"], pre["open"])))


def support_pullback(g, i):
    """缩量回踩支撑位 = 当日 MA20(回踩的均线支撑)。"""
    ma = g["close"].rolling(20).mean()
    v = ma.iloc[i]
    return float(v) if not np.isnan(v) else float(g.iloc[i]["close"])


def detect(ctx, cal, ts, names, hot_at, regime,
           min_history=120, use_theme=True, bull_only=True):
    """对扫描日 ts 产出候选 list: {code,name,kind,support,close}。"""
    out = []
    rg = regime.get(ts, "ranging")
    if bull_only and rg == "bear":
        return out  # 熊市放弃所有信号(口诀: 熊市仅"放量砸盘恐慌止"有用, 此处不建模空方)
    hot0 = hot_at.get(ts, (set(), [], {}))[0]
    for code, g in ctx.items():
        g = VS.ensure_ctx_indicators(g)
        if g.empty:
            continue
        if ts not in g.index:
            continue
        i = g.index.get_loc(ts)
        if i < min_history:
            continue
        close = float(g.loc[ts, "close"])
        name = names.get(code, code)
        if VS._is_garbage(name, close):
            continue
        # 板块主线共振: 信号股须属当日资金流 TOP_K 热门题材(主力在场)
        if use_theme and code not in hot0:
            continue
        sb = bool(VS.signal_breakout(g).iloc[i])
        sp = bool(VS.signal_pullback(g).iloc[i])
        if sb and VS.platform_filter(g, ts):
            sup = support_breakout(g, i, VS.N_BREAK)
            out.append(dict(code=code, name=name, kind="breakout",
                            support=round(sup, 2), close=round(close, 2)))
        if sp:
            sup = support_pullback(g, i)
            out.append(dict(code=code, name=name, kind="pullback",
                            support=round(sup, 2), close=round(close, 2)))
    return out


def rank(cands, top_n=20):
    """按"次日回踩到支撑位的难度"升序: gap 越小 = 现价越贴近支撑 = 越易回踩买入。"""
    for c in cands:
        c["gap"] = round((c["close"] - c["support"]) / c["close"] * 100, 2)
    cands.sort(key=lambda c: c["gap"])
    return cands[:top_n]


def build_picks(cands):
    picks = []
    for c in cands:
        if c["kind"] == "breakout":
            sl, tp = 0.92, 1.10   # 对应回测最优 stop=-8%
        else:
            sl, tp = 0.95, 1.10   # 对应回测最优 stop=-5%
        note = (
            f"突破放量: 次日盘中回踩¥{c['support']}附近(±2%)且量比企稳低吸; "
            f"跌破¥{round(c['support']*0.98,2)}放弃"
            if c["kind"] == "breakout" else
            f"缩量回踩: 次日盘中回踩¥{c['support']}(20日线)附近缩量企稳低吸; "
            f"跌破¥{round(c['support']*0.98,2)}止损放弃"
        )
        picks.append(dict(code=c["code"], name=c["name"], price=c["support"],
                          sl_ratio=sl, tp_ratio=tp, kind=c["kind"],
                          support=c["support"], close=c["close"], gap=c["gap"],
                          status="PLAN", note=note))
    return picks


def bark_push(title, body):
    key = os.environ.get("BARK_DEVICE_KEY", "").strip()
    if not key:
        return
    import requests
    if key.startswith("http"):
        parts = key.rstrip("/").split("/")
        key = parts[-1] if parts[-1] else (parts[-2] if len(parts) > 1 else key)
    try:
        requests.post("https://api.day.app/push",
                      json={"device_key": key, "title": title,
                            "body": body[:3800], "group": "VolPrice"}, timeout=10)
    except Exception as e:
        logger.warning(f"Bark 推送失败: {e}")


def run(scan_date=None, top_n=20, no_track=False, no_bark=False, bull_only=True):
    H = _load_harness()
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    if not cal:
        logger.error("无K线数据")
        return []
    if scan_date:
        cand = [t for t in cal if str(t)[:10] <= scan_date]
        T = cand[-1] if cand else cal[-1]
    else:
        T = cal[-1]
    ts = str(T)[:10]
    names = _load_names()
    fmap = H.load_fundamentals()
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)

    cands = detect(ctx, cal, ts, names, hot_at, regime, bull_only=bull_only)
    cands = rank(cands, top_n)
    picks = build_picks(cands)

    # ---- 控制台报告 ----
    rg = regime.get(ts, "ranging")
    print(f"\n{'='*64}\n  价量口诀·盘后计划池 | 扫描日 {ts} | 候选 {len(cands)} 只 (取 top{top_n})\n{'='*64}")
    print(f"  大盘环境: {rg}  ｜ 板块共振过滤: 开 ｜ 熊市放弃: 开 ｜ 小盘倾斜: 不涉及")
    for k, label in (("breakout", "突破放量"), ("pullback", "缩量回踩")):
        sub = [c for c in cands if c["kind"] == k]
        print(f"\n  ── {label} ({len(sub)}只) ──")
        for c in sub:
            print(f"   {c['code']} {c['name']:<8} 支撑¥{c['support']:<8} 现价¥{c['close']:<8} 回踩空间{c['gap']}%")
    print("\n  操作: 次日盘中按量比确认回踩支撑位附近低吸, 不回踩不买 (计划池, 非持仓)")

    # ---- 登记计划池(PLAN 状态, position_monitor 不监控) ----
    if not no_track and picks:
        from tools.tracking_utils import add_picks
        b = [dict(code=p["code"], name=p["name"], price=p["support"], status="PLAN")
             for p in picks if p["kind"] == "breakout"]
        p = [dict(code=p["code"], name=p["name"], price=p["support"], status="PLAN")
             for p in picks if p["kind"] == "pullback"]
        n1 = add_picks(b, "VP_BREAKOUT", sl_ratio=0.92, tp_ratio=1.10, status="PLAN") if b else 0
        n2 = add_picks(p, "VP_PULLBACK", sl_ratio=0.95, tp_ratio=1.10, status="PLAN") if p else 0
        logger.info(f"登记计划池: 突破{n1} 回踩{n2}")

    # ---- 直接 Bark 结构化计划池(含"无信号"回执) ----
    if not no_bark:
        if picks:
            lines = [f"📐 价量盘后计划池 | {ts}", f"环境:{rg}"]
            for k, label in (("breakout", "突破放量"), ("pullback", "缩量回踩")):
                sub = [c for c in cands if c["kind"] == k]
                if not sub:
                    continue
                lines.append(f"【{label}】(次日盘中回踩支撑位低吸)")
                for c in sub:
                    lines.append(f"  {c['code']} {c['name']} 支撑¥{c['support']} 空间{c['gap']}%")
            lines.append("操作: 盘中量比确认回踩支撑位附近低吸, 不回踩不买")
            bark_push(f"价量盘后计划池 {ts}", "\n".join(lines))
            logger.info(f"已推送计划池 Bark ({len(picks)} 只)")
        else:
            # 无信号回执: 让用户确认扫描已执行(熊市/无板块共振等均可能为空, 避免静默误以为未跑)
            reason = ("熊市门禁已放弃所有信号" if (bull_only and rg == "bear")
                      else "无符合板块主线共振的价量信号")
            bark_push(f"价量盘后计划池 {ts}",
                      f"✅ 扫描完成 | 大盘环境:{rg}\n今日无符合条件信号\n({reason})\n次日继续监控")
            logger.info("已推送无信号回执 Bark")

    # ---- 存计划池 JSON(含操作提示) ----
    try:
        out = ROOT / "data" / "volume_price_plan.json"
        os.makedirs(out.parent, exist_ok=True)
        out.write_text(json.dumps(dict(date=ts, regime=rg,
                                       picks=[{k: p[k] for k in ("code", "name", "kind",
                                                "support", "close", "gap", "note")}
                                              for p in picks]),
                                  ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return picks


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=os.environ.get("VPSCAN_DATE"))
    ap.add_argument("--top", type=int, default=int(os.environ.get("VP_TOP_N", "20")))
    ap.add_argument("--no-track", action="store_true")
    ap.add_argument("--no-bark", action="store_true")
    ap.add_argument("--no-bull-filter", action="store_true",
                   help="演示/验证用: 关闭熊市放弃(真实盘后扫描应保持默认开)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run(scan_date=a.date, top_n=a.top, no_track=a.no_track, no_bark=a.no_bark,
        bull_only=not a.no_bull_filter)
