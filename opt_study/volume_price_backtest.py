# -*- coding: utf-8 -*-
"""
价量口诀策略回测
================
买点: 信号日(t)次日收盘(对齐"盘后选股做计划, 盘中验证做执行"——此处用次日收盘近似盘中确认,
      因日频回测无盘中数据; 实盘扫描层给人工盘中确认入口)。
卖点: 固定持有 HOLD 交易日 或 跌破止损 STOP(默认 -8%)。
分策略: breakout(突破放量) / pullback(缩量回踩) 单独回测, 并合并。
分环境: 用大盘 proxy 把每笔交易划入 牛/熊/震荡, 看各环境胜率。
复用 harness_oversold_quality.simulate(它已含 N槽/冷却/同股禁重复/主题上限/质量过滤)。

用法:
  python opt_study/volume_price_backtest.py [--hold 15] [--stop -0.08] [--no-quality]
产物: opt_study/volume_price_backtest_report.html + _metrics.json + _trades.csv
"""
import os
import sys
import json
import sqlite3
import importlib.util
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location(
    "harness_oversold_quality", HERE + "/harness_oversold_quality.py")
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)
H.DB = os.path.join(ROOT, "qlib_pro_v16.db")
H.ROOT = ROOT
H.WINDOW_START = "2024-01-01"
H.WINDOW_END = "2099-12-31"

import opt_study.volume_price_strategy as VS  # noqa: E402

OUT_DIR = os.path.join(HERE, "volume_price_out")
os.makedirs(OUT_DIR, exist_ok=True)


def _load_names():
    p = os.path.join(ROOT, "data", "stock_names.json")
    if os.path.exists(p):
        try:
            return json.loads(open(p, encoding="utf-8").read())
        except Exception:
            pass
    return {}


def run(hold=15, stop=-0.08, quality_on=True):
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    print(f"  标的数={len(ctx)} 交易日={len(cal)}", flush=True)
    fmap = H.load_fundamentals() if quality_on else {}
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    names = _load_names()

    inv = VS.build_inv(ctx, cal, names, hot_at, regime,
                       use_theme_resonance=True, bull_only=True)
    print(f"  突破放量信号日数={len(inv['breakout'])} 缩量回踩信号日数={len(inv['pullback'])}", flush=True)

    cfg = dict(hot_on=False, quality_on=quality_on, pe_pb_on=True,
               theme_cap=0, regime_on=False)

    def sim(inv_map):
        # 把信号索引喂给 harness.simulate: 它要求 inv 的候选在 hot_on 关闭时不限热门,
        # 但我们的 inv 已含板块过滤, 故 hot_on=False, quality_on 透传。
        return H.simulate(ctx, cal, inv_map, hot_at, fmap, hold, "close", stop, cfg)

    res = {}
    for key in ("breakout", "pullback"):
        tr, eq = sim(inv[key])
        m = H.metrics(tr, eq)
        m["label"] = ("突破放量" if key == "breakout" else "缩量回踩")
        # 分环境
        env = defaultdict(list)
        for t in tr:
            env[regime.get(str(t["buy_t"])[:10], "ranging")].append(t["ret"])
        env_m = {k: dict(n=len(v), winrate=round(100 * sum(1 for x in v if x > 0) / len(v), 1),
                         avg=round(100 * np.mean(v), 2)) for k, v in env.items()}
        res[key] = dict(metrics=m, env=env_m, trades=tr)

    # 合并(两信号并集, 同一天同票去重)
    merged = defaultdict(list)
    seen = set()
    for key in ("breakout", "pullback"):
        for ts, codes in inv[key].items():
            for c in codes:
                if (ts, c) not in seen:
                    seen.add((ts, c))
                    merged[ts].append(c)
    tr_all, eq_all = sim(merged)
    m_all = H.metrics(tr_all, eq_all)
    m_all["label"] = "合并(突破+回踩)"
    env_all = defaultdict(list)
    for t in tr_all:
        env_all[regime.get(str(t["buy_t"])[:10], "ranging")].append(t["ret"])
    env_all_m = {k: dict(n=len(v), winrate=round(100 * sum(1 for x in v if x > 0) / len(v), 1),
                         avg=round(100 * np.mean(v), 2)) for k, v in env_all.items()}
    res["merged"] = dict(metrics=m_all, env=env_all_m, trades=tr_all)

    return dict(res=res, regime=regime, hold=hold, stop=stop, quality_on=quality_on)


def build_html(R):
    res = R["res"]
    regime = R["regime"]
    n_bull = sum(1 for v in regime.values() if v == "bull")
    n_bear = sum(1 for v in regime.values() if v == "bear")
    n_ran = sum(1 for v in regime.values() if v == "ranging")

    def kpi(m):
        return "".join(
            f"<div class='card'><b>{m[k]}</b>{lbl}</div>"
            for k, lbl in [("total_ret", "总收益%"), ("winrate", "胜率%"), ("n", "笔数"),
                          ("avg_ret", "平均每笔%"), ("sharpe", "夏普"), ("maxdd", "回撤%")])

    def env_block(env):
        if not env:
            return "<p style='color:#999'>无交易</p>"
        rows = "".join(
            f"<tr><td>{k}</td><td>{v['n']}</td><td>{v['winrate']}%</td><td>{v['avg']}%</td></tr>"
            for k, v in env.items())
        return ("<table><tr><th>环境</th><th>笔数</th><th>胜率</th><th>平均每笔</th></tr>"
                + rows + "</table>")

    sec = ""
    for key in ("breakout", "pullback", "merged"):
        d = res[key]
        m = d["metrics"]
        vcls = "verdict" if m["total_ret"] > 0 else "verdict warn"
        sec += (f"<h2>{m['label']}</h2><div class='{vcls}'>"
                f"总收益 {m['total_ret']}% ｜ 胜率 {m['winrate']}% ｜ 笔数 {m['n']} ｜ "
                f"夏普 {m['sharpe']} ｜ 回撤 {m['maxdd']}%</div>"
                f"<div class='kpi'>{kpi(m)}</div>"
                f"<h3>分环境</h3>{env_block(d['env'])}")

    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:21px}} h2{{font-size:16px;margin-top:24px;border-left:4px solid #2b6cb0;padding-left:8px}}
h3{{font-size:14px;margin-top:14px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}}
th{{background:#f4f6f8}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:10px 14px;min-width:88px;display:inline-block;margin:4px}}
.kpi{{margin-top:8px}}
.verdict{{background:#eafaf0;border:1px solid #9fd9b0;padding:10px 14px;border-radius:8px;font-size:14px;font-weight:600;margin-top:8px}}
.verdict.warn{{background:#fff4e5;border:1px solid #f0c36d}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6;margin-top:10px}}
</style></head><body>
<h1>价量口诀选股策略 · 回测报告</h1>
<p>买点=信号次日收盘 ｜ 卖点=固定持有 {R['hold']} 日 或 止损 {int(R['stop']*100)}% ｜
质量过滤={ '开' if R['quality_on'] else '关' } ｜ 窗口 {H.WINDOW_START}~{H.WINDOW_END}</p>
<div class='note'>大盘环境分布: 牛 {n_bull} 日 / 熊 {n_bear} 日 / 震荡 {n_ran} 日。
策略已叠加三大过滤器: 大盘(熊市放弃突破/回踩信号) + 板块主线共振(信号股须属当日热门题材) + 平台长度(突破前≥40日横盘)。
买点用次日收盘近似"盘中验证执行"(日频无盘中数据)。</div>
{sec}
<div class='note' style='margin-top:18px'><b>说明:</b>
① 板块共振依赖 stock_sector_cache 行业分类 + 资金流 proxy, 与已发布组合一致;
② 熊市过滤会让突破/回踩在下跌市几乎不出信号(符合口诀"下跌市放弃突破");
③ 样本偏小(各策略数笔~数十笔), 单笔影响大, 结论需 forward 复核。</div>
</body></html>"""
    return html


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold", type=int, default=15)
    ap.add_argument("--stop", type=float, default=-0.08)
    ap.add_argument("--no-quality", action="store_true")
    args = ap.parse_args()

    R = run(hold=args.hold, stop=args.stop, quality_on=not args.no_quality)

    res = R["res"]
    summary = {}
    for key in ("breakout", "pullback", "merged"):
        d = res[key]
        summary[key] = dict(metrics=d["metrics"], env=d["env"])

    json.dump(dict(hold=R["hold"], stop=R["stop"], quality_on=R["quality_on"],
                   summary=summary),
              open(os.path.join(OUT_DIR, "volume_price_backtest_metrics.json"), "w"),
              ensure_ascii=False, indent=2,
              default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)

    # 合并成交 CSV
    all_tr = res["merged"]["trades"]
    with open(os.path.join(OUT_DIR, "volume_price_backtest_trades.csv"), "w",
              newline="", encoding="utf-8-sig") as f:
        import csv
        w = csv.writer(f)
        w.writerow(["#", "代码", "买入日", "买价", "卖出日", "卖价", "收益%", "持有日", "退出"])
        for i, t in enumerate(all_tr, 1):
            w.writerow([i, t["code"], str(t["buy_t"])[:10], round(t["buy_px"], 2),
                        str(t["sell_t"])[:10], round(t["sell_px"], 2),
                        round(t["ret"] * 100, 2), t["hold_days"], t["reason"]])

    html = build_html(R)
    open(os.path.join(OUT_DIR, "volume_price_backtest_report.html"), "w").write(html)

    print("\n=== 回测汇总 ===")
    for key in ("breakout", "pullback", "merged"):
        m = res[key]["metrics"]
        print(f"  {m['label']}: n={m['n']} 胜率={m['winrate']}% 总收益={m['total_ret']}% "
              f"夏普={m['sharpe']} 回撤={m['maxdd']}%")
        print(f"    分环境: " + ", ".join(f"{k}:n={v['n']}/{v['winrate']}%" for k, v in res[key]["env"].items()))
    print(f"  报告: {os.path.join(OUT_DIR, 'volume_price_backtest_report.html')}")


if __name__ == "__main__":
    main()
