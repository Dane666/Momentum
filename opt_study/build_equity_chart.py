# -*- coding: utf-8 -*-
"""
画出 龙头策略 三条净值曲线叠加 + 大盘vsMA60 对照, 并把"开盘前版"的空仓信号日标出。
自包含 SVG(无外网依赖), 输出 c_ma60_equity_chart.html。
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TESTS = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(TESTS))

import harness as H
from momentum import config as cfg
from harness_sector import build_sector_heat, slice_test_dates
from harness_compare3 import build_day_returns, topn_leaders
from harness_compare3_stop import build_price_lookup
from harness_c_ma60 import simulate_with_log_gated, metrics_from_equity

INIT_CAPITAL = 100_000.0
TOP_K = 8
N = 3
HOLD = 3


def svg_line_chart(series, xlabels, bands, y_label, title, h=460):
    """series: list of dict(name,color,values); xlabels: list[str]; bands: set of x indices"""
    W = len(xlabels)
    left, right, top, bot = 72, 30, 34, 56
    width, plotw = 1120, 1120 - left - right
    ploth = h - top - bot
    allv = [v for s in series for v in s["values"] if v is not None]
    ymin, ymax = min(allv), max(allv)
    pad = (ymax - ymin) * 0.06 or 1.0
    ymin, ymax = ymin - pad, ymax + pad

    def X(i):
        return left + (i / (W - 1)) * plotw if W > 1 else left

    def Y(v):
        return top + (ymax - v) / (ymax - ymin) * ploth

    # 网格 + Y刻度
    yticks = ""
    for k in range(6):
        v = ymin + (ymax - ymin) * k / 5
        y = Y(v)
        yticks += f'<line x1="{left}" y1="{y:.1f}" x2="{left+plotw}" y2="{y:.1f}" stroke="#eee"/>'
        yticks += f'<text x="{left-8}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#888">{v:,.0f}</text>'
    # X刻度(每 ~W/8)
    xticks = ""
    step = max(1, W // 8)
    for i in range(0, W, step):
        x = X(i)
        xticks += f'<text x="{x:.1f}" y="{top+ploth+18:.1f}" text-anchor="middle" font-size="11" fill="#888">{xlabels[i][5:]}</text>'
    # 空仓带
    bands_svg = ""
    bw = max(1.5, plotw / W * 1.4)
    for bi in bands:
        x = X(bi)
        bands_svg += f'<rect x="{x-bw/2:.1f}" y="{top}" width="{bw:.1f}" height="{ploth}" fill="#ff5252" opacity="0.10"/>'
    # 曲线
    paths = ""
    legend = ""
    for s in series:
        pts = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in enumerate(s["values"]) if v is not None)
        paths += f'<polyline points="{pts}" fill="none" stroke="{s["color"]}" stroke-width="1.8"/>'
        legend += f'<span style="color:{s["color"]};font-weight:700;margin-right:16px">■ {s["name"]}</span>'
    return f'''<div class="chart"><h3>{title}</h3>
    <div style="margin:2px 0 8px;font-size:12.5px">{legend}</div>
    <svg viewBox="0 0 {width} {h}" width="100%" preserveAspectRatio="xMidYMid meet">
    {yticks}{xticks}{bands_svg}{paths}
    <line x1="{left}" y1="{top}" x2="{left}" y2="{top+ploth}" stroke="#ccc"/>
    <line x1="{left}" y1="{top+ploth}" x2="{left+plotw}" y2="{top+ploth}" stroke="#ccc"/>
    <text x="{left-60}" y="{top-12}" font-size="12" fill="#555">{y_label}</text>
    </svg></div>'''


def main():
    print("[1/4] 载入数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    mkt_nav_s, _ = H.build_market_proxy(data_cache, calendar)
    ma60_s = mkt_nav_s.rolling(60).mean()

    td = slice_test_dates(calendar, HOLD, 0)
    reb = td[::HOLD]
    cal_idx = {t: i for i, t in enumerate(calendar)}
    sig_set = set(cal_idx[d] for d in reb if d in cal_idx)
    lo = min(sig_set)
    window = calendar[lo:]
    xlabels = [str(t)[:10] for t in window]

    def run(gate):
        _, eq, _, _, _ = simulate_with_log_gated(
            N, HOLD, 0.0, calendar, price_lookup, date_idx, date_list,
            hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, gate=gate)
        return eq  # aligned from lo

    print("[2/4] 跑三变体净值曲线 ...", flush=True)
    eq_base = run(None)                       # 始终在场
    # open: gate[T] = cross[T-1]
    cross = {}
    for t in calendar:
        ts = pd.Timestamp(t); nav = mkt_nav_s.get(ts, np.nan); m = ma60_s.get(ts, np.nan)
        cross[ts] = bool(nav > m) if (pd.notna(nav) and pd.notna(m)) else True
    gate_open = {}
    for t in calendar:
        ts = pd.Timestamp(t); i = cal_idx[t]
        p = calendar[i - 1] if i - 1 >= 0 else None
        gate_open[ts] = cross[pd.Timestamp(p)] if p is not None else True
    eq_open = run(gate_open)
    # close1445_proxy: gate[T] = cross[T]
    gate_close = {pd.Timestamp(t): cross[pd.Timestamp(t)] for t in calendar}
    eq_close = run(gate_close)

    # 对齐到 window
    def align(eq):
        return list(eq)  # eq already from lo -> aligns to window
    b = align(eq_base); o = align(eq_open); c = align(eq_close)

    # 空仓信号日(open) -> 在 window 中的索引
    wait_idx = set()
    for t in reb:
        ts = pd.Timestamp(t)
        if not gate_open.get(ts, True):
            wait_idx.add(cal_idx[t] - lo)

    # 大盘 vs MA60(仅 window 段)
    nav_win = [mkt_nav_s.get(pd.Timestamp(t), np.nan) for t in window]
    ma_win = [ma60_s.get(pd.Timestamp(t), np.nan) for t in window]

    print(f"      窗口长度={len(window)} 空仓信号日={len(wait_idx)}", flush=True)

    chart1 = svg_line_chart(
        [{"name": "始终在场", "color": "#9e9e9e", "values": b},
         {"name": "开盘前(最优)", "color": "#1565c0", "values": o},
         {"name": "14:45(当日收盘)", "color": "#ef6c00", "values": c}],
        xlabels, wait_idx, "账户净值(¥)", "一、三变体净值曲线叠加(红线=开盘前版空仓信号日)")
    chart2 = svg_line_chart(
        [{"name": "等权全A净值", "color": "#2e7d32", "values": nav_win},
         {"name": "MA60", "color": "#c62828", "values": ma_win}],
        xlabels, wait_idx, "指数水平", "二、大盘 vs MA60 对照(同一批红线=开盘前版空仓日)")

    # 空仓日清单(前若干)
    wait_dates = [xlabels[i] for i in sorted(wait_idx)]
    wait_list = "、".join(wait_dates[:40]) + (" …" if len(wait_dates) > 40 else "")

    mb = metrics_from_equity(o)
    HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>龙头策略净值曲线对比</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1180px;margin:0 auto}} h1{{font-size:21px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:18px}}
.card{{background:#fff;border-radius:12px;padding:18px 20px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.chart{{margin-top:6px}} .chart h3{{font-size:15px;margin:0 0 4px;color:#1a4}}
.note{{font-size:13.5px;line-height:1.78;color:#444}} .note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{flex:1;min-width:130px;background:#f0f6ff;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:18px;color:#156}} .kpi span{{font-size:12px;color:#667}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:14px}}
</style></head><body><div class="wrap">
<h1>龙头策略 · 净值曲线叠加对比(开盘前版最优)</h1>
<div class="sub">初始资金 ¥{INIT_CAPITAL:,} · N=3 · 持3天 · 无止损 · 回测窗口 {xlabels[0]}~{xlabels[-1]} ·
红线标记"开盘前版"的空仓信号日, 便于核对空仓期是否落在大盘弱势段</div>

<div class="card"><h2>关键指标(持3天, 10万)</h2>
<div class="kpi">
<div><b>¥{b[-1]:,.0f}</b><span>始终在场 期末</span></div>
<div><b>¥{o[-1]:,.0f}</b><span>开盘前(最优) 期末</span></div>
<div><b>¥{c[-1]:,.0f}</b><span>14:45 期末</span></div>
<div><b>{mb['最大回撤%']:.1f}%</b><span>开盘前 最大回撤</span></div>
<div><b>{len(wait_idx)}</b><span>开盘前 空仓信号日</span></div>
</div></div>

<div class="card">{chart1}</div>
<div class="card">{chart2}</div>

<div class="card"><h2>三、怎么读这两张图</h2>
<p class="note">
① <b>图一</b>里灰色(始终在场)曲线一路向上但波动大; 蓝色(开盘前)曲线在<b>红色竖带</b>处变平——那就是空仓等待期, 现金不动、不接飞刀。<br>
② <b>图二</b>把同样的红色竖带叠在大盘(绿)与 MA60(红)上: 可以清楚看到, 开盘前版的空仓日<b>基本都落在"大盘贴近或跌破 MA60"的弱势段</b>, 即闸口确实在趋势转弱时空手。<br>
③ 蓝色曲线在空仓期"少跌"、在行情段"跟涨", 因此最终收益(+{mb['总收益%']:+.1f}%)比始终在场(+{(metrics_from_equity(b)['总收益%']):+.1f}%)还高、回撤却更低。<br>
④ 个别红线可能出现在大盘仍略高于 MA60 的位置——那是"T-1 已破、但 T 当日小反抽"的边界日, 属正常(开盘前用 T-1 判定)。
</p>
<p class="note"><b>开盘前版空仓信号日清单(共 {len(wait_dates)} 个):</b><br><code>{wait_list}</code></p>
</div></div></body></html>"""
    out = HERE / "c_ma60_equity_chart.html"
    out.write_text(HTML, encoding="utf-8")
    print(f"完成 -> {out.name}")


if __name__ == "__main__":
    main()
