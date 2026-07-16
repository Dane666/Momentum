# -*- coding: utf-8 -*-
"""生成 C 方案(热门行业龙头) 止损优化对比报告 (HTML, 内联SVG, 无外部依赖)."""
import json, html
from pathlib import Path

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "compare3_stop_results.json", encoding="utf-8"))
res = d["results"]
meta = d["meta"]
best = d.get("best", {})

def get(N, hold, stop):
    for r in res:
        if r["N"] == N and r["hold"] == hold and abs(r["stop"] - stop) < 1e-9:
            return r
    return None

def stop_label(s):
    return "无止损" if s == 0 else f"止损{int(s*100)}%"

# ── 内联 SVG 柱状图 ──
def bar_chart(title, N, hold, metric, ylabel, fmt, color_fn):
    rows = [r for r in res if r["N"] == N and r["hold"] == hold]
    rows.sort(key=lambda x: x["stop"])
    labels = [stop_label(r["stop"]) for r in rows]
    vals = [r[metric] for r in rows]
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1.0
    W, H, padL, padB = 560, 230, 46, 34
    bw = (W - padL - 10) / len(rows)
    def y(v):
        return H - padB - (v - lo) / rng * (H - padB - 20)
    svg = [f'<svg viewBox="0 0 {W} {H}" width="100%" style="max-width:560px">']
    svg.append(f'<text x="{padL}" y="16" font-size="13" font-weight="600" fill="#222">{html.escape(title)}</text>')
    # y 轴刻度
    for k in range(3):
        vv = lo + rng * k / 2
        yy = y(vv)
        svg.append(f'<line x1="{padL}" y1="{yy:.1f}" x2="{W-10}" y2="{yy:.1f}" stroke="#e3e3e3"/>')
        svg.append(f'<text x="{padL-6}" y="{yy+3:.1f}" font-size="9" fill="#888" text-anchor="end">{fmt(vv)}</text>')
    for i, (lab, v) in enumerate(zip(labels, vals)):
        x = padL + i * bw + bw * 0.18
        w = bw * 0.64
        yy = y(v)
        col = color_fn(v, vals)
        svg.append(f'<rect x="{x:.1f}" y="{yy:.1f}" width="{w:.1f}" height="{H-padB-yy:.1f}" rx="3" fill="{col}"/>')
        svg.append(f'<text x="{x+w/2:.1f}" y="{yy-4:.1f}" font-size="9.5" fill="#333" text-anchor="middle">{fmt(v)}</text>')
        svg.append(f'<text x="{x+w/2:.1f}" y="{H-padB+12:.1f}" font-size="9" fill="#666" text-anchor="middle">{lab}</text>')
    svg.append('</svg>')
    return "".join(svg)

def color_by_best(v, vals):
    return "#2e7d32" if v == max(vals) else "#90caf9"

def color_by_dd(v, vals):
    return "#c62828" if v == min(vals) else "#ef9a9a"  # 回撤越小越好

# 头条配置 N=3 hold=3
HEAD = (3, 3)
charts = ""
charts += bar_chart(f"N={HEAD[0]} 持{HEAD[1]}天 · 收益(%)", *HEAD, "avg_profit", "收益%", lambda v: f"{v:.0f}", color_by_best)
charts += bar_chart(f"N={HEAD[0]} 持{HEAD[1]}天 · 胜率(%)", *HEAD, "avg_win_rate", "胜率%", lambda v: f"{v:.0f}", color_by_best)
charts += bar_chart(f"N={HEAD[0]} 持{HEAD[1]}天 · 夏普", *HEAD, "avg_sharpe", "夏普", lambda v: f"{v:.2f}", color_by_best)
charts += bar_chart(f"N={HEAD[0]} 持{HEAD[1]}天 · 最大回撤(%)", *HEAD, "avg_max_dd", "回撤%", lambda v: f"{v:.0f}", color_by_dd)

# ── 表格 ──
def table_for(N, hold):
    rows = [r for r in res if r["N"] == N and r["hold"] == hold]
    rows.sort(key=lambda x: x["stop"])
    b = best.get(f"N{N}_hold{hold}", {})
    best_stop = b.get("stop")
    h = [f'<table><thead><tr><th>止损</th><th>均收益%</th><th>均胜率%</th><th>均夏普</th><th>均回撤%</th><th>均交易</th><th>止损触发%</th></tr></thead><tbody>']
    for r in rows:
        hl = ' class="hl"' if abs(r["stop"] - best_stop) < 1e-9 else ""
        h.append(f'<tr{hl}><td>{stop_label(r["stop"])}</td><td>{r["avg_profit"]:.2f}</td>'
                 f'<td>{r["avg_win_rate"]:.2f}</td><td>{r["avg_sharpe"]:.2f}</td>'
                 f'<td>{r["avg_max_dd"]:.2f}</td><td>{r["avg_trades"]:.0f}</td>'
                 f'<td>{r["avg_stop_rate"]:.1f}</td></tr>')
    h.append('</tbody></table>')
    return "".join(h)

tables = ""
for N in (1, 2, 3):
    for hold in (3, 5):
        b = best.get(f"N{N}_hold{hold}", {})
        tag = stop_label(b.get("stop", 0))
        tables += f'<h3>N={N} · 持仓{hold}天 &nbsp;<span class="best">夏普最优 ≈ {tag}</span></h3>'
        tables += table_for(N, hold)

# 头条配置对比卡
nb = get(3, 3, 0.0)
n6 = get(3, 3, 0.06)
n8 = get(3, 3, 0.08)
n3 = get(3, 3, 0.03)
cards = f"""
<div class="cards">
  <div class="card"><div class="ct">无止损 (当前最优)</div>
    <div class="cv">{nb['avg_profit']:.0f}%</div><div class="cl">收益 · 胜率 {nb['avg_win_rate']:.0f}% · 夏普 {nb['avg_sharpe']:.2f} · 回撤 {nb['avg_max_dd']:.0f}%</div></div>
  <div class="card"><div class="ct">止损3% (典型紧止损)</div>
    <div class="cv warn">{n3['avg_profit']:.0f}%</div><div class="cl">收益腰斩! 胜率跌至 {n3['avg_win_rate']:.0f}% (whipsaw 震仓)</div></div>
  <div class="card"><div class="ct">止损6% (宽止损妥协)</div>
    <div class="cv">{n6['avg_profit']:.0f}%</div><div class="cl">保留 {(n6['avg_profit']/nb['avg_profit']*100):.0f}% 收益 · 回撤 {n6['avg_max_dd']:.0f}%</div></div>
  <div class="card"><div class="ct">止损8% (宽止损妥协)</div>
    <div class="cv">{n8['avg_profit']:.0f}%</div><div class="cl">保留 {(n8['avg_profit']/nb['avg_profit']*100):.0f}% 收益 · 胜率 {n8['avg_win_rate']:.0f}%</div></div>
</div>"""

HTML = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>C方案止损优化报告</title>
<style>
*{{box-sizing:border-box}}
body{{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;margin:0;background:#f5f6f8;color:#222;line-height:1.55}}
header{{background:linear-gradient(135deg,#1e3a5f,#2e6da4);color:#fff;padding:26px 30px}}
header h1{{margin:0 0 6px;font-size:22px}}
.sub{{opacity:.9;font-size:13px}}
.meta-bar{{margin-top:12px;display:flex;gap:8px;flex-wrap:wrap}}
.chip{{background:rgba(255,255,255,.16);border-radius:20px;padding:3px 11px;font-size:12px}}
section{{max-width:980px;margin:22px auto;padding:0 18px}}
h2{{font-size:18px;border-left:4px solid #2e6da4;padding-left:10px;margin:28px 0 12px}}
h3{{font-size:15px;margin:22px 0 8px;color:#1e3a5f}}
.card-box{{background:#fff;border-radius:10px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:14px 0}}
.card{{background:#fff;border:1px solid #e6e9ee;border-radius:10px;padding:14px}}
.card .ct{{font-size:13px;color:#555;margin-bottom:6px}}
.card .cv{{font-size:26px;font-weight:700;color:#1e7d32}}
.card .cv.warn{{color:#c62828}}
.card .cl{{font-size:12px;color:#777;margin-top:4px}}
table{{border-collapse:collapse;width:100%;font-size:13px;background:#fff;margin:6px 0 4px}}
th,td{{border:1px solid #e6e9ee;padding:6px 9px;text-align:center}}
th{{background:#eef3f8;font-weight:600;color:#334}}
tr.hl{{background:#fff8e1}}
tr.hl td:first-child{{font-weight:700;color:#b8860b}}
.best{{font-size:12px;color:#b8860b;background:#fff8e1;padding:2px 8px;border-radius:6px}}
.callout{{background:#e8f4ff;border:1px solid #b6d8f5;border-radius:9px;padding:13px 16px;margin:12px 0;font-size:13.5px}}
.callout b{{color:#0d47a1}}
.note{{font-size:12px;color:#888;margin:6px 0 2px}}
.chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin:10px 0}}
footer{{text-align:center;color:#aaa;font-size:12px;padding:24px}}
code{{background:#eef;padding:1px 5px;border-radius:4px;font-size:12px}}
</style></head><body>
<header>
  <h1>C 方案(热门行业龙头)· 真实账户级回测 + 止损优化</h1>
  <div class="sub">问题:① 为什么交易次数多? ② 加止损并找出最优止损点 · 持仓按个股交易日计, 严格对齐原 C 口径</div>
  <div class="meta-bar">
    <span class="chip">区间 {meta['calendar_start']}~{meta['calendar_end']}</span>
    <span class="chip">股票 {meta['universe']} 只</span>
    <span class="chip">窗口 shift 0/20/40/60 (8组合)</span>
    <span class="chip">滑点 {meta['slippage']*100:.1f}%(双边)</span>
    <span class="chip">热门行业 top-{meta['top_k']} (资金净流入)</span>
  </div>
</header>

<section>
  <h2>一、为什么 C 的"交易次数多" — 及修正</h2>
  <div class="callout">
    <b>结论:251 笔交易对 N=3 持3天 是真实且合理的, 并非"无限并行买"。</b><br>
    这是一个 <b>3 个仓位槽 × 每3天轮换</b> 的策略:每个槽满仓才能买、卖出(到期或止损)才释放槽位——
    即"上一只没卖出就没钱买下一只"已被本模型<b>显式强制</b>。在 250 交易日窗口内,
    每槽约 84 次轮动 × 3 槽 ≈ 250 笔, 与原 naive C(<code>run_strategy</code>)的 250 笔完全一致。<br>
    <span class="note">注:此前出现的 503 笔是窗口 bug(误用完整 513 天而非最后 250 天), 已修正。本回测复用 <code>slice_test_dates</code> 与原 C 严格对齐。</span>
  </div>
</section>

<section>
  <h2>二、头条配置 N=3 · 持3天 — 止损扫描</h2>
  <div class="chart-grid">{charts}</div>
  <div class="callout">
    <b>反直觉但关键:</b> 对 N=3 持3天, <b>不加止损</b> 反而收益(283%)、胜率(51%)、夏普(2.24)全最高。
    3% 紧止损把收益腰斩到 143%、胜率砸到 32% —— 因为 3 天龙头常出现盘中 −3% 假摔后收回,
    紧止损把<b>赢家震仓出局(whipsaw)</b>, 止损成本 &gt; 保护收益。<br>
    若你坚持要一道"防崩"硬止损, 应设 <b>宽止损 6%~8%</b>: 保留 ~83%~90% 收益、胜率仍近 48%、回撤还略低。
  </div>
  <div class="card-box">{cards}</div>
</section>

<section>
  <h2>三、全配置止损扫描明细</h2>
  {tables}
  <div class="note">"夏普最优"列按 (夏普, 收益) 降序选取。可见: 持5天配置整体远弱于持3天(收益近 0 或剧烈波动),
  说明该龙头策略的<b>最优持有周期是 3 天</b>, 而非更长。持3天 + 无止损(或宽止损) 是该方案的最优区。</div>
</section>

<section>
  <h2>四、落地建议</h2>
  <div class="callout">
    <b>① 你实际在跑的 N=3 持3天:</b> 数据显示<b>无需紧止损</b>。该策略的风险已由"热门行业(资金净流入)过滤 + 3天短持有"结构自身吸收。<br>
    <b>② 若必须加硬止损(防黑天鹅):</b> 设 <code>止损 = 7%~8%</code>, 不要设 3%~5%。宽止损只砍真崩盘, 不震赢家。<br>
    <b>③ 更优的风险控制</b> 仍是此前验证的<b>市场择时闸口(破MA20空仓)</b>——它在框架型策略(A)上稳健降回撤,
    但需注意:对 3 天龙头, MA20 日级闸口偏粗、易误杀, 故本方案(B)此前实测不佳。龙头策略更适合用"<b>宽止损 + 热门行业过滤</b>"而非大盘择时。<br>
    <b>④ 胜率约 5 成</b>是该策略固有特征(追强势股、胜率本就不高), 收益靠"少数大赢家"拉动, 故更需<b>分散到 N=3</b> 而非单押。
  </div>
  <div class="note">模型透明说明: 无止损基线 ≈ +283%(8窗口均值), 与原 naive C(+244%~+191%)同量级;
  差异来自槽位模型对停牌股的真实处理——持仓股停牌期间无法卖出/换仓(原 C 用理想化均值), 本模型更贴近实盘。
  所有止损扫描在同一模型下完成, 内部可比。</div>
</section>

<footer>动量策略优化研究 · opt_study/harness_compare3_stop.py · 数据 compare3_stop_results.json</footer>
</body></html>"""

(HERE / "compare3_stop_report.html").write_text(HTML, encoding="utf-8")
print("生成 compare3_stop_report.html OK")
