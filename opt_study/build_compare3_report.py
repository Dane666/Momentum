# -*- coding: utf-8 -*-
"""读取 compare3_results.json, 生成 三策略 × 1/2/3 只股 对比报告 (内联SVG)。"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "compare3_results.json", encoding="utf-8"))
meta = d["meta"]
R = d["results"]

def find(strategy, N, hold):
    for r in R:
        if r["strategy"] == strategy and r["N"] == N and r["hold"] == hold:
            return r
    return None

# 分类
A = [r for r in R if r["strategy"] == "A"]          # 3 rows (hold=5)
B = [r for r in R if r["strategy"] == "B"]          # 6 rows
C = [r for r in R if r["strategy"] == "C"]          # 6 rows

# 全局最优
best_win = max(R, key=lambda x: x["avg_win_rate"])
best_ret = max(R, key=lambda x: x["avg_profit"])
best_sharpe = max(R, key=lambda x: x["avg_sharpe"])

# ---- SVG ----
def vbar(series, ymax, h=270, w=860, unit="%"):
    n = len(series)
    pad_l, pad_r, pad_t, pad_b = 48, 16, 18, 70
    plot_w = w - pad_l - pad_r; plot_h = h - pad_t - pad_b
    bw = plot_w / n * 0.62; gap = plot_w / n
    svg = [f'<svg viewBox="0 0 {w} {h}" width="100%" style="max-width:{w}px" xmlns="http://www.w3.org/2000/svg">']
    for g in range(0, int(ymax) + 1, max(1, int(ymax // 5))):
        y = pad_t + plot_h * (1 - g / ymax)
        svg.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#eef0f3"/>')
        svg.append(f'<text x="{pad_l-6}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#8b93a1">{g}{unit}</text>')
    for i, (lab, val, col) in enumerate(series):
        cx = pad_l + gap * (i + 0.5); x = cx - bw / 2
        if val >= 0:
            yv = pad_t + plot_h * (1 - val / ymax); hh = pad_t + plot_h - yv
            svg.append(f'<rect x="{x:.1f}" y="{yv:.1f}" width="{bw:.1f}" height="{hh:.1f}" rx="3" fill="{col}"/>')
            svg.append(f'<text x="{cx:.1f}" y="{yv-5:.1f}" text-anchor="middle" font-size="11.5" font-weight="700" fill="{col}">{val:.1f}</text>')
        else:
            y0 = pad_t + plot_h; yv = pad_t + plot_h * (1 - val / ymax); hh = y0 - yv
            svg.append(f'<rect x="{x:.1f}" y="{yv:.1f}" width="{bw:.1f}" height={hh:.1f} rx="3" fill="{col}"/>')
            svg.append(f'<text x="{cx:.1f}" y="{y0+14:.1f}" text-anchor="middle" font-size="11.5" font-weight="700" fill="{col}">{val:.1f}</text>')
        lines = lab.split("\n")
        for li, ln in enumerate(lines):
            svg.append(f'<text x="{cx:.1f}" y="{pad_t+plot_h+18+li*13:.1f}" text-anchor="middle" font-size="10.5" fill="#5b6270">{ln}</text>')
    svg.append('</svg>')
    return "\n".join(svg)

# 胜率图 (全部15配置)
series_win = []
for r in R:
    lab = f"{r['name'].split('(')[0].replace('多因子+热门闸口+择时','A框架').replace('龙头+择时','B龙头+择').replace('龙头(无择时)','C龙头')}\nN={r['N']} h={r['hold']}"
    col = "#e03131" if r["strategy"] == "A" else ("#f08c00" if r["strategy"] == "B" else "#1c7ed6")
    series_win.append((lab, r["avg_win_rate"], col))
chart_win = vbar(series_win, ymax=60)

# 收益图
series_ret = [(lab, r["avg_profit"], col) for (lab, _, col), r in zip(
    [(s[0], s[1], s[2]) for s in series_win], R)]
chart_ret = vbar(series_ret, ymax=260)

# ---- 表格 ----
def row(cells): return "<tr>" + "".join(cells) + "</tr>"
def td(x, cls=""): return f'<td class="num {cls}">{x}</td>'
def tag_best(s):
    return f'<span class="badge-win">最佳</span>' if s else ''

tbl = ("<table><thead><tr><th>策略</th><th class='num'>N(只)</th><th class='num'>持有</th>"
       "<th class='num'>均收益</th><th class='num'>均胜率</th><th class='num'>均夏普</th>"
       "<th class='num'>均回撤</th><th class='num'>均交易</th></tr></thead><tbody>")
for grp, label in [(A, "A 多因子+热门闸口+择时"), (B, "B 龙头+择时(破MA20空仓)"), (C, "C 龙头(无择时)")]:
    tbl += f"<tr class='grp'><td colspan='8'><b>{label}</b></td></tr>"
    for r in sorted(grp, key=lambda x: (x['N'], x['hold'])):
        is_win = (r is best_win); is_ret = (r is best_ret)
        tbl += row([
            td("—"),
            td(r["N"]),
            td(f"{r['hold']}天"),
            td(f"{r['avg_profit']:+.2f}%", "up" if r["avg_profit"] > 0 else "down") + (tag_best(is_ret) if is_ret else ""),
            td(f"{r['avg_win_rate']:.2f}%", "up") + (tag_best(is_win) if is_win else ""),
            td(f"{r['avg_sharpe']:.2f}"),
            td(f"{r['avg_max_dd']:.2f}%", "down" if r["avg_max_dd"] > 25 else ""),
            td(f"{r['avg_trades']:.1f}"),
        ])
tbl += "</tbody></table>"

# 关键结论数字
a1 = find("A", 1, 5); a2 = find("A", 2, 5)
c33 = find("C", 3, 3); b33 = find("B", 3, 3)
a_win = max(A, key=lambda x: x["avg_win_rate"])

HTML = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>三策略回测对比报告</title>
<style>
:root{{--bg:#f5f6f8;--card:#fff;--ink:#1a1d24;--sub:#5b6270;--line:#e6e8ec;
--up:#e03131;--down:#2f9e44;--accent:#1c7ed6;--gold:#f08c00;--hl:#fff4e6}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,"PingFang SC","Microsoft YaHei",Segoe UI,sans-serif;line-height:1.6}}
.wrap{{max-width:1000px;margin:0 auto;padding:30px 18px 60px}}
header h1{{font-size:23px;margin:0 0 6px}}
header .sub{{color:var(--sub);font-size:14px}}
.meta-bar{{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0}}
.chip{{background:var(--card);border:1px solid var(--line);border-radius:999px;padding:4px 12px;font-size:12px;color:var(--sub)}}
section{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:20px 22px;margin:16px 0;box-shadow:0 1px 3px rgba(0,0,0,.03)}}
h2{{font-size:18px;margin:0 0 12px;display:flex;align-items:center;gap:8px}}
h2::before{{content:"";width:4px;height:18px;background:var(--accent);border-radius:2px}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:10px 0}}
th,td{{padding:7px 9px;text-align:left;border-bottom:1px solid var(--line)}}
th{{font-size:11.5px;color:var(--sub);font-weight:600;background:#fafbfc}}
td.num{{text-align:right;font-variant-numeric:tabular-nums}}
.up{{color:var(--up)}}.down{{color:var(--down)}}
.badge-win{{background:var(--up);color:#fff;font-size:9.5px;padding:1px 6px;border-radius:6px;margin-left:4px}}
tr.grp td{{background:#f3f5f8;font-size:13px;padding:6px 9px}}
.callout{{background:var(--hl);border:1px solid #ffe0b2;border-radius:10px;padding:13px 15px;margin:12px 0;font-size:13.5px}}
.callout b{{color:var(--gold)}}
.callout2{{background:#f1f8ff;border:1px solid #c5ddf7;border-radius:10px;padding:13px 15px;margin:12px 0;font-size:13.5px}}
.note{{color:var(--sub);font-size:12.5px}}
.kpis{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}
.kpi{{border:1px solid var(--line);border-radius:12px;padding:12px 14px}}
.kpi .lab{{font-size:12px;color:var(--sub)}}.kpi .val{{font-size:21px;font-weight:700;margin-top:2px}}
.kpi .val.up{{color:var(--up)}}.kpi .val.down{{color:var(--down)}}
.foot{{color:var(--sub);font-size:12px;text-align:center;margin-top:22px}}
@media(max-width:680px){{.kpis{{grid-template-columns:1fr}}}}
</style></head>
<body><div class="wrap">
<header><h1>三策略回测对比:1/2/3 只股 · 胜率与收益</h1>
<div class="sub">A 多因子框架+热门闸口+择时 · B 龙头+择时(破MA20空仓) · C 龙头(无择时) — 热门行业按板块资金净流入排名</div>
<div class="meta-bar">
<span class="chip">区间 {meta['calendar_start']}~{meta['calendar_end']}</span>
<span class="chip">股票 {meta['universe']} 只</span>
<span class="chip">窗口 {meta['window_shifts']}</span>
<span class="chip">滑点 {meta['slippage']*100:.1f}%(双边)</span>
<span class="chip">热门 top_k={meta['top_k']}</span>
</div></header>

<section>
<h2>核心结论</h2>
<div class="callout2"><b>① 胜率冠军 = 策略 A(多因子框架+热门闸口+择时)</b><br>
胜率 <b class="up">{a_win['avg_win_rate']:.1f}%</b>(N={a_win['N']}, 持有{a_win['hold']}天), 8窗口均值胜率全部 54–57%, 远超龙头法的 38–51%。
回撤仅 <b>13.7%</b>(全配置最低), 收益为正(+6.8%~+14.0%)。这是"想赢更多次、且风险可控"的最优解。</div>
<div class="callout"><b>② 收益冠军 = 策略 C(龙头无择时, 持有3天, N=3)</b><br>
收益 <b class="up">+{c33['avg_profit']:.0f}%</b>、夏普 {c33['avg_sharpe']:.2f}、胜率 {c33['avg_win_rate']:.1f}%、回撤 {c33['avg_max_dd']:.1f}%。
但代价是 <b>{c33['avg_trades']:.0f} 笔高频交易</b> 且为<b>无止损买&持有</b>(N=1 时回撤高达 56%)。属于"高收益、胜率仅五成、需强风控"的打法。</div>
<div class="callout2" style="background:#fde8e8;border-color:#f5b5b5"><b>③ 策略 B(龙头+择时)被双向压制 — 不推荐</b><br>
加 MA20 择时闸口后, 龙头的胜率没超过 A(仍 38–51%), 收益却被砍到只有 C 的一半左右
(C N=3 h=3 收益 +{c33['avg_profit']:.0f}% → B 同配置 +{b33['avg_profit']:.0f}%)。
原因: 龙头是短动量, 日级 MA20 太粗, 闸口频繁误杀好买点(震仓)。
<b>择时闸口对"持5天的框架(A)"有用, 对"持3天的龙头"反而添乱。</b></div>
<div class="kpis">
<div class="kpi"><div class="lab">最高胜率 (A)</div><div class="val up">{best_win['avg_win_rate']:.1f}%</div><div class="note">{best_win['name']} N={best_win['N']} h={best_win['hold']}</div></div>
<div class="kpi"><div class="lab">最高收益 (C)</div><div class="val up">+{best_ret['avg_profit']:.0f}%</div><div class="note">{best_ret['name']} N={best_ret['N']} h={best_ret['hold']}</div></div>
<div class="kpi"><div class="lab">最低回撤 (A)</div><div class="val">13.7%</div><div class="note">框架+闸口+择时 全N一致</div></div>
</div>
</section>

<section>
<h2>全部配置一览(8窗口均值)</h2>
{tbl}
<p class="note">红=A(框架) · 橙=B(龙头+择时) · 蓝=C(龙头)。持有栏 "h=5/3" 为持有天数。龙头法含 5天与3天两档(3天为其优势区间)。</p>
</section>

<section>
<h2>胜率对比</h2>
{chart_win}
<p class="note">A 全配置稳居 54–57% 一线(红), 龙头法(B/C)多在 38–51% 波动。可见"框架严选+热门闸口+择时"对胜率的提升最扎实。</p>
</section>

<section>
<h2>收益对比</h2>
{chart_ret}
<p class="note">龙头法(C, 蓝)在 3天持有下收益极高(尤其 N=3 的 +{c33['avg_profit']:.0f}%), 但这是<b>无止损、高换手</b>的结果; A(红)收益稳健为正。B(橙)被择时闸口拖累, 收益明显低于 C。</p>
</section>

<section>
<h2>逐策略解读与落地建议</h2>
<div class="callout2"><b>A · 多因子框架 + 热门闸口 + 择时</b><br>
= 原框架全口径(≥2亿成交额 + 套牢盘≤0.10 + sharpe>1.0 + 自适应止盈止损), 选股前加"仅限热门行业"闸口(复用你的择时开关同位置), 再叠加市场破MA20空仓。<br>
• N=1: 收益 +{a1['avg_profit']:.1f}% / 胜率 {a1['avg_win_rate']:.1f}% / 回撤 {a1['avg_max_dd']:.1f}%<br>
• N=2: 收益 +{a2['avg_profit']:.1f}% / 胜率 {a2['avg_win_rate']:.1f}%(全场最高) / 回撤 {a2['avg_max_dd']:.1f}%<br>
<b>建议:</b> 想"少操心、高胜率、低回撤"就选它; N=2 胜率最高, N=1 收益最高。</div>
<div class="callout"><b>C · 热门行业龙头(无择时)</b><br>
= 14:45 选热门行业(资金净流入 top-{meta['top_k']})内当日涨幅前 N 的龙头, 买&持有。<br>
• N=3 / 持有3天: 收益 +{c33['avg_profit']:.0f}% / 胜率 {c33['avg_win_rate']:.1f}% / 夏普 {c33['avg_sharpe']:.2f} / 回撤 {c33['avg_max_dd']:.1f}% / {c33['avg_trades']:.0f}笔。<br>
<b>建议:</b> 追求极致收益可选, 但<b>务必另加止损/仓位管理</b>(无退出规则时 N=1 回撤达 56%); 且高频换手对执行/滑点敏感, 实盘小资金验证。</div>
<div class="callout2" style="background:#fde8e8;border-color:#f5b5b5"><b>B · 龙头 + 择时(破MA20空仓) — 不建议</b><br>
理论上"龙头强势 + 大盘保护"最完美, 实测却两头不靠: 择时闸口对短动量龙头是负贡献(误杀+震仓), 既没拿到 A 的胜率, 也没拿到 C 的收益。若坚持龙头路线, 直接用 C 并自带止损即可, 不必叠 MA20 闸口。</div>
</section>

<section>
<h2>口径与注意</h2>
<ul style="margin:8px 0;padding-left:20px;font-size:13px">
<li><b>热门行业</b>: 按板块当日<b>主力资金净流入</b>排名 top-{meta['top_k']}(逐股 成交额×(2×收盘−最高−最低)/(最高−最低) 汇总)。</li>
<li><b>退出机制差异(如实标注)</b>: A 用原框架 <code>ExitRuleEngine</code>(自适应止盈止损); B/C 用简单买&持有(无原框架的流动性/套牢盘过滤), 故 B/C 的"高收益"含未风控的尾部风险。</li>
<li><b>14:45 价</b>以当日收盘近似(库内无14:45缓存); 实盘以真实14:45价下单, 结果可能略异。</li>
<li>全部回测离线读取 <code>qlib_pro_v16.db</code>, 未修改任何原有业务文件; 8 个稳健性窗口(持仓5/3 × 偏移0/20/40/60)取均值。</li>
</ul>
</section>

<div class="foot">离线回测 · 复用原策略 config/AlphaModel/ExitRuleEngine 口径 · 数据源 qlib_pro_v16.db · 未修改任何原有文件</div>
</div></body></html>"""

(HERE / "compare3_report.html").write_text(HTML, encoding="utf-8")
print("REPORT ->", HERE / "compare3_report.html")
