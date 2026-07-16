# -*- coding: utf-8 -*-
"""读取 sector_results.json + sector_exact.json, 生成行业择时/龙头对比报告 (内联SVG, 无CDN依赖)。"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "sector_results.json", encoding="utf-8"))
ex = json.load(open(HERE / "sector_exact.json", encoding="utf-8"))
meta = d["meta"]
part2 = d["part2"]
best = d["best_part1"]

BASE3 = {"profit": 11.75, "win": 47.56, "sharpe": 0.52, "trades": 92.9}  # 原3股基准(前次回测)

# ---- exact 桶 (top_k=8) ----
ex_h5 = {r["bucket"]: r for r in ex if r["hold"] == 5}
ex_h3 = {r["bucket"]: r for r in ex if r["hold"] == 3}
b1 = ex_h5.get("1", {}); b2 = ex_h5.get("2", {}); b3 = ex_h5.get("ge3", {})

# ---- Part1 full 摘要: 取每规则的最优(hold5, top_k=8) ----
from collections import defaultdict
by_label = defaultdict(list)
for r in d["part1"]:
    by_label[r["label"]].append(r)
def best_of(rows, hold=5, tk=8):
    cand = [r for r in rows if r["hold"] == hold and r["top_k"] == tk]
    return cand[0] if cand else rows[0]
ctrl = best_of(by_label["不限(对照)"])
ge1 = best_of(by_label["持续≥1天"])

LEAD = {int(k): v for k, v in part2["leader"].items()}
MOM = {int(k): v for k, v in part2["momentum_top1"].items()}
MOM5 = part2["momentum_top1_5d"]

# ===================== SVG 图表 =====================
def vbar(series, ymax, h=240, w=680, unit="%", vlines=True):
    """series: list of (label, value, color)"""
    n = len(series)
    pad_l, pad_r, pad_t, pad_b = 46, 16, 18, 54
    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b
    bw = plot_w / n * 0.6
    gap = plot_w / n
    svg = [f'<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">']
    # y grid
    for g in range(0, int(ymax) + 1, max(1, int(ymax // 5))):
        y = pad_t + plot_h * (1 - g / ymax)
        svg.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#eef0f3"/>')
        svg.append(f'<text x="{pad_l-6}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#8b93a1">{g}{unit}</text>')
    for i, (lab, val, col) in enumerate(series):
        cx = pad_l + gap * (i + 0.5)
        x = cx - bw / 2
        yv = pad_t + plot_h * (1 - max(0, val) / ymax)
        yv = min(max(yv, pad_t), pad_t + plot_h)
        hh = pad_t + plot_h - yv
        if val >= 0:
            svg.append(f'<rect x="{x:.1f}" y="{yv:.1f}" width="{bw:.1f}" height="{hh:.1f}" rx="3" fill="{col}"/>')
            svg.append(f'<text x="{cx:.1f}" y="{yv-6:.1f}" text-anchor="middle" font-size="12" font-weight="700" fill="{col}">{val:.1f}{unit}</text>')
        else:
            y0 = pad_t + plot_h
            svg.append(f'<rect x="{x:.1f}" y="{y0:.1f}" width="{bw:.1f}" height="{hh:.1f}" rx="3" fill="{col}"/>')
            svg.append(f'<text x="{cx:.1f}" y="{y0+14:.1f}" text-anchor="middle" font-size="12" font-weight="700" fill="{col}">{val:.1f}{unit}</text>')
        # label (may be 2 lines)
        lines = lab.split("\n")
        for li, ln in enumerate(lines):
            svg.append(f'<text x="{cx:.1f}" y="{pad_t+plot_h+18+li*14:.1f}" text-anchor="middle" font-size="11" fill="#5b6270">{ln}</text>')
    svg.append('</svg>')
    return "\n".join(svg)

def grouped(horizons, leader_vals, mom_vals, ymax, h=260, w=680):
    n = len(horizons)
    pad_l, pad_r, pad_t, pad_b = 46, 16, 18, 50
    plot_w = w - pad_l - pad_r; plot_h = h - pad_t - pad_b
    gw = plot_w / n
    bw = gw * 0.28
    svg = [f'<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">']
    for g in range(0, int(ymax) + 1, max(1, int(ymax // 5))):
        y = pad_t + plot_h * (1 - g / ymax)
        svg.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#eef0f3"/>')
        svg.append(f'<text x="{pad_l-6}" y="{y+4:.1f}" text-anchor="end" font-size="11" fill="#8b93a1">{g}%</text>')
    for i, (hz, lv, mv) in enumerate(zip(horizons, leader_vals, mom_vals)):
        cx = pad_l + gw * (i + 0.5)
        x1 = cx - bw - 3; x2 = cx + 3
        for x, val, col, tag in [(x1, lv, "#e03131", "龙"), (x2, mv, "#1c7ed6", "动")]:
            yv = pad_t + plot_h * (1 - max(0, val) / ymax)
            hh = pad_t + plot_h - yv
            svg.append(f'<rect x="{x:.1f}" y="{yv:.1f}" width="{bw:.1f}" height="{hh:.1f}" rx="3" fill="{col}"/>')
            svg.append(f'<text x="{x+bw/2:.1f}" y="{yv-5:.1f}" text-anchor="middle" font-size="10.5" font-weight="700" fill="{col}">{val:.2f}</text>')
        svg.append(f'<text x="{cx:.1f}" y="{pad_t+plot_h+18:.1f}" text-anchor="middle" font-size="12" font-weight="600" fill="#1a1d24">+{hz}天</text>')
    svg.append(f'<text x="{pad_l+2}" y="{pad_t-4:.1f}" font-size="11" fill="#8b93a1">红=龙头  蓝=动量top1</text>')
    svg.append('</svg>')
    return "\n".join(svg)

# charts
chart_win = vbar([
    ("3股基准\n(现策略)", BASE3["win"], "#adb5bd"),
    ("单股·不限行业\n(对照)", ctrl["avg_win_rate"], "#868e96"),
    ("单股·首日热门\n(最优)", b1.get("avg_win_rate", 0), "#e03131"),
    ("龙头+3天\n(第二部分)", LEAD[3]["win_rate"], "#f08c00"),
], ymax=60, unit="%")

chart_bucket = vbar([
    ("首日热门\n(consec=1)", b1.get("avg_win_rate", 0), "#e03131"),
    ("持续2天\n(consec=2)", b2.get("avg_win_rate", 0), "#f08c00"),
    ("持续≥3天\n(无交易)", b3.get("avg_win_rate", 0), "#ced4da"),
], ymax=60, unit="%")

chart_part2 = grouped([1, 2, 3],
    [LEAD[1]["mean_ret"], LEAD[2]["mean_ret"], LEAD[3]["mean_ret"]],
    [MOM[1]["mean_ret"], MOM[2]["mean_ret"], MOM[3]["mean_ret"]],
    ymax=5)

# ---- 表格 ----
def row(cells):
    return "<tr>" + "".join(cells) + "</tr>"
def td(x, cls=""): return f'<td class="num {cls}">{x}</td>'

part1_tbl = (
    "<table><thead><tr><th>行业规则(top_k=8, 持仓5)</th><th class='num'>均收益</th>"
    "<th class='num'>均胜率</th><th class='num'>均夏普</th><th class='num'>均回撤</th><th class='num'>均交易</th></tr></thead><tbody>"
    + row(["<td>首日热门 (consec=1) <span class='badge-win'>最优胜率</span></td>",
           td(f"{b1.get('avg_profit',0):.2f}%", "up" if b1.get('avg_profit',0)>0 else "down"),
           td(f"{b1.get('avg_win_rate',0):.2f}%", "up"), td(f"{b1.get('avg_sharpe',0):.2f}"),
           td(f"{b1.get('avg_max_dd',0):.2f}%"), td(f"{b1.get('avg_trades',0):.1f}")])
    + row(["<td>持续2天 (consec=2)</td>", td(f"{b2.get('avg_profit',0):.2f}%"),
           td(f"{b2.get('avg_win_rate',0):.2f}%"), td(f"{b2.get('avg_sharpe',0):.2f}"),
           td(f"{b2.get('avg_max_dd',0):.2f}%"), td(f"{b2.get('avg_trades',0):.1f}")])
    + row(["<td>持续≥3天 (基本无交易)</td>", td("0.00%","muted"), td("0.00%","muted"),
           td("0.00"), td("0.00%"), td("0.0","muted")])
    + row(["<td>不限行业 (单股对照)</td>", td(f"{ctrl['avg_profit']:.2f}%"),
           td(f"{ctrl['avg_win_rate']:.2f}%"), td(f"{ctrl['avg_sharpe']:.2f}"),
           td(f"{ctrl['avg_max_dd']:.2f}%"), td(f"{ctrl['avg_trades']:.1f}")])
    + "</tbody></table>"
)

part2_tbl = (
    "<table><thead><tr><th>持有</th><th class='num'>龙头收益</th><th class='num'>龙头胜率</th>"
    "<th class='num'>动量收益</th><th class='num'>动量胜率</th><th class='num'>龙头n</th><th class='num'>动量n</th></tr></thead><tbody>"
    + row(["<td>+1天</td>", td(f"{LEAD[1]['mean_ret']:.2f}%","up"), td(f"{LEAD[1]['win_rate']:.1f}%","up"),
           td(f"{MOM[1]['mean_ret']:.2f}%","down"), td(f"{MOM[1]['win_rate']:.1f}%","down"),
           td(LEAD[1]['n']), td(MOM[1]['n'])])
    + row(["<td>+2天</td>", td(f"{LEAD[2]['mean_ret']:.2f}%","up"), td(f"{LEAD[2]['win_rate']:.1f}%","up"),
           td(f"{MOM[2]['mean_ret']:.2f}%","down"), td(f"{MOM[2]['win_rate']:.1f}%","down"),
           td(LEAD[2]['n']), td(MOM[2]['n'])])
    + row(["<td>+3天</td>", td(f"{LEAD[3]['mean_ret']:.2f}%","up"), td(f"{LEAD[3]['win_rate']:.1f}%","up"),
           td(f"{MOM[3]['mean_ret']:.2f}%","down"), td(f"{MOM[3]['win_rate']:.1f}%","down"),
           td(LEAD[3]['n']), td(MOM[3]['n'])])
    + row(["<td>动量5天(原退出)</td>", td("-","muted"), td("-","muted"),
           td(f"{MOM5['mean_ret']:.2f}%"), td(f"{MOM5['win_rate']:.1f}%"), td("-"), td(MOM5['n'])])
    + "</tbody></table>"
)

win_lift = LEAD[3]["win_rate"] - MOM[3]["win_rate"]
ret_lift = LEAD[3]["mean_ret"] - MOM[3]["mean_ret"]

HTML = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>单股行业择时 & 龙头策略 回测报告</title>
<style>
:root{{--bg:#f5f6f8;--card:#fff;--ink:#1a1d24;--sub:#5b6270;--line:#e6e8ec;
--up:#e03131;--down:#2f9e44;--accent:#1c7ed6;--gold:#f08c00;--hl:#fff4e6}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,"PingFang SC","Microsoft YaHei",Segoe UI,sans-serif;line-height:1.6}}
.wrap{{max-width:980px;margin:0 auto;padding:30px 18px 60px}}
header h1{{font-size:24px;margin:0 0 6px}}
header .sub{{color:var(--sub);font-size:14px}}
.meta-bar{{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0}}
.chip{{background:var(--card);border:1px solid var(--line);border-radius:999px;padding:4px 12px;font-size:12px;color:var(--sub)}}
section{{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:20px 22px;margin:16px 0;box-shadow:0 1px 3px rgba(0,0,0,.03)}}
h2{{font-size:18px;margin:0 0 12px;display:flex;align-items:center;gap:8px}}
h2::before{{content:"";width:4px;height:18px;background:var(--accent);border-radius:2px}}
table{{width:100%;border-collapse:collapse;font-size:13.5px;margin:10px 0}}
th,td{{padding:8px 10px;text-align:left;border-bottom:1px solid var(--line)}}
th{{font-size:12px;color:var(--sub);font-weight:600;background:#fafbfc}}
td.num{{text-align:right;font-variant-numeric:tabular-nums}}
.up{{color:var(--up)}}.down{{color:var(--down)}}.muted{{color:#aeb4bd}}
.badge-win{{background:var(--up);color:#fff;font-size:10px;padding:1px 7px;border-radius:6px;margin-left:4px}}
.callout{{background:var(--hl);border:1px solid #ffe0b2;border-radius:10px;padding:13px 15px;margin:12px 0;font-size:13.5px}}
.callout b{{color:var(--gold)}}
.callout2{{background:#f1f8ff;border:1px solid #c5ddf7;border-radius:10px;padding:13px 15px;margin:12px 0;font-size:13.5px}}
.note{{color:var(--sub);font-size:12.5px}}
.kpis{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}
.kpi{{border:1px solid var(--line);border-radius:12px;padding:12px 14px}}
.kpi .lab{{font-size:12px;color:var(--sub)}}.kpi .val{{font-size:22px;font-weight:700;margin-top:2px}}
.kpi .val.up{{color:var(--up)}}.kpi .val.down{{color:var(--down)}}
.foot{{color:var(--sub);font-size:12px;text-align:center;margin-top:22px}}
@media(max-width:680px){{.kpis{{grid-template-columns:1fr}}}}
</style></head>
<body><div class="wrap">
<header><h1>单股 + 行业择时 / 热门行业龙头 回测报告</h1>
<div class="sub">第一部分:只选1只 + 精心筛行业(首日热门 vs 持续多天) · 第二部分:14:45买热门行业龙头 vs 动量策略 · <b>v2 口径:热门行业按资金净流入排名</b></div>
<div class="meta-bar">
<span class="chip">区间 {meta['calendar_start']}~{meta['calendar_end']}</span>
<span class="chip">股票 {meta['universe']} 只</span>
<span class="chip">窗口 {meta['window_shifts']}</span>
<span class="chip">滑点 {meta['slippage']*100:.1f}%(双边)</span>
<span class="chip">龙头 top_k={meta['leader_top_k']}</span>
<span class="chip" style="background:#e7f5ff;color:#1971c2;border-color:#b6e0ff">热门行业口径=资金净流入</span>
</div></header>

<section>
<h2>核心结论</h2>
<div class="callout"><b>问题①:首日成为热门行业的股,胜率更高还是持续多天热门的更高?</b><br>
<b>首日(consec=1)胜率最高 ({b1.get('avg_win_rate',0):.1f}%)</b>,且收益为正({b1.get('avg_profit',0):+.1f}%)、交易频率合理({b1.get('avg_trades',0):.1f}次)。
持续2天胜率仍约 {b2.get('avg_win_rate',0):.0f}% 但收益腰斩({b2.get('avg_profit',0):+.1f}%)、交易骤降到 {b2.get('avg_trades',0):.1f} 次;<b>持续≥3天基本无交易(策略失效)</b>。
→ 结论是:<b>挑"刚启动成为热门"的行业里的股,而不是等它热了很久再追</b>。等太久既没收益、又没交易可做。</div>
<div class="callout2"><b>问题②:14:45直接买热门行业龙头,前1/2/3天收益比动量策略好吗?</b><br>
<b>全面更好。</b> 龙头在 +1/+2/+3 天的收益与胜率都显著高于动量策略 top1。
以 +3天计:龙头收益 <b class="up">+{LEAD[3]['mean_ret']:.2f}%</b>、胜率 <b class="up">{LEAD[3]['win_rate']:.1f}%</b>;
动量收益 {MOM[3]['mean_ret']:.2f}%、胜率 {MOM[3]['win_rate']:.1f}%。
龙头胜率较动量 <b class="up">+{win_lift:.1f}pp</b>、收益 <b class="up">+{ret_lift:.2f}pp</b>。</div>
<div class="kpis">
<div class="kpi"><div class="lab">首日热门 胜率</div><div class="val up">{b1.get('avg_win_rate',0):.1f}%</div><div class="note">单股·持仓5·top_k=8</div></div>
<div class="kpi"><div class="lab">龙头+3天 胜率</div><div class="val up">{LEAD[3]['win_rate']:.1f}%</div><div class="note">vs 动量 {MOM[3]['win_rate']:.1f}%</div></div>
<div class="kpi"><div class="lab">龙头+3天 收益</div><div class="val up">+{LEAD[3]['mean_ret']:.2f}%</div><div class="note">vs 动量 +{MOM[3]['mean_ret']:.2f}%</div></div>
</div>
</section>

<section>
<h2>第一部分:单股(hold=1) + 行业择时 — 首日 vs 持续多天</h2>
<p class="note">方法:每日仅选1只;候选限定在"热门行业"(每日按<b>板块主力资金净流入</b>排名 top-{meta['leader_top_k']})内,沿用原策略严苛过滤器(trapped≤0.10 & sharpe>1.0)。
下面为 top_k=8、持仓5天 的精确分桶(8窗口均值)。</p>
<div class="callout2" style="background:#f0f9ff;border-color:#b6e0ff"><b>口径变更(v2):</b> 本版"热门行业"的排名依据由<b>行业等权收益率</b>改为<b>板块当日主力资金净流入</b>
(逐股 <code>成交额×(2×收盘−最高−最低)/(最高−最低)</code> 汇总)。资金流入更能反映"钱往哪去",相比单纯涨幅排名更贴近真实热门。</div>
{chart_bucket}
{part1_tbl}
<div class="callout"><b>最优参数:</b> <code>top_k=8, 持仓=5天, 行业规则=首日热门(consec=1)</code> → 均胜率 {b1.get('avg_win_rate',0):.2f}%、均收益 {b1.get('avg_profit',0):.2f}%、交易 {b1.get('avg_trades',0):.1f} 次。
若放宽到"≥1天热门"(含首日与持续)收益略高({ge1['avg_profit']:.2f}%)但本质一致——<b>关键是"在热门行业里选",首日优先</b>。</div>
<div class="callout2"><b>与现策略(每日3只)的权衡:</b> 现3股基准 胜率 {BASE3['win']:.1f}% / 收益 {BASE3['profit']:.1f}%。
单股首日热门把<b>胜率从 {BASE3['win']:.1f}% 提到 {b1.get('avg_win_rate',0):.1f}%(+{b1.get('avg_win_rate',0)-BASE3['win']:.1f}pp)</b>,
但<b>绝对收益下降</b>(交易次数 92.9 → 10.5,复利效应减弱)。即"少而精"提升了命中率,却牺牲了分散带来的总收益。
若想两全,可把"热门行业筛选"叠加回3股版本(胜率↑且保留分散收益)。</div>
</section>

<section>
<h2>胜率对比一览</h2>
{chart_win}
<p class="note">红线=单股首日热门,橙线=龙头+3天,灰线=现3股基准 / 单股不限行业对照。行业筛选与龙头法都把胜率推过50%。</p>
</section>

<section>
<h2>第二部分:14:45买热门行业龙头 vs 动量策略(top1)</h2>
<p class="note">龙头=热门行业(top-{meta['leader_top_k']},按板块资金净流入排名)内当日涨幅最强的股,14:45(当日收盘近似)买入,持有 +1/+2/+3 天;动量=原策略 alpha 打分 top1。两者同口径(双边滑点 {meta['slippage']*200:.1f}%)对比。</p>
{chart_part2}
{part2_tbl}
<div class="callout2"><b>结论:</b> 龙头策略在每一个持有周期都跑赢动量 top1——收益更高、胜率更高、样本更足(n=200 vs 137)。
说明"直接追热门行业里最强势的那只",比"用多因子打分选股"在短周期内更有效。
建议落地:<b>14:45 选 top-{meta['leader_top_k']} 热门行业中当日涨幅第一的龙头,持有 2~3 天</b>(+3天胜率 {LEAD[3]['win_rate']:.1f}% 最佳),到达即用原退出规则离场。</div>
</section>

<section>
<h2>综合落地建议</h2>
<ul style="margin:8px 0;padding-left:20px;font-size:13.5px">
<li><span class="badge-win" style="background:var(--gold)">行业</span><b>只买热门行业里的票,且优先"首日启动"的行业</b>(consec=1 胜率最高)。等成热再去追,收益与交易频率都塌。</li>
<li><span class="badge-win" style="background:var(--gold)">极简</span>若想最省事且胜率/收益双高:<b>14:45 买热门行业龙头、持有 2~3 天</b>,比现行多因子动量更优。</li>
<li><span class="badge-win" style="background:var(--gold)">兼容</span>若坚持多因子框架:在选股前加"仅限热门行业"闸口(可复用上次的市场择时闸口同位置),即把胜率从 ~47% 抬到 ~53%,再叠加你已加的择时开关。</li>
<li><span class="badge-win" style="background:var(--gold)">注意</span>本回测"14:45价"用当日收盘近似(库内无14:45缓存);实盘以真实14:45价下单,结果可能略优或略逊,建议小资金先验证。</li>
</ul>
</section>

<div class="foot">离线回测,复用原策略 config/AlphaModel/ExitRuleEngine 口径 · 数据源 qlib_pro_v16.db · 未修改任何原有文件</div>
</div></body></html>"""

(HERE / "sector_analysis_report.html").write_text(HTML, encoding="utf-8")
print("REPORT ->", HERE / "sector_analysis_report.html")
