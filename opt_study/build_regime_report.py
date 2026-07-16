# -*- coding: utf-8 -*-
"""生成龙头策略×大盘状态分析 HTML 报告。"""
from __future__ import annotations
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "c_regime_results.json", encoding="utf-8"))
seg = d["segmentation"]["3"]
scan = [r for r in d["scan"] if r["hold"] == 3]
best = d["best"]["3"]
recent = d["recent"]["3"]
meta = d["meta"]

def seg_rows(key):
    return "".join(
        f"<tr><td>{r['区间'] if '区间' in r else r['状态']}</td>"
        f"<td>{r['笔数']}</td><td>{r['胜率%']}%</td><td>{r['均值%']:+}%</td></tr>"
        for r in seg[key])

scan_rows = ""
for r in scan:
    hl = " class='best'" if r["filter"] == best["filter"] else ""
    scan_rows += (f"<tr{hl}><td>{r['filter']}</td><td>{r['总收益%']:+}%</td>"
                  f"<td>{r['夏普']}</td><td>{r['最大回撤%']}%</td>"
                  f"<td>{r['空仓占比%']}%</td><td>{r['交易笔数']}</td></tr>")

# 始终在场 baseline
base = next(r for r in scan if r["filter"] == "始终在场")

HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>龙头策略 × 大盘状态分析</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;
background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1080px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:20px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;
box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th,td{{padding:7px 10px;border-bottom:1px solid #eee;text-align:right}}
th{{background:#fafbfc;color:#555;font-weight:600}}
td:first-child,th:first-child{{text-align:left}}
tr.best{{background:#e8f5e9;font-weight:600}}
.neg{{color:#c0392b}} .pos{{color:#27ae60}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap;margin:6px 0 4px}}
.kpi div{{flex:1;min-width:120px;background:#f0f6ff;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:19px;color:#156}} .kpi span{{font-size:12px;color:#667}}
.note{{font-size:13.5px;line-height:1.75;color:#444}}
.note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:14px}}
.warn{{background:#fff4e5;border-left:4px solid #e67e22;padding:10px 14px;border-radius:6px;margin:10px 0}}
</style></head><body><div class="wrap">
<h1>龙头策略 × 大盘状态分析:什么时候该空仓等待</h1>
<div class="sub">策略 C(热门行业龙头, N=3, 持3天, 无止损) · 回测区间 {meta['区间']} · 初始资金 ¥{meta['init']:,}</div>

<div class="card">
<h2>一、你的直觉是对的:近期龙头策略确实在亏钱</h2>
<p class="note">把"始终在场"和"加择时"都跑最后 60 个交易日对比:</p>
<div class="kpi">
<div><b class="neg">{recent['始终在场_近60日%']:+}%</b><span>近60日·始终在场(你的现状)</span></div>
<div><b class="pos">{recent['过滤后_近60日%']:+}%</b><span>近60日·加择时过滤后</span></div>
</div>
<p class="note">近两个月大盘偏弱,龙头策略"始终在场"亏 <b>{recent['始终在场_近60日%']}%</b>;
而加上大盘开关后翻正到 <b>{recent['过滤后_近60日%']}%</b>——说明<b>不是策略失效,而是它在"不对的大盘环境"里被反复打脸</b>,
只要这种环境空仓等待,就能躲开。</p>
</div>

<div class="card">
<h2>二、龙头策略在什么大盘环境下亏钱?(按入场日状态分桶)</h2>
<p class="note">把 250 笔交易按"入场当天的大盘状态"切开,看胜率和平均收益:</p>
<p class="note"><b>市场宽度(上涨家数占比的20日均线)</b>是最锋利的刀:</p>
<table><tr><th>宽度MA20</th><th>笔数</th><th>胜率</th><th>均值收益</th></tr>{seg_rows('breadth_ma20')}</table>
<p class="note"><b>大盘20日动量</b>:深度下跌时直接变负:</p>
<table><tr><th>20日动量</th><th>笔数</th><th>胜率</th><th>均值收益</th></tr>{seg_rows('mom20')}</table>
<p class="note"><b>站上/跌破 MA60(中期趋势)</b>:</p>
<table><tr><th>状态</th><th>笔数</th><th>胜率</th><th>均值收益</th></tr>{seg_rows('above_ma60')}</table>
<div class="warn">结论:龙头策略是"顺大势"的品种——<b>市场宽度弱(&lt;45%个股上涨)、或20日动量深度为负(&lt;-5%)、或大盘跌破MA60</b>时,
它平均收益明显下滑甚至转负。这正是它"近期全负"的根源。</div>
</div>

<div class="card">
<h2>三、择时过滤扫描:哪个开关最有效?(持3天)</h2>
<table><tr><th>过滤条件(信号日不对则空仓)</th><th>总收益</th><th>夏普</th><th>最大回撤</th><th>空仓占比</th><th>交易笔数</th></tr>
{scan_rows}</table>
<p class="note">注:之前 B 方案用的"破 MA20 空仓"在本表对应 <b>站上MA20</b>(收益 {next(r['总收益%'] for r in scan if r['filter']=='站上MA20'):+}%),
比始终在场(216.7%)腰斩——<b>MA20 太短太吵,对3天持有期是负贡献</b>。
而 <b>站上MA60</b> 是最优:夏普最高、回撤最低,收益只少一点点。</p>
</div>

<div class="card">
<h2>四、最优规则:大盘站上 60 日均线才做,否则空仓等待</h2>
<div class="kpi">
<div><b class="pos">+{best['总收益%']}%</b><span>过滤后总收益</span></div>
<div><b>{best['夏普']}</b><span>夏普(始终在场 {base['夏普']})</span></div>
<div><b class="pos">{best['最大回撤%']}%</b><span>最大回撤(始终在场 {base['最大回撤%']}%)</span></div>
<div><b>{best['空仓占比%']}%</b><span>时间处于空仓等待</span></div>
</div>
<p class="note">对比"始终在场":收益 {base['总收益%']:+}% → <b>+{best['总收益%']}%</b>(仅少约 {base['总收益%']-best['总收益%']:.0f} 个点),
但<b>最大回撤从 {base['最大回撤%']}% 砍到 {best['最大回撤%']}%</b>、夏普略升,且有 <b>{best['空仓占比%']}%</b> 的时间干脆空仓——
这正是你要的"机会不对就空仓等待"。</p>
<div class="warn">为什么是 MA60 不是 MA20?因为龙头策略只持 3 天,MA20 级别的波动只是噪声,频繁误杀;
MA60 代表<b>中期趋势</b>,龙头策略本质上赚的是"中期向上 + 板块轮动"的钱,中期趋势一破,动量龙头整体失效,
这时空仓比硬做明智得多。</div>
<p class="note"><span class="tag">实操规则</span>
每个信号日(每3个交易日)开盘前看一眼<b>等权全A指数(或沪深300/中证全指)是否站上60日均线</b>:
<ul style="margin:6px 0 0 18px;line-height:1.8">
<li>站上 MA60 → 正常买入热门行业龙头第1/2/3名,持3天;</li>
<li>跌破 MA60 → <b>本信号周期不新建仓位,现金等待</b>;已持有的老仓位让它们自然到期离场,不中途砍。</li>
</ul></p>
</div>

<div class="card">
<h2>五、落地建议</h2>
<p class="note">
1. <b>首选:MA60 总开关</b>。规则极简、易执行,回撤降 1/3,近期实盘能躲开亏损。<br>
2. 若想更严,可叠加"宽度MA20&gt;45%"(弱市宽度下龙头已证明平均亏损),但会牺牲不少收益与交易机会,本数据中组合过滤反而拉低夏普,不推荐优先。<br>
3. <b>坚持持3天、别用持5天</b>:持5天在择时后近60日仍 -8.64%,全周期也远弱于持3天。<br>
4. 实盘仍要计佣金(万2.5)+ 卖出印花税(千0.5),会再压低约 1~2 个点/年,但方向不变。
</p>
</div>
</div></body></html>"""

(HERE / "c_regime_report.html").write_text(HTML, encoding="utf-8")
print("OK -> c_regime_report.html")
