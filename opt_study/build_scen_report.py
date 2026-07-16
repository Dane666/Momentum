# -*- coding: utf-8 -*-
"""生成 4场景 + 基准动量 对比报告 HTML。"""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "c_scen_results.json", encoding="utf-8"))
meta = d["meta"]; ST = d["strategies"]; SR = d["buy_seal_rate"]

ORDER = ["动量基准", "A炸板低吸", "B回踩均线", "C尾盘偷袭板", "D断板反包"]
GATE = "MA60开门"


def m(sel, hold, gate=GATE):
    return ST[sel][gate][f"持{hold}天"]


# 主表行 (MA60开门, 持2/3天)
rows = ""
for sel in ORDER:
    m2 = m(sel, 2); m3 = m(sel, 3)
    best = sel == "C尾盘偷袭板"
    cls = ' class="real"' if best else ""
    rows += (f'<tr{cls}><td>{sel}</td>'
             f'<td>{m2["总收益%"]:+.1f}%</td><td>{m2["夏普"]}</td><td>{m2["最大回撤%"]:.1f}%</td>'
             f'<td>{m2["买入日涨停封板率%"]}%</td><td>{m2["交易笔数"]}</td>'
             f'<td>{m3["总收益%"]:+.1f}%</td><td>{m3["夏普"]}</td><td>{m3["最大回撤%"]:.1f}%</td>'
             f'<td>{m3["买入日涨停封板率%"]}%</td><td>{m3["交易笔数"]}</td></tr>')

# 封板率对比表 (所有策略, MA60开门, 持3天)
seal_rows = ""
for sel in ORDER:
    sr2 = SR[sel][GATE]["持2天"]; sr3 = SR[sel][GATE]["持3天"]
    nog = SR[sel]["无闸口"]["持3天"]
    note = "追强势=追涨停" if sel == "动量基准" else "低吸/分歧日, 可成交"
    hi = ' class="real"' if sr3 == 0.0 and sel != "动量基准" else (' class="fake"' if sr3 >= 50 else "")
    seal_rows += (f'<tr{hi}><td>{sel}</td><td>{nog}%</td><td>{sr2}%</td><td>{sr3}%</td><td>{note}</td></tr>')

# 无闸口对照 (持3天)
nog_rows = ""
for sel in ORDER:
    mn = m(sel, 3, "无闸口")
    nog_rows += (f'<tr><td>{sel}</td><td>{mn["总收益%"]:+.1f}%</td><td>{mn["夏普"]}</td>'
                 f'<td>{mn["最大回撤%"]:.1f}%</td><td>{mn["买入日涨停封板率%"]}%</td>'
                 f'<td>{mn["交易笔数"]}</td></tr>')

# CSV 样例 (C 偷袭板)
df_c = pd.read_csv(HERE / "c_scen_C尾盘偷袭板_h3_ma60.csv", encoding="utf-8-sig")
samp = ""
for _, x in df_c.head(16).iterrows():
    cls = {"买入": "buy", "卖出": "sell", "空仓等待": "wait"}.get(x["动作"], "")
    pnl = "" if (pd.isna(x.get("实现盈亏元")) or x.get("实现盈亏元") == "") else f'{x["实现盈亏元"]:.0f}'
    pct = "" if (pd.isna(x.get("实现收益%")) or x.get("实现收益%") == "") else f'{x["实现收益%"]:.2f}%'
    code = x["代码"] if (not pd.isna(x.get("代码")) and x.get("代码") != "") else ""
    samp += (f'<tr class="{cls}"><td>{x["序号"]}</td><td>{x["日期"]}</td><td class="act">{x["动作"]}</td>'
             f'<td>{x["槽位"]}</td><td>{code}</td><td>{x["行业"]}</td><td>{x["成交价"]}</td>'
             f'<td>{x["股数"]}</td><td>{x["成交额"]}</td><td>{pct}</td><td>{pnl}</td>'
             f'<td>{x["账户净值"]:,.0f}</td><td>{x["大盘MA60"]}</td><td>{x["备注"]}</td></tr>')

c2 = m("C尾盘偷袭板", 2); c3 = m("C尾盘偷袭板", 3)
HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>四种可成交龙头场景 × 基准动量</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;
background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1280px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:18px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th,td{{padding:6px 8px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}}
th{{background:#fafbfc;color:#555;font-weight:600}}
td:nth-child(2),td:nth-child(5),td:nth-child(6),td:nth-child(12),td:nth-child(13){{text-align:left}}
.act{{font-weight:700}} tr.buy .act{{color:#c0392b}} tr.sell .act{{color:#27ae60}} tr.wait .act{{color:#888}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{flex:1;min-width:160px;background:#e8f5e9;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:20px;color:#1b7}} .kpi span{{font-size:12px;color:#888}}
.note{{font-size:13.5px;line-height:1.8;color:#444}} .note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.alert{{background:#eaf6ff;border-left:4px solid #2980b9;padding:12px 16px;border-radius:8px;
font-size:14px;line-height:1.8;color:#155;margin-bottom:18px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:13px}}
tr.real{{background:#e8f5e9}} tr.fake{{background:#fdecea}}
</style></head><body><div class="wrap">
<h1>四种"可成交龙头"场景 × 基准动量策略 对比回测</h1>
<div class="sub">初始资金 ¥{meta['init']:,} · N={meta['N']} · 回测区间 {meta['区间']} ·
所有策略统一账户级引擎(热门行业内选股; 买入=收盘×(1+滑点); 持2/3天到期; 可选MA60开门闸口)</div>

<div class="alert">
<b>核心结论</b><br>
① <b>可成交性已验证</b>: 4个场景(炸板低吸/回踩均线/尾盘偷袭板/断板反包)买入日涨停封板率
<b>全部 0.0%</b>, 而同框架动量基准仍达 <b>54~58%</b>(追强势=追涨停, 又踩回买不进/追高站岗)。
你提的"买分歧日/没封板的票"思路, 从根上解决了上一轮发现的涨停买不进陷阱。<br>
② <b>能成交 ≠ 能赚钱</b>: 在 2024-06~2026-07 样本(含近期弱势段)里, 只有
<b>C 尾盘偷袭板稳定正收益</b>(MA60开门 持2天 +{c2['总收益%']:.1f}% / 持3天 +{c3['总收益%']:.1f}%, 回撤仅
{c2['最大回撤%']:.1f}%~{c3['最大回撤%']:.1f}%); 其余 A炸板(-59%)、B回踩(-33%)、D断板(-8%)均亏损。<br>
③ <b>口径诚实说明</b>: 本表"动量基准"是<b>简化同框架版</b>(热门行业按5日动量选股+简单持有), 不代表生产动量。
生产动量策略(harness.py原框架, 含套牢盘过滤+自适应止盈止损)主口径为 <b>+32.4%/胜率52.1%/夏普1.31/回撤12.4%</b>,
是更可信的实盘基准——4场景中仅 C 接近它, 其余均不及。
</div>

<div class="card">
<h2>一、策略对比矩阵(MA60开门闸口 · 持2天 / 持3天)</h2>
<table>
<tr><th>策略</th><th colspan="5">持2天</th><th colspan="5">持3天</th></tr>
<tr><th></th><th>收益</th><th>夏普</th><th>回撤</th><th>封板率</th><th>笔数</th>
<th>收益</th><th>夏普</th><th>回撤</th><th>封板率</th><th>笔数</th></tr>
{rows}
</table>
<p class="note" style="margin-top:10px">绿色=唯一正收益策略(C尾盘偷袭板)。其余场景在本样本均亏损。
C 持2天优于持3天(隔天博弈逻辑), 回撤最小; D断板反包交易笔数极少(仅6笔), 样本不足。
<b>动量基准(同框架)</b>大幅亏损(-37%), 因其专挑 mom5 最强=近期涨停最多, 买入日过半封板、简单持有不退出, 遇回调即站岗。</p>
</div>

<div class="card">
<h2>二、买入日涨停封板率(证明可成交性)</h2>
<table>
<tr><th>策略</th><th>无闸口 持3天封板率</th><th>MA60开门 持2天</th><th>MA60开门 持3天</th><th>说明</th></tr>
{seal_rows}
</table>
<p class="note" style="margin-top:10px">封板率=买入日恰好涨停封死(收盘≈最高且达涨停线)的比例。
<b>4场景全部 0%</b> → 选的全是"没封板的强势股/分歧日", 14:45 真能成交。
对照: 此前"追当日涨幅最强龙头"封板率 95%(虚假收益); 同框架动量基准 54~58%(仍半数为涨停, 追高站岗)。
可见"低吸/分歧日"思路在<b>可成交性</b>上完胜追涨。</p>
</div>

<div class="card">
<h2>三、无闸口对照(持3天, 看策略本身好坏)</h2>
<table>
<tr><th>策略</th><th>收益</th><th>夏普</th><th>回撤</th><th>封板率</th><th>笔数</th></tr>
{nog_rows}
</table>
<p class="note" style="margin-top:10px">去掉大盘闸口后, 亏损策略亏得更狠(尤其A炸板无闸口 -71%), 说明<b>大盘环境(MA60)对低吸类策略同样重要</b>——
弱势段做低吸是"接飞刀"。C尾盘偷袭板无闸口仍 +15.2%, 抗跌性最好。</p>
</div>

<div class="card">
<h2>四、场景逻辑 · 请你核对(日线近似口径)</h2>
<p class="note">
<span class="tag">4场景定义</span>
<ol style="margin:8px 0 0 18px;line-height:1.9">
<li><b>A 炸板低吸</b>: 当日最高曾摸涨停价但未封死(炸板)且仍收涨, 或早盘低开≥1%震荡企稳(未大跌、振幅&lt;9%); 且近3日曾涨停/近5日涨超15%(强势股)。→ 全天观察型低吸。</li>
<li><b>B 回踩均线</b>: 收盘回踩5日线(±3.5%)或10日线(±5%), 缩量(量&lt;5日均量×0.85), 且近期曾涨停(拉过板的龙头)。→ 二波低吸。</li>
<li><b>C 尾盘偷袭板</b>: 收盘涨幅&gt;5%且收在最高附近(尾盘拉起), 开盘不在涨停附近(非早盘板), 且最高曾摸涨停未封死(试探)。→ 博弈隔天冲高溢价。</li>
<li><b>D 断板反包</b>: 前1~2日曾涨停(是龙头), 今日断板(未涨停)且回调不深(-6%~+3%), 缩量, 尾盘企稳(收盘在当日上半区)。→ 赌隔天板块修复反包。</li>
</ol>
<b>请你重点检查的逻辑漏洞:</b>
<ul style="margin:6px 0 0 18px;line-height:1.9">
<li>① <b>日线近似分时行为</b>: "尾盘拉起""炸板""企稳"用 开/高/低/收+量能 近似, 非分钟级。例如 C 用"收盘≈最高+开盘非涨停"近似尾盘偷袭, 可能误纳入"全天强势股"(已用开盘非涨停过滤, 但仍不精确)。有分钟数据可更准。</li>
<li>② <b>动量基准≠生产动量</b>: 同框架动量基准(-37%)是简化版(只按mom5选、简单持有、无套牢盘/自适应退出), 仅供同框架对比; 真实生产动量 +32.4% 更可信, 4场景仅C接近。</li>
<li>③ <b>样本区间偏差</b>: 2024-06~2026-07 含牛市中段+近期(2025下半年后)弱势段, 低吸/二波类(A/B)在弱势易被套; 强牛市二波环境可能不同, 结论有周期依赖。</li>
<li>④ <b>持有期设定</b>: C/D 是"赌隔天", 持2天更合理(已测, C持2更优); A/B 是"等二波", 理论上需持5天+, 本报告仅测2/3天, 可能低估B的二波收益。</li>
<li>⑤ <b>封板率0%的代价</b>: 虽规避买不进, 但"买非封板的弱势分歧"本身α较弱——C能赚钱靠的是"尾盘转强"的隔天动量, 其余场景的"低吸"在样本里α不足。</li>
</ul>
</p>
</div>

<div class="card">
<h2>五、逐笔样例 · C尾盘偷袭板(MA60开门 持3天, 唯一正收益策略)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th><th>行业</th>
<th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th><th>账户净值</th>
<th>大盘MA60</th><th>备注</th></tr></thead><tbody>{samp}</tbody></table>
<p class="note" style="margin-top:10px">完整逐笔见 5 个 CSV:
<code>c_scen_动量基准_h3_ma60.csv</code> / <code>c_scen_A炸板低吸_h3_ma60.csv</code> /
<code>c_scen_B回踩均线_h3_ma60.csv</code> / <code>c_scen_C尾盘偷袭板_h3_ma60.csv</code> /
<code>c_scen_D断板反包_h3_ma60.csv</code>(均为 MA60开门 持3天)。</p>
</div>

<div class="card">
<h2>六、结论与下一步</h2>
<p class="note">
<b>结论</b>: 4个场景在"可成交性"上完胜追涨类(封板率0% vs 54~95%), 但样本内仅 <b>C尾盘偷袭板</b> 真实赚钱(+17~20%, 回撤7~13%)。
A炸板低吸/B回踩均线/D断板反包在当前区间均亏损——"低吸分歧"思路对, 但日线级别的简单低吸α不足, 且弱势段易接飞刀。<br><br>
<b>若要进一步接近/超过生产动量(+32.4%), 建议:</b>
<ul style="margin:6px 0 0 18px;line-height:1.9">
<li>(1) <b>深耕 C 尾盘偷袭板</b>: 缩短持有至1~2天 + "隔天高开即走"止盈; 叠加板块强度/涨停家数过滤, 提升胜率。它已是最接近实盘可行的方向。</li>
<li>(2) <b>接回生产框架的退出机制</b>: 把 C 的选股接入 harness.py 的 ExitRuleEngine(自适应止盈止损)+套牢盘过滤, 而非简单持有, 预期能压回撤、提夏普。</li>
<li>(3) <b>A/B/D 需重定义或换环境</b>: 二波/低吸可能在强牛市有效, 可加"大盘多头+板块主升"双重过滤后再测; 或把"断板反包"改成"断板当日不买、次日确认反包再打板"。</li>
<li>(4) <b>区分时效</b>: 本报告是2024-06~2026-07全样本; 建议分"强市/弱市"两段看 C 的稳健性, 再决定是否上实盘。</li>
</ul>
</p>
</div>
</div></body></html>"""

(HERE / "c_scen_report.html").write_text(HTML, encoding="utf-8")
print("完成 -> c_scen_report.html")
