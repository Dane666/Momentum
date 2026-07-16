# -*- coding: utf-8 -*-
"""生成 龙头策略涨停买不进 对比报告 HTML。"""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "c_limitup_results.json", encoding="utf-8"))
meta = d["meta"]; st = d["stats"]; bt = d["backtest"]
sa, sg = st["始终在场"], st["MA60开门"]
b_nog = bt["无闸口_baseline"]; b_bl = bt["MA60开门_baseline"]
b_sk = bt["MA60开门_skip"]; b_nx = bt["MA60开门_next"]

df_next = pd.read_csv(HERE / "c_tradelog_N3_hold3_ma60_limitup_next.csv", encoding="utf-8-sig")
df_skip = pd.read_csv(HERE / "c_tradelog_N3_hold3_ma60_limitup_skip.csv", encoding="utf-8-sig")


def rows_df(df, n=14, acts=None):
    r = df
    if acts:
        r = df[df["动作"].isin(acts)]
    out = ""
    for _, x in r.head(n).iterrows():
        cls = {"买入": "buy", "卖出": "sell", "空仓等待": "wait", "涨停跳过": "limit"}.get(x["动作"], "")
        pnl = "" if (pd.isna(x.get("实现盈亏元")) or x.get("实现盈亏元") == "") else f'{x["实现盈亏元"]:.0f}'
        pct = "" if (pd.isna(x.get("实现收益%")) or x.get("实现收益%") == "") else f'{x["实现收益%"]:.2f}%'
        code = x["代码"] if (not pd.isna(x.get("代码")) and x.get("代码") != "") else ""
        out += (f'<tr class="{cls}"><td>{x["序号"]}</td><td>{x["日期"]}</td>'
                f'<td class="act">{x["动作"]}</td><td>{x["槽位"]}</td><td>{code}</td>'
                f'<td>{x["行业"]}</td><td>{x["成交价"]}</td><td>{x["股数"]}</td>'
                f'<td>{x["成交额"]}</td><td>{pct}</td><td>{pnl}</td>'
                f'<td>{x["账户净值"]:,.0f}</td><td>{x["大盘MA60"]}</td><td>{x["备注"]}</td></tr>')
    return out


rows_skip = rows_df(df_skip, 14, ["涨停跳过", "买入", "卖出"])
rows_next = rows_df(df_next, 14, ["买入", "卖出", "空仓等待"])

HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>龙头策略·涨停买不进现实约束分析</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;
background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1240px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:18px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th,td{{padding:6px 8px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}}
th{{background:#fafbfc;color:#555;font-weight:600}}
td:nth-child(2),td:nth-child(5),td:nth-child(6),td:nth-child(13),td:nth-child(14){{text-align:left}}
.act{{font-weight:700}} tr.buy .act{{color:#c0392b}} tr.sell .act{{color:#27ae60}}
tr.wait .act{{color:#888}} tr.limit .act{{color:#e67e22}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{flex:1;min-width:150px;background:#fff3e0;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:20px;color:#d35400}} .kpi span{{font-size:12px;color:#888}}
.note{{font-size:13.5px;line-height:1.8;color:#444}} .note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.alert{{background:#fdecea;border-left:4px solid #e74c3c;padding:12px 16px;border-radius:8px;
font-size:14px;line-height:1.8;color:#922;margin-bottom:18px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:13px}}
tr.fake{{background:#fdecea}} tr.real{{background:#e8f5e9}}
</style></head><body><div class="wrap">
<h1>龙头策略 · "涨停买不进" 现实约束分析</h1>
<div class="sub">初始资金 ¥{meta['init']:,} · N={meta['N']} · 持{meta['hold']}天 · 回测区间 {meta['区间']} ·
涨停判定: 主板10%/双创20%, 收盘封板(收盘≈最高且达涨停线)视为14:45买不进</div>

<div class="alert">
<b>⚠️ 颠覆性结论: 龙头策略原回测收益是"虚假"的。</b><br>
选股逻辑专挑<b>当日涨幅最强的热门行业龙头</b>, 而这些票尾盘 14:45 时绝大多数已<b>涨停封死、散户根本排不进</b>。
原回测(harness_c_ma60 等)直接用<b>收盘价成交、不检查涨停板</b>, 等于假设"以涨停价买进了根本买不进的票",
把收益严重高估。一旦加入"涨停买不进"的现实约束, 真实可执行的收益<b>趋近于零甚至为负</b>。
此前所有"龙头策略 +MA60 最优 +183%~+322%"的结论, 其前提(收盘价能买进价)不成立, 需重新审视。
</div>

<div class="card">
<h2>一、涨停买不进 · 量化统计</h2>
<div class="kpi">
<div><b>{sa['封板占比%']}%</b><span>始终在场 Top3龙头 涨停封板率</span></div>
<div><b>{sg['封板占比%']}%</b><span>MA60开门 Top3龙头 涨停封板率</span></div>
<div><b>{sa['首选买不进日占比%']}%</b><span>始终在场 首选(第1名)买不进日占比</span></div>
<div><b>{sg['首选买不进日占比%']}%</b><span>MA60开门 首选(第1名)买不进日占比</span></div>
</div>
<table style="margin-top:14px">
<tr><th>口径</th><th>信号日</th><th>Top3样本</th><th>涨停封板率</th><th>接近涨停率</th>
<th>首选买不进日</th><th>当天有封板龙头日</th><th>当天Top3全封板日</th></tr>
<tr><td>始终在场</td><td>{sa['信号日数']}</td><td>{sa['Top3龙头样本数']}</td>
<td><b>{sa['封板占比%']}%</b></td><td>{sa['接近涨停占比%']}%</td>
<td><b>{sa['首选买不进日占比%']}%</b></td><td>{sa['当天有封板龙头日占比%']}%</td><td>{sa['当天Top3全封板日占比%']}%</td></tr>
<tr><td>MA60开门</td><td>{sg['信号日数']}</td><td>{sg['Top3龙头样本数']}</td>
<td><b>{sg['封板占比%']}%</b></td><td>{sg['接近涨停占比%']}%</td>
<td><b>{sg['首选买不进日占比%']}%</b></td><td>{sg['当天有封板龙头日占比%']}%</td><td>{sg['当天Top3全封板日占比%']}%</td></tr>
</table>
<p class="note" style="margin-top:10px">解读: 龙头策略"选当日涨幅最强"的本质, 决定了选出的几乎都是涨停封板票。
<b>97.6% 的信号日, 你最想买的第1名龙头已经涨停封死</b>——这不是个别现象, 是策略结构性的流动性和前视陷阱。</p>
</div>

<div class="card">
<h2>二、修正回测: 考虑"涨停买不进"后收益变多少</h2>
<table>
<tr><th>方案</th><th>期末净值</th><th>总收益</th><th>夏普</th><th>最大回撤</th><th>涨停跳过槽次</th><th>性质</th></tr>
<tr class="fake"><td>无闸口 baseline(原口径, 无视涨停)</td><td>{b_nog['期末净值']:,.0f}</td>
<td>{b_nog['总收益%']:+.2f}%</td><td>{b_nog['夏普']}</td><td>{b_nog['最大回撤%']:.2f}%</td>
<td>{b_nog['涨停跳过槽次']}</td><td>❌ 虚假(买买不进的涨停板)</td></tr>
<tr class="fake"><td>MA60开门 baseline(原口径)</td><td>{b_bl['期末净值']:,.0f}</td>
<td>{b_bl['总收益%']:+.2f}%</td><td>{b_bl['夏普']}</td><td>{b_bl['最大回撤%']:.2f}%</td>
<td>{b_bl['涨停跳过槽次']}</td><td>❌ 虚假</td></tr>
<tr><td>MA60开门 skip(封板该槽空置)</td><td>{b_sk['期末净值']:,.0f}</td>
<td>{b_sk['总收益%']:+.2f}%</td><td>{b_sk['夏普']}</td><td>{b_sk['最大回撤%']:.2f}%</td>
<td>{b_sk['涨停跳过槽次']}</td><td>基本空仓, 极少交易</td></tr>
<tr class="real"><td>MA60开门 next(顺延非封板候选)</td><td>{b_nx['期末净值']:,.0f}</td>
<td>{b_nx['总收益%']:+.2f}%</td><td>{b_nx['夏普']}</td><td>{b_nx['最大回撤%']:.2f}%</td>
<td>{b_nx['涨停跳过槽次']}</td><td>✅ 现实能买到(次强龙头)</td></tr>
</table>
<p class="note" style="margin-top:10px">
<b>skip(封板就空着该槽)</b>: 收益从 +322.6% 暴跌到 <b>+4.8%</b>——因为 95% 的候选都封板, 你几乎天天空仓, 只在少数非封板日勉强交易。<br>
<b>next(顺延到没封板的次强龙头)</b>: 反而 <b>-3.1%</b>——能买到的"非封板候选"其实是相对弱势股, 真正的 α 恰好在那些你买不到的封板龙头里。<br>
<b>结论: 龙头策略的 alpha 几乎完全建立在"买到涨停板"上, 而涨停板买不到。真实可执行的收益趋近于零。</b></p>
</div>

<div class="card">
<h2>三、文字逻辑 · 请你核对</h2>
<p class="note">
<span class="tag">逻辑链</span>
<ol style="margin:8px 0 0 18px;line-height:1.9">
<li><b>选股目标=当日涨幅最强</b>。龙头策略 topn_leaders 按 day_ret(当日涨幅)降序取前 N 名。涨幅最强 ⇔ 当天已大涨 → 自然逼近涨停。</li>
<li><b>14:45 选股时, 涨停板已封死</b>。A股涨停板一旦封单, 14:45 之后散户几乎无法成交(排队轮不到)。我们用"收盘封板(收盘≈最高且达涨停线)"作为代理——收盘封说明尾盘已封, 14:45 基本买不进。这是日线数据下最宽松的"买不进"定义, 实际比例只会更高(部分票 14:45 已封、尾盘未开)。</li>
<li><b>原回测未检查此约束</b>。harness_c_ma60 等用 <code>entry_fill = close*(1+SLIP)</code> 直接按收盘价成交, 等于假设能买到涨停板 → 收益虚假高估。</li>
<li><b>修正后收益崩塌</b>。skip 模式(封板空置)只剩 +4.8%(几乎没交易); next 模式(改买次强非封板)反而 -3.1%。说明 alpha 在"买不到的封板龙头"里。</li>
</ol>
<b>请你重点检查以下逻辑漏洞:</b>
<ul style="margin:6px 0 0 18px;line-height1.9">
<li>① <b>涨停判定口径</b>: 我用"收盘涨停封板"代理 14:45 状态。若你有分钟级数据, 可改用"14:45 实时价是否已达涨停且封单"精确判定, 结论可能更严苛(买不进比例更高)。</li>
<li>② <b>板块涨停幅度</b>: 我按代码前缀(30/688=20%, 其余=10%)区分, 未处理 ST(5%)与北交所(30%)。样本里龙头多为热门行业主板/双创, 影响极小, 但需知此近似。</li>
<li>③ <b>"顺延次强"是否现实</b>: next 模式假设 14:45 你能识别封板并改选次强非封板龙头。现实中可行(看实时涨幅), 但日线回测用收盘判定, 已是最乐观假设; 即便如此仍 -3.1%, 说明策略本身在现实约束下不赚钱。</li>
<li>④ <b>前视偏差再确认</b>: 原回测用"当日涨幅"排序选股, 而当日涨幅要收盘才知道。严格说 14:45 选股应基于"14:45 实时涨幅", 但这只会让"选出的票更可能是涨停"——不改变"买不进"结论, 反而加强。</li>
</ul>
</p>
</div>

<div class="card">
<h2>四、逐笔样例 · 涨停跳过(skip版, 看哪些买不进)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th><th>行业</th>
<th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th><th>账户净值</th>
<th>大盘MA60</th><th>备注</th></tr></thead><tbody>{rows_skip}</tbody></table>
</div>

<div class="card">
<h2>五、逐笔样例 · 现实可买(next版, 顺延非封板候选)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th><th>行业</th>
<th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th><th>账户净值</th>
<th>大盘MA60</th><th>备注</th></tr></thead><tbody>{rows_next}</tbody></table>
<p class="note" style="margin-top:10px">完整逐笔见 <code>c_tradelog_N3_hold3_ma60_limitup_skip.csv</code>(被跳过的封板票)
与 <code>c_tradelog_N3_hold3_ma60_limitup_next.csv</code>(现实能买到的次强龙头)。</p>
</div>

<div class="card">
<h2>六、结论与可行方向</h2>
<p class="note">
<b>结论</b>: 龙头策略(14:45 选当日涨幅最强、持3天)在"收盘价能买进"假设下有 +183%~+322% 收益, 但这是虚假的——
95% 的候选票涨停封板、买不进。真实可执行的收益趋近于零(next 模式 -3.1%)。
此前报告里"龙头 + MA60 最优"的结论因此需打折扣, 它衡量的是"如果能买到涨停板会怎样", 而非"实盘能赚多少"。<br><br>
<b>若想在实盘落地, 可考虑的方向(需另行回测):</b>
<ul style="margin:6px 0 0 18px;line-height:1.9">
<li>(a) <b>改选"强但未封板"的票</b>: 在热门行业里挑当日涨幅第 N 名之后、未触及涨停的强势股(主动避开流动性陷阱), 本质是另一套选股逻辑, 需重新验证 alpha。</li>
<li>(b) <b>T+1 开盘追入</b>: 当日涨停买不进, 次日开盘挂单追。但次日常高开低走, 且改变了"持3天"的计时起点, 需重测。</li>
<li>(c) <b>盘前/早盘选股</b>: 用早盘(如 10:00)涨幅与板块强度提前布局, 避开尾盘涨停封板, 但引入盘中波动与前视新问题。</li>
<li>(d) <b>回归多因子动量框架</b>: 原动量策略(套牢盘过滤+自适应止盈止损)本就分散选股、不专挑涨停, 流动性陷阱小得多——结合此前"动量加 MA60 反而更差"的结论, 动量策略或许比"纯龙头"更贴近实盘。</li>
</ul>
</p>
</div>
</div></body></html>"""

(HERE / "c_limitup_report.html").write_text(HTML, encoding="utf-8")
print("完成 -> c_limitup_report.html")
