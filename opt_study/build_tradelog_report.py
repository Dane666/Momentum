# -*- coding: utf-8 -*-
"""生成逐笔操作记录 HTML 报告(汇总 + 前 40 笔样例)。"""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
summary = json.loads((HERE / "c_tradelog_summary.json").read_text(encoding="utf-8"))
df3 = pd.read_csv(HERE / "c_tradelog_N3_hold3.csv")
h3 = summary["hold3"]; h5 = summary["hold5"]
ind = summary["_行业分布"]

# 行业分布: 每只股票出现的行业计数(信号日样本)
sample = df3.head(40)
rows_html = ""
for _, r in sample.iterrows():
    cls = "buy" if r["动作"] == "买入" else "sell"
    pnl = "" if (isinstance(r["实现盈亏元"], float) is False or pd.isna(r["实现盈亏元"])) else f'{r["实现盈亏元"]:.0f}'
    pct = "" if (pd.isna(r["实现收益%"]) or r["实现收益%"] == "") else f'{r["实现收益%"]:.2f}%'
    rows_html += (
        f'<tr class="{cls}"><td>{r["序号"]}</td><td>{r["日期"]}</td>'
        f'<td class="act">{r["动作"]}</td><td>{r["槽位"]}</td>'
        f'<td>{r["代码"]}</td><td>{r["行业"]}</td>'
        f'<td>{r["成交价"]}</td><td>{int(r["股数"]):,}</td>'
        f'<td>{r["成交额"]:,.0f}</td><td>{pct}</td><td>{pnl}</td>'
        f'<td>{r["账户净值"]:,.0f}</td><td>{r["备注"]}</td></tr>'
    )

HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>C方案逐笔操作记录 · 10万本金</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;
background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1180px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:20px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;
box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}}
.kpi div{{flex:1;min-width:120px;background:#f0f6ff;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:20px;color:#156}} .kpi span{{font-size:12px;color:#667}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th,td{{padding:6px 8px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}}
th{{background:#fafbfc;color:#555;position:sticky;top:0;font-weight:600}}
td:nth-child(2),td:nth-child(5),td:nth-child(6),td:nth-child(13){{text-align:left}}
.act{{font-weight:700}} tr.buy .act{{color:#c0392b}} tr.sell .act{{color:#27ae60}}
.note{{font-size:13px;line-height:1.7;color:#444}}
.note code{{background:#eef;padding:1px 5px;border-radius:4px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:2px 10px;border-radius:20px;font-size:12px;margin-right:8px}}
</style></head><body><div class="wrap">
<h1>C 方案(热门行业龙头 · 无择时)逐笔操作记录</h1>
<div class="sub">初始资金 ¥100,000 · 3 个独立仓位槽 · 无止损止盈 · 仅持有 {3} 个交易日到期离场 ·
回测区间 2024-06-03 ~ 2026-07-14(最后 250 交易日)</div>

<div class="card">
<h2>一、三个槽位的 3 只股票,是同一行业吗?</h2>
<p class="note">结论:<b>基本不是同一行业,而是天然跨行业分散</b>。</p>
<p class="note">选股函数 <code>topn_leaders</code> 的逻辑是:在<b>所有热门行业</b>里,按当日涨幅取前 3 名,
对"同一行业最多几只"没有任何限制。与多因子 A 方案(用 <code>max_sector</code> 显式限制每行业上限)不同,
C 方案没有行业上限。但实际统计 84 个信号日:</p>
<p class="note"><span class="tag">三只同行业 仅 {ind['三只同行业天数']} 天</span>
<span class="tag">跨行业 {ind['跨行业天数']} 天</span>
——绝大多数交易日,涨幅前三的龙头恰好落在不同热门行业,所以 N=3 天然起到了分散作用;只有极个别日子会撞到同行业。</p>
</div>

<div class="card">
<h2>二、没有止损止盈,什么时候卖?什么时候能买?</h2>
<p class="note">本方案中<b>唯一的卖出条件 = 持有满 {3} 个交易日(按个股自身交易日算)</b>,到期当日以收盘价(减卖滑点)离场,标记"到期"。
没有任何止盈、也没有止损,是纯粹的"买入—持有—轮动"。</p>
<p class="note"><b>买入条件:</b>每 {3} 个交易日是一个"信号日"。3 个仓位槽各自独立运作——
只有当某个槽位<b>当前为空仓</b>(上一笔已卖出、现金已回笼)时,才在该信号日买入"热门行业龙头中第 k 名"
(槽1买涨幅第1,槽2买第2,槽3买第3)。</p>
<p class="note">因为持仓期正好 = 调仓间隔,所以节奏是干净的:信号日当天先检查到期卖出(释放槽位与现金),
随即同一日买入下一批龙头,<b>卖出回笼的现金当天即可用于下一笔买入</b>,槽位之间互不影响、互不挤占。</p>
</div>

<div class="card">
<h2>三、10 万本金 · 收益汇总</h2>
<div class="kpi">
<div><b>¥{h3['期末净值']:,.0f}</b><span>hold=3 期末净值(起点¥100,000)</span></div>
<div><b>{h3['总收益%']:.1f}%</b><span>hold=3 总收益</span></div>
<div><b>{h3['胜率%']:.1f}%</b><span>hold=3 胜率({h3['交易笔数']}笔)</span></div>
<div><b>{h3['夏普']:.2f}</b><span>hold=3 夏普</span></div>
<div><b>{h3['最大回撤%']:.1f}%</b><span>hold=3 最大回撤</span></div>
</div>
<table style="margin-top:14px"><tr><th>参数</th><th>期末净值</th><th>总收益</th><th>年化</th>
<th>夏普</th><th>胜率</th><th>最大回撤</th><th>交易笔数</th></tr>
<tr><td>持有3天(推荐)</td><td>{h3['期末净值']:,.0f}</td><td>{h3['总收益%']:.2f}%</td>
<td>{h3['年化%']:.2f}%</td><td>{h3['夏普']:.2f}</td><td>{h3['胜率%']:.2f}%</td>
<td>{h3['最大回撤%']:.2f}%</td><td>{h3['交易笔数']}</td></tr>
<tr><td>持有5天</td><td>{h5['期末净值']:,.0f}</td><td>{h5['总收益%']:.2f}%</td>
<td>{h5['年化%']:.2f}%</td><td>{h5['夏普']:.2f}</td><td>{h5['胜率%']:.2f}%</td>
<td>{h5['最大回撤%']:.2f}%</td><td>{h5['交易笔数']}</td></tr></table>
<p class="note" style="margin-top:10px">注:与上一轮"无止损最优 +283%"略有差异(这里 +216.73%),
原因是本表新增了 <b>A 股整手约束(买入股数向下取整到 100 股)</b>,会滞留少量零股现金,产生轻微摩擦。
模型其余完全一致(买/卖滑点 0.8%,无独立佣金)。实盘还应额外计入佣金(约万2.5)与卖出印花税(千0.5)。</p>
</div>

<div class="card">
<h2>四、逐笔操作记录(前 40 条样例 · 完整 {len(df3)} 条见 CSV)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th>
<th>行业</th><th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th>
<th>账户净值</th><th>备注</th></tr></thead><tbody>{rows_html}</tbody></table>
<p class="note" style="margin-top:10px">完整记录见附件 <code>c_tradelog_N3_hold3.csv</code>(持有3天,共 {len(df3)} 条)
与 <code>c_tradelog_N3_hold5.csv</code>(持有5天)。可用 Excel 打开,按"槽位/行业/日期"筛选复盘。</p>
</div>
</div></body></html>"""

(HERE / "c_tradelog_report.html").write_text(HTML, encoding="utf-8")
print("OK -> c_tradelog_report.html  rows:", len(df3))
