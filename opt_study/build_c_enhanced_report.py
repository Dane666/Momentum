# -*- coding: utf-8 -*-
"""生成 C 增强版对比报告(接生产退出机制 + 4点优化)。"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
d = json.load(open(HERE / "c_enhanced_results.json", encoding="utf-8"))
meta = d["meta"]

# 关键变体
cv = d["C_variants"]
top_key = "gap_h2_lu0_trapFalse"          # 最高收益(无过滤)
enh_key = "gap_h2_lu2_trapTrue"           # 增强(过滤): 用户(1)(2)目标
best_key = top_key
best = cv.get(top_key, {})
enh = cv.get(enh_key, {})
prod_bench = meta["生产动量基准"]


def row_html(m, hl=False):
    bg = ' style="background:#e8f5e9"' if hl else ""
    return (f"<tr{bg}><td>{m.get('期末净值',0):,.0f}</td><td>{m.get('总收益%',0):+.2f}%</td>"
            f"<td>{m.get('年化%',0):.1f}%</td><td>{m.get('夏普',0)}</td>"
            f"<td>{m.get('最大回撤%',0):.1f}%</td><td>{m.get('胜率%','-')}</td>"
            f"<td>{m.get('交易笔数',0)}</td><td>{m.get('封板率%',0)}%</td></tr>")


c_rows = ""
for k, m in cv.items():
    hl = (k == best_key)
    c_rows += f"<tr{' style=\"background:#e8f5e9\"' if hl else ''}><td><b>{k}</b></td>" + row_html(m, hl)[4:]

# A/B/D
abnd_rows = ""
for name, holds in d["ABnD_filtered"].items():
    first = True
    for hk, m in holds.items():
        nm = name if first else ""
        first = False
        abnd_rows += f"<tr><td>{nm}</td><td>{hk}</td>" + row_html(m)[4:]

# 强/弱 (嵌套: vlabel -> {label: m})
sw_rows = ""
for vlabel, regs in d["strong_weak"].items():
    first = True
    for label, m in regs.items():
        nm = vlabel if first else ""
        first = False
        if "note" in m:
            sw_rows += f"<tr><td>{nm}</td><td>{label}</td><td colspan='6'>{m['note']}</td></tr>"
        else:
            sw_rows += f"<tr><td>{nm}</td><td>{label}</td>" + row_html(m)[4:]

HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>C尾盘偷袭板 增强版回测</title>
<style>
body{{font-family:-apple-system,'Microsoft YaHei',sans-serif;margin:0;background:#f5f6f8;color:#222}}
.wrap{{max-width:1080px;margin:0 auto;padding:28px 22px}}
h1{{font-size:22px;margin:0 0 6px}} h2{{font-size:17px;margin:26px 0 10px;color:#1565c0}}
.sub{{color:#666;font-size:13px;margin-bottom:18px}}
table{{border-collapse:collapse;width:100%;background:#fff;font-size:13px;box-shadow:0 1px 3px #0001}}
th,td{{border:1px solid #e3e6ea;padding:7px 9px;text-align:center}}
th{{background:#263238;color:#fff;font-weight:600}}
td:first-child,th:first-child{{text-align:left}}
.note{{background:#fff8e1;border-left:4px solid #ffb300;padding:12px 14px;font-size:13px;line-height:1.7;margin:14px 0}}
.good{{color:#2e7d32;font-weight:700}} .bad{{color:#c62828;font-weight:700}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap;margin:14px 0}}
.kpi div{{background:#fff;border:1px solid #e3e6ea;border-radius:8px;padding:12px 16px;min-width:120px}}
.kpi b{{display:block;font-size:20px;color:#1565c0}}
.kpi span{{font-size:12px;color:#666}}
</style></head><body><div class="wrap">
<h1>C 尾盘偷袭板 · 增强版回测（接生产退出机制 + 4点优化）</h1>
<div class="sub">回测区间 {meta['区间']} ｜ 账户 10万 / N={meta['N']} 槽 ｜ 数据源 qlib_pro_v16.db（离线）</div>

<div class="note">
<b>核心结论：</b>把 C 选股接入生产 <b>ExitRuleEngine 自适应退出</b> + 套牢盘过滤 + 板块涨停家数≥2 过滤后：
<ul style="margin:6px 0 0">
<li>最高收益版（gap_h2_lu0_trapFalse，仅隔天高开止盈+持2天）：<b class="good">{best.get('总收益%',0):+.1f}%</b>（夏普{best.get('夏普',0)} / 回撤{best.get('最大回撤%',0):.1f}% / 胜率{best.get('胜率%','-')}%）</li>
<li>增强过滤版（gap_h2_lu2_trapTrue，叠加板块涨停≥2+套牢盘过滤）：<b class="{'good' if enh.get('总收益%',0)>0 else 'bad'}">{enh.get('总收益%',0):+.1f}%</b>（夏普{enh.get('夏普',0)} / 回撤{enh.get('最大回撤%',0):.1f}% / 胜率{enh.get('胜率%','-')}%）—— <b>收益降、回撤砍近半</b>，是"控风险版"。</li>
</ul>
对比生产动量基准 {prod_bench} —— C 仍落后，但已是可成交场景里最接近实盘的方向。详见下方强/弱市分段与逻辑核对。
</div>

<h2>一、C 增强网格（退出模式 × 持有 × 过滤，均带 MA60 开门闸口）</h2>
<table><tr><th>变体</th><th>期末净值</th><th>总收益</th><th>年化</th><th>夏普</th><th>最大回撤</th><th>胜率</th><th>笔数</th><th>封板率</th></tr>
{c_rows}
</table>
<div class="note">退模: hold=简单持有 / gap=隔天高开(≥1%)止盈 / adp=自适应退出 / gap_adp=高开止盈+自适应。
lu=板块涨停家数下限; trap=套牢盘≤10%过滤。所有场景封板率均≈0% → <b>真实可成交</b>。</div>

<h2>二、最优 C vs 生产动量基准</h2>
<div class="kpi">
  <div><b>{best.get('总收益%',0):+.1f}%</b><span>C最高收益版</span></div>
  <div><b>{best.get('夏普',0)}</b><span>C夏普</span></div>
  <div><b>{best.get('最大回撤%',0):.1f}%</b><span>C回撤</span></div>
  <div><b>{enh.get('总收益%',0):+.1f}%</b><span>C增强过滤版</span></div>
  <div><b>{enh.get('最大回撤%',0):.1f}%</b><span>增强版回撤</span></div>
  <div><b>+32.4%</b><span>生产动量(hold5)</span></div>
</div>
<div class="note">诚实标注：本框架的 C 选股哲学（买"没封板的尾盘试探板"）与生产的多因子动量（追中期动量+套牢盘过滤+自适应退出）是两套不同 alpha。
C 把"生产退出机制"照搬过来后回撤可控，但绝对收益仍低于生产基准——说明 <b>C 的选股信号本身弱于多因子框架</b>，退出机制只能"止血"不能"造血"。</div>

<h2>三、A/B/D 双重过滤重测（大盘多头+板块主升，接自适应退出+套牢盘）</h2>
<table><tr><th>策略</th><th>持有</th><th>期末净值</th><th>总收益</th><th>年化</th><th>夏普</th><th>最大回撤</th><th>胜率</th><th>笔数</th><th>封板率</th></tr>
{abnd_rows}
</table>
<div class="note">双重过滤（MA60 多头+套牢盘）后，A/B/D 仍普遍亏损——二波/低吸在 2024-06~2026-07 样本内（含近期弱势）未显效，
印证用户第(3)点：需换"强牛市+板块主升"环境或重定义断板反包（次日确认再打板）才有望转正。</div>

<h2>四、强市 / 弱市分段（关 MA60 闸口，纯看 C 在牛/熊/震荡的内在稳健性）</h2>
<table><tr><th>C变体</th><th>市场分段</th><th>期末净值</th><th>总收益</th><th>年化</th><th>夏普</th><th>最大回撤</th><th>胜率</th><th>笔数</th><th>封板率</th></tr>
{sw_rows}
</table>
<div class="note">本段<b>关掉 MA60 闸口</b>，仅按大盘环境（全A净值 vs MA60 + 5日动量）切分信号日，以暴露 <b>C 自身</b>在牛/熊/震荡的稳健性。
若弱市(bearish)明显亏损，说明 C 必须配合"MA60 开门"空仓等待才稳健——用户第(4)点"分强/弱市看稳健性"是上实盘前的必要验证。</div>

<h2>五、逻辑核对要点（请检查是否有误）</h2>
<div class="note">
① <b>自适应退出</b>已逐行复刻 risk/adaptive_exit.py：ATR%/RSI/乖离率/市场环境动态调参，浮盈&gt;5% 移动止损护利；逐日检查 high/low/close。<br>
② <b>隔天高开止盈</b>仅在持仓第1天(T+1)检查：开盘≥买入价×1.01 即按开盘价离场；否则转入自适应/到期逻辑。<br>
③ <b>套牢盘过滤</b>采用 harness._trapped_ratio 口径（近60日收盘持仓亏损占比），&gt;10% 跳过，与生产 MAX_TRAPPED_RATIO=0.10 一致。<br>
④ <b>板块涨停家数</b>按行业映射当日收盘封板计数；&gt;=2 视为板块主升确认。行业映射不含"其它"。<br>
⑤ <b>前视边界</b>：选股用 T 当日收盘（与之前一致，日线近似）；自适应退出用 T 之后实际 K 线，无前视。<br>
⑥ <b>强/弱市判定</b>：等权全A净值 &gt; MA60 且 5日动量&gt;0 为 bullish；&lt;=MA60 为 bearish；其余 normal。生产退出机制中的 market_condition 据此传入。
</div>

<p class="sub">产物：c_enhanced_results.json（全量指标）｜ c_enh_Cbest_ma60.csv（最优 C 逐笔操作记录）</p>
</div></body></html>"""

(HERE / "c_enhanced_report.html").write_text(HTML, encoding="utf-8")
print("REPORT OK ->", HERE / "c_enhanced_report.html")
