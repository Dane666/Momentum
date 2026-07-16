# -*- coding: utf-8 -*-
"""
生成对比报告：最高收益版 C(gap_h2_lu0_trapFalse) vs 生产动量策略
- 参数设置对比
- 胜率 / 收益对比
- 逐笔交易差异
数据来源：
  c_enh_Ctop_ma60.csv           C 逐笔
  prod_momentum_tradelog_hold3.csv / hold5.csv  生产动量逐笔
  c_enhanced_results.json       C 指标
  prod_momentum_tradelog_metrics.json  生产指标
不修改任何原有文件，仅生成 HTML。
"""
import csv, json, os
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))

def load_c_sells():
    rows = []
    with open(os.path.join(HERE, 'c_enh_Ctop_ma60.csv'), encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            if r['动作'] == '卖出':
                rows.append(dict(date=r['日期'], code=r['代码'], sec=r['行业'],
                                 ret=float(r['实现收益%']), pnl=float(r['实现盈亏元']),
                                 reason=r['备注'], nav=float(r['账户净值'])))
    return rows

def load_prod(fn):
    rows = []
    with open(os.path.join(HERE, fn), encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            if r['代码'] and r['前向收益%']:
                rows.append(dict(date=r['调仓日'], code=r['代码'], sec=r['名称行业'],
                                 ret=float(r['前向收益%']), pnl=float(r['盈亏元']),
                                 reason=r['退出原因'], hold=r['持有天数']))
    return rows

def stats(rows):
    n = len(rows)
    win = [t for t in rows if t['ret'] > 0]
    loss = [t for t in rows if t['ret'] <= 0]
    return dict(
        n=n, win=len(win), loss=len(loss),
        wr=round(100*len(win)/n, 1) if n else 0,
        avg_win=round(sum(t['ret'] for t in win)/len(win), 2) if win else 0,
        avg_loss=round(sum(t['ret'] for t in loss)/len(loss), 2) if loss else 0,
        best=round(max(t['ret'] for t in rows), 2) if rows else 0,
        worst=round(min(t['ret'] for t in rows), 2) if rows else 0,
        codes=set(t['code'] for t in rows),
    )

def bucket(rows):
    b = {'≥+10%':0, '+5~10%':0, '0~+5%':0, '-5~0%':0, '≤-5%':0}
    for t in rows:
        r = t['ret']
        if r >= 10: b['≥+10%'] += 1
        elif r >= 5: b['+5~10%'] += 1
        elif r >= 0: b['0~+5%'] += 1
        elif r > -5: b['-5~0%'] += 1
        else: b['≤-5%'] += 1
    return b

c = load_c_sells()
p3 = load_prod('prod_momentum_tradelog_hold3.csv')
p5 = load_prod('prod_momentum_tradelog_hold5.csv')
cs, ps3, ps5 = stats(c), stats(p3), stats(p5)
cm = json.load(open(os.path.join(HERE, 'c_enhanced_results.json')))['C_variants']['gap_h2_lu0_trapFalse'] \
    if 'C_variants' in json.load(open(os.path.join(HERE, 'c_enhanced_results.json'))) else None
# robust extraction
rawc = json.load(open(os.path.join(HERE, 'c_enhanced_results.json')))
def deep_find(o, key):
    if isinstance(o, dict):
        if key in o and isinstance(o[key], dict) and '总收益%' in o[key]:
            return o[key]
        for v in o.values():
            r = deep_find(v, key)
            if r: return r
    return None
cm = deep_find(rawc, 'gap_h2_lu0_trapFalse')
pm = json.load(open(os.path.join(HERE, 'prod_momentum_tradelog_metrics.json')))

cbk, pbk = bucket(c), bucket(p3)
overlap = sorted(cs['codes'] & ps3['codes'])
c_sec = Counter(t['sec'] for t in c).most_common(8)

# 强弱市分段
sw = deep_find(rawc, 'gap_h2_lu0_trapFalse')  # placeholder
strong_weak = None
for k, v in rawc.items():
    if isinstance(v, dict):
        for kk, vv in v.items():
            if 'gap_h2_lu0' in str(kk) and isinstance(vv, dict) and '强市bullish' in vv:
                strong_weak = vv
if strong_weak is None:
    # search recursively
    def find_sw(o):
        if isinstance(o, dict):
            if '强市bullish' in o: return o
            for v in o.values():
                r = find_sw(v)
                if r: return r
        return None
    strong_weak = find_sw(rawc)

def pct_cls(v):
    return 'pos' if v > 0 else ('neg' if v < 0 else '')

# ---------- 构建 HTML ----------
def trow_c(i, t):
    return f"<tr><td>{i}</td><td>{t['date']}</td><td class='code'>{t['code']}</td><td>{t['sec']}</td><td class='num {pct_cls(t['ret'])}'>{t['ret']:+.2f}%</td><td class='num {pct_cls(t['pnl'])}'>{t['pnl']:+,.0f}</td><td>{t['reason']}</td></tr>"

def trow_p(i, t):
    return f"<tr><td>{i}</td><td>{t['date']}</td><td class='code'>{t['code']}</td><td>{t['sec']}</td><td class='num {pct_cls(t['ret'])}'>{t['ret']:+.2f}%</td><td class='num {pct_cls(t['pnl'])}'>{t['pnl']:+,.0f}</td><td>{t['hold']}天</td><td>{t['reason']}</td></tr>"

c_rows = '\n'.join(trow_c(i+1, t) for i, t in enumerate(c))
p_rows = '\n'.join(trow_p(i+1, t) for i, t in enumerate(p3))

# reason groups
c_reason = Counter(t['reason'] for t in c)
p_reason = Counter()
for t in p3:
    rs = t['reason']
    if rs.startswith('Adaptive_SL'): p_reason['止损(Adaptive_SL)'] += 1
    elif rs.startswith('Adaptive_TP'): p_reason['止盈(Adaptive_TP)'] += 1
    elif rs.startswith('Adaptive_Bias'): p_reason['乖离止盈(Bias)'] += 1
    elif rs.startswith('Adaptive_MA5'): p_reason['破MA5'] += 1
    elif rs.startswith('Adaptive_RSI'): p_reason['RSI超买'] += 1
    elif rs.startswith('Time_Exit'): p_reason['到期'] += 1
    else: p_reason[rs] += 1

def dist_bar(bk, total, color):
    out = []
    for k, v in bk.items():
        w = round(100*v/total, 1) if total else 0
        out.append(f"<div class='distrow'><span class='distlbl'>{k}</span><div class='distbar'><div class='distfill' style='width:{w}%;background:{color}'></div></div><span class='distval'>{v} ({w}%)</span></div>")
    return '\n'.join(out)

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>C尾盘偷袭板(最高收益版) vs 生产动量策略 — 全维度对比</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system,'PingFang SC','Microsoft YaHei',sans-serif; background:#f4f6f9; color:#1a2233; line-height:1.6; padding:32px 20px; }}
  .wrap {{ max-width:1180px; margin:0 auto; }}
  h1 {{ font-size:26px; font-weight:800; margin-bottom:6px; }}
  .sub {{ color:#5a6b85; font-size:14px; margin-bottom:24px; }}
  .card {{ background:#fff; border:1px solid #e3e8f0; border-radius:14px; padding:24px 26px; margin-bottom:22px; box-shadow:0 2px 10px rgba(20,40,80,.04); }}
  h2 {{ font-size:19px; font-weight:700; margin-bottom:14px; display:flex; align-items:center; gap:9px; }}
  h2 .dot {{ width:9px; height:9px; border-radius:50%; background:#3b6fe0; }}
  h3 {{ font-size:15px; font-weight:700; margin:18px 0 10px; color:#33405a; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th,td {{ padding:8px 10px; text-align:left; border-bottom:1px solid #eef1f6; }}
  th {{ background:#f7f9fc; font-weight:700; color:#3a4761; font-size:12.5px; white-space:nowrap; }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  td.code {{ font-family:'SF Mono',Consolas,monospace; color:#2456c9; }}
  .pos {{ color:#d1342f; font-weight:600; }}   /* 涨=红 */
  .neg {{ color:#0a8f52; font-weight:600; }}   /* 跌=绿 */
  .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:22px; }}
  .kpi {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }}
  .kbox {{ background:#f8fafd; border:1px solid #e8edf5; border-radius:10px; padding:14px 16px; }}
  .kbox .lbl {{ font-size:12px; color:#6c7a94; margin-bottom:4px; }}
  .kbox .val {{ font-size:22px; font-weight:800; }}
  .kbox .val.c {{ color:#c0392b; }}
  .kbox .val.p {{ color:#2c5aa0; }}
  .tag {{ display:inline-block; padding:2px 9px; border-radius:20px; font-size:11.5px; font-weight:700; }}
  .tag.c {{ background:#fdecea; color:#c0392b; }}
  .tag.p {{ background:#e8f0fb; color:#2c5aa0; }}
  .cmp td:nth-child(2){{ background:#fdf6f5; }}
  .cmp td:nth-child(3){{ background:#f4f8fd; }}
  .cmp th:nth-child(2){{ background:#fbe9e7; color:#c0392b;}}
  .cmp th:nth-child(3){{ background:#e3edfb; color:#2c5aa0;}}
  .note {{ background:#fff8e6; border:1px solid #f2e2b0; border-radius:10px; padding:13px 16px; font-size:13px; color:#6b5a1e; margin:14px 0; }}
  .note b {{ color:#8a6d0f; }}
  .win {{ background:#fef4f3; border-left:4px solid #d1342f; padding:12px 16px; border-radius:8px; margin:10px 0; font-size:13.5px; }}
  .distrow {{ display:flex; align-items:center; gap:10px; margin-bottom:7px; font-size:12.5px; }}
  .distlbl {{ width:58px; color:#556; text-align:right; }}
  .distbar {{ flex:1; background:#eef1f6; height:16px; border-radius:8px; overflow:hidden; }}
  .distfill {{ height:100%; border-radius:8px; }}
  .distval {{ width:86px; color:#445; }}
  ul {{ padding-left:20px; font-size:13.5px; }}
  li {{ margin-bottom:7px; }}
  .scroll {{ max-height:520px; overflow-y:auto; border:1px solid #eef1f6; border-radius:10px; }}
  .scroll table th {{ position:sticky; top:0; z-index:2; }}
  .foot {{ text-align:center; color:#8a97ad; font-size:12px; margin-top:8px; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>C 尾盘偷袭板(最高收益版) vs 生产动量策略 — 全维度对比</h1>
  <div class="sub">对比对象：<span class="tag c">C · gap_h2_lu0_trapFalse</span> &nbsp;vs&nbsp; <span class="tag p">生产动量 · config.py</span>　｜　同一 DB(qlib_pro_v16)、同一测试窗口(最近约250交易日)、初始资金 10 万、双边滑点 0.008</div>

  <!-- ========== 1. 核心结论 ========== -->
  <div class="card">
    <h2><span class="dot"></span>核心结论</h2>
    <div class="kpi">
      <div class="kbox"><div class="lbl">C 总收益</div><div class="val c">+{cm['总收益%']}%</div></div>
      <div class="kbox"><div class="lbl">生产动量 总收益(持3)</div><div class="val p">{pm['hold3']['总收益%']:+}%</div></div>
      <div class="kbox"><div class="lbl">C 胜率</div><div class="val c">{cm['胜率%']}%</div></div>
      <div class="kbox"><div class="lbl">生产动量 胜率(持3)</div><div class="val p">{pm['hold3']['胜率%']}%</div></div>
    </div>
    <div class="win">
      <b>一句话：</b>在当前数据库口径下，C(隔天高开止盈+持2天+MA60开门闸口)以 <b>29 笔交易</b>取得 <b>+{cm['总收益%']}%</b>、胜率 <b>{cm['胜率%']}%</b>、最大回撤仅 <b>{cm['最大回撤%']}%</b>、夏普 <b>{cm['夏普']}</b>；
      生产动量策略在同期同库下持3天仅 <b>{pm['hold3']['总收益%']:+}%</b>(胜率 {pm['hold3']['胜率%']}%、121 笔)、持5天 <b>{pm['hold5']['总收益%']:+}%</b>(胜率 {pm['hold5']['胜率%']}%、75 笔)。C 全面占优的核心不是"选得更准"，而是<b>交易极少、专打热门行业龙头、让盈利奔跑而快速止损</b>。
    </div>
    <div class="note">
      <b>诚实提示(重要)：</b>历史 <code>results.json</code> 中曾记录生产动量 +32.4%(持5)/+33.98%(持3)，那是<b>旧数据快照</b>。本次用 harness 自有函数在<b>当前更新后的 DB</b> 重跑，生产动量已回落到持3=+{pm['hold3']['总收益%']}% / 持5={pm['hold5']['总收益%']}%(逐笔账户模型逐位吻合)。本页所有对比均基于<b>当前 DB 的真实重跑数字</b>，两个策略同库同期，口径一致、可比。
    </div>
  </div>

  <!-- ========== 2. 参数设置对比 ========== -->
  <div class="card">
    <h2><span class="dot"></span>1. 参数设置对比</h2>
    <table class="cmp">
      <thead><tr><th style="width:26%">参数维度</th><th>C · gap_h2_lu0_trapFalse</th><th>生产动量 · config.py</th></tr></thead>
      <tbody>
        <tr><td>选股逻辑</td><td>热门行业(资金净流入 Top-K=8)内的<b>尾盘偷袭板龙头</b>：涨幅&gt;5% 且 收盘≈最高(≥high×0.99) 且 开盘未涨停 且 当日未封板</td><td>Alpha 多因子打分：mom_5/mom_20/sharpe/chip_rate/big_order + divergence，POOL_SIZE=150 内排序</td></tr>
        <tr><td>选股时点</td><td>每日 <b>14:45</b> 尾盘</td><td>每日 <b>14:44</b> 尾盘</td></tr>
        <tr><td>持仓数 N</td><td><b>3</b>(独立仓位槽，事件驱动)</td><td>MAX_TOTAL_PICKS=<b>3</b>，每行业≤2</td></tr>
        <tr><td>持有周期</td><td><b>2 天</b>(hold=2)</td><td>HOLD_PERIOD_DEFAULT=<b>5</b>(本报告并列展示持3对照)</td></tr>
        <tr><td>退出机制</td><td><b>隔天高开止盈</b>：T+1 开盘≥买入价×1.01 即走；否则持满到期以收盘价卖出。<b>不接自适应退出</b></td><td><b>自适应退出引擎</b>(simulate_adaptive_exit)：按 ATR%/RSI/乖离/市场环境动态调参，浮盈&gt;5% 移动止损护利；固定兜底止损5%/止盈10%/破MA5/破MA20/乖离20%/RSI80</td></tr>
        <tr><td>市场择时闸口</td><td><b>MA60 开门闸口(开盘前)</b>：以 T-1 收盘判断大盘是否站上 60 日均线，跌破则当日空仓</td><td>REGIME MA20：ENABLE_REGIME_FILTER=True，REGIME_MA_WINDOW=20，指数 000001</td></tr>
        <tr><td>套牢盘过滤</td><td><b>关闭</b>(trapped_filter=False)</td><td><b>开启</b>：ENABLE_TRAPPED_FILTER=True，MAX_TRAPPED_RATIO=0.10</td></tr>
        <tr><td>板块强度过滤</td><td><b>无</b>(lu_min=0，不要求板块涨停家数)</td><td>无(该概念不适用)</td></tr>
        <tr><td>夏普/流动性门槛</td><td>沿用龙头选股(热门行业+龙头即可)</td><td>MIN_SHARPE=1.0，MIN_AMOUNT=2亿</td></tr>
        <tr><td>涨停封板防御</td><td>开盘涨停/当日封板<b>跳过不买</b>(封板率 0%)</td><td>无专门涨停防御</td></tr>
        <tr><td>滑点 / 整手 / 本金</td><td>0.008 双边 / LOT=100 / 10 万</td><td>SLIPPAGE=0.008 / — / INITIAL_CAPITAL=10万</td></tr>
      </tbody>
    </table>
    <div class="note"><b>本质差异：</b>生产动量是"<b>宽选股 + 强退出</b>"(多因子海选、自适应止盈止损频繁进出)；C 是"<b>窄选股 + 弱退出</b>"(只打热门龙头当日偷袭板、几乎持满2天让利润奔跑)。两者退出哲学完全相反——这正是逐笔差异的根源。</div>
  </div>

  <!-- ========== 3. 胜率 / 收益对比 ========== -->
  <div class="card">
    <h2><span class="dot"></span>2. 胜率 &amp; 收益对比</h2>
    <table class="cmp">
      <thead><tr><th style="width:30%">指标</th><th>C · gap_h2_lu0_trapFalse</th><th>生产动量(持3 / 持5)</th></tr></thead>
      <tbody>
        <tr><td>期末净值(10万起)</td><td class="num pos">¥{cm['期末净值']:,.0f}</td><td class="num">¥{pm['hold3']['期末净值']:,.0f} / ¥{pm['hold5']['期末净值']:,.0f}</td></tr>
        <tr><td>总收益%</td><td class="num pos">+{cm['总收益%']}%</td><td class="num">{pm['hold3']['总收益%']:+}% / {pm['hold5']['总收益%']:+}%</td></tr>
        <tr><td>年化%</td><td class="num pos">+{cm['年化%']}%</td><td class="num">{pm['hold3']['年化%']:+}% / {pm['hold5']['年化%']:+}%</td></tr>
        <tr><td>夏普比率</td><td class="num pos">{cm['夏普']}</td><td class="num">{pm['hold3']['夏普']} / {pm['hold5']['夏普']}</td></tr>
        <tr><td>最大回撤%</td><td class="num">{cm['最大回撤%']}%</td><td class="num">{pm['hold3']['最大回撤%']}% / {pm['hold5']['最大回撤%']}%</td></tr>
        <tr><td>胜率%</td><td class="num pos">{cm['胜率%']}%</td><td class="num">{pm['hold3']['胜率%']}% / {pm['hold5']['胜率%']}%</td></tr>
        <tr><td>交易笔数</td><td class="num">{cs['n']}</td><td class="num">{ps3['n']} / {ps5['n']}</td></tr>
        <tr><td>平均盈利%(盈利笔)</td><td class="num pos">+{cs['avg_win']}%</td><td class="num">+{pm['hold3']['平均盈利%']}% / +{pm['hold5']['平均盈利%']}%</td></tr>
        <tr><td>平均亏损%(亏损笔)</td><td class="num neg">{cs['avg_loss']}%</td><td class="num">{pm['hold3']['平均亏损%']}% / {pm['hold5']['平均亏损%']}%</td></tr>
        <tr><td>单笔最佳 / 最差</td><td class="num"><span class="pos">+{cs['best']}%</span> / <span class="neg">{cs['worst']}%</span></td><td class="num"><span class="pos">+{ps3['best']}%</span> / <span class="neg">{ps3['worst']}%</span></td></tr>
        <tr><td>平均持有天数</td><td class="num">~2.0</td><td class="num">{pm['hold3']['平均持有天数']} / {pm['hold5']['平均持有天数']}</td></tr>
      </tbody>
    </table>

    <h3>强 / 弱 / 震荡市分段稳健性(C，关闸口纯看内在)</h3>
    <table class="cmp">
      <thead><tr><th>市场环境</th><th>C 总收益% / 胜率% / 笔数</th><th>说明</th></tr></thead>
      <tbody>
        <tr><td>强市 bullish</td><td class="num pos">+{strong_weak['强市bullish']['总收益%']}% / {strong_weak['强市bullish']['胜率%']}% / {strong_weak['强市bullish']['交易笔数']}</td><td>主升段，机会最多、贡献最大</td></tr>
        <tr><td>震荡 normal</td><td class="num pos">+{strong_weak['震荡normal']['总收益%']}% / {strong_weak['震荡normal']['胜率%']}% / {strong_weak['震荡normal']['交易笔数']}</td><td>机会减少但仍正收益</td></tr>
        <tr><td>弱市 bearish</td><td class="num pos">+{strong_weak['弱市bearish']['总收益%']}% / {strong_weak['弱市bearish']['胜率%']}% / {strong_weak['弱市bearish']['交易笔数']}</td><td>胜率最高(84.6%)，隔天高开止盈+MA60空仓有效护盘</td></tr>
      </tbody>
    </table>
    <div class="note"><b>解读：</b>C 在三种市况下均为正收益，弱市胜率反而最高——说明"MA60 空仓 + 隔天高开落袋"的组合对下行有天然防御。而生产动量在同期整体接近盈亏平衡甚至亏损，说明多因子+自适应退出在当前行情下未能跑赢简单的"龙头偷袭板+快落袋"。</div>
  </div>

  <!-- ========== 4. 逐笔交易差异 ========== -->
  <div class="card">
    <h2><span class="dot"></span>3. 逐笔交易差异</h2>

    <h3>3.1 收益分布(每笔实现收益落点)</h3>
    <div class="grid2">
      <div>
        <div style="font-weight:700;margin-bottom:8px;color:#c0392b">C · 29 笔</div>
        {dist_bar(cbk, cs['n'], '#e06055')}
      </div>
      <div>
        <div style="font-weight:700;margin-bottom:8px;color:#2c5aa0">生产动量(持3) · 121 笔</div>
        {dist_bar(pbk, ps3['n'], '#5a86d0')}
      </div>
    </div>
    <div class="note">
      <b>最关键的一处差异：</b>C 有 <b>4 笔 ≥+10%</b>(最高 +19.09%)的"肥尾大赢家"，而生产动量<b>一笔都没有</b>——其单笔最佳仅 +{ps3['best']}%。原因是生产动量的自适应止盈把上涨过早锁死(浮盈2%~8%即走)，而 C 让龙头持满2天充分释放行情。代价是 C 的亏损笔也更极端(最差 -10.37%)，但因为笔数极少(仅7笔亏损)且胜率高，整体大幅占优。生产动量则陷入"<b>{pbk['-5~0%']} 笔小亏</b>"的钝刀割肉困境。
    </div>

    <h3>3.2 退出原因分布</h3>
    <div class="grid2">
      <div>
        <div style="font-weight:700;margin-bottom:8px;color:#c0392b">C 退出原因</div>
        <table><tbody>
        {''.join(f"<tr><td>{k}</td><td class='num'>{v} 笔</td></tr>" for k,v in c_reason.most_common())}
        </tbody></table>
      </div>
      <div>
        <div style="font-weight:700;margin-bottom:8px;color:#2c5aa0">生产动量退出原因(持3)</div>
        <table><tbody>
        {''.join(f"<tr><td>{k}</td><td class='num'>{v} 笔</td></tr>" for k,v in p_reason.most_common())}
        </tbody></table>
      </div>
    </div>
    <div class="note"><b>解读：</b>C 的退出高度集中于"<b>到期(持满2天收盘卖)</b>"，只有 3 笔提前隔天高开止盈——即绝大多数时候<b>让子弹飞满2天</b>。生产动量退出高度碎片化：止损({p_reason['止损(Adaptive_SL)']}笔)与各类止盈/乖离/破线交织，反映其"频繁干预、快进快出"的风格，交易摩擦(滑点)成本因此被放大。</div>

    <h3>3.3 选股与行业集中度</h3>
    <ul>
      <li>C 只成交 <b>{len(cs['codes'])} 只个股</b>；生产动量成交 <b>{len(ps3['codes'])} 只</b>——C 选股更"挑"，命中率更高。</li>
      <li>两策略仅 <b>{len(overlap)} 只重叠</b>：{'、'.join(overlap)}——说明两套逻辑选出的票几乎是两批人，C 专攻热门资金流入行业龙头，生产动量偏 Alpha 因子高分票(含大量"其它"行业)。</li>
      <li>C 行业集中在题材热点：{'、'.join(f"{s}×{n}" for s,n in c_sec[:6])}——集中在液冷、盐湖提锂、小金属、PCB、减速器等强题材。</li>
      <li>生产动量有 <b>23 个调仓日无合格标的(候选0)</b>被迫空仓，选股约束(sharpe&gt;1+套牢盘≤0.1+套牢过滤)在弱势期常常筛不出票。</li>
    </ul>
  </div>

  <!-- ========== 5. 逻辑核对要点 ========== -->
  <div class="card">
    <h2><span class="dot"></span>4. 逻辑核对要点(请你检查)</h2>
    <ul>
      <li><b>可比性：</b>两策略均用同一 DB、同一测试窗口、10万本金、0.008 双边滑点、事件驱动满仓模型。生产动量额外并列持3/持5两版，C 为其最优单版。</li>
      <li><b>C 的收益来源：</b>= 高胜率(75.9%) × 少数肥尾大赢家(4笔≥+10%) − 极少的极端亏损(7笔)，本质是"<b>低频、精选、让利润奔跑</b>"。若未来热门题材熄火，机会数会锐减(强市20笔 vs 弱市13笔)。</li>
      <li><b>C 的脆弱点：</b>①单笔最差 -10.37%(整仓无硬止损，仅靠隔天高开与到期)；②依赖题材行情，机会数不稳定；③14:45 尾盘偷袭板的真实成交/冲击成本可能高于回测滑点假设。</li>
      <li><b>生产动量为何落后：</b>当前 DB 下自适应退出"<b>止盈太早+止损太频</b>"，导致 62 笔小亏、0 笔大赢，胜率不足五成，被摩擦成本蚕食。</li>
      <li><b>不是"C 永远赢"：</b>results.json 旧快照里生产动量曾 +33%，说明其表现<b>强依赖数据窗口</b>；C 的优势在当前窗口显著，但同样需要题材活跃度支撑。建议把 C 作为"<b>题材活跃期的进攻卫星仓</b>"，而非全天候替代生产核心策略。</li>
    </ul>
  </div>

  <!-- ========== 6. 逐笔明细 ========== -->
  <div class="card">
    <h2><span class="dot"></span>附：C 全部 29 笔逐笔明细</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>#</th><th>卖出日</th><th>代码</th><th>行业</th><th>收益%</th><th>盈亏(元)</th><th>退出原因</th></tr></thead>
        <tbody>{c_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <h2><span class="dot"></span>附：生产动量(持3) 全部 {ps3['n']} 笔逐笔明细</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>#</th><th>调仓日</th><th>代码</th><th>名称行业</th><th>收益%</th><th>盈亏(元)</th><th>持有</th><th>退出原因</th></tr></thead>
        <tbody>{p_rows}</tbody>
      </table>
    </div>
  </div>

  <div class="foot">数据源：qlib_pro_v16.db · harness 离线回测框架 · 未修改任何生产文件 · 生成于 opt_study/</div>
</div>
</body>
</html>"""

out = os.path.join(HERE, 'compare_prod_vs_C_report.html')
with open(out, 'w', encoding='utf-8') as f:
    f.write(html)
print('OK ->', out)
print('C trades', cs['n'], 'winrate', cs['wr'], '| Prod3 trades', ps3['n'], 'winrate', ps3['wr'])
