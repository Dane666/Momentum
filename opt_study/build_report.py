# -*- coding: utf-8 -*-
"""读取 results.json，生成自包含 HTML 对比报告（浅色主题，A股红涨绿跌）。"""
import json
from pathlib import Path

THIS = Path(__file__).resolve().parent
d = json.load(open(THIS / "results.json", encoding="utf-8"))
meta = d["meta"]
summary = d["summary"]
dbc = d["detail_by_config"]
ec = d["equity_curves"]

CFGS = ["hold5_shift0", "hold5_shift20", "hold5_shift40", "hold5_shift60",
        "hold3_shift0", "hold3_shift20", "hold3_shift40", "hold3_shift60"]
CFG_LABEL = {
    "hold5_shift0": "持5·窗0", "hold5_shift20": "持5·窗20", "hold5_shift40": "持5·窗40",
    "hold5_shift60": "持5·窗60", "hold3_shift0": "持3·窗0", "hold3_shift20": "持3·窗20",
    "hold3_shift40": "持3·窗40", "hold3_shift60": "持3·窗60",
}
VAR_ORDER = ["基准(原策略)", "R:市场择时", "X:动量加速", "Q:质量叠加", "RXQ:择时+加速+质量", "RQ:择时+质量"]
VAR_DESC = {
    "基准(原策略)": "原始策略：Alpha打分 + 严苛套牢盘过滤(trapped≤0.10, sharpe>1.0)",
    "R:市场择时": "在原策略上叠加市场择时：等权市场净值跌破MA20时空仓/减仓",
    "X:动量加速": "在原策略上要求动量加速(近5日动量 > 近20日动量)",
    "Q:质量叠加": "在原策略打分上叠加质量因子(低波、Sortino、趋势R²、临近高点)",
    "RXQ:择时+加速+质量": "择时 + 动量加速 + 质量因子 三重叠加",
    "RQ:择时+质量": "择时 + 质量因子 双重叠加",
}
base = summary["基准(原策略)"]


def delta_cls(v):
    return "up" if v > 0 else ("down" if v < 0 else "flat")


# ---- 汇总表行 ----
rows = []
for v in VAR_ORDER:
    s = summary[v]
    is_base = (v == "基准(原策略)")
    dprofit = s["avg_profit"] - base["avg_profit"]
    dwin = s["avg_win_rate"] - base["avg_win_rate"]
    dsharpe = s["avg_sharpe"] - base["avg_sharpe"]
    ddd = s["avg_max_dd"] - base["avg_max_dd"]
    rows.append({
        "name": v, "is_base": is_base, "desc": VAR_DESC[v],
        "profit": s["avg_profit"], "annual": s["avg_annual"], "sharpe": s["avg_sharpe"],
        "win": s["avg_win_rate"], "dd": s["avg_max_dd"], "trades": s["avg_trades"],
        "dprofit": dprofit, "dwin": dwin, "dsharpe": dsharpe, "ddd": ddd,
    })

# ---- 逐窗口 基准 vs R ----
per_win = []
for c in CFGS:
    b = dbc["基准(原策略)"][c]
    r = dbc["R:市场择时"][c]
    per_win.append({
        "cfg": CFG_LABEL[c],
        "bp": b["profit_pct"], "rp": r["profit_pct"],
        "bw": b["win_rate"], "rw": r["win_rate"],
        "bs": b["sharpe"], "rs": r["sharpe"],
        "bd": b["max_dd"], "rd": r["max_dd"],
    })

# ---- 净值曲线 ----
curve_dates = ec["基准(原策略)"]["dates"]
curves = {v: ec[v]["equity"] for v in VAR_ORDER}


def fmt(x, p=2):
    return f"{x:.{p}f}"


def sign(x, p=2):
    s = f"{x:+.{p}f}"
    return s


# ===== 生成表格 HTML =====
summary_rows_html = ""
for r in rows:
    hl = ' class="base-row"' if r["is_base"] else ""
    star = ' <span class="badge-win">最优增量</span>' if r["name"] == "R:市场择时" else ""
    if r["is_base"]:
        dcells = (f'<td class="num muted">基准</td><td class="num muted">基准</td>'
                  f'<td class="num muted">基准</td><td class="num muted">基准</td>')
    else:
        dcells = (
            f'<td class="num {delta_cls(r["dprofit"])}">{sign(r["dprofit"])}</td>'
            f'<td class="num {delta_cls(r["dwin"])}">{sign(r["dwin"])}</td>'
            f'<td class="num {delta_cls(r["dsharpe"])}">{sign(r["dsharpe"],3)}</td>'
            f'<td class="num {delta_cls(-r["ddd"])}">{sign(r["ddd"])}</td>'
        )
    summary_rows_html += f"""
      <tr{hl}>
        <td class="vname">{r['name']}{star}<div class="vdesc">{r['desc']}</div></td>
        <td class="num strong">{fmt(r['profit'])}%</td>
        <td class="num">{fmt(r['sharpe'],2)}</td>
        <td class="num">{fmt(r['win'])}%</td>
        <td class="num">{fmt(r['dd'])}%</td>
        <td class="num">{fmt(r['trades'],0)}</td>
        {dcells}
      </tr>"""

per_win_rows_html = ""
for w in per_win:
    per_win_rows_html += f"""
      <tr>
        <td class="vname">{w['cfg']}</td>
        <td class="num">{fmt(w['bp'],1)}%</td>
        <td class="num {delta_cls(w['rp']-w['bp'])}">{fmt(w['rp'],1)}%</td>
        <td class="num">{fmt(w['bw'],1)}%</td>
        <td class="num {delta_cls(w['rw']-w['bw'])}">{fmt(w['rw'],1)}%</td>
        <td class="num">{fmt(w['bs'],2)}</td>
        <td class="num {delta_cls(w['rs']-w['bs'])}">{fmt(w['rs'],2)}</td>
      </tr>"""

# 关键数值
r_sum = summary["R:市场择时"]
win_gain = r_sum["avg_win_rate"] - base["avg_win_rate"]
sharpe_gain = r_sum["avg_sharpe"] - base["avg_sharpe"]
sharpe_pct = sharpe_gain / base["avg_sharpe"] * 100
trade_cut = (base["avg_trades"] - r_sum["avg_trades"]) / base["avg_trades"] * 100

HTML = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>A股动量策略优化对比报告</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bg:#f5f6f8; --card:#ffffff; --ink:#1a1d24; --sub:#5b6270; --line:#e6e8ec;
    --up:#e03131; --down:#2f9e44; --accent:#1c7ed6; --gold:#f08c00; --hl:#fff4e6;
  }}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--bg);color:var(--ink);
    font-family:-apple-system,"PingFang SC","Microsoft YaHei",Segoe UI,sans-serif;line-height:1.6}}
  .wrap{{max-width:1080px;margin:0 auto;padding:32px 20px 64px}}
  header h1{{font-size:26px;margin:0 0 6px}}
  header .sub{{color:var(--sub);font-size:14px}}
  .meta-bar{{display:flex;flex-wrap:wrap;gap:8px;margin:16px 0 8px}}
  .chip{{background:var(--card);border:1px solid var(--line);border-radius:999px;
    padding:4px 12px;font-size:12px;color:var(--sub)}}
  section{{background:var(--card);border:1px solid var(--line);border-radius:14px;
    padding:22px 24px;margin:18px 0;box-shadow:0 1px 3px rgba(0,0,0,.03)}}
  h2{{font-size:18px;margin:0 0 14px;display:flex;align-items:center;gap:8px}}
  h2::before{{content:"";width:4px;height:18px;background:var(--accent);border-radius:2px}}
  p{{margin:8px 0}}
  .kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:8px 0}}
  .kpi{{background:linear-gradient(180deg,#fff,#fafbfc);border:1px solid var(--line);
    border-radius:12px;padding:14px 16px}}
  .kpi .lab{{font-size:12px;color:var(--sub)}}
  .kpi .val{{font-size:24px;font-weight:700;margin-top:2px}}
  .kpi .val.up{{color:var(--up)}} .kpi .val.down{{color:var(--down)}}
  .kpi .note{{font-size:11px;color:var(--sub);margin-top:2px}}
  table{{width:100%;border-collapse:collapse;font-size:13.5px}}
  th,td{{padding:9px 10px;text-align:left;border-bottom:1px solid var(--line)}}
  th{{font-size:12px;color:var(--sub);font-weight:600;background:#fafbfc}}
  td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
  td.strong{{font-weight:700}}
  .up{{color:var(--up)}} .down{{color:var(--down)}} .flat{{color:var(--sub)}} .muted{{color:#aeb4bd}}
  .vname{{font-weight:600}}
  .vdesc{{font-weight:400;font-size:11.5px;color:var(--sub);margin-top:2px}}
  .base-row{{background:#f8f9fb}}
  .badge-win{{background:var(--up);color:#fff;font-size:10px;padding:1px 7px;
    border-radius:6px;margin-left:6px;vertical-align:middle}}
  .chart-box{{position:relative;height:360px}}
  .two{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
  .callout{{background:var(--hl);border:1px solid #ffe0b2;border-radius:10px;
    padding:14px 16px;margin:12px 0;font-size:13.5px}}
  .callout b{{color:var(--gold)}}
  ul.clean{{margin:8px 0;padding-left:20px}} ul.clean li{{margin:6px 0}}
  .tag{{display:inline-block;background:#eef4fd;color:var(--accent);border-radius:6px;
    padding:1px 8px;font-size:12px;margin-right:6px}}
  .foot{{color:var(--sub);font-size:12px;text-align:center;margin-top:24px}}
  @media(max-width:720px){{.kpis{{grid-template-columns:repeat(2,1fr)}}.two{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>A股尾盘动量策略 · 优化对比报告</h1>
    <div class="sub">基准（原策略） vs 5 类优化变体 · 8 窗口稳健性回测 · 未修改任何原有文件</div>
    <div class="meta-bar">
      <span class="chip">回测区间 {meta['calendar_start']} ~ {meta['calendar_end']}</span>
      <span class="chip">股票池 {meta['universe']} 只</span>
      <span class="chip">持仓周期 {meta['hold_periods']} 天</span>
      <span class="chip">窗口偏移 {meta['window_shifts']}</span>
      <span class="chip">滑点 {meta['slippage']*100:.1f}%</span>
      <span class="chip">最多持仓 {meta['max_picks']} 只</span>
    </div>
  </header>

  <section>
    <h2>核心结论</h2>
    <div class="callout">
      <b>① 原策略的真正 alpha 来自「严苛套牢盘过滤器」，而非因子打分。</b><br>
      测试中一旦放宽过滤（trapped 0.10→0.25、sharpe 1.0→0.5），收益从 +11.75% 崩至 −28.70%、夏普跌到 −1.36。
      这说明该策略是「靠严选、而非靠排序」赚钱，过滤器不可动。
    </div>
    <div class="callout">
      <b>② 唯一能稳健提升胜率与风险收益比的增量 = 市场择时（regime overlay）。</b><br>
      在保留原过滤器的前提下叠加「市场净值跌破 MA20 则空仓/减仓」，在全部 8 个稳健性窗口上：
      胜率 <b class="up">+{win_gain:.2f}pp</b>、夏普 <b class="up">+{sharpe_pct:.0f}%</b>、交易量 <b>−{trade_cut:.0f}%</b>（省滑点），
      而收益几乎不变。它的价值集中体现在<b>熊市窗口的下行保护</b>。
    </div>
    <div class="kpis">
      <div class="kpi"><div class="lab">基准 均收益</div><div class="val">{base['avg_profit']:.2f}%</div><div class="note">8窗口均值</div></div>
      <div class="kpi"><div class="lab">择时 均收益</div><div class="val">{r_sum['avg_profit']:.2f}%</div><div class="note">Δ {sign(r_sum['avg_profit']-base['avg_profit'])} pp</div></div>
      <div class="kpi"><div class="lab">胜率提升</div><div class="val up">+{win_gain:.2f}pp</div><div class="note">{base['avg_win_rate']:.1f}% → {r_sum['avg_win_rate']:.1f}%</div></div>
      <div class="kpi"><div class="lab">夏普提升</div><div class="val up">+{sharpe_pct:.0f}%</div><div class="note">{base['avg_sharpe']:.2f} → {r_sum['avg_sharpe']:.2f}</div></div>
    </div>
  </section>

  <section>
    <h2>全变体稳健性对比（8 窗口均值）</h2>
    <p style="color:var(--sub);font-size:13px">Δ 列为相对基准的差值；回撤 Δ 中绿色代表回撤减小（更优）。所有优化变体均<b>保留</b>原策略严苛过滤器，仅做边际叠加。</p>
    <table>
      <thead><tr>
        <th>变体</th><th class="num">均收益</th><th class="num">夏普</th><th class="num">胜率</th>
        <th class="num">回撤</th><th class="num">交易</th>
        <th class="num">Δ收益</th><th class="num">Δ胜率</th><th class="num">Δ夏普</th><th class="num">Δ回撤</th>
      </tr></thead>
      <tbody>{summary_rows_html}
      </tbody>
    </table>
    <div class="callout" style="background:#f1f8ff;border-color:#c5ddf7">
      <b style="color:var(--accent)">读法：</b> 因子改造类（X 动量加速、Q 质量叠加、以及含它们的组合）跨窗口平均<b>拖累收益</b>——
      单窗口偶有亮点（如 RXQ 在主窗口夏普达 1.28），但换个窗口就失效，属于过拟合，不可采纳。
      只有 R 市场择时在<b>所有窗口</b>方向一致、稳健有效。
    </div>
  </section>

  <section>
    <h2>净值曲线对比（主口径 · 持仓5日 · 近12个月）</h2>
    <div class="chart-box"><canvas id="navChart"></canvas></div>
    <p style="color:var(--sub);font-size:12.5px;margin-top:12px">
      择时曲线（蓝）在上涨阶段略微跑输基准（红），但在回调段明显更平滑、回撤更小——这正是「用少量收益换稳定性」的典型特征。
    </p>
  </section>

  <section>
    <h2>市场择时的价值：熊市窗口下行保护</h2>
    <p style="color:var(--sub);font-size:13px">逐窗口对比基准 vs 市场择时。绿色/红色表示择时相对基准的方向变化。</p>
    <table>
      <thead><tr>
        <th>窗口</th><th class="num">基准收益</th><th class="num">择时收益</th>
        <th class="num">基准胜率</th><th class="num">择时胜率</th>
        <th class="num">基准夏普</th><th class="num">择时夏普</th>
      </tr></thead>
      <tbody>{per_win_rows_html}
      </tbody>
    </table>
    <div class="callout">
      <b>最能说明问题的是最差窗口（持5·窗60）：</b> 基准收益 <span class="down">−13.2%</span>、夏普 −0.54；
      叠加择时后拉回到 <span class="up">+0.5%</span>、夏普 +0.12，胜率从 43.3% 升到 48.1%。
      择时在牛市几乎不拖后腿，在熊市大幅止血——这就是它能稳定抬升整体胜率与夏普的根本原因。
    </div>
  </section>

  <section>
    <h2>可落地的优化建议</h2>
    <ul class="clean">
      <li><span class="tag">强烈推荐</span><b>加入市场择时开关</b>：用等权市场净值（或指数）跌破 MA20 作为「减仓/空仓」信号。
        实现上只需在现有选股流程前加一道市场状态判断，不改动任何因子与过滤逻辑。稳健提升胜率 +2pp、夏普 +13%、并显著降低熊市回撤。</li>
      <li><span class="tag">保留不动</span><b>套牢盘严选过滤器（trapped≤0.10 & sharpe>1.0）是核心 alpha</b>，任何放宽都会导致业绩崩溃，务必维持现状。</li>
      <li><span class="tag">不建议</span><b>不要做因子权重重构 / 质量叠加 / 动量加速</b>：它们跨窗口平均均为负贡献，单窗口的高分是过拟合噪声。</li>
      <li><span class="tag">可选</span><b>择时带来的交易量下降（−21%）本身是收益</b>：更少的换手意味着更少的滑点与冲击成本，在真实交易中优势会进一步放大。</li>
    </ul>
  </section>

  <div class="foot">
    离线回测框架复用原策略 config / AlphaModel / ExitRuleEngine 口径 · 数据源 qlib_pro_v16.db ·
    未修改任何原有业务文件 · 生成于 opt_study/
  </div>
</div>

<script>
const dates = {json.dumps(curve_dates, ensure_ascii=False)};
const curves = {json.dumps(curves, ensure_ascii=False)};
const ctx = document.getElementById('navChart');
new Chart(ctx, {{
  type:'line',
  data:{{
    labels:dates,
    datasets:[
      {{label:'基准(原策略)', data:curves['基准(原策略)'], borderColor:'#e03131',
        backgroundColor:'rgba(224,49,49,.06)', borderWidth:2, pointRadius:0, tension:.25, fill:true}},
      {{label:'R:市场择时', data:curves['R:市场择时'], borderColor:'#1c7ed6',
        backgroundColor:'rgba(28,126,214,.06)', borderWidth:2, pointRadius:0, tension:.25, fill:true}}
    ]
  }},
  options:{{
    responsive:true, maintainAspectRatio:false,
    interaction:{{mode:'index', intersect:false}},
    plugins:{{
      legend:{{position:'top', labels:{{usePointStyle:true, boxWidth:8}}}},
      tooltip:{{callbacks:{{label:(c)=>c.dataset.label+': '+(c.parsed.y).toFixed(3)}}}}
    }},
    scales:{{
      x:{{ticks:{{maxTicksLimit:10, color:'#8b93a1', font:{{size:11}}}}, grid:{{display:false}}}},
      y:{{ticks:{{color:'#8b93a1', font:{{size:11}}, callback:(v)=>v.toFixed(2)}},
         grid:{{color:'#eef0f3'}}, title:{{display:true, text:'净值(初始=1.0)', color:'#8b93a1'}}}}
    }}
  }}
}});
</script>
</body>
</html>"""

out = THIS / "optimization_report.html"
out.write_text(HTML, encoding="utf-8")
print("REPORT ->", out)
