# -*- coding: utf-8 -*-
"""
超跌绩优反弹 · 前向验证(Forward / OOS) 可重复脚本
=================================================
把"最优组合样本外验证"固化为可重复、可调度(月度)的脚本.

子命令:
  validate  [--mode auto|forward|holdout]   前向/样本外验证
  watchlist [--lookback N]                   生成"当前候选"观察清单(接实盘小仓观察)

设计要点:
  - 路径相对仓库根解析(覆盖 harness_oversold_quality 的硬编码绝对路径), 脱离本机.
  - 加载全部可用 K 线(把 WINDOW_END 顶到 2099), 以便前向验证能用 IN_SAMPLE_END 之后的真实数据.
  - build_signal_index 仅依赖信号口径(mode/dd/gap/rsi/...), 与 theme_cap/stop 无关,
    故全数据集只构建一次, 所有 (theme_cap, stop) 组合复用同一 inv, 仅 simulate 不同.
  - forward 模式: 用 IN_SAMPLE_END 之后的真实数据, 固定套用已发布组合(参数来自样本内),
    零前视泄漏 —— 这才是真正的"前瞻"验证.
  - 每次成功的 forward 运行都会追加到 oos_tracker.json, 逐月累积样本(扩充历史样本).

已发布组合(样本内选出, 固定套用): V2(深度超跌+绩优+热门) + 单题材上限1 + 止损-15% + 无大盘择时.
"""
import os, sys, json, csv, argparse, datetime as _dt
from collections import defaultdict

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import harness_oversold_quality as H

# ---- 脱离本机绝对路径: 覆盖底层 harness 的常量 ----
DB_PATH = os.environ.get("QLIB_DB") or os.path.join(REPO_ROOT, "qlib_pro_v16.db")
H.DB = DB_PATH
H.ROOT = REPO_ROOT
# 加载全部可用数据(前向验证需要 IN_SAMPLE_END 之后的数据)
H.WINDOW_START = "2024-07-01"
H.WINDOW_END = "2099-12-31"

# ---- 核心常量 ----
IN_SAMPLE_END = "2026-07-15"   # 样本内选参终点(已发布组合基于此之前的数据选出)
PUB_THEME_CAP = 1
PUB_STOP = -0.15
HOLD = H.HOLD_DEF
ENTRY = H.ENTRY_MODE
CD = H.SAME_CODE_COOLDOWN
STOP_GRID = [-0.03, -0.05, -0.07, -0.10, -0.12, -0.15, -0.20]

# auto 模式门槛: 样本外窗口需至少这么多交易日才做真·前瞻, 否则退化为 holdout,
# 避免 CI 预热后只多几天新数据就跑出 n=0 的误导性 forward 结果.
MIN_OOS_TRADING_DAYS = 20

# universe 门禁: 覆盖标的数低于此阈值说明预热不足/库损坏, 前向验证结果为子集估计,
# 必须 FAIL(非零退出) 而非假绿. 全市场非ST A股约 5000 只.
MIN_UNIVERSE = 1500

TRAIN_START, TRAIN_END = "2024-07-01", "2025-09-30"
TEST_START, TEST_END = "2025-10-01", "2026-07-15"

OUT_DIR = os.path.join(REPO_ROOT, "opt_study")
TRACKER = os.path.join(OUT_DIR, "oos_tracker.json")


# ----------------------------------------------------------------------------
def slice_cal(cal, a, b):
    return [t for t in cal if a <= str(t)[:10] <= b]


def get_v2_cfg():
    for vname, cfg in H.make_variants():
        if vname == "V2 深度超跌+绩优+热门":
            return dict(cfg)
    raise RuntimeError("V2 变体未找到")


def pub_cfg():
    c = get_v2_cfg()
    c["theme_cap"] = PUB_THEME_CAP
    return c


def simulate_on(ctx, cal_slice, inv, hot_at, fmap, theme_cap, stop):
    cfg = dict(get_v2_cfg())
    if theme_cap:
        cfg["theme_cap"] = theme_cap
    trades, eq = H.simulate(ctx, cal_slice, inv, hot_at, fmap, HOLD, ENTRY, stop, cfg, CD, None)
    m = H.metrics(trades, eq)
    m["theme_cap"] = theme_cap or 0
    m["stop"] = stop
    return trades, eq, m


def _build_cap_dist(mmap):
    """返回 (month_q, month_arr): 各月末快照流通市值分位阈值与原始数组(point-in-time)."""
    from collections import defaultdict
    month_circ = defaultdict(list)
    for code, recs in mmap.items():
        for rec in recs:
            if rec["circ_mv"]:
                month_circ[rec["trade_date"]].append(rec["circ_mv"])
    month_q, month_arr = {}, {}
    for d, lst in month_circ.items():
        a = np.array(lst, float)
        month_arr[d] = a
        month_q[d] = {p: float(np.percentile(a, p * 100)) for p in (0.2, 0.3, 0.5, 0.7, 0.8)}
    return month_q, month_arr


def _tilt_inv_small(inv, mmap, month_q, p_small=0.5):
    """将候选过滤为流通市值 <= 该月末 p_small 分位的"小盘"(point-in-time)."""
    from collections import defaultdict
    out = defaultdict(list)
    for ts, codes in inv.items():
        for code in codes:
            rec = H.market_stats_at(mmap, code, ts)
            if not rec or not rec["circ_mv"] or rec["circ_mv"] <= 0:
                continue
            q = month_q.get(rec["trade_date"], {}).get(p_small)
            if q is None or rec["circ_mv"] > q:
                continue
            out[ts].append(code)
    return out


def _tag_cap(trades, mmap, month_arr):
    """对成交逐笔标注流通市值分位(小盘/大盘), 返回 [{code,ret,bucket,rank}]."""
    rows = []
    for t in trades:
        ts = str(t["buy_t"])[:10]
        rec = H.market_stats_at(mmap, t["code"], ts)
        cm = rec["circ_mv"] if rec else None
        snap = rec["trade_date"] if rec else None
        arr = month_arr.get(snap) if snap else None
        if cm and arr is not None and len(arr):
            rank = float((arr < cm).mean() * 100)
            bucket = "小盘" if rank <= 50 else "大盘"
        else:
            rank = float("nan"); bucket = "无数据"
        rows.append(dict(code=t["code"], ret=t["ret"], bucket=bucket, rank=rank))
    return rows


def load_all():
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    cal = sorted({t for g in ctx.values() for t in g.index})
    print(f"  标的数={len(ctx)} 全窗口交易日={len(cal)}", flush=True)
    print("预计算指标...", flush=True)
    ctx = H.build_ctx(ctx)
    print("加载基本面...", flush=True)
    fmap = H.load_fundamentals()
    print(f"  基本面覆盖 {len(fmap)} 只", flush=True)
    return ctx, cal, fmap


# ----------------------------------------------------------------------------
#  equity SVG(动态 x 轴)
def build_equity_svg(series_list, div_date, title, data_min, data_max):
    """series_list: list of (list[(date_str,value)], color). 公共 x 轴 [data_min,data_max]."""
    if not series_list:
        return "<p>无权益数据</p>"
    W, Hh = 920, 240
    pad = 42
    d0 = _dt.date.fromisoformat(data_min)
    d1 = _dt.date.fromisoformat(data_max)
    span = (d1 - d0).days or 1

    def xmap(ds):
        y, m, dd = map(int, ds.split("-"))
        return pad + (W - 2 * pad) * (_dt.date(y, m, dd) - d0).days / span

    allv = [v for s, _ in series_list for _, v in s]
    ymin = min(allv + [100]); ymax = max(allv + [100])
    rng = (ymax - ymin) or 1

    def ymap(v):
        return Hh - pad - (Hh - 2 * pad) * (v - ymin) / rng

    grid = ""
    for gv in [ymin, 100.0, ymax]:
        yy = ymap(gv)
        grid += f"<line x1='{pad}' y1='{yy:.1f}' x2='{W-pad}' y2='{yy:.1f}' stroke='#eee'/>"
        grid += f"<text x='{pad-6}' y='{yy+3:.1f}' font-size='9' fill='#888' text-anchor='end'>{gv:.0f}</text>"
    if div_date and data_min <= div_date <= data_max:
        dx = xmap(div_date)
        grid += f"<line x1='{dx:.1f}' y1='{pad}' x2='{dx:.1f}' y2='{Hh-pad}' stroke='#bbb' stroke-dasharray='4 3'/>"
        grid += f"<text x='{dx+4:.1f}' y='{pad+10:.1f}' font-size='9' fill='#666'>样本内/外分界</text>"
    polys = ""
    for s, color in series_list:
        pts = " ".join(f"{xmap(x):.1f},{ymap(y):.1f}" for x, y in s)
        polys += f"<polyline points='{pts}' fill='none' stroke='{color}' stroke-width='2'/>"
    return (f"<svg viewBox='0 0 {W} {Hh}' width='100%' style='max-width:920px'>"
            f"{grid}{polys}<text x='{pad}' y='14' font-size='11' fill='#333'>{title}</text></svg>")


def build_html_validate(R, out_path):
    mode = R["mode"]
    ise = R.get("in_sample_end", IN_SAMPLE_END)
    fwd = R.get("forward")          # dict or None
    ho = R.get("holdout")           # dict or None
    ins = R["in_sample_baseline"]
    tracker = R.get("tracker", [])

    def kpi(m):
        return ("".join(
            f"<div class='card'><b>{m[k]}</b>{lbl}</div>"
            for k, lbl in [("total_ret", "总收益%"), ("winrate", "胜率%"), ("n", "笔数"),
                          ("avg_ret", "平均每笔%"), ("sharpe", "夏普"), ("maxdd", "回撤%")]))

    def row(m, hl=False):
        bcls = "pos" if m["total_ret"] > 0 else "neg"
        h = " style='background:#eaf6ff'" if hl else ""
        return (f"<tr{h}><td>{m.get('label','')}</td><td>{m.get('theme_cap',0)}</td>"
                f"<td>{int(m.get('stop',-0.15)*100)}%</td><td>{m['n']}</td>"
                f"<td>{m['winrate']}%</td><td>{m['avg_ret']}%</td>"
                f"<td class='num {bcls}'>{m['total_ret']}%</td>"
                f"<td>{m['sharpe']}</td><td>{m['maxdd']}%</td></tr>")

    # 核心结果 = forward(真OOS) 或 holdout 的 TEST
    key = fwd if fwd else (ho["published_on_test"] if ho else ins)
    key_trades = R.get("forward_trades") or (ho.get("test_trades") if ho else [])

    tr_rows = ""
    for i, t in enumerate(key_trades, 1):
        cls = "pos" if t["ret"] > 0 else "neg"
        tr_rows += (f"<tr><td>{i}</td><td>{t['code']}</td><td>{str(t['buy_t'])[:10]}</td>"
                    f"<td>{t['buy_px']:.2f}</td><td>{str(t['sell_t'])[:10]}</td>"
                    f"<td>{t['sell_px']:.2f}</td><td class='num {cls}'>{t['ret']*100:.2f}%</td>"
                    f"<td>{t['shares']}</td><td>{t['hold_days']}</td><td>{t['reason']}</td>"
                    f"<td>{t['roe']}</td><td>{t['np_yoy']}</td></tr>")

    # tracker 表
    tk_rows = ""
    for e in tracker:
        bcls = "pos" if e["total_ret"] > 0 else "neg"
        tk_rows += (f"<tr><td>{e['run_at']}</td><td>{e['window_start']}~{e['window_end']}</td>"
                    f"<td>{e['n']}</td><td class='num {bcls}'>{e['total_ret']}%</td>"
                    f"<td>{e['winrate']}%</td><td>{e['sharpe']}</td><td>{e['maxdd']}%</td></tr>")
    # 累积
    if tracker:
        tot_n = sum(e["n"] for e in tracker)
        avg_ret = round(np.mean([e["total_ret"] for e in tracker]), 2)
        agg = (f"累计 forward 运行 {len(tracker)} 次, 累积交易 {tot_n} 笔, "
               f"平均每次总收益 {avg_ret}% (逐月累积即扩充了样本外验证样本).")
    else:
        agg = "尚无 forward 运行记录(需数据库更新到 IN_SAMPLE_END 之后才有真实前瞻数据)."

    # verdict
    if mode == "forward":
        o = fwd
        if o["total_ret"] > 0:
            verdict = (f"✅ 真·前瞻验证(数据 {R['data_end']} 晚于样本内终点 {ise}): "
                       f"已发布组合在未见过的真实未来行情上总收益 +{o['total_ret']}%、胜率 {o['winrate']}%、"
                       f"夏普 {o['sharpe']} —— 组合具备真实泛化能力(零前视泄漏).")
            vcls = "verdict"
        else:
            verdict = (f"⛔ 真·前瞻验证转负({o['total_ret']}%), 组合在真实未来行情失效, "
                       f"样本内 +{ins['total_ret']}% 很可能过拟合.")
            vcls = "verdict warn"
        decay = (f"盈利衰减(对照): 样本内 +{ins['total_ret']}% → 前瞻 +{o['total_ret']}%; "
                 f"夏普 {ins['sharpe']} → {o['sharpe']}; 平均每笔 {ins['avg_ret']}% → {o['avg_ret']}%.")
    else:
        o = ho["published_on_test"]
        ratio = (o["total_ret"] / ins["total_ret"]) if ins["total_ret"] else 0
        if o["total_ret"] > 0 and ratio >= 0.6:
            verdict = (f"✅ 样本外(TEST)总收益为正且衰减可控(+{o['total_ret']}% vs 样本内+{ins['total_ret']}%, "
                       f"约 {ratio*100:.0f}%), 组合具备泛化能力.")
            vcls = "verdict"
        elif o["total_ret"] > 0:
            verdict = (f"⚠️ 样本外(TEST)仍为正(+{o['total_ret']}%)但衰减至样本内约 {ratio*100:.0f}%, "
                       f"样本内显著高估真实前向表现(典型过拟合/时段聚集).")
            vcls = "verdict warn"
        else:
            verdict = f"⛔ 样本外(TEST)转负({o['total_ret']}%), 组合在保留段失效."
            vcls = "verdict warn"
        decay = ("注: 当前为 holdout(时间切分)模式 —— 因数据库尚未更新到 2026H2, "
                 "无真实未来数据, 故用 TEST 段(2025-10~2026-07)作保留段. "
                 "选参用的全窗口含 TEST 段, 存在轻微前视泄漏(详见说明). "
                 "待数据库更新后, 本脚本将自动切换为 forward 真·前瞻模式.")

    # equity
    eq_ins = R.get("eq_ins"); eq_key = R.get("eq_key")
    series = []
    if eq_ins:
        base = eq_ins[0]
        series.append(([(str(R["in_sample_cal"][i])[:10], v/base*100) for i, v in enumerate(eq_ins)], "#2b6cb0"))
    if eq_key:
        base = eq_key[0]
        series.append(([(str(R["key_cal"][i])[:10], v/base*100) for i, v in enumerate(eq_key)], "#e08a1e"))
    eq_svg = build_equity_svg(series, ise, "权益曲线(归一化至100)", R["data_min"], R["data_max"])

    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:21px}} h2{{font-size:16px;margin-top:24px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}}
th{{background:#f4f6f8}} .num{{text-align:right}}
.pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7e34;font-weight:600}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:10px 14px;min-width:92px;display:inline-block;margin:4px}}
.kpi{{margin-top:8px}} .note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}
.verdict{{background:#eafaf0;border:1px solid #9fd9b0;padding:10px 14px;border-radius:8px;font-size:14px;font-weight:600;line-height:1.7}}
.verdict.warn{{background:#fff4e5;border:1px solid #f0c36d}}
.decay{{background:#f0f4ff;border:1px solid #c3d0ea;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.7;margin-top:8px}}
.mode{{display:inline-block;background:#eef;color:#245;padding:3px 10px;border-radius:5px;font-size:13px;font-weight:600}}
</style></head><body>
<h1>超跌绩优反弹 · 前向验证报告</h1>
<p>模式: <span class='mode'>{mode.upper()}</span> ｜ 已发布组合 = V2 + 单题材上限{PUB_THEME_CAP} + 止损{int(PUB_STOP*100)}% + 无择时 ｜
数据截至 <b>{R['data_end']}</b>, 样本内终点 <b>{ise}</b></p>
<p style='font-size:12px;color:#666'>模式判定: {R.get('auto_note','')} ｜ 样本外窗口交易日数: {R.get('forward_window_days',0)} ｜ 覆盖全市场 {R.get('coverage_universe','?')} 只</p>

<div class='{vcls}'>{verdict}</div>
<div class='decay'>{decay}</div>

<h2>核心结果</h2>
<div class='kpi'>{kpi(key)}</div>
<table><tr><th>配置 / 区间</th><th>主题上限</th><th>止损</th><th>笔数</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>
{row(ins)}<tr><td colspan='9' style='background:#fafafa;color:#555;font-size:12px'>↑ 样本内基线(已发布组合, 数据截至 {ise})</td></tr>
"""

    if mode == "forward" and fwd:
        html += row(fwd, hl=True) + ("<tr><td colspan='9' style='background:#eaf6ff;color:#245;font-size:12px'>"
                f"↑★ 真·前瞻(数据 {R['data_end']}, {ise} 之后, 零泄漏)</td></tr>")
    elif ho:
        html += (row(ho["published_on_test"], hl=True) +
                 "<tr><td colspan='9' style='background:#eaf6ff;color:#245;font-size:12px'>"
                 "↑★ 已发布组合 在 TEST(保留段) = 样本外参照</td></tr>")
    html += "</table>"

    if ho:
        html += (f"<div class='note' style='margin-top:10px'>{ho['train_note']}</div>"
                 "<h2>TRAIN 选参网格(独立重选, 成交≥5 取总收益最高)</h2>"
                 "<table><tr><th>配置</th><th>主题上限</th><th>止损</th><th>笔数</th><th>胜率</th>"
                 "<th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>")
        for m in sorted(ho["train_grid"], key=lambda x: (x["theme_cap"], x["stop"])):
            html += row(m)
        html += "</table>"

    html += (f"<h2>权益曲线(归一化至100)</h2><p>蓝线=样本内基线; "
             f"{'橙线=真·前瞻' if mode=='forward' else '橙线=样本外(TEST)'}."
             f" 虚线=样本内/外分界({ise}).</p>{eq_svg}")

    html += (f"<h2>{'真·前瞻' if mode=='forward' else '样本外(TEST)'}全量逐笔交易({key['n']}笔)</h2>"
             "<table><tr><th>#</th><th>代码</th><th>买入日</th><th>买价</th><th>卖出日</th><th>卖价</th>"
             "<th>收益</th><th>股数</th><th>持有日</th><th>退出</th><th>ROE%</th><th>净利同比%</th></tr>"
             f"{tr_rows}</table>")

    # 小盘倾斜 edge 复核段
    edge_html = ""
    edge = R.get("smallcap_edge")
    if edge:
        if edge.get("note"):
            edge_body = f"<div class='note'>{edge['note']}</div>"
        else:
            holds_txt = {True: "✅ 小盘优于大盘(方向一致)", False: "⛔ 小盘弱于大盘(方向相反)",
                         None: "— 样本不足无法判定"}[edge.get("holds")]
            sp = "pos" if (edge.get("small_avg") or 0) > 0 else "neg"
            lp = "pos" if (edge.get("large_avg") or 0) > 0 else "neg"
            edge_body = f"""
<table><tr><th>分组</th><th>笔数</th><th>平均收益</th></tr>
<tr><td>小盘(≤市值50%分位)</td><td>{edge['small_n']}</td><td class='num {sp}'>{edge['small_avg']}%</td></tr>
<tr><td>大盘(>市值50%分位)</td><td>{edge['large_n']}</td><td class='num {lp}'>{edge['large_avg']}%</td></tr>
</table>
<div class='kpi'>
  <div class='card'><b>{edge['tilt_total_ret']}%</b>小盘过滤变体·总收益</div>
  <div class='card'><b>{edge['tilt_n']}</b>小盘过滤变体·笔数</div>
  <div class='card'><b>{edge['tilt_winrate']}%</b>小盘过滤变体·胜率</div>
  <div class='card'><b>{edge['tilt_sharpe']}</b>小盘过滤变体·夏普</div>
</div>
<div class='verdict' style='margin-top:10px'>{holds_txt}</div>
<div class='note' style='margin-top:8px'>方法: 在样本外/前瞻窗口的已发布组合成交上, 按流通市值分位标注小盘/大盘并比较平均收益;
同时跑"已发布组合 + 小盘底部50%硬过滤"变体(阈值与样本内选参一致). 当前为 holdout 模式(无 2026H2 真·前瞻数据),
且小盘样本仅 {edge['small_n']} 笔, 结论<b>仅方向性参考</b>; 待数据库更新后切 forward 真·前瞻再复核.</div>
"""
        edge_html = (f"<h2>小盘倾斜 Edge 复核（样本外/前瞻）</h2>"
                     f"<p style='font-size:12px;color:#666'>OOS 总成交 {edge.get('oos_n')} 笔; "
                     f"样本内结论=小盘正向 edge(小盘交易平均 +15.4% vs 大盘 +1.2%), 此处复核是否稳健.</p>"
                     f"{edge_body}")
    html += edge_html

    html += (f"<h2>累积样本跟踪(oos_tracker.json)</h2><div class='note'>{agg}</div>"
             "<table><tr><th>运行时间</th><th>窗口</th><th>笔数</th><th>总收益</th><th>胜率</th>"
             "<th>夏普</th><th>回撤</th></tr>" + tk_rows + "</table>")

    html += ("""
<div class='note' style='margin-top:18px'><b>诚实说明:</b>
① 已发布组合(V2+上限1+止损-15%)的参数来自样本内(截至 2026-07-15)选出, 存在选参过拟合风险;
② <b>forward 模式</b>(数据库晚于样本内终点时)用从未参与选参的真实未来行情验证, 零前视泄漏, 结论最可信;
③ <b>holdout 模式</b>(当前, 无 2026H2 数据)用时间切分保留段, 因全窗口被选参间接"见过"TEST, 含轻微泄漏, 结论偏乐观;
④ 沿用原 harness 全部假设: 无分钟表(买=14:45收盘×滑点, 卖=15:00收盘×滑点); 基本面 point-in-time;
概念快照近似; 资金流代理; 样本仍偏小(各段数笔), 单笔大盈大亏影响显著.
</div>
</body></html>""")
    with open(out_path, "w") as f:
        f.write(html)


# ----------------------------------------------------------------------------
def cmd_validate(args):
    if not os.path.exists(DB_PATH):
        msg = (f"数据未就绪: 未找到数据库 {DB_PATH}。前向验证需要 qlib_pro_v16.db 存在且含 "
               f"晚于 {IN_SAMPLE_END} 的行情。请在本地/自托管 runner 中确保数据库已更新后重跑。")
        print("WARN:", msg, flush=True)
        # 仍产出一份说明性 HTML, 便于 CI 产物查看; 但判定为 FAIL(非零退出)避免假绿
        out = os.path.join(OUT_DIR, "forward_validation_report.html")
        open(out, "w").write(f"<html><body><h1>前向验证数据未就绪</h1><p>{msg}</p></body></html>")
        print("FAIL: 数据库缺失, 前向验证无法运行.", flush=True)
        sys.exit(1)

    mode = args.mode
    # 允许通过参数覆盖样本内终点(便于滚动扩展)
    in_end = args.in_sample_end or IN_SAMPLE_END

    ctx, cal, fmap = load_all()
    universe = len(ctx)
    print(f"K线覆盖全市场标的数={universe}", flush=True)
    universe_ok = universe >= MIN_UNIVERSE
    if not universe_ok:
        print(f"FAIL-GATE: K线覆盖仅 {universe} 只 < {MIN_UNIVERSE} 阈值(全市场约5000只), "
              f"前向验证样本严重不足, 结果不可信.", flush=True)
    elif universe < 4000:
        print(f"WARN: K线覆盖 {universe} 只(全市场约5000只), 前向验证为子集估计, 偏乐观.", flush=True)
    full_end = str(cal[-1])[:10]
    data_min = str(cal[0])[:10]
    data_max = full_end

    # hot_at 在【全窗口】上构建一次(资金流 point-in-time, 无未来泄漏)
    print("构建热门题材(全窗口)...", flush=True)
    hot_at = H.build_hot_themes(ctx, cal)

    # 信号索引在【全窗口】上构建一次(与 theme_cap/stop 无关, 所有组合复用)
    base = get_v2_cfg()
    inv = H.build_signal_index(ctx, cal, base)

    # 样本内基线: 已发布组合 在 [start, in_end]
    in_sample_cal = slice_cal(cal, H.WINDOW_START, in_end)
    _, eq_ins, ins_m = simulate_on(ctx, in_sample_cal, inv, hot_at, fmap, PUB_THEME_CAP, PUB_STOP)
    ins_m["label"] = "已发布组合·样本内基线"

    genuine_forward = full_end > in_end
    # auto 模式门槛: 样本外窗口需足够长(≥ MIN_OOS_TRADING_DAYS 个交易日)才做真·前瞻,
    # 否则退化为 holdout —— 避免"仅多几天新数据"导致 forward 在极薄样本上跑出 n=0 的误导性结果.
    if mode == "auto":
        if genuine_forward:
            fwd_window = slice_cal(cal, in_end, "2099-12-31")
            if len(fwd_window) >= MIN_OOS_TRADING_DAYS:
                mode = "forward"
                auto_note = (f"auto: 样本外窗口 {len(fwd_window)} 交易日 ≥ {MIN_OOS_TRADING_DAYS}, "
                             f"走真·前瞻(数据 {full_end} 晚于样本内终点 {in_end})")
            else:
                mode = "holdout"
                auto_note = (f"auto: 样本外窗口仅 {len(fwd_window)} 交易日 < {MIN_OOS_TRADING_DAYS}, "
                             f"退化为 holdout(避免薄样本误导); 数据库末端 {full_end}")
        else:
            mode = "holdout"
            auto_note = f"auto: 数据库末端 {full_end} 未晚于样本内终点 {in_end}, 走 holdout"
        print(auto_note, flush=True)
    else:
        fwd_window = slice_cal(cal, in_end, "2099-12-31") if genuine_forward else []
        auto_note = f"显式模式={mode}"

    R = dict(mode=mode, data_end=full_end, in_sample_end=in_end, in_sample_baseline=ins_m,
             in_sample_cal=in_sample_cal, eq_ins=eq_ins,
             data_min=data_min, data_max=data_max, forward=None, holdout=None,
             coverage_universe=universe,
             auto_note=auto_note, forward_window_days=len(fwd_window) if genuine_forward else 0)
    tracker = load_tracker()

    if mode == "forward":
        forward_cal = fwd_window  # 复用已计算的样本外窗口(严格晚于样本内)
        fwd_tr, eq_key, fwd_m = simulate_on(ctx, forward_cal, inv, hot_at, fmap, PUB_THEME_CAP, PUB_STOP)
        fwd_m["label"] = f"已发布组合·真·前瞻({full_end})"
        R["forward"] = fwd_m
        R["forward_trades"] = fwd_tr
        R["eq_key"] = eq_key
        R["key_cal"] = forward_cal
        # 累积 tracker(逐月扩充样本)
        add_tracker(tracker, dict(window_start=in_end, window_end=full_end,
                                  n=fwd_m["n"], total_ret=fwd_m["total_ret"],
                                  winrate=fwd_m["winrate"], sharpe=fwd_m["sharpe"], maxdd=fwd_m["maxdd"]))
        print(f"★ 真·前瞻: n={fwd_m['n']} 总收益={fwd_m['total_ret']}% 夏普={fwd_m['sharpe']} "
              f"回撤={fwd_m['maxdd']}%", flush=True)
    else:
        # holdout: TRAIN 重选 + TEST 验证
        train_cal = slice_cal(cal, TRAIN_START, TRAIN_END)
        test_cal = slice_cal(cal, TEST_START, TEST_END)
        print("TRAIN 选参...", flush=True)
        grid = []
        best = None
        raw_best = None
        for cap in [0, 1, 2]:
            for sl in STOP_GRID:
                _, _, m = simulate_on(ctx, train_cal, inv, hot_at, fmap, cap, sl)
                m["label"] = f"cap{cap}+sl{int(sl*100)}%"
                grid.append(m)
                if raw_best is None or m["total_ret"] > raw_best["total_ret"]:
                    raw_best = dict(m)
                if m["n"] >= 5 and (best is None or m["total_ret"] > best["total_ret"]):
                    best = dict(m)
        if best is None:
            best = dict(theme_cap=PUB_THEME_CAP, stop=PUB_STOP, total_ret=float("nan"), n=0)
        cap_sel, stop_sel = int(best["theme_cap"]), best["stop"]
        print(f"  TRAIN 实际最优: cap{int(raw_best['theme_cap'])} sl{int(raw_best['stop']*100)}% "
              f"n={raw_best['n']} 总收益={raw_best['total_ret']}%", flush=True)
        train_note = (f"TRAIN 段(2024-07~2025-09)V2 仅成交 {raw_best['n']} 笔, 低于选优阈值 n>=5, "
                      f"无法独立重选; 故 OOS 直接套用已发布组合. TRAIN 段实际最优: "
                      f"cap{int(raw_best['theme_cap'])} sl{int(raw_best['stop']*100)}% "
                      f"→ 总收益 {raw_best['total_ret']}%, 夏普 {raw_best['sharpe']}.")
        # 评估四种情形
        _, _, pub_train_m = simulate_on(ctx, train_cal, inv, hot_at, fmap, PUB_THEME_CAP, PUB_STOP)
        pub_train_m["label"] = "已发布组合·TRAIN"
        test_tr, eq_key, pub_test_m = simulate_on(ctx, test_cal, inv, hot_at, fmap, PUB_THEME_CAP, PUB_STOP)
        pub_test_m["label"] = "已发布组合·TEST(样本外)"
        _, _, sel_train_m = simulate_on(ctx, train_cal, inv, hot_at, fmap, cap_sel, stop_sel)
        sel_train_m["label"] = "TRAIN选出·TRAIN"
        _, _, sel_test_m = simulate_on(ctx, test_cal, inv, hot_at, fmap, cap_sel, stop_sel)
        sel_test_m["label"] = "TRAIN选出·TEST"
        R["holdout"] = dict(published_on_train=pub_train_m, published_on_test=pub_test_m,
                            selected_on_train=sel_train_m, selected_on_test=sel_test_m,
                            train_grid=grid, train_note=train_note)
        R["forward_trades"] = test_tr
        R["eq_key"] = eq_key
        R["key_cal"] = test_cal
        print(f"  ★ 已发布·TEST(OOS): n={pub_test_m['n']} 总收益={pub_test_m['total_ret']}% "
              f"夏普={pub_test_m['sharpe']}", flush=True)

    R["tracker"] = tracker

    # ---- 小盘倾斜 edge 复核(样本外/前瞻) ----
    # 在 OOS/前瞻窗口的已发布组合成交上: (1) 按市值分位标注, 比较小盘 vs 大盘交易平均收益;
    # (2) 跑"已发布组合 + 小盘底部50%过滤"变体, 看小盘过滤在 OOS 是否仍提升。
    # 阈值沿用样本内选出的底部50%, 与因子回测一致。数据不足时诚实标注"无法判定"。
    edge = None
    try:
        mmap = H.load_market_stats()
        if mmap:
            month_q, month_arr = _build_cap_dist(mmap)
            oos_trades = R.get("forward_trades") or []
            oos_cal = R.get("key_cal") or []
            if oos_trades and oos_cal:
                tagged = _tag_cap(oos_trades, mmap, month_arr)
                small = [r for r in tagged if r["bucket"] == "小盘"]
                large = [r for r in tagged if r["bucket"] == "大盘"]
                def _avg(rs):
                    return round(100 * float(np.mean([r["ret"] for r in rs])), 2) if rs else None
                inv_tilted = _tilt_inv_small(inv, mmap, month_q, 0.5)
                _, _, tilt_m = simulate_on(ctx, oos_cal, inv_tilted, hot_at, fmap,
                                           PUB_THEME_CAP, PUB_STOP)
                small_avg, large_avg = _avg(small), _avg(large)
                holds = (small_avg > large_avg) if (small and large
                        and small_avg is not None and large_avg is not None) else None
                edge = dict(oos_n=len(oos_trades), small_n=len(small), small_avg=small_avg,
                            large_n=len(large), large_avg=large_avg,
                            tilt_total_ret=tilt_m["total_ret"], tilt_n=tilt_m["n"],
                            tilt_winrate=tilt_m["winrate"], tilt_sharpe=tilt_m["sharpe"],
                            holds=holds)
                print(f"[edge] OOS 小盘倾斜: 小盘(n={edge['small_n']})={small_avg}% vs "
                      f"大盘(n={edge['large_n']})={large_avg}% | 小盘过滤变体 总收益={tilt_m['total_ret']}% "
                      f"n={tilt_m['n']}", flush=True)
            else:
                edge = dict(oos_n=len(oos_trades), holds=None, note="OOS 无成交, 样本不足")
                print(f"[edge] OOS 成交 {len(oos_trades)} 笔, 样本不足, 无法判定小盘倾斜", flush=True)
    except Exception as e:
        print(f"[edge] 小盘倾斜复核跳过: {e}", flush=True)
    R["smallcap_edge"] = edge

    build_html_validate(R, os.path.join(OUT_DIR, "forward_validation_report.html"))

    # CSV
    key_trades = R.get("forward_trades") or []
    with open(os.path.join(OUT_DIR, "forward_validation_trades.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["#", "代码", "买入日", "买价", "卖出日", "卖价", "收益%", "股数", "持有日", "退出", "ROE%", "净利同比%"])
        for i, t in enumerate(key_trades, 1):
            w.writerow([i, t["code"], str(t["buy_t"])[:10], round(t["buy_px"], 2), str(t["sell_t"])[:10],
                        round(t["sell_px"], 2), round(t["ret"] * 100, 2), t["shares"], t["hold_days"],
                        t["reason"], t["roe"], t["np_yoy"]])

    # JSON
    summary = dict(mode=R["mode"], generated_at=_dt.datetime.now().isoformat(timespec="seconds"),
                   data_end=full_end, in_sample_end=in_end, coverage_universe=universe,
                   auto_note=R.get("auto_note", ""), forward_window_days=R.get("forward_window_days", 0),
                   published_combo=dict(base="V2", theme_cap=PUB_THEME_CAP, stop=PUB_STOP, regime="none"),
                   in_sample_baseline=ins_m, forward=R["forward"], holdout=R["holdout"],
                   tracker=tracker, smallcap_edge=R.get("smallcap_edge"))
    json.dump(summary, open(os.path.join(OUT_DIR, "forward_validation_metrics.json"), "w"),
              ensure_ascii=False, indent=2, default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)

    # universe 门禁: 样本严重不足时判定 FAIL(非零退出), 避免"假绿"掩盖预热/库损坏
    if not universe_ok:
        print(f"校验失败: 覆盖 {universe} 只 < {MIN_UNIVERSE} 阈值, 前向验证判定为 FAIL(避免假绿).",
              flush=True)
        sys.exit(1)

    print("完成. 报告: forward_validation_report.html | 逐笔: forward_validation_trades.csv | "
          "指标: forward_validation_metrics.json", flush=True)


# ----------------------------------------------------------------------------
def cmd_watchlist(args):
    """生成"当前候选"观察清单(接实盘小仓观察). 不交易, 仅列出近期满足入场信号的标的与指标."""
    if not os.path.exists(DB_PATH):
        print("WARN: 数据库缺失, 无法生成观察清单", flush=True)
        return
    ctx, cal, fmap = load_all()
    hot_at = H.build_hot_themes(ctx, cal)
    cfg = pub_cfg()
    inv = H.build_signal_index(ctx, cal, cfg)   # 已含 theme_cap 的候选(实际 gating 在 simulate, 这里取 base 候选即可)
    # 取最近 lookback 个交易日的候选
    last_dates = cal[-args.lookback:]
    cands = {}  # code -> signal_date(取最近一次)
    for d in last_dates:
        ds = str(d)[:10]
        for code in inv.get(ds, []):
            cands[code] = ds
    rows = []
    for code, sdate in cands.items():
        g = ctx[code]
        ts = pd.Timestamp(sdate)
        if ts not in g.index:
            continue
        r = g.loc[ts]
        close = r["close"]
        ok, pe, pb, roe, np_yoy = H.quality_ok(fmap, code, sdate, close, True)
        if not ok:
            continue
        dd = r["dd60"]; rsi = r["rsi14"]
        score = round((-dd * 100) * 0.6 + (roe or 0) * 0.4, 2)
        themes = hot_at.get(sdate, (set(), [], {}))[2].get(code, [])
        rows.append(dict(code=code, sdate=sdate, close=round(close, 2), dd=round(dd * 100, 2),
                         rsi=round(rsi, 1), roe=roe, np_yoy=np_yoy, pe=(round(pe, 1) if pe else None),
                         pb=(round(pb, 2) if pb else None), score=score, themes=";".join(themes)))
    rows.sort(key=lambda x: -x["score"])

    # CSV
    csv_path = os.path.join(OUT_DIR, "live_watchlist.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["代码", "信号日", "收盘价", "距60高回撤%", "RSI", "ROE%", "净利同比%", "PE", "PB", "评分", "热门题材"])
        for x in rows:
            w.writerow([x["code"], x["sdate"], x["close"], x["dd"], x["rsi"], x["roe"],
                        x["np_yoy"], x["pe"], x["pb"], x["score"], x["themes"]])

    # HTML
    tr = "".join(
        f"<tr><td>{x['code']}</td><td>{x['sdate']}</td><td>{x['close']}</td><td>{x['dd']}%</td>"
        f"<td>{x['rsi']}</td><td>{x['roe']}</td><td>{x['np_yoy']}</td><td>{x['pe']}</td>"
        f"<td>{x['pb']}</td><td><b>{x['score']}</b></td><td>{x['themes']}</td></tr>"
        for x in rows)
    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a}}
h1{{font-size:20px}} table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}} th{{background:#f4f6f8}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}</style>
</head><body>
<h1>超跌绩优反弹 · 当前候选观察清单(接实盘小仓观察)</h1>
<p>生成时间 {_dt.datetime.now().isoformat(timespec='seconds')} ｜ 数据截至 {str(cal[-1])[:10]} ｜
近 {args.lookback} 个交易日出现的候选共 <b>{len(rows)}</b> 只(已过滤基本面不合格). 评分=回撤深度×0.6+ROE×0.4.</p>
<div class='note'>本清单仅作<b>人工观察</b>参考, 非交易指令. 实盘小仓下单请结合自身风控(仓位/止损-15%/单题材上限1).</div>
<table><tr><th>代码</th><th>信号日</th><th>收盘</th><th>距60高回撤%</th><th>RSI</th><th>ROE%</th>
<th>净利同比%</th><th>PE</th><th>PB</th><th>评分</th><th>热门题材</th></tr>{tr}</table>
</body></html>"""
    open(os.path.join(OUT_DIR, "live_watchlist.html"), "w").write(html)
    print(f"观察清单: {len(rows)} 只 → live_watchlist.html / live_watchlist.csv", flush=True)


# ----------------------------------------------------------------------------
def load_tracker():
    if os.path.exists(TRACKER):
        try:
            return json.load(open(TRACKER))
        except Exception:
            return []
    return []


def add_tracker(tracker, entry):
    entry["run_at"] = _dt.datetime.now().isoformat(timespec="seconds")
    # 同窗口去重(同 window_end 仅保留最新一次)
    tracker[:] = [e for e in tracker if e.get("window_end") != entry["window_end"]]
    tracker.append(entry)
    json.dump(tracker, open(TRACKER, "w"), ensure_ascii=False, indent=2,
              default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="超跌绩优反弹 前向验证 / 观察清单")
    sub = ap.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="前向/样本外验证")
    v.add_argument("--mode", choices=["auto", "forward", "holdout"], default="auto")
    v.add_argument("--in-sample-end", default=None, help="覆盖样本内终点(用于滚动扩展)")
    v.set_defaults(func=cmd_validate)

    w = sub.add_parser("watchlist", help="当前候选观察清单")
    w.add_argument("--lookback", type=int, default=10, help="回看最近 N 个交易日")
    w.set_defaults(func=cmd_watchlist)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
