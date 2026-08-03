# -*- coding: utf-8 -*-
"""
factor_tilt_backtest.py — 小盘 / 红利 因子倾斜回测
=================================================

在已发布最优组合之上检验两类因子倾斜是否提升策略表现:

  已发布最优组合 = V2(深度超跌+绩优+热门) + 单题材持仓上限1 + 止损-15% + 无大盘择时
  (与 MEMORY "已发布最优组合" 一致; 样本内全窗口总收益 +23.61%)

两类检验:
  (A) 策略层 filter 回测: 在超跌信号候选上叠加"小盘 / 高红利"硬过滤, 对比策略指标(n/胜率/
      平均每笔/总收益/夏普/回撤). 注意: 硬过滤会缩减候选与成交笔数, 样本本就小(n≈10),
      故该层仅作方向性参考.
  (B) 交易层截面归因(主): 对基线实际成交逐笔标注 流通市值 / 股息率 分位, 比较各组
      平均收益与胜率. 该层不缩减样本, 是更稳健的因子有效性判别.

数据源: market_stats(月频 流通市值/总市值) + dividend_stats(静态 年均股息率%)
口径: point-in-time, 月度月末快照向前取最近月(与 HOQ.load_market_stats/market_stats_at 同口径).
      股息率为"上市以来平均年度股息率%"近似(非精确 TTM), 用于相对排序.

严肃声明: 回测窗口 = HOQ.WINDOW (2024-07-01 ~ 2026-07-15) 为样本内; 倾斜阈值事后选取,
存在过拟合风险; 样本量小, 结论须保守, 应以 forward_validation 后向验证复核.

用法: python factor_tilt_backtest.py
"""
import os, sys, json
from collections import defaultdict
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import harness_oversold_quality as HOQ
HOQ.DB = os.path.join(ROOT, "qlib_pro_v16.db")
HOQ.ROOT = ROOT

# 已发布最优组合
BASE_CFG = dict(mode="deep", dd=-0.18, gap=0.03, rsi_th=35,
                ma60_rising=False, vol_confirm=False, macd_rsi=False,
                hot_on=True, pe_pb_on=True, quality_on=True, theme_cap=1)
STOP_LOSS = -0.15
HOLD = HOQ.HOLD_DEF
COOLDOWN = HOQ.SAME_CODE_COOLDOWN
ENTRY = HOQ.ENTRY_MODE

# 策略层 filter 扫描的倾斜配置: (名称, p_small, p_div)
#   p_small: 保留流通市值 <= 该分位的"小盘"(0.5=底部50%, 0.3=底部30%, 0.2=底部20%)
#   p_div  : 保留股息率 >= (1-p_div) 分位的"高红利"(0.5=顶部50%, 0.3=顶部30%, 0.2=顶部20%)
TILT_CONFIGS = [
    ("基线(无倾斜)", None, None),
    ("小盘50%", 0.5, None),
    ("小盘30%", 0.3, None),
    ("小盘20%", 0.2, None),
    ("红利50%", None, 0.5),
    ("红利30%", None, 0.3),
    ("红利20%", None, 0.2),
    ("小盘50%+红利50%", 0.5, 0.5),
    ("小盘30%+红利30%", 0.3, 0.3),
]


def build_month_dist(mmap):
    """按月末快照构建 流通市值 分位阈值 + 股息率 静态分位阈值 + 原始数组(用于排名)."""
    month_circ = defaultdict(list)
    for code, recs in mmap.items():
        for rec in recs:
            if rec["circ_mv"]:
                month_circ[rec["trade_date"]].append(rec["circ_mv"])
    month_q = {}        # date -> {p: 分位阈值}
    month_arr = {}      # date -> np.array(流通市值)
    for d, lst in month_circ.items():
        a = np.array(lst, float)
        month_arr[d] = a
        month_q[d] = {p: float(np.percentile(a, p * 100)) for p in [0.2, 0.3, 0.5, 0.7, 0.8]}
    # 注意: dividend_yield 可能为 NaN(非 None), 必须显式剔除, 否则 np.percentile 返回 NaN
    divs = []
    for recs in mmap.values():
        for rec in recs:
            dy = rec["dividend_yield"]
            if dy is not None and not (isinstance(dy, float) and np.isnan(dy)):
                divs.append(dy)
    all_div = np.array(divs, float)
    div_q = {p: float(np.percentile(all_div, p * 100)) for p in [0.2, 0.3, 0.5, 0.7, 0.8]}
    return month_q, month_arr, div_q, all_div


def tilt_pass(rec, snap, month_q, div_q, p_small, p_div):
    if p_small is not None:
        cm = rec["circ_mv"]
        if cm is None or (isinstance(cm, float) and np.isnan(cm)) or cm <= 0:
            return False
        q = month_q.get(snap, {}).get(p_small)
        if q is None or cm > q:
            return False
    if p_div is not None:
        dy = rec["dividend_yield"]
        if dy is None or (isinstance(dy, float) and np.isnan(dy)):
            return False
        q = div_q.get(1 - p_div)
        if q is None or dy < q:
            return False
    return True


def make_tilted_inv(inv, mmap, month_q, div_q, p_small=None, p_div=None):
    out = defaultdict(list)
    dropped = 0
    for ts, codes in inv.items():
        for code in codes:
            rec = HOQ.market_stats_at(mmap, code, ts)
            if rec is None:
                dropped += 1
                continue
            if tilt_pass(rec, rec["trade_date"], month_q, div_q, p_small, p_div):
                out[ts].append(code)
            else:
                dropped += 1
    return out, dropped


def rank_in(x, arr):
    """x 在 arr 中的百分秩(0~100, 越小越靠后/越低). NaN 视为无数据."""
    if arr is None or len(arr) == 0 or x is None or (isinstance(x, float) and np.isnan(x)):
        return float("nan")
    return float((arr < x).mean() * 100)


def tag_trades(trades, mmap, month_arr, all_div):
    rows = []
    for t in trades:
        ts = str(t["buy_t"])[:10]
        rec = HOQ.market_stats_at(mmap, t["code"], ts)
        circ = rec["circ_mv"] if rec else None
        dy = rec["dividend_yield"] if rec else None
        snap = rec["trade_date"] if rec else None
        arr = month_arr.get(snap) if snap else None
        cap_r = rank_in(circ, arr)        # 0=最小盘, 100=最大盘
        div_r = rank_in(dy, all_div)      # 0=最低红利, 100=最高红利
        cap_bucket = "小盘" if cap_r <= 50 else ("大盘" if cap_r > 50 else "无数据")
        div_bucket = "高红利" if div_r >= 50 else ("低红利" if div_r < 50 else "无数据")
        cap_q = int(np.ceil(cap_r / 20)) if not np.isnan(cap_r) else None   # 1..5
        div_qn = int(np.ceil(div_r / 20)) if not np.isnan(div_r) else None  # 1..5
        rows.append(dict(code=t["code"], buy_t=ts, ret=t["ret"], reason=t["reason"],
                         circ_mv=circ, div_yield=dy, cap_rank=cap_r, div_rank=div_r,
                         cap_bucket=cap_bucket, div_bucket=div_bucket,
                         cap_q=cap_q, div_q=div_qn))
    return rows


def group_stats(rows, key):
    g = defaultdict(list)
    for r in rows:
        g[r[key]].append(r)
    out = []
    for k, lst in sorted(g.items(), key=lambda x: (x[0] is None, x[0])):
        rets = [r["ret"] for r in lst]
        wins = [r for r in rets if r > 0]
        out.append(dict(group=k, n=len(rets),
                        winrate=round(100 * len(wins) / len(rets), 1) if rets else 0,
                        avg_ret=round(100 * float(np.mean(rets)), 2) if rets else 0,
                        avg_win=round(100 * float(np.mean(wins)), 2) if wins else 0,
                        avg_loss=round(100 * float(np.mean([r for r in rets if r <= 0])), 2) if any(r <= 0 for r in rets) else 0))
    return out


def run_filter(inv_tilted, ctx, cal_slice, hot_at, fmap):
    trades, eq = HOQ.simulate(ctx, cal_slice, inv_tilted, hot_at, fmap,
                              HOLD, ENTRY, STOP_LOSS, BASE_CFG, COOLDOWN)
    return trades, eq, HOQ.metrics(trades, eq)


def build_html(path, base_m, filter_rows, cap_attr, div_attr, cap_q_attr, div_q_attr,
               n_base_sig, window, dropped_note):
    def mrow(name, m, n_sig=None, hl=False, dropped=None):
        bcls = "pos" if m["total_ret"] > 0 else "neg"
        h = " style='background:#eaf6ff'" if hl else ""
        sig = f"<td>{n_sig}</td>" if n_sig is not None else ""
        dr = f"<td>{dropped}</td>" if dropped is not None else ""
        return (f"<tr{h}><td>{name}</td>{sig}<td>{m['n']}</td><td>{m['winrate']}%</td>"
                f"<td>{m['avg_ret']}%</td><td class='num {bcls}'>{m['total_ret']}%</td>"
                f"<td>{m['sharpe']}</td><td>{m['maxdd']}%</td>{dr}</tr>")

    def arow(r):
        bcls = "pos" if r["avg_ret"] > 0 else "neg"
        return (f"<tr><td>{r['group']}</td><td>{r['n']}</td><td>{r['winrate']}%</td>"
                f"<td class='num {bcls}'>{r['avg_ret']}%</td><td>{r['avg_win']}%</td>"
                f"<td>{r['avg_loss']}%</td></tr>")

    fhtml = "".join(mrow(*r) for r in filter_rows)
    cap_html = "".join(arow(r) for r in cap_attr)
    div_html = "".join(arow(r) for r in div_attr)
    capq_html = "".join(arow(r) for r in cap_q_attr)
    divq_html = "".join(arow(r) for r in div_q_attr)

    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:22px}} h2{{font-size:17px;margin-top:26px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}}
th{{background:#f4f6f8}} .num{{text-align:right}}
.pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7e34;font-weight:600}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}
.kpi{{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:12px 16px;min-width:110px}}
.card b{{font-size:20px;display:block}}
</style></head><body>
<h1>小盘 / 红利 因子倾斜回测</h1>
<p>基线 = 已发布最优组合(V2 深度超跌+绩优+热门 + 单题材上限1 + 止损-15% + 无大盘择时) ｜ 窗口 {window[0]}~{window[1]}</p>
<div class='kpi'>
 <div class='card'><b>{base_m['total_ret']}%</b>基线总收益</div>
 <div class='card'><b>{base_m['winrate']}%</b>基线胜率</div>
 <div class='card'><b>{base_m['n']}</b>基线成交笔数</div>
 <div class='card'><b>{n_base_sig}</b>基线超跌信号候选数</div>
</div>
<div class='note'><b>严肃声明:</b> 窗口为样本内(2024-07~2026-07-15); 倾斜阈值事后选取, 存在过拟合风险;
样本量小(n≈10), 结论须保守, 应以后向验证(forward_validation)复核. 股息率为"上市以来平均年度股息率%"近似, 用于相对排序.</div>

<h2>一、策略层 filter 回测(硬过滤候选)</h2>
<p class='note'>在超跌信号候选上叠加小盘/红利硬过滤后跑完整策略. 注意: 硬过滤会缩减候选与成交笔数(样本本就小),
故该层仅作方向性参考; 真正的因子有效性见第二节截面归因. "dropped" = 因不满足倾斜或缺失市值数据被剔除的候选数.</p>
<table><tr><th>配置</th><th>超跌候选</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>最大回撤</th><th>剔除候选</th></tr>
{fhtml}</table>

<h2>二、交易层截面归因(主) — 按流通市值分位</h2>
<p class='note'>对基线实际成交逐笔标注流通市值百分秩(0=最小盘,100=最大盘), 比较各组平均收益与胜率. 不缩减样本.</p>
<table><tr><th>市值分组</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>平均盈</th><th>平均亏</th></tr>
{cap_html}</table>
<h3>市值五分位 (Q1=最小盘 ... Q5=最大盘)</h3>
<table><tr><th>市值五分位</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>平均盈</th><th>平均亏</th></tr>
{capq_html}</table>

<h2>三、交易层截面归因(主) — 按股息率分位</h2>
<p class='note'>对基线实际成交逐笔标注股息率百分秩(0=最低红利,100=最高红利).</p>
<table><tr><th>股息分组</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>平均盈</th><th>平均亏</th></tr>
{div_html}</table>
<h3>股息五分位 (Q1=最低红利 ... Q5=最高红利)</h3>
<table><tr><th>股息五分位</th><th>成交</th><th>胜率</th><th>平均每笔</th><th>平均盈</th><th>平均亏</th></tr>
{divq_html}</table>

<div class='note' style='margin-top:18px'><b>读图提示:</b> 若"小盘"组平均收益/胜率明显高于"大盘"组, 且五分位呈单调(越小盘越好),
说明小盘倾斜对该策略有正贡献; 红利同理. 反之则该因子在该策略中无显著 edge.</div>
</body></html>"""
    with open(path, "w") as f:
        f.write(html)


def main():
    print("加载K线...", flush=True)
    ctx = HOQ.load_kline()
    cal = sorted({t for g in ctx.values() for t in g.index})
    cal_slice = [t for t in cal if HOQ.WINDOW_START <= str(t)[:10] <= HOQ.WINDOW_END]
    print(f"  标的数={len(ctx)} 交易日={len(cal_slice)}", flush=True)
    print("预计算指标...", flush=True)
    ctx = HOQ.build_ctx(ctx)
    print("加载基本面...", flush=True)
    fmap = HOQ.load_fundamentals()
    print("构建热门题材...", flush=True)
    hot_at = HOQ.build_hot_themes(ctx, cal_slice)
    print("加载市值/股息...", flush=True)
    mmap = HOQ.load_market_stats()
    print(f"  落库股票数={len(mmap)}", flush=True)
    month_q, month_arr, div_q, all_div = build_month_dist(mmap)

    print("构建基线信号(V2)...", flush=True)
    base_inv = HOQ.build_signal_index(ctx, cal_slice, BASE_CFG)
    n_base_sig = sum(len(v) for v in base_inv.values())
    print(f"  超跌信号候选数={n_base_sig}", flush=True)

    base_trades, base_eq, base_m = run_filter(base_inv, ctx, cal_slice, hot_at, fmap)
    print(f"  基线: n={base_m['n']} 胜率={base_m['winrate']}% 平均每笔={base_m['avg_ret']}% "
          f"总收益={base_m['total_ret']}% 夏普={base_m['sharpe']} 回撤={base_m['maxdd']}%", flush=True)

    # ---- (A) 策略层 filter 回测 ----
    print("\n策略层 filter 扫描...", flush=True)
    filter_rows = []
    for name, ps, pd_ in TILT_CONFIGS:
        inv_t, dropped = make_tilted_inv(base_inv, mmap, month_q, div_q, ps, pd_)
        tr, eq, m = run_filter(inv_t, ctx, cal_slice, hot_at, fmap)
        n_sig = sum(len(v) for v in inv_t.values())
        hl = (ps is None and pd_ is None)
        filter_rows.append((name, m, n_sig, hl, dropped))
        print(f"  {name}: 候选={n_sig} 成交={m['n']} 胜率={m['winrate']}% 总收益={m['total_ret']}% "
              f"夏普={m['sharpe']} 回撤={m['maxdd']}% 剔除={dropped}", flush=True)

    # ---- (B) 交易层截面归因 ----
    print("\n交易层截面归因...", flush=True)
    tagged = tag_trades(base_trades, mmap, month_arr, all_div)
    cap_attr = group_stats(tagged, "cap_bucket")
    div_attr = group_stats(tagged, "div_bucket")
    cap_q_attr = group_stats(tagged, "cap_q")
    div_q_attr = group_stats(tagged, "div_q")
    for label, rows in [("市值二分", cap_attr), ("股息二分", div_attr),
                        ("市值五分位", cap_q_attr), ("股息五分位", div_q_attr)]:
        print(f"  [{label}]", flush=True)
        for r in rows:
            print(f"    {r['group']}: n={r['n']} 胜率={r['winrate']}% 平均={r['avg_ret']}%", flush=True)

    # ---- 输出 ----
    out_html = os.path.join(HERE, "factor_tilt_backtest_report.html")
    build_html(out_html, base_m, filter_rows, cap_attr, div_attr, cap_q_attr, div_q_attr,
               n_base_sig, [HOQ.WINDOW_START, HOQ.WINDOW_END], "")
    out_json = os.path.join(HERE, "factor_tilt_backtest_metrics.json")
    json.dump(dict(window=[HOQ.WINDOW_START, HOQ.WINDOW_END],
                   base=base_m, n_base_signal=n_base_sig,
                   filter=[dict(name=n, metrics=m, n_signal=ns, dropped=d)
                           for n, m, ns, hl, d in filter_rows],
                   attribution=dict(cap=cap_attr, div=div_attr, cap_q=cap_q_attr, div_q=div_q_attr),
                   tagged_trades=tagged),
              open(out_json, "w"), ensure_ascii=False, indent=2)
    # CSV: 基线成交 + 倾斜标注
    import csv
    out_csv = os.path.join(HERE, "factor_tilt_backtest_trades.csv")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["代码", "买入日", "收益%", "退出", "流通市值(元)", "股息率%",
                    "市值百分秩", "股息百分秩", "市值组", "股息组", "市值五分位", "股息五分位"])
        for r in tagged:
            w.writerow([r["code"], r["buy_t"], round(r["ret"] * 100, 2), r["reason"],
                        round(r["circ_mv"], 2) if r["circ_mv"] else "",
                        round(r["div_yield"], 2) if r["div_yield"] is not None else "",
                        round(r["cap_rank"], 1) if not (isinstance(r["cap_rank"], float) and np.isnan(r["cap_rank"])) else "",
                        round(r["div_rank"], 1) if not (isinstance(r["div_rank"], float) and np.isnan(r["div_rank"])) else "",
                        r["cap_bucket"], r["div_bucket"], r["cap_q"], r["div_q"]])
    print(f"\n完成. HTML={out_html}\n      JSON={out_json}\n      CSV={out_csv}", flush=True)


if __name__ == "__main__":
    main()
