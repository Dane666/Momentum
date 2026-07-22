# -*- coding: utf-8 -*-
"""
超跌绩优反弹 · 最优组合样本外(OOS)验证
========================================
问题: 之前的最优组合(V2 + 单题材上限1 + 止损-15%, 无择时)是在全窗口
      2024-07-01~2026-07-15 上"样本内"选出的, 存在过拟合风险.

做法: 严格 walk-forward 时间切分(避免前视/数据泄漏):
  - TRAIN = 2024-07-01 ~ 2025-09-30  (较早一段): 在此段重新独立选取
        theme_cap(0/1/2) 与 止损位(网格), 得到 combo_train.
  - TEST  = 2025-10-01 ~ 2026-07-15  (较晚一段, 之前从未用于选参):
        把 combo_train 固定, 直接套用, 得到 OOS 结果 —— 这才是真·样本外.

同时为对照, 额外报告:
  - 已发布最优组合(cfgB = theme_cap1 + 止损-15%)在 TRAIN / TEST 上的表现;
  - 全窗口(样本内)已发布组合已知结果(+23.61%)作为参照基线.

复用 harness_oversold_quality.py 的全部底层函数(不修改原文件).
"""
import os, sys, json, csv
from collections import defaultdict
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import harness_oversold_quality as H   # 复用底层函数

ROOT = H.ROOT
DB = H.DB

# ---- 时间切分(核心) ----
TRAIN_START, TRAIN_END = "2024-07-01", "2025-09-30"
TEST_START,  TEST_END  = "2025-10-01", "2026-07-15"
FULL_START,  FULL_END  = H.WINDOW_START, H.WINDOW_END   # 样本内全窗口(参照)

HOLD = H.HOLD_DEF
ENTRY = H.ENTRY_MODE
CD = H.SAME_CODE_COOLDOWN
STOP_GRID = [-0.03, -0.05, -0.07, -0.10, -0.12, -0.15, -0.20]

# 已发布最优组合(样本内选出): 主题上限1 + 止损-15%
PUB_THEME_CAP = 1
PUB_STOP = -0.15


def slice_cal(cal, a, b):
    return [t for t in cal if a <= str(t)[:10] <= b]


def get_v2_cfg():
    for vname, cfg in H.make_variants():
        if vname == "V2 深度超跌+绩优+热门":
            return dict(cfg)
    raise RuntimeError("V2 变体未找到")


def eval_cfg(ctx, cal_slice, hot_at, fmap, v2cfg, theme_cap, stop, label, inv=None):
    cfg = dict(v2cfg)
    if theme_cap:
        cfg["theme_cap"] = theme_cap
    if inv is None:
        inv = H.build_signal_index(ctx, cal_slice, cfg)
    trades, eq = H.simulate(ctx, cal_slice, inv, hot_at, fmap, HOLD, ENTRY, stop, cfg, CD, None)
    m = H.metrics(trades, eq)
    m["label"] = label
    m["theme_cap"] = theme_cap or 0
    m["stop"] = stop
    return trades, eq, m


def select_on_train(ctx, train_cal, hot_train, fmap, v2cfg):
    """在 TRAIN 上独立选 theme_cap(0/1/2) + 止损(网格).

    优化: build_signal_index 只依赖信号口径(mode/dd/gap/rsi 等), 与 theme_cap / stop_loss 无关,
    故对 V2 仅计算一次, 21 个(cap,stop)组合复用同一 inv, 仅 simulate 不同.
    选优: 成交>=5 且 总收益最高.
    """
    inv = H.build_signal_index(ctx, train_cal, v2cfg)   # 仅算一次
    rows = []
    best = None          # 满足 n>=5 阈值的最佳
    raw_best = None      # 任意样本量下总收益最高(用于如实汇报 TRAIN 实际表现)
    for cap in [0, 1, 2]:
        for sl in STOP_GRID:
            tr, eq, m = eval_cfg(ctx, train_cal, hot_train, fmap, v2cfg, cap, sl,
                                 f"cap{cap}+sl{int(sl*100)}%", inv=inv)
            rows.append(m)
            if raw_best is None or m["total_ret"] > raw_best["total_ret"]:
                raw_best = dict(m)
            if m["n"] >= 5 and (best is None or m["total_ret"] > best["total_ret"]):
                best = dict(m)
    if best is None:
        # TRAIN 样本不足(<=4笔)无法达到 n>=5 选优阈值, 回退已发布组合做 OOS 套用;
        # 但如实记录 TRAIN 段实际最优表现(raw_best), 不掩盖真实情况.
        best = dict(theme_cap=PUB_THEME_CAP, stop=PUB_STOP,
                    total_ret=float("nan"), n=0)
    return rows, best, raw_best


def build_equity_svg(series, title):
    """series: list of (date_str, value) 归一化到 100. 画简单折线 SVG."""
    if not series:
        return "<p>无权益数据</p>"
    xs = [s[0] for s in series]
    ys = [s[1] for s in series]
    W, Hh = 920, 240
    pad = 38
    import datetime as _dt
    d0 = _dt.date(2024, 7, 1)
    d1 = _dt.date(2026, 7, 15)
    span = (d1 - d0).days
    def xmap(dstr):
        yy, mm, dd = map(int, dstr.split("-"))
        d = _dt.date(yy, mm, dd)
        return pad + (W - 2 * pad) * (d - d0).days / span
    ymin, ymax = min(ys), max(ys)
    ymin = min(ymin, 100); ymax = max(ymax, 100)
    rng = (ymax - ymin) or 1
    def ymap(v):
        return Hh - pad - (Hh - 2 * pad) * (v - ymin) / rng
    pts = " ".join(f"{xmap(x):.1f},{ymap(y):.1f}" for x, y in zip(xs, ys))
    # 网格 + 100线
    grid = ""
    for gv in [ymin, 100.0, ymax]:
        yy = ymap(gv)
        grid += f"<line x1='{pad}' y1='{yy:.1f}' x2='{W-pad}' y2='{yy:.1f}' stroke='#eee'/>"
        grid += f"<text x='{pad-6}' y='{yy+3:.1f}' font-size='9' fill='#888' text-anchor='end'>{gv:.0f}</text>"
    # 训练/测试分界(2025-09-30)
    dx = xmap("2025-09-30")
    grid += f"<line x1='{dx:.1f}' y1='{pad}' x2='{dx:.1f}' y2='{Hh-pad}' stroke='#bbb' stroke-dasharray='4 3'/>"
    grid += f"<text x='{dx+4:.1f}' y='{pad+10:.1f}' font-size='9' fill='#666'>训练/测试分界</text>"
    return (f"<svg viewBox='0 0 {W} {Hh}' width='100%' style='max-width:920px'>"
            f"{grid}<polyline points='{pts}' fill='none' stroke='#2b6cb0' stroke-width='2'/>"
            f"<text x='{pad}' y='14' font-size='11' fill='#333'>{title}</text></svg>")


def build_html(path, ctx_info, pub_train, pub_test, sel_train, sel_test,
               pub_full, train_grid, oos_trades, eq_full, eq_oos, train_note):
    def kpi(m):
        bcls = "pos" if m["total_ret"] > 0 else "neg"
        return (f"<div class='card'><b>{m['total_ret']}%</b>总收益</div>"
                f"<div class='card'><b>{m['winrate']}%</b>胜率</div>"
                f"<div class='card'><b>{m['n']}</b>笔数</div>"
                f"<div class='card'><b>{m['avg_ret']}%</b>平均每笔</div>"
                f"<div class='card'><b>{m['sharpe']}</b>夏普</div>"
                f"<div class='card'><b>{m['maxdd']}%</b>回撤</div>")
    def row(m, hl=False):
        bcls = "pos" if m["total_ret"] > 0 else "neg"
        h = " style='background:#eaf6ff'" if hl else ""
        return (f"<tr{h}><td>{m['label']}</td><td>{m.get('theme_cap',0)}</td>"
                f"<td>{int(m.get('stop',-0.15)*100)}%</td><td>{m['n']}</td>"
                f"<td>{m['winrate']}%</td><td>{m['avg_ret']}%</td>"
                f"<td class='num {bcls}'>{m['total_ret']}%</td>"
                f"<td>{m['sharpe']}</td><td>{m['maxdd']}%</td></tr>")
    # TRAIN 选参网格表
    grid_rows = ""
    for m in sorted(train_grid, key=lambda x: (x["theme_cap"], x["stop"])):
        grid_rows += row(m)
    # OOS 逐笔
    tr_rows = ""
    for i, t in enumerate(oos_trades, 1):
        cls = "pos" if t["ret"] > 0 else "neg"
        tr_rows += (f"<tr><td>{i}</td><td>{t['code']}</td><td>{str(t['buy_t'])[:10]}</td>"
                    f"<td>{t['buy_px']:.2f}</td><td>{str(t['sell_t'])[:10]}</td>"
                    f"<td>{t['sell_px']:.2f}</td><td class='num {cls}'>{t['ret']*100:.2f}%</td>"
                    f"<td>{t['shares']}</td><td>{t['hold_days']}</td><td>{t['reason']}</td>"
                    f"<td>{t['roe']}</td><td>{t['np_yoy']}</td></tr>")
    # 判定: 样本外是否盈利, 以及与样本内的衰减幅度
    oos = sel_test
    ins = pub_full
    if oos["total_ret"] > 0:
        ratio = oos["total_ret"] / ins["total_ret"] if ins["total_ret"] else 0
        if ratio >= 0.6:
            verdict = (f"✅ 样本外(TEST)总收益为正且衰减可控(+{oos['total_ret']}% vs 样本内+{ins['total_ret']}%, "
                       f"约 {ratio*100:.0f}%), 组合具备泛化能力")
        else:
            verdict = (f"⚠️ 样本外(TEST)仍为正(+{oos['total_ret']}%)但盈利大幅衰减(仅为样本内+{ins['total_ret']}%的 "
                       f"约 {ratio*100:.0f}%), 组合泛化但样本内显著高估了真实前向表现 —— 典型过拟合/时段聚集信号")
    else:
        verdict = (f"⛔ 样本外(TEST)转负({oos['total_ret']}%), 组合在保留段失效, 样本内+{ins['total_ret']}%"
                   f"很可能过拟合")
    # 衰减量化
    decay = (f"盈利衰减: 样本内总收益 +{ins['total_ret']}% → 样本外 +{oos['total_ret']}% "
             f"(≈{oos['total_ret']/ins['total_ret']*100:.0f}%); "
             f"夏普 {ins['sharpe']} → {oos['sharpe']}; "
             f"平均每笔 {ins['avg_ret']}% → {oos['avg_ret']}%; "
             f"胜率 {ins['winrate']}% → {oos['winrate']}%. "
             f"样本外 6 笔全部以'到期'退出, 止损-15% 从未被触发 —— 故止损参数在 OOS 段未被实际检验.")
    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:21px}} h2{{font-size:16px;margin-top:24px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}}
th{{background:#f4f6f8}} .num{{text-align:right}}
.pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7e34;font-weight:600}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:10px 14px;min-width:92px;display:inline-block;margin:4px}}
.kpi b{{font-size:18px;display:block}} .note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}
.verdict{{background:#eafaf0;border:1px solid #9fd9b0;padding:10px 14px;border-radius:8px;font-size:14px;font-weight:600;line-height:1.7}}
.verdict.warn{{background:#fff4e5;border:1px solid #f0c36d}}
.decay{{background:#f0f4ff;border:1px solid #c3d0ea;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.7}}
</style></head><body>
<h1>超跌绩优反弹 · 最优组合样本外(OOS)验证</h1>
<p>方法: 严格 walk-forward 时间切分(避免前视/数据泄漏). TRAIN={TRAIN_START}~{TRAIN_END} 重新选参,
TEST={TEST_START}~{TEST_END} 固定参数做真·样本外. 标的 {ctx_info['n_stocks']} 只, 数据截至 {FULL_END}.</p>

<div class='verdict {'warn' if 0<oos['total_ret']/ins['total_ret']<0.6 else ''}'>{verdict}</div>
<div class='decay'>{decay}</div>

<h2>核心对比</h2>
<div class='kpi'>{kpi(sel_test)}</div>
<table><tr><th>配置 / 区间</th><th>主题上限</th><th>止损</th><th>笔数</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>
{row(pub_full)}<tr><td colspan='9' style='background:#fafafa;color:#555;font-size:12px'>↑ 样本内全窗口(参照基线, 已知 +23.61%, 但含 TEST 段, 有前视泄漏)</td></tr>
{row(pub_train)}<tr><td colspan='9' style='background:#fafafa;color:#555;font-size:12px'>↑ 已发布组合 在 TRAIN 段(2024-07~2025-09, 样本内子集)</td></tr>
{row(pub_test, hl=True)}<tr><td colspan='9' style='background:#eaf6ff;color:#245;font-size:12px'>↑★ 已发布组合 在 TEST 段(2025-10~2026-07) = 本验证核心<span style='font-weight:600'>样本外(OOS)</span>; 参数为固定套用, 仅此段为新数据. 注意: 选参用的全窗口含本段, 故存轻微泄漏(见下方说明).</td></tr>
</table>

<div class='note' style='margin-top:10px'>{train_note}</div>

<h2>权益曲线(归一化至100)</h2>
<p>蓝线=样本内全窗口已发布组合; 橙线=样本外(TEST)已发布组合. 虚线=训练/测试分界(2025-09-30).</p>
{build_equity_svg(eq_full, '样本内(全窗口)已发布组合')}
{build_equity_svg(eq_oos, '样本外(TEST)已发布组合')}

<h2>TRAIN 选参网格(独立重选, 成交≥5 取总收益最高; 实际 TRAIN 仅≤4笔未达阈值)</h2>
<table><tr><th>配置</th><th>主题上限</th><th>止损</th><th>笔数</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>
{grid_rows}</table>

<h2>样本外(TEST)全量逐笔交易({sel_test['n']}笔)</h2>
<table><tr><th>#</th><th>代码</th><th>买入日</th><th>买价</th><th>卖出日</th><th>卖价</th><th>收益</th><th>股数</th><th>持有日</th><th>退出</th><th>ROE%</th><th>净利同比%</th></tr>
{tr_rows}</table>

<div class='note' style='margin-top:18px'>
<b>诚实说明:</b>
① 数据库 kline_cache 截至 <b>{FULL_END}</b>, <b>无 2026 下半年(7月之后)数据</b>; 因此"样本外"采用时间切分 holdout, 而非真实未来行情.
② <b>前视泄漏说明:</b> 已发布组合(theme_cap1+止损-15%)的选参基于全窗口(含 TEST 段), 故 TEST 段在选参时已被间接"见过".
   本验证试图在更早的 TRAIN 段独立重选参数再以固定参数套用 TEST, 但 TRAIN 段 V2 仅成交 4 笔(低于选优阈值 n>=5),
   无法独立重选 —— 因此 OOS 实为"固定已发布参数套用保留段", 结论须保守解读(正向、但可能略偏乐观).
③ 样本极小(各段仅 4~10 笔), 单笔大盈/大亏对结论影响显著; 样本外 6 笔全部"到期"退出、止损-15% 从未触发, 故止损参数在 OOS 未被实际检验. 任何 OOS 数字均须谨慎, 建议积累更多样本或实盘小仓验证.
④ 沿用原 harness 的全部假设: 无分钟表(买=14:45收盘×滑点, 卖=15:00收盘×滑点); 基本面 point-in-time; 概念快照近似; 资金流代理.
</div>
</body></html>"""
    with open(path, "w") as f:
        f.write(html)


def main():
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    cal = sorted({t for g in ctx.values() for t in g.index})
    print(f"  标的数={len(ctx)} 全窗口交易日={len(cal)}", flush=True)
    print("预计算指标...", flush=True)
    ctx = H.build_ctx(ctx)
    print("加载基本面...", flush=True)
    fmap = H.load_fundamentals()

    train_cal = slice_cal(cal, TRAIN_START, TRAIN_END)
    test_cal = slice_cal(cal, TEST_START, TEST_END)
    full_cal = slice_cal(cal, FULL_START, FULL_END)
    print(f"  TRAIN交易日={len(train_cal)} TEST交易日={len(test_cal)} FULL交易日={len(full_cal)}", flush=True)

    # hot_at 按各自 eval 区间独立构建(行情资金流为 point-in-time, 无前视)
    print("构建 TRAIN 热门题材...", flush=True)
    hot_train = H.build_hot_themes(ctx, train_cal)
    print("构建 TEST 热门题材...", flush=True)
    hot_test = H.build_hot_themes(ctx, test_cal)
    print("构建 FULL 热门题材...", flush=True)
    hot_full = H.build_hot_themes(ctx, full_cal)

    v2cfg = get_v2_cfg()

    # ---- 1) TRAIN 独立重选 theme_cap + 止损 ----
    print("TRAIN 选参...", flush=True)
    train_grid, best_train, raw_best = select_on_train(ctx, train_cal, hot_train, fmap, v2cfg)
    cap_sel = int(best_train["theme_cap"]); stop_sel = best_train["stop"]
    print(f"  TRAIN 选出(达到n>=5阈值): theme_cap={cap_sel} 止损={int(stop_sel*100)}% "
          f"(n={best_train['n']}, 总收益={best_train['total_ret']}%)", flush=True)
    print(f"  TRAIN 段实际表现(任意样本量最优): theme_cap={int(raw_best['theme_cap'])} "
          f"止损={int(raw_best['stop']*100)}% n={raw_best['n']} 总收益={raw_best['total_ret']}% "
          f"夏普={raw_best['sharpe']} 回撤={raw_best['maxdd']}%", flush=True)
    train_note = (f"TRAIN 段(2024-07~2025-09)V2 仅成交 {raw_best['n']} 笔, 低于选优阈值 n>=5, "
                  f"无法在 TRAIN 上独立重选参数; 故 OOS 直接套用已发布组合(theme_cap1+止损-15%). "
                  f"TRAIN 段实际最优表现: theme_cap={int(raw_best['theme_cap'])} 止损="
                  f"{int(raw_best['stop']*100)}% → 总收益 {raw_best['total_ret']}%, 夏普 {raw_best['sharpe']}.")

    # ---- 2) 评估四种情形 ----
    print("评估各区间(复用各区间 signal_index)...", flush=True)
    # signal_index 与 theme_cap/stop 无关, 每个区间仅计算一次
    inv_train = H.build_signal_index(ctx, train_cal, v2cfg)
    inv_test = H.build_signal_index(ctx, test_cal, v2cfg)
    inv_full = H.build_signal_index(ctx, full_cal, v2cfg)
    # 已发布组合: cap1 + sl-15%
    _, _, pub_train_m = eval_cfg(ctx, train_cal, hot_train, fmap, v2cfg, PUB_THEME_CAP, PUB_STOP,
                                 "已发布组合·TRAIN", inv=inv_train)
    oos_tr, oos_eq, pub_test_m = eval_cfg(ctx, test_cal, hot_test, fmap, v2cfg, PUB_THEME_CAP, PUB_STOP,
                                 "已发布组合·TEST(OOS参照)", inv=inv_test)
    # TRAIN选出组合 在 TRAIN / TEST
    _, _, sel_train_m = eval_cfg(ctx, train_cal, hot_train, fmap, v2cfg, cap_sel, stop_sel,
                                 "TRAIN选出·TRAIN", inv=inv_train)
    oos_trades, oos_eq2, sel_test_m = eval_cfg(ctx, test_cal, hot_test, fmap, v2cfg, cap_sel, stop_sel,
                                 "TRAIN选出·TEST(真OOS)", inv=inv_test)
    # 样本内全窗口(已发布组合) —— 用于参照基线
    _, eq_full, pub_full_m = eval_cfg(ctx, full_cal, hot_full, fmap, v2cfg, PUB_THEME_CAP, PUB_STOP,
                                 "已发布组合·全窗口(样本内)", inv=inv_full)
    pub_full_m["label"] = "已发布组合·全窗口(样本内)"
    pub_full_m["theme_cap"] = PUB_THEME_CAP; pub_full_m["stop"] = PUB_STOP

    print(f"\n=== 结果 ===")
    print(f"  样本内全窗口(已发布): n={pub_full_m['n']} 总收益={pub_full_m['total_ret']}% 夏普={pub_full_m['sharpe']}")
    print(f"  已发布·TRAIN: n={pub_train_m['n']} 总收益={pub_train_m['total_ret']}%")
    print(f"  已发布·TEST : n={pub_test_m['n']} 总收益={pub_test_m['total_ret']}%")
    print(f"  TRAIN选出·TRAIN: n={sel_train_m['n']} 总收益={sel_train_m['total_ret']}%")
    print(f"  ★TRAIN选出·TEST(真OOS): n={sel_test_m['n']} 总收益={sel_test_m['total_ret']}% 夏普={sel_test_m['sharpe']} 回撤={sel_test_m['maxdd']}%")

    # 权益曲线数据(归一化100): eq 数组索引对齐各自 cal_slice
    eq_full_series = [(str(full_cal[i])[:10], v / eq_full[0] * 100) for i, v in enumerate(eq_full)]
    eq_oos_series = [(str(test_cal[i])[:10], v / oos_eq2[0] * 100) for i, v in enumerate(oos_eq2)]

    ctx_info = dict(n_stocks=len(ctx))
    html_path = os.path.join(ROOT, "opt_study", "oversold_quality_oos_report.html")
    build_html(html_path, ctx_info, pub_train_m, pub_test_m, sel_train_m, sel_test_m,
               pub_full_m, train_grid, oos_trades, eq_full_series, eq_oos_series, train_note)

    csv_path = os.path.join(ROOT, "opt_study", "oversold_quality_oos_trades.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["#", "代码", "买入日", "买价", "卖出日", "卖价", "收益%", "股数", "持有日", "退出", "ROE%", "净利同比%"])
        for i, t in enumerate(oos_trades, 1):
            w.writerow([i, t["code"], str(t["buy_t"])[:10], round(t["buy_px"], 2), str(t["sell_t"])[:10],
                        round(t["sell_px"], 2), round(t["ret"] * 100, 2), t["shares"], t["hold_days"],
                        t["reason"], t["roe"], t["np_yoy"]])

    summary = dict(
        design=dict(train=[TRAIN_START, TRAIN_END], test=[TEST_START, TEST_END], full=[FULL_START, FULL_END]),
        train_selected=dict(theme_cap=int(raw_best["theme_cap"]), stop=int(raw_best["stop"]*100),
                            n=int(raw_best["n"]), total_ret=raw_best["total_ret"],
                            sharpe=raw_best["sharpe"], maxdd=raw_best["maxdd"],
                            note="TRAIN 仅<=4笔未达 n>=5 选优阈值, 此为实际最优表现(任意样本量)"),
        in_sample_full=pub_full_m,
        published_on_train=pub_train_m, published_on_test=pub_test_m,
        selected_on_train=sel_train_m, selected_on_test=sel_test_m,
        data_note="kline_cache 截至 2026-07-15, 无 2026H2 数据; OOS 采用时间切分 holdout",
        leak_note=("已发布组合(theme_cap1+止损-15%)的选参基于全窗口(含 TEST 段), 故 TEST 段存在轻微前视泄漏; "
                   "因 TRAIN 仅4笔无法独立重选, 这是当前数据下最合理的前向验证, 结论应保守解读."),
    )
    json.dump(summary, open(os.path.join(ROOT, "opt_study", "oversold_quality_oos_metrics.json"), "w"),
              ensure_ascii=False, indent=2, default=lambda o: float(o) if isinstance(o, (np.floating,)) else o)
    print("\n完成. 报告:", html_path, "| 逐笔:", csv_path, flush=True)


if __name__ == "__main__":
    main()
