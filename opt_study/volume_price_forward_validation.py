# -*- coding: utf-8 -*-
"""
价量口诀·回踩支撑策略 —— 压力位卖点 前向验证(Forward / OOS)
=============================================================
验证核心问题: "把卖点锁在压力位附近(前60日最高价×0.98, cap=20)" 这个改进,
是否在样本外(未见过的真实未来行情)同样有效? 还是只是样本内过拟合?

方法(严格零前视泄漏):
  1) 样本内(选参): [2024-07-01, IN_SAMPLE_END=2026-07-15]
     - 固定买点 dip_buf buf=0.02(已验证口径) + 止损-5%(已验证)
     - 仅扫"卖点"参数网格 sell_buf × cap, 选出收益最高的 (sell_buf*, cap*)
  2) 样本外(前瞻): (IN_SAMPLE_END, 数据末端]
     - 套用选出的 (sell_buf*, cap*) 跑压力位卖点, 得到 OOS 真·前瞻表现
     - 同时跑 OOS 固定持有基线(对比"卖点改进"是否在 OOS 仍贡献)
     - 并直接套用"已发布声称配置"(sell_buf=0.02, cap=20) 看声称值在 OOS 是否成立
  3) 每次成功运行追加到 oos_tracker_vp.json, 逐次扩充样本外样本.

注: 买点参数(buf=0.02)沿用已发表口径(视为先验常数, 不在本题 sell 研究中重选),
故本验证聚焦"卖点"是否泛化, 不引入买点选参泄漏. OOS 窗口较短(约数周),
样本笔数可能偏小, 结论标注 n 与置信提示.
"""
import os, sys, json, argparse, datetime as _dt
from collections import defaultdict

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, REPO_ROOT)

import volume_price_exit_study as ES   # 复用 simulate_custom / summarize / _pressure
import volume_price_strategy as VS

# 脱离本机: 覆盖底层 harness 窗口, 加载全部可用数据(含 IN_SAMPLE_END 之后)
H = ES.H
H.WINDOW_START = "2024-07-01"
H.WINDOW_END = "2099-12-31"
H.DB = os.environ.get("QLIB_DB") or os.path.join(REPO_ROOT, "qlib_pro_v16.db")

IN_SAMPLE_END = "2026-01-31"   # 样本内选参终点; OOS = 其后至数据末端(~130交易日, 有足够交易做前瞻)
KIND = "ma20"          # 缩量回踩(卖点研究聚焦对象)
KEY = "pullback"
BUY_BUF = 0.02         # 固定买点 dip_buf buf(已验证)
STOP = -0.05           # 固定止损(已验证)
BASE_HOLD = 20         # 固定持有基线天数

# 卖点网格(样本内选参)
SELL_BUFS = [0.0, 0.01, 0.02, 0.03]
CAPS = [20, 30, 45]

OUT_DIR = HERE
TRACKER = os.path.join(OUT_DIR, "oos_tracker_vp.json")


def slice_cal(cal, a, b):
    return [t for t in cal if a <= str(t)[:10] <= b]


def load_all():
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    ctx = H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    print(f"  标的数={len(ctx)} 全窗口交易日={len(cal)} 末端={str(cal[-1])[:10]}", flush=True)
    hot_at = H.build_hot_themes(ctx, cal)
    nav = H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    inv = VS.build_inv(ctx, cal, {}, hot_at, regime, min_history=120,
                       use_theme_resonance=True, bull_only=True)
    inv_counts = {k: len(v) for k, v in inv.items()}
    print(f"  信号日数(inv): {inv_counts}", flush=True)
    return ctx, cal, inv


def run_exit(ctx, cal_slice, inv, inv_key, exit_mode, sell_buf=0.0, cap=None):
    tr, eq, rc = ES.simulate_custom(ctx, cal_slice, inv[inv_key], BASE_HOLD, STOP,
                                    "dip_buf", KIND, buf=BUY_BUF,
                                    exit_mode=exit_mode, sell_buf=sell_buf, cap=cap)
    s = ES.summarize(tr, eq)
    s["reasons"] = rc
    return s, tr, eq


def load_tracker():
    if os.path.exists(TRACKER):
        try:
            return json.load(open(TRACKER))
        except Exception:
            return []
    return []


def add_tracker(tracker, entry):
    entry["run_at"] = _dt.datetime.now().isoformat(timespec="seconds")
    tracker[:] = [e for e in tracker if e.get("window_end") != entry["window_end"]]
    tracker.append(entry)
    json.dump(tracker, open(TRACKER, "w"), ensure_ascii=False, indent=2,
              default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)


def build_html(R, out_path):
    ins = R["in_sample"]
    oos_p = R["oos_pressure"]
    oos_b = R["oos_baseline"]
    oos_pub = R["oos_published"]

    def row(m, hl=False):
        bcls = "pos" if m["total_ret"] > 0 else "neg"
        h = " style='background:#eaf6ff'" if hl else ""
        return (f"<tr{h}><td>{m.get('label','')}</td><td>{m['n']}</td>"
                f"<td>{m['winrate']}%</td><td>{m['avg_ret']}%</td>"
                f"<td class='num {bcls}'>{m['total_ret']}%</td>"
                f"<td>{m['sharpe']}</td><td>{m['maxdd']}%</td></tr>")

    grid_rows = ""
    for g in ins["grid"]:
        grid_rows += (f"<tr><td>sell_buf={g['sell_buf']} cap={g['cap']}</td><td>{g['n']}</td>"
                      f"<td>{g['winrate']}%</td><td>{g['avg_ret']}%</td>"
                      f"<td class='num {'pos' if g['total_ret']>0 else 'neg'}'>{g['total_ret']}%</td>"
                      f"<td>{g['sharpe']}</td><td>{g['maxdd']}%</td></tr>")

    # verdict
    if oos_p["total_ret"] > oos_b["total_ret"] and oos_p["total_ret"] > 0:
        verdict = (f"✅ 样本外: 压力位卖点(+{oos_p['total_ret']}%) 优于 固定持有基线(+{oos_b['total_ret']}%), "
                   f"卖点改进在未见过的行情上仍有效(零泄漏).")
        vcls = "verdict"
    elif oos_p["total_ret"] > 0:
        verdict = (f"⚠️ 样本外压力位卖点仍为正(+{oos_p['total_ret']}%)但弱于固定持有(+{oos_b['total_ret']}%), "
                   f"改进的样本外增益不稳定(小样本, 谨慎).")
        vcls = "verdict warn"
    else:
        verdict = (f"⛔ 样本外压力位卖点转负({oos_p['total_ret']}%), 样本内改进可能是过拟合.")
        vcls = "verdict warn"

    tk = R.get("tracker", [])
    tk_rows = "".join(
        f"<tr><td>{e['run_at']}</td><td>{e['window_start']}~{e['window_end']}</td>"
        f"<td>{e['n']}</td><td class='num {'pos' if e['total_ret']>0 else 'neg'}'>{e['total_ret']}%</td>"
        f"<td>{e['winrate']}%</td><td>{e['sharpe']}</td></tr>" for e in tk)

    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:20px}} h2{{font-size:15px;margin-top:22px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}} th{{background:#f4f6f8}} .num{{text-align:right}}
.pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7e34;font-weight:600}}
.verdict{{background:#eafaf0;border:1px solid #9fd9b0;padding:10px 14px;border-radius:8px;font-size:14px;font-weight:600;line-height:1.7}}
.verdict.warn{{background:#fff4e5;border:1px solid #f0c36d}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.6}}
.meta{{font-size:12px;color:#666}}</style></head><body>
<h1>价量·回踩支撑策略 · 压力位卖点 前向验证</h1>
<p class='meta'>数据截至 <b>{R['data_end']}</b> ｜ 样本内终点 <b>{IN_SAMPLE_END}</b> ｜ 样本内窗口 {R['in_sample_window']} 交易日 ｜ 样本外窗口 {R['oos_window']} 交易日</p>
<div class='{vcls}'>{verdict}</div>

<h2>① 样本内选参(卖点网格, 固定买点 dip_buf buf={BUY_BUF} 止损{STOP})</h2>
<table><tr><th>配置</th><th>笔数</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>
{grid_rows}
</table>
<p class='meta'>选出最佳卖点: sell_buf={ins['best']['sell_buf']} cap={ins['best']['cap']} → 样本内收益 {ins['best']['total_ret']}%</p>

<h2>② 样本外(前瞻)核心对比</h2>
<table><tr><th>配置</th><th>笔数</th><th>胜率</th><th>平均每笔</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>
{row(oos_b, hl=False)}<tr><td colspan='7' style='background:#fafafa;color:#555;font-size:12px'>↑ 固定持有基线(卖点=持有{BASE_HOLD}日)</td></tr>
{row(oos_p, hl=True)}<tr><td colspan='7' style='background:#eaf6ff;color:#245;font-size:12px'>↑★ 压力位卖点(套用样本内选出 sell_buf={ins['best']['sell_buf']} cap={ins['best']['cap']}) · 零泄漏</td></tr>
{row(oos_pub, hl=False)}<tr><td colspan='7' style='background:#f0f4ff;color:#245;font-size:12px'>↑ 已发布声称配置(sell_buf=0.02 cap=20)直接套 OOS</td></tr>
</table>

<h2>③ 逐笔明细(样本外 压力位卖点)</h2>
<table><tr><th>代码</th><th>收益</th><th>退出原因</th></tr>
{''.join(f"<tr><td>{t['code']}</td><td class='num {'pos' if t['ret']>0 else 'neg'}'>{t['ret']*100:.2f}%</td><td>{t['reason']}</td></tr>" for t in R['oos_pressure_trades'])}
</table>

<h2>④ 累积样本跟踪(oos_tracker_vp.json)</h2>
<div class='note'>逐次 forward 运行累积(扩充样本外样本). 当前 {len(tk)} 次.</div>
<table><tr><th>运行时间</th><th>窗口</th><th>笔数</th><th>总收益</th><th>胜率</th><th>夏普</th></tr>{tk_rows}</table>

<div class='note' style='margin-top:16px'><b>诚实说明:</b>
① 卖点参数(sell_buf, cap)仅在样本内(≤{IN_SAMPLE_END})选出, 样本外严格零泄漏;
② 买点 buf={BUY_BUF} 沿用已发表口径(视为先验常数, 不在本题重选), 故本验证隔离"卖点"效应;
③ 样本外窗口仅 {R['oos_window']} 交易日, 成交可能仅数笔, 单笔影响大, 结论<b>方向性参考</b>;
④ 信号需"次日盘中回踩支撑"才建仓, 样本外信号数与建仓数取决于行情.
</div></body></html>"""
    open(out_path, "w").write(html)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-sample-end", default=IN_SAMPLE_END)
    args = ap.parse_args()
    in_end = args.in_sample_end

    ctx, cal, inv = load_all()
    full_end = str(cal[-1])[:10]
    in_cal = slice_cal(cal, H.WINDOW_START, in_end)
    oos_cal = [t for t in cal if str(t)[:10] > in_end]   # 严格晚于样本内终点, 零泄漏
    print(f"样本内交易日={len(in_cal)} 样本外交易日={len(oos_cal)} 数据末端={full_end}", flush=True)

    # ---- ① 样本内选参(卖点网格) ----
    print("样本内选参(卖点网格)...", flush=True)
    grid = []
    for sell_buf in SELL_BUFS:
        for cap in CAPS:
            s, _, _ = run_exit(ctx, in_cal, inv, KEY, "pressure", sell_buf=sell_buf, cap=cap)
            s["sell_buf"] = sell_buf; s["cap"] = cap
            s["label"] = f"压力位 sb={sell_buf} cap={cap}"
            grid.append(s)
            print(f"  sb={sell_buf} cap={cap}: n={s['n']} 收益={s['total_ret']}% 胜率={s['winrate']}% 夏普={s['sharpe']}", flush=True)
    best = max(grid, key=lambda g: g["total_ret"])
    print(f"  >>> 样本内最佳卖点: sell_buf={best['sell_buf']} cap={best['cap']} 收益={best['total_ret']}%", flush=True)

    # ---- ② 样本外 ----
    print("样本外验证...", flush=True)
    s_base, tr_base, _ = run_exit(ctx, oos_cal, inv, KEY, "hold")
    s_base["label"] = "固定持有基线(OOS)"
    s_p, tr_p, _ = run_exit(ctx, oos_cal, inv, KEY, "pressure", sell_buf=best["sell_buf"], cap=best["cap"])
    s_p["label"] = f"压力位卖点(OOS, sb={best['sell_buf']} cap={best['cap']})"
    s_pub, tr_pub, _ = run_exit(ctx, oos_cal, inv, KEY, "pressure", sell_buf=0.02, cap=20)
    s_pub["label"] = "已发布声称(OOS, sb=0.02 cap=20)"
    print(f"  OOS 基线: n={s_base['n']} 收益={s_base['total_ret']}%", flush=True)
    print(f"  OOS 压力位(选出): n={s_p['n']} 收益={s_p['total_ret']}% 分布={s_p['reasons']}", flush=True)
    print(f"  OOS 压力位(声称): n={s_pub['n']} 收益={s_pub['total_ret']}%", flush=True)

    R = dict(data_end=full_end, in_sample_end=in_end,
             in_sample_window=len(in_cal), oos_window=len(oos_cal),
             in_sample=dict(best=dict(sell_buf=best["sell_buf"], cap=best["cap"], total_ret=best["total_ret"]),
                            grid=grid),
             oos_baseline=s_base, oos_pressure=s_p, oos_published=s_pub,
             oos_pressure_trades=tr_p)

    # tracker
    tracker = load_tracker()
    add_tracker(tracker, dict(window_start=in_end, window_end=full_end,
                              n=s_p["n"], total_ret=s_p["total_ret"],
                              winrate=s_p["winrate"], sharpe=s_p["sharpe"], maxdd=s_p["maxdd"]))
    R["tracker"] = tracker

    build_html(R, os.path.join(OUT_DIR, "volume_price_forward_validation_report.html"))
    json.dump(R, open(os.path.join(OUT_DIR, "volume_price_forward_validation_metrics.json"), "w"),
              ensure_ascii=False, indent=2,
              default=lambda o: float(o) if isinstance(o, (np.floating, np.integer)) else o)
    print("完成. 报告: volume_price_forward_validation_report.html | 指标: ..._metrics.json", flush=True)


if __name__ == "__main__":
    main()
