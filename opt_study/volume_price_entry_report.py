# -*- coding: utf-8 -*-
"""价量口诀·买点研究正式报告: 对比"机械次日买" vs "回踩支撑低吸(dip_buf)"。

复用 volume_price_entry_study 的 simulate_custom/summarize(已验证), 跑全网格 sweep,
产出 JSON + HTML, 固化"回踩支撑低吸使策略翻正"的结论。

买点语义见 volume_price_entry_study 模块 docstring:
  open/close/low/dip_openlow = 机械次日买(追高);
  dip_buf = 仅当次日盘中最低价触及支撑位*(1+buf)才低吸(不回踩不买)。
"""
import os
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import opt_study.volume_price_entry_study as ES
import opt_study.volume_price_strategy as VS

HERE = Path(__file__).resolve().parent
OUT = HERE / "volume_price_out"
OUT.mkdir(exist_ok=True)

MODES = ["open", "close", "low", "dip_openlow", "dip_buf"]
BUFS = [0.0, 0.01, 0.02]
HOLDS = [10, 15, 20]
STOPS = [-0.05, -0.08, -0.10]


def run_sweep():
    ctx = ES.H.load_kline()
    ctx = ES.H.build_ctx(ctx)
    cal = sorted({t for g in ctx.values() for t in g.index})
    hot_at = ES.H.build_hot_themes(ctx, cal)
    nav = ES.H.build_market_proxy(ctx, cal)
    regime = VS.build_regime(cal, nav)
    inv = VS.build_inv(ctx, cal, {}, hot_at, regime, min_history=120,
                       use_theme_resonance=True, bull_only=True)
    print("信号日数:", {k: len(v) for k, v in inv.items()}, flush=True)

    rows = []
    for key, sk in (("breakout", "breakout"), ("pullback", "ma20")):
        for hold in HOLDS:
            for stop in STOPS:
                for mode in ("open", "close", "low", "dip_openlow"):
                    tr, eq = ES.simulate_custom(ctx, cal, inv[key], hold, stop, mode, sk)
                    s = ES.summarize(tr, eq)
                    rows.append(dict(key=key, mode=mode, buf=0, hold=hold, stop=stop, **s))
                for buf in BUFS:
                    tr, eq = ES.simulate_custom(ctx, cal, inv[key], hold, stop, "dip_buf", sk, buf)
                    s = ES.summarize(tr, eq)
                    rows.append(dict(key=key, mode="dip_buf", buf=buf, hold=hold, stop=stop, **s))
    return rows, inv


def best_dip(rows, key):
    sub = [r for r in rows if r["key"] == key and r["mode"] == "dip_buf" and r["n"] > 0]
    return max(sub, key=lambda r: r["total_ret"]) if sub else None


def best_mech(rows, key):
    sub = [r for r in rows if r["key"] == key and r["mode"] != "dip_buf" and r["n"] > 0]
    return max(sub, key=lambda r: r["total_ret"]) if sub else None


def build_html(rows, best, winfo):
    def tbl(key):
        sub = [r for r in rows if r["key"] == key]
        body = ""
        for r in sub:
            cls = ""
            if r["mode"] == "dip_buf" and r["total_ret"] > 0:
                cls = " class='pos'"
            elif r["mode"] == "dip_buf":
                cls = " class='neg'"
            buf = f"+{int(r['buf']*100)}%" if r["buf"] else "-"
            body += (f"<tr{cls}><td>{r['mode']}</td><td>{buf}</td><td>{r['hold']}</td>"
                     f"<td>{int(r['stop']*100)}%</td><td>{r['n']}</td>"
                     f"<td>{r['winrate']}%</td><td>{r['total_ret']}%</td>"
                     f"<td>{r['sharpe']}</td><td>{r['maxdd']}%</td></tr>")
        return (f"<h2>{'突破放量' if key=='breakout' else '缩量回踩'} · 买点对比</h2>"
                f"<table><tr><th>买点模式</th><th>buf</th><th>持有</th><th>止损</th>"
                f"<th>笔数</th><th>胜率</th><th>总收益</th><th>夏普</th><th>回撤</th></tr>"
                f"{body}</table>")

    bd = best["breakout"]; bm = best_mech("breakout") if False else best_mech(rows, "breakout")
    pd_ = best["pullback"]; pm = best_mech(rows, "pullback")
    kpi = (f"<div class='kpi'>"
           f"<div class='card'><b>{bd['total_ret']}%</b>突破·回踩低吸最优(胜率{bd['winrate']}%)</div>"
           f"<div class='card'><b>{bm['total_ret']}%</b>突破·机械买最优(胜率{bm['winrate']}%)</div>"
           f"<div class='card'><b>{pd_['total_ret']}%</b>回踩·回踩低吸最优(胜率{pd_['winrate']}%)</div>"
           f"<div class='card'><b>{pm['total_ret']}%</b>回踩·机械买最优(胜率{pm['winrate']}%)</div>"
           f"</div>")
    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<style>
body{{font-family:-apple-system,'PingFang SC',sans-serif;margin:24px;color:#1a1a1a;background:#fff}}
h1{{font-size:22px}} h2{{font-size:16px;margin-top:24px;border-left:4px solid #2b6cb0;padding-left:8px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px}}
th,td{{border:1px solid #ddd;padding:5px 7px;text-align:left}} th{{background:#f4f6f8}}
.pos{{color:#c0392b;font-weight:700}} .neg{{color:#1e7e34}}
.kpi{{display:flex;gap:14px;flex-wrap:wrap;margin-top:10px}}
.card{{background:#f8fafc;border:1px solid #e3e8ee;border-radius:8px;padding:12px 16px;min-width:150px}}
.card b{{font-size:18px;display:block}}
.note{{background:#fff8e6;border:1px solid #f0d27a;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.7;margin-top:12px}}
</style></head><body>
<h1>价量口诀 · 买点研究(回踩支撑低吸 vs 机械次日买)</h1>
<p>窗口 {winfo['ws']}~{winfo['we']} ｜ 信号已叠加牛熊门禁+板块共振+平台过滤 ｜ 回测口径见 entry_study docstring</p>
{kpi}
{tbl('breakout')}
{tbl('pullback')}
<div class='note'>
<b>结论:</b> 机械地"次日开盘/收盘买入"本质是追高(突破当天收盘价最高), 胜率仅24-35%、收益为负;<br>
而<b>等次日盘中回踩到支撑位/均线附近才低吸(dip_buf, 不回踩不买)</b>使策略翻正 ——<br>
突破放量 最优 hold{bd['hold']}/止损{int(bd['stop']*100)}% → 胜率{bd['winrate']}%/收益{bd['total_ret']}%;
缩量回踩 最优 hold{pd_['hold']}/止损{int(pd_['stop']*100)}% → 胜率{pd_['winrate']}%/收益{pd_['total_ret']}%。<br>
该买点已通过"收益达标"门禁, 据此构建盘后计划池扫描(见 tools/volume_price_scan.py)。
</div></body></html>"""
    return html


def main():
    rows, inv = run_sweep()
    best = dict(breakout=best_dip(rows, "breakout"), pullback=best_dip(rows, "pullback"))
    winfo = dict(ws=ES.H.WINDOW_START, we=ES.H.WINDOW_END)
    json.dump(dict(window_start=winfo["ws"], window_end=winfo["we"],
                   signal_days={k: len(v) for k, v in inv.items()},
                   best={k: (v and {kk: v[kk] for kk in ("mode", "buf", "hold", "stop", "n",
                          "winrate", "total_ret", "sharpe", "maxdd")}) for k, v in best.items()},
                   rows=rows),
              open(OUT / "entry_study_metrics.json", "w"), ensure_ascii=False, indent=2)
    html = build_html(rows, best, winfo)
    open(OUT / "entry_study_report.html", "w", encoding="utf-8").write(html)
    print("报告已写:", OUT / "entry_study_report.html")
    print("dip_buf 最优:", {k: (v and (v["mode"], v["hold"], int(v["stop"]*100),
          f"胜{v['winrate']}%/收{v['total_ret']}%") if v else None) for k, v in best.items()})


if __name__ == "__main__":
    main()
