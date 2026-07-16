# -*- coding: utf-8 -*-
"""
龙头策略 × "大盘MA60闸口判定时点" 对比回测
================================================================
只改变【闸口判定所依据的信息时点】, 龙头选股/买入价/持有期全部一致, 保证干净对比。

时点语义(严格区分, 避免前视偏差):
  baseline         : 始终在场(无限闸)
  open             : 开盘前判定 -> gate[T] = (T-1收盘 站上 MA60[T-1])         [无前视, 可隔夜定]
  close1445_proxy  : 14:45近似(日线无盘中指数, 用T收盘近似) -> gate[T]=(T收盘 站上 MA60[T])
                     = 等价于"收盘确认+收盘买入", 即原 harness_c_ma60 的口径
  close1445_strict : 严格无前视 -> gate[T] = (T收盘 站上 MA60[T-1])           [14:45实时指数 vs 已知MA60]
  persist2_open    : 开盘前版, 要求 T-1,T-2 连续2日收盘均站上MA60
  persist3_open    : 开盘前版, 要求 T-1..T-3 连续3日
  persist2_close   : 14:45版, 要求 T,T-1 连续2日
  persist3_close   : 14:45版, 要求 T..T-2 连续3日

重要: 龙头选股始终用信号日T自身数据(14:44选股/收盘买入); 闸口只做"是否动手"的总开关。
"""
from __future__ import annotations
import sys, json
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TESTS = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(TESTS))

import harness as H
from momentum import config as cfg
from harness_sector import build_sector_heat, slice_test_dates
from harness_compare3 import build_day_returns, topn_leaders
from harness_compare3_stop import build_price_lookup
from harness_c_ma60 import build_ma60_gate, simulate_with_log_gated, metrics_from_equity

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
LOT = 100
N = 3
HOLDS = [3, 5]

MODES = [
    "baseline", "open", "close1445_proxy", "close1445_strict",
    "persist2_open", "persist3_open", "persist2_close", "persist3_close",
]
MODE_LABEL = {
    "baseline": "始终在场(无限闸)",
    "open": "开盘前 (T-1收盘站上MA60[T-1])",
    "close1445_proxy": "14:45近似 (T收盘站上MA60[T])",
    "close1445_strict": "14:45严格无前视 (T收盘站上MA60[T-1])",
    "persist2_open": "持续2天·开盘前 (T-1,T-2均站上)",
    "persist3_open": "持续3天·开盘前 (T-1..T-3均站上)",
    "persist2_close": "持续2天·14:45 (T,T-1均站上)",
    "persist3_close": "持续3天·14:45 (T..T-2均站上)",
}


def build_cross(data_cache, calendar):
    """返回 mkt_nav_s, ma60_s, cross(dict ts->bool), valid(dict ts->bool)
    cross: 当日收盘是否站上MA60; 早期MA60未成型时默认 True(不强制空仓)。"""
    mkt_nav_s, _ = H.build_market_proxy(data_cache, calendar)
    ma60_s = mkt_nav_s.rolling(60).mean()
    cross, valid = {}, {}
    for t in calendar:
        ts = pd.Timestamp(t)
        nav = mkt_nav_s.get(ts, np.nan)
        m = ma60_s.get(ts, np.nan)
        ok = pd.notna(nav) and pd.notna(m)
        valid[ts] = ok
        cross[ts] = bool(nav > m) if ok else True
    return mkt_nav_s, ma60_s, cross, valid


def make_gate(mode, calendar, cross, valid, mkt_nav_s, ma60_s):
    cal = [pd.Timestamp(t) for t in calendar]
    idx = {t: i for i, t in enumerate(cal)}
    g = {}
    for t in cal:
        i = idx[t]
        if mode == "baseline":
            g[t] = True; continue
        if mode == "open":
            p = cal[i - 1] if i - 1 >= 0 else None
            g[t] = cross[p] if (p is not None and valid.get(p, True)) else True
        elif mode == "close1445_proxy":
            g[t] = cross[t]
        elif mode == "close1445_strict":
            p = cal[i - 1] if i - 1 >= 0 else None
            if p is not None and valid.get(p, True):
                nav = mkt_nav_s.get(t, np.nan); m = ma60_s.get(p, np.nan)
                g[t] = bool(nav > m) if (pd.notna(nav) and pd.notna(m)) else True
            else:
                g[t] = True
        elif mode.startswith("persist"):
            k = int(mode.split("persist")[1].split("_")[0])
            ref = mode.split("_")[-1]  # open / close
            ok = True
            for j in range(k):
                d = (cal[i - 1 - j] if ref == "open" else cal[i - j]) if (i - 1 - j >= 0 if ref == "open" else i - j >= 0) else None
                if d is None or not valid.get(d, True):
                    ok = True  # 早期MA60未成型 -> 不强制空仓
                elif not cross[d]:
                    ok = False; break
            g[t] = ok
    return g


def main():
    print("[1/5] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}  共{len(calendar)}交易日", flush=True)

    print("[2/5] 行业热度 + 龙头池 + 价格查找 + MA60 cross ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    mkt_nav_s, ma60_s, cross, valid = build_cross(data_cache, calendar)

    # 统计各时点 go/off 信号日
    out = {"meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                    "N": N, "init": INIT_CAPITAL, "top_k": TOP_K,
                    "规则": "信号日(每hold交易日)按不同信息时点判定大盘MA60闸口; "
                            "开盘前/持续-open版用T-1收盘(无前视), 14:45版用T收盘(近似), "
                            "龙头选股/买入/持有期完全一致"},
            "modes": MODE_LABEL, "hold": {}}

    print("[3/5] 逐变体回测 ...", flush=True)
    best = {}
    for hold in HOLDS:
        td = slice_test_dates(calendar, hold, 0)
        reb = td[::hold]
        n_sig = len(reb)
        out["hold"][str(hold)] = {"信号日总数": n_sig, "variants": {}}
        for mode in MODES:
            gate = make_gate(mode, calendar, cross, valid, mkt_nav_s, ma60_s)
            ops, eq, tr, sdt, sdo = simulate_with_log_gated(
                N, hold, 0.0, calendar, price_lookup, date_idx, date_list,
                hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, gate=gate)
            m = metrics_from_equity(eq)
            m["交易笔数"] = len(tr)
            m["信号日总数"] = sdt
            m["空仓信号日"] = sdo
            m["空仓占比%"] = round(sdo / sdt * 100, 1) if sdt else 0.0
            m["胜率%"] = round(sum(1 for x in tr if x > 0) / len(tr) * 100, 1) if tr else 0.0
            out["hold"][str(hold)]["variants"][mode] = m
            print(f"      hold={hold} {mode:16s} 收益{m['总收益%']:+.1f}% 胜率{m['胜率%']:.1f}% "
                  f"夏普{m['夏普']} 回撤{m['最大回撤%']:.1f}% 空仓{m['空仓占比%']}% 笔数{len(tr)}", flush=True)
            if hold == 3 and mode != "baseline":
                # 记录候选最优(先按收益, 次按胜率)
                key = (round(m["总收益%"], 2), round(m["胜率%"], 1))
                best[mode] = (key, m)
        # 选 hold=3 最优
        if hold == 3:
            ranked = sorted(best.items(), key=lambda kv: (kv[1][0][0], kv[1][0][1]), reverse=True)
            out["hold"]["3"]["best_mode"] = ranked[0][0]
            out["hold"]["3"]["best_label"] = MODE_LABEL[ranked[0][0]]
            # 同时给"最高胜率"口径
            by_win = sorted(best.items(), key=lambda kv: (kv[1][0][1], kv[1][0][0]), reverse=True)
            out["hold"]["3"]["best_win_mode"] = by_win[0][0]
            out["hold"]["3"]["best_win_label"] = MODE_LABEL[by_win[0][0]]
            print(f"      >>> hold=3 收益最优: {ranked[0][0]} | 胜率最优: {by_win[0][0]}", flush=True)

    # 最优变体 hold=3 的完整逐笔 CSV
    print("[4/5] 生成最优变体逐笔CSV ...", flush=True)
    bm = out["hold"]["3"]["best_mode"]
    gate = make_gate(bm, calendar, cross, valid, mkt_nav_s, ma60_s)
    td = slice_test_dates(calendar, 3, 0)
    reb = td[::3]
    ops, _, _, _, _ = simulate_with_log_gated(
        N, 3, 0.0, calendar, price_lookup, date_idx, date_list,
        hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, gate=gate)
    df = pd.DataFrame(ops)
    csv_path = HERE / f"c_tradelog_N3_hold3_{bm}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"      最优变体 {bm} 逐笔记录 -> {csv_path.name} ({len(df)}行)", flush=True)

    (HERE / "c_ma60_timing_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_ma60_timing_results.json")

    # [5/5] HTML 报告
    build_html(out, df, bm)


def build_html(out, df_best, best_mode):
    h3 = out["hold"]["3"]
    base = h3["variants"]["baseline"]
    rows = ""
    order = ["baseline", "open", "close1445_proxy", "close1445_strict",
             "persist2_open", "persist3_open", "persist2_close", "persist3_close"]
    for mode in order:
        m = h3["variants"][mode]
        hl = ' style="background:#e8f5e9;font-weight:600"' if mode == best_mode else ""
        rows += (f"<tr{hl}><td>{MODE_LABEL[mode]}</td><td>{m['总收益%']:+.2f}%</td>"
                 f"<td>{m['胜率%']:.1f}%</td><td>{m['年化%']:.1f}%</td><td>{m['夏普']}</td>"
                 f"<td>{m['最大回撤%']:.2f}%</td><td>{m['交易笔数']}</td>"
                 f"<td>{m['空仓占比%']}%</td></tr>")
    bm = h3["variants"][best_mode]
    bm_label = MODE_LABEL[best_mode]
    # 样例
    srows = ""
    for _, r in df_best.head(40).iterrows():
        cls = {"买入": "buy", "卖出": "sell", "空仓等待": "wait"}.get(r["动作"], "")
        pnl = "" if (pd.isna(r["实现盈亏元"]) or r["实现盈亏元"] == "") else f'{r["实现盈亏元"]:.0f}'
        pct = "" if (pd.isna(r["实现收益%"]) or r["实现收益%"] == "") else f'{r["实现收益%"]:.2f}%'
        srows += (f'<tr class="{cls}"><td>{r["序号"]}</td><td>{r["日期"]}</td><td class="act">{r["动作"]}</td>'
                  f'<td>{r["槽位"]}</td><td>{r["代码"]}</td><td>{r["行业"]}</td><td>{r["成交价"]}</td>'
                  f'<td>{r["股数"]}</td><td>{r["成交额"]}</td><td>{pct}</td><td>{pnl}</td>'
                  f'<td>{r["账户净值"]:,.0f}</td><td>{r["大盘MA60"]}</td><td>{r["备注"]}</td></tr>')
    HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>龙头策略·MA60闸口时点对比</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1240px;margin:0 auto}} h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:20px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}} th,td{{padding:6px 8px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}}
th{{background:#fafbfc;color:#555;font-weight:600}} td:nth-child(1),td:nth-child(2),td:nth-child(13),td:nth-child(14){{text-align:left}}
.act{{font-weight:700}} tr.buy .act{{color:#c0392b}} tr.sell .act{{color:#27ae60}} tr.wait .act{{color:#888}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{flex:1;min-width:120px;background:#f0f6ff;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:19px;color:#156}} .kpi span{{font-size:12px;color:#667}}
.note{{font-size:13.5px;line-height:1.78;color:#444}} .note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:14px}}
.warn{{background:#fff4e5;border-left:4px solid #e67e22;padding:10px 14px;border-radius:6px;font-size:13px;color:#8a4b00;line-height:1.7}}
</style></head><body><div class="wrap">
<h1>龙头策略 · "大盘站上MA60才做" 的判定时点对比</h1>
<div class="sub">初始资金 ¥{INIT_CAPITAL:,} · N=3 · 持3天 · 无止损 · 回测区间 {out['meta']['区间']} ·
仅改变【闸口判定时点】, 龙头选股/买入价/持有期完全一致</div>

<div class="card"><h2>一、三种时点的问题映射</h2>
<p class="note">
<b>问题1 开盘前判定</b> → 变体 <code>open</code>:信号日T开盘前只用T-1收盘, <code>gate[T]=T-1收盘站上MA60[T-1]</code>, 完全无前视, 可隔夜定计划。<br>
<b>问题2 14:45选股时判定</b> → 变体 <code>close1445_proxy</code>(日线无盘中指数, 用T收盘近似)与 <code>close1445_strict</code>(严格无前视: T收盘 vs MA60[T-1])。<br>
<b>问题3 持续2/3天在MA60上方</b> → 变体 <code>persist2/3_open</code>(开盘前版, 用T-1往前数)与 <code>persist2/3_close</code>(14:45版, 用T往前数)。
</p>
<div class="warn"><b>逻辑注意点(请检查):</b>
① 龙头选股始终用信号日<b>T自身</b>数据(14:44选股、收盘买入); 闸口只决定"本周期是否动手", 不改变买哪只。<br>
② <code>open</code>版用T-1状态定闸口, 但若T当日大跌收盘破MA60, 仍会在T收盘买入——它放行的是"昨天还好、今天可能变坏"的日子。<br>
③ <code>close1445_proxy</code>用T收盘定闸口, 等价于"收盘确认收盘买", 比open版多一丝前视(因为14:44时尚无T收盘)。<br>
④ 日线数据<b>没有盘中指数</b>, 真正的"14:45实时指数 vs MA60[T-1]"无法精确回测, proxy版是标准近似; strict版把MA60换成T-1已知值, 二者几乎相等(MA60变化极小)。<br>
⑤ <code>persist</code>版更严格(动手更少、等待更多), 每个动手日都处于更深的上升趋势中, 但交易笔数更少、可能少赚。
</div></div>

<div class="card"><h2>二、回测结果矩阵(持3天, 10万本金)</h2>
<table><tr><th>闸口变体</th><th>总收益</th><th>胜率</th><th>年化</th><th>夏普</th><th>最大回撤</th><th>交易笔数</th><th>空仓占比</th></tr>
{rows}</table>
<p class="note" style="margin-top:10px">基准(始终在场): 收益{base['总收益%']:+.2f}% / 胜率{base['胜率%']:.1f}% / 回撤{base['最大回撤%']:.2f}% / 空仓0%。
所有带闸口变体都牺牲少量收益换取更低回撤与"等"的权利。收益最优高亮行见上表。</p></div>

<div class="card"><h2>三、最优情况描述(供逻辑核对)</h2>
<p class="note"><span class="tag">收益最优: {bm_label}</span></p>
<p class="note"><b>判定逻辑:</b>每个信号日(每3个交易日)按"该变体定义的信息时点"检查大盘(等权全A净值代理)是否站上60日均线:
<ul style="margin:6px 0 0 18px;line-height:1.95">
<li><b>满足</b> → 正常买入当日热门行业龙头第1/2/3名, 收盘成交, 持有至第3个交易日收盘"到期"离场;</li>
<li><b>不满足</b> → 本信号周期<b>不新建仓、现金等待</b>(记录"空仓等待"); 已持有的老仓位仍自然到期离场, 不中途砍;</li>
<li>下一个信号日(再+3交易日)重新判定, 满足则再动手。</li>
</ul></p>
<p class="note"><b>该最优变体结果:</b>期末净值 ¥{bm['期末净值']:,.0f} / 总收益 {bm['总收益%']:+.2f}% / 胜率 {bm['胜率%']:.1f}% /
夏普 {bm['夏普']} / 最大回撤 {bm['最大回撤%']:.2f}% / 共 {bm['交易笔数']} 笔 / 空仓等待占 {bm['空仓占比%']}%( {bm['空仓信号日']}/{bm['信号日总数']} 个信号周期)。</p>
<p class="note">对比始终在场: 收益少约 {base['总收益%']-bm['总收益%']:.0f} 点, 但回撤从 {base['最大回撤%']:.1f}% 降到 {bm['最大回撤%']:.1f}%,
且有近 {bm['空仓占比%']}% 时间干脆空仓——机会不对就等。请对照上表确认该变体的"判定时点"描述是否符合你的直觉。</p>
<p class="note">若更看重<b>胜率</b>而非总收益, 胜率最高变体为 <b>{out['hold']['3']['best_win_label']}</b>
(收益 {h3['variants'][out['hold']['3']['best_win_mode']]['总收益%']:+.2f}% / 胜率 {h3['variants'][out['hold']['3']['best_win_mode']]['胜率%']:.1f}%)。</p>
</div>

<div class="card"><h2>四、最优变体完整逐笔记录(前40条样例 · 完整 {len(df_best)} 条见CSV)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th><th>行业</th>
<th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th><th>账户净值</th>
<th>大盘MA60</th><th>备注</th></tr></thead><tbody>{srows}</tbody></table>
<p class="note" style="margin-top:10px">完整记录见 <code>c_tradelog_N3_hold3_{best_mode}.csv</code>(按"动作=空仓等待"可筛出所有空仓周期)。</p>
</div></div></body></html>"""
    (HERE / "c_ma60_timing_report.html").write_text(HTML, encoding="utf-8")
    print("完成 -> c_ma60_timing_report.html")


if __name__ == "__main__":
    main()
