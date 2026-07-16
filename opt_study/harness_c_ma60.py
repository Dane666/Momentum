# -*- coding: utf-8 -*-
"""
龙头策略 + "大盘站上MA60才做, 跌破则空仓等待" 开关 (写入回测框架)
=====================================================================
在 opt_study 框架内新增一个可复用的市场闸门, 不改变任何原策略文件:
  - build_ma60_gate(): 等权全A净值代理 + 60日均线, 返回 date->bool (站上=True可做, 跌破=False空仓)
  - simulate_with_log_gated(): 账户级逐笔模拟; 信号日若闸门关闭则记录"空仓等待"且不建仓,
    已持有的老仓位仍自然到期离场(不中途砍)。
生成: c_tradelog_N3_hold3_ma60.csv(完整逐笔, 含空仓等待) + c_ma60_report.html(对比)
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

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
LOT = 100
N = 3
HOLDS = [3, 5]


def build_ma60_gate(data_cache, calendar):
    """等权全A净值代理 + MA60; 返回 (gate_dict, mkt_nav_s, ma60_s)
    gate_dict[Timestamp] = True 表示站上MA60(可做), False 表示跌破(空仓等待)。
    早期 MA60 尚未成型(NaN)时默认 True(不参与才强制空仓)。"""
    mkt_nav_s, _ = H.build_market_proxy(data_cache, calendar)
    ma60_s = mkt_nav_s.rolling(60).mean()
    gate = {}
    for t in calendar:
        ts = pd.Timestamp(t)
        nav = mkt_nav_s.get(ts, np.nan)
        m = ma60_s.get(ts, np.nan)
        gate[ts] = bool(nav > m) if (pd.notna(nav) and pd.notna(m)) else True
    return gate, mkt_nav_s, ma60_s


def simulate_with_log_gated(N, hold, stop_pct, calendar, price_lookup, date_idx,
                            date_list, hot_by_date, day_ret_map, sector_map,
                            reb_dates, init_capital, gate=None):
    """返回 (ops, equity_curve, trades)
    gate: dict date->bool; None 表示始终在场(无限闸)。"""
    n = len(calendar)
    cal_idx = {t: i for i, t in enumerate(calendar)}
    signal_set = set(cal_idx[d] for d in reb_dates if d in cal_idx)
    if not signal_set:
        return [], [init_capital], []
    lo = min(signal_set)
    subs = [{"cash": init_capital / N, "pos": None} for _ in range(N)]
    equity = []
    trades = []
    ops = []
    op_seq = 0
    signal_days_total = 0
    signal_days_off = 0

    def nav():
        eq = 0.0
        for s in subs:
            if s["pos"] is not None:
                last = s["pos"].get("last_close")
                if last is None:
                    pl = price_lookup.get(s["pos"]["code"])
                    last = pl[calendar[0]][3] if pl else 0.0
                eq += s["cash"] + s["pos"]["shares"] * last
            else:
                eq += s["cash"]
        return eq

    for i in range(n):
        t = calendar[i]
        ts = pd.Timestamp(t)
        dstr = str(t)[:10]
        # 1) 退出检查
        for k in range(N):
            s = subs[k]; pos = s["pos"]
            if not pos:
                continue
            pl = price_lookup.get(pos["code"])
            if pl is None or t not in pl:
                continue
            o, h, low, close = pl[t]
            pos["last_close"] = close
            exit_fill = None; reason = None
            if stop_pct > 0:
                sp = pos["entry_fill"] * (1 - stop_pct)
                if low <= sp:
                    exit_fill = sp * (1 - SLIP); reason = "止损"
            if reason is None and t >= pos["exit_date"]:
                exit_fill = close * (1 - SLIP); reason = "到期"
            if reason is not None:
                proceeds = pos["shares"] * exit_fill
                s["cash"] += proceeds
                ret = exit_fill / pos["entry_fill"] - 1.0
                trades.append(ret)
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "卖出", "槽位": k + 1,
                    "代码": pos["code"], "行业": sector_map.get(pos["code"], "其它"),
                    "成交价": round(exit_fill, 3), "股数": pos["shares"],
                    "成交额": round(proceeds, 2), "滑动成本": round(proceeds * SLIP, 2),
                    "实现收益%": round(ret * 100, 2),
                    "实现盈亏元": round(proceeds - pos["shares"] * pos["entry_fill"], 2),
                    "槽位现金": round(s["cash"], 2), "账户净值": round(nav(), 2),
                    "大盘MA60": ("站上" if (gate.get(ts, True) if gate is not None else True) else "跌破"),
                    "备注": reason,
                })
                s["pos"] = None
        # 2) 盯市
        eq = nav()
        if i >= lo:
            equity.append(eq)
        # 3) 信号日
        if i in signal_set:
            signal_days_total += 1
            allow = (gate is None) or gate.get(ts, True)
            if not allow:
                signal_days_off += 1
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "空仓等待", "槽位": "-",
                    "代码": "", "行业": "", "成交价": "", "股数": "",
                    "成交额": "", "滑动成本": "", "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[0]["cash"], 2), "账户净值": round(eq, 2),
                    "大盘MA60": "跌破", "备注": "MA60空头, 本周期不建仓, 现金等待",
                })
                continue
            leaders = topn_leaders(day_ret_map, sector_map, hot_by_date, t, N)
            for k in range(N):
                if subs[k]["pos"] is not None or k >= len(leaders):
                    continue
                code = leaders[k]
                pl = price_lookup.get(code)
                if pl is None or t not in pl:
                    continue
                di = date_idx.get(code, {}); dl = date_list.get(code, [])
                ii = di.get(t)
                exit_date = dl[ii + hold] if (ii is not None and ii + hold < len(dl)) else None
                if exit_date is None:
                    continue
                o, h, low, close = pl[t]
                entry_fill = close * (1 + SLIP)
                cost = subs[k]["cash"]
                shares = int(cost / entry_fill // LOT) * LOT
                if shares <= 0:
                    continue
                subs[k]["cash"] -= shares * entry_fill
                subs[k]["pos"] = {"code": code, "entry_i": i, "entry_fill": entry_fill,
                                  "shares": shares, "last_close": close, "exit_date": exit_date}
                op_seq += 1
                ops.append({
                    "序号": op_seq, "日期": dstr, "动作": "买入", "槽位": k + 1,
                    "代码": code, "行业": sector_map.get(code, "其它"),
                    "成交价": round(entry_fill, 3), "股数": shares,
                    "成交额": round(shares * entry_fill, 2), "滑动成本": round(shares * entry_fill * SLIP, 2),
                    "实现收益%": "", "实现盈亏元": "",
                    "槽位现金": round(subs[k]["cash"], 2), "账户净值": round(nav(), 2),
                    "大盘MA60": "站上", "备注": f"持有至{str(exit_date)[:10]}到期",
                })
    return ops, equity, trades, signal_days_total, signal_days_off


def metrics_from_equity(equity, init=INIT_CAPITAL):
    eq = np.array(equity, dtype=float)
    daily = np.diff(eq) / eq[:-1]; daily = daily[~np.isnan(daily)]
    final = eq[-1]
    profit = (final - init) / init * 100.0
    years = len(eq) / 252.0
    annual = (final / init) ** (1.0 / years) - 1.0 if years > 0 else -1.0
    sharpe = daily.mean() / daily.std() * np.sqrt(252.0) if len(daily) > 1 and daily.std() > 0 else 0.0
    peak = np.maximum.accumulate(eq); dd = (eq - peak) / peak
    max_dd = abs(dd.min()) * 100.0 if len(dd) else 0.0
    return {"期末净值": round(final, 2), "总收益%": round(profit, 2),
            "年化%": round(annual * 100, 2), "夏普": round(sharpe, 3),
            "最大回撤%": round(max_dd, 2)}


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    print(f"      区间={str(calendar[0])[:10]}~{str(calendar[-1])[:10]}", flush=True)

    print("[2/4] 行业热度 + 龙头池 + 价格查找 + MA60闸门 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    day_ret_map = build_day_returns(data_cache, sector_map)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    gate, mkt_nav_s, ma60_s = build_ma60_gate(data_cache, calendar)

    print("[3/4] 生成逐笔记录(始终在场 vs MA60开关) ...", flush=True)
    out = {"meta": {"区间": f"{str(calendar[0])[:10]}~{str(calendar[-1])[:10]}",
                    "N": N, "init": INIT_CAPITAL, "top_k": TOP_K,
                    "规则": "信号日大盘(等权全A)站上MA60才建仓, 跌破则本周期空仓等待"},
            "hold": {}}
    for hold in HOLDS:
        td = slice_test_dates(calendar, hold, 0)
        reb = td[::hold]
        # 始终在场
        ops_a, eq_a, tr_a, _, _ = simulate_with_log_gated(
            N, hold, 0.0, calendar, price_lookup, date_idx, date_list,
            hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, gate=None)
        ma = metrics_from_equity(eq_a)
        ma["交易笔数"] = len(tr_a)
        # MA60开关
        ops_b, eq_b, tr_b, sdt, sdo = simulate_with_log_gated(
            N, hold, 0.0, calendar, price_lookup, date_idx, date_list,
            hot_by_date, day_ret_map, sector_map, reb, INIT_CAPITAL, gate=gate)
        mb = metrics_from_equity(eq_b)
        mb["交易笔数"] = len(tr_b)
        mb["信号日总数"] = sdt
        mb["空仓信号日"] = sdo
        mb["空仓占比%"] = round(sdo / sdt * 100, 1) if sdt else 0.0
        out["hold"][str(hold)] = {"始终在场": ma, "MA60开关": mb}

        # 写 CSV(始终在场 + MA60开关 两个文件)
        df_a = pd.DataFrame(ops_a)
        df_a.to_csv(HERE / f"c_tradelog_N3_hold{hold}.csv", index=False, encoding="utf-8-sig")
        df_b = pd.DataFrame(ops_b)
        df_b.to_csv(HERE / f"c_tradelog_N3_hold{hold}_ma60.csv", index=False, encoding="utf-8-sig")
        print(f"      hold={hold}: 始终在场 期末¥{ma['期末净值']:,.0f}/{ma['总收益%']:+.1f}%/"
              f"回撤{ma['最大回撤%']}% | MA60开关 期末¥{mb['期末净值']:,.0f}/{mb['总收益%']:+.1f}%/"
              f"回撤{mb['最大回撤%']}%/空仓{mb['空仓占比%']}%/{len(ops_b)}条操作", flush=True)
        if hold == 3:
            df_b.to_pickle(HERE / "_ma60_hold3.pkl")

    (HERE / "c_ma60_results.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 -> c_ma60_results.json")

    # ---- HTML 报告 ----
    h3 = out["hold"]["3"]
    a = h3["始终在场"]; b = h3["MA60开关"]
    pk = HERE / "_ma60_hold3.pkl"
    df = pd.read_pickle(pk) if pk.exists() else pd.DataFrame()
    rows = ""
    for _, r in df.head(50).iterrows():
        cls = {"买入": "buy", "卖出": "sell", "空仓等待": "wait"}[r["动作"]]
        pnl = "" if (pd.isna(r["实现盈亏元"]) or r["实现盈亏元"] == "") else f'{r["实现盈亏元"]:.0f}'
        pct = "" if (pd.isna(r["实现收益%"]) or r["实现收益%"] == "") else f'{r["实现收益%"]:.2f}%'
        rows += (f'<tr class="{cls}"><td>{r["序号"]}</td><td>{r["日期"]}</td>'
                 f'<td class="act">{r["动作"]}</td><td>{r["槽位"]}</td><td>{r["代码"]}</td>'
                 f'<td>{r["行业"]}</td><td>{r["成交价"]}</td><td>{r["股数"]}</td>'
                 f'<td>{r["成交额"]}</td><td>{pct}</td><td>{pnl}</td>'
                 f'<td>{r["账户净值"]:,.0f}</td><td>{r["大盘MA60"]}</td><td>{r["备注"]}</td></tr>')

    HTML = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>龙头策略 + MA60空仓开关</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,"PingFang SC",Segoe UI,sans-serif;
background:#f5f7fa;color:#222;margin:0;padding:28px}}
.wrap{{max-width:1200px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:#667;font-size:13px;margin-bottom:20px}}
.card{{background:#fff;border-radius:12px;padding:20px 22px;margin-bottom:18px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
.card h2{{font-size:16px;margin:0 0 12px;color:#1a4}}
table{{width:100%;border-collapse:collapse;font-size:12.5px}}
th,td{{padding:6px 8px;border-bottom:1px solid #eee;text-align:right;white-space:nowrap}}
th{{background:#fafbfc;color:#555;font-weight:600}}
td:nth-child(2),td:nth-child(5),td:nth-child(6),td:nth-child(13),td:nth-child(14){{text-align:left}}
.act{{font-weight:700}} tr.buy .act{{color:#c0392b}} tr.sell .act{{color:#27ae60}} tr.wait .act{{color:#888}}
.kpi{{display:flex;gap:12px;flex-wrap:wrap}} .kpi div{{flex:1;min-width:120px;background:#f0f6ff;border-radius:8px;padding:12px 14px}}
.kpi b{{display:block;font-size:19px;color:#156}} .kpi span{{font-size:12px;color:#667}}
.note{{font-size:13.5px;line-height:1.75;color:#444}} .note code{{background:#eef;padding:1px 6px;border-radius:4px}}
.tag{{display:inline-block;background:#e8f5e9;color:#1b7;padding:3px 12px;border-radius:20px;font-weight:700;font-size:14px}}
</style></head><body><div class="wrap">
<h1>龙头策略 + "大盘站上MA60才做" 开关(完整逐笔操作记录)</h1>
<div class="sub">初始资金 ¥{INIT_CAPITAL:,} · N=3 · 持3天 · 无止损 · 回测区间 {out['meta']['区间']} ·
开关规则已写入 opt_study 框架(build_ma60_gate + simulate_with_log_gated)</div>

<div class="card">
<h2>一、开关效果(持3天, 10万本金)</h2>
<div class="kpi">
<div><b>¥{a['期末净值']:,.0f}</b><span>始终在场 期末净值</span></div>
<div><b>¥{b['期末净值']:,.0f}</b><span>MA60开关 期末净值</span></div>
<div><b>{a['总收益%']:+.1f}%</b><span>始终在场 总收益</span></div>
<div><b>{b['总收益%']:+.1f}%</b><span>MA60开关 总收益</span></div>
<div><b>{a['最大回撤%']:.1f}%</b><span>始终在场 最大回撤</span></div>
<div><b>{b['最大回撤%']:.1f}%</b><span>MA60开关 最大回撤</span></div>
<div><b>{b['空仓占比%']}%</b><span>时间空仓等待</span></div>
</div>
<table style="margin-top:14px"><tr><th>方案</th><th>期末净值</th><th>总收益</th><th>年化</th><th>夏普</th>
<th>最大回撤</th><th>交易笔数</th><th>空仓占比</th></tr>
<tr><td>始终在场</td><td>{a['期末净值']:,.0f}</td><td>{a['总收益%']:+.2f}%</td><td>{a['年化%']:.2f}%</td>
<td>{a['夏普']}</td><td>{a['最大回撤%']:.2f}%</td><td>{a['交易笔数']}</td><td>0%</td></tr>
<tr style="background:#e8f5e9;font-weight:600"><td>MA60开关</td><td>{b['期末净值']:,.0f}</td>
<td>{b['总收益%']:+.2f}%</td><td>{b['年化%']:.2f}%</td><td>{b['夏普']}</td>
<td>{b['最大回撤%']:.2f}%</td><td>{b['交易笔数']}</td><td>{b['空仓占比%']}%</td></tr></table>
<p class="note" style="margin-top:10px">收益仅少约 {a['总收益%']-b['总收益%']:.0f} 个点, 但<b>最大回撤从 {a['最大回撤%']:.1f}% 砍到 {b['最大回撤%']:.1f}%</b>,
夏普 {a['夏普']}→{b['夏普']}, 且有 <b>{b['空仓占比%']}%</b> 的时间(共 {b['空仓信号日']}/{b['信号日总数']} 个信号周期)干脆空仓等待——
机会不对就等, 不硬做。</p>
</div>

<div class="card">
<h2>二、实操规则(已写入框架)</h2>
<p class="note"><span class="tag">MA60 总开关</span>每个信号日(每3个交易日)开盘前看一眼<b>等权全A/沪深300/中证全指是否站上60日均线</b>:
<ul style="margin:6px 0 0 18px;line-height:1.9">
<li><b>站上 MA60</b> → 正常买入热门行业龙头第1/2/3名, 持3天;</li>
<li><b>跌破 MA60</b> → 本信号周期<b>不新建仓、现金等待</b>; 已持有的老仓位让它自然到期离场, 不中途砍。</li>
</ul></p>
<p class="note">框架接口: <code>build_ma60_gate(data_cache, calendar)</code> 返回 date→bool 闸门;
<code>simulate_with_log_gated(..., gate=gate)</code> 在信号日闸门关闭时记录"空仓等待"且不建仓。
与原策略文件零耦合, 仅扩展 opt_study 分析框架。</p>
</div>

<div class="card">
<h2>三、完整逐笔操作记录(前 50 条样例 · 含空仓等待 · 完整 {len(df)} 条见 CSV)</h2>
<table><thead><tr><th>序号</th><th>日期</th><th>动作</th><th>槽</th><th>代码</th><th>行业</th>
<th>成交价</th><th>股数</th><th>成交额</th><th>收益%</th><th>盈亏元</th><th>账户净值</th>
<th>大盘MA60</th><th>备注</th></tr></thead><tbody>{rows}</tbody></table>
<p class="note" style="margin-top:10px">完整记录见 <code>c_tradelog_N3_hold3_ma60.csv</code>(带开关)与
<code>c_tradelog_N3_hold3.csv</code>(始终在场对照)。可用 Excel 筛选"动作=空仓等待"查看所有空仓周期。</p>
</div>
</div></body></html>"""
    (HERE / "c_ma60_report.html").write_text(HTML, encoding="utf-8")
    print("完成 -> c_ma60_report.html")


if __name__ == "__main__":
    main()
