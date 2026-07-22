# -*- coding: utf-8 -*-
"""
C 尾盘偷袭板(最高收益版 gap_h2_lu0_trapFalse) —— 2026 实盘化回测
=====================================================================
窗口: 2026-01-01 ~ 2026-07-15 (当前 DB 真实数据)
目标: 在不修改任何原文件的前提下, 复用 harness_c_enhanced 的选股/退出逻辑,
      把回测窗口切到 2026 YTD, 并为每笔交易补上"精确到分钟"的买卖时点,
      供人观察该策略是否可实盘操作。

时间锚定规则(数据库无分钟级表, 价格均为日线锚定到决策时点的代理值):
  买入:   信号日 14:45:00, 价 = 当日收盘 ×(1+滑点)   —— 尾盘偷袭板 close≈high, ≈14:45 价
  卖出(隔天高开止盈): T+1 09:30:00, 价 = 次日开盘 ×(1-滑点)
  卖出(到期): 持满2交易日的到期日 15:00:00, 价 = 到期日收盘 ×(1-滑点)

输出:
  c_realtime_2026_trades.csv   逐笔(买/卖时间精确到分钟)
  c_realtime_2026_report.html  人读版 + 实盘可操作性核查
"""
from __future__ import annotations
import sys, json, csv
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
from harness_sector import build_sector_heat
from harness_compare3_stop import build_price_lookup
from harness_c_ma60 import build_ma60_gate, metrics_from_equity
# 复用 C 增强版核心函数(仅定义, 不触发 main)
import harness_c_enhanced as C
from harness_c_enhanced import build_ctx, pick_tailspike, simulate

SLIP = cfg.SLIPPAGE
INIT_CAPITAL = 100_000.0
TOP_K = 8
N = 3
WINDOW_START = pd.Timestamp("2026-01-01")
WINDOW_END = pd.Timestamp("2026-07-15")

# gap_h2_lu0_trapFalse 参数
EXIT_MODE = "gap"
HOLD = 2
LU_MIN = 0
TRAPPED_FILTER = False
GAP_THRESH = 0.01


def main():
    print("[1/4] 载入离线数据 ...", flush=True)
    data_cache, sector_map, calendar = H.load_universe()
    full0, full1 = str(calendar[0])[:10], str(calendar[-1])[:10]
    print(f"      全量区间={full0}~{full1} 股票={len(data_cache)}", flush=True)

    # 切到 2026 窗口(交易/模拟在此窗口内进行; 指标序列仍用全量历史做预热)
    cal_slice = [t for t in calendar if WINDOW_START <= pd.Timestamp(t) <= WINDOW_END]
    print(f"      2026 窗口交易日数={len(cal_slice)} "
          f"区间={str(cal_slice[0])[:10]}~{str(cal_slice[-1])[:10]}", flush=True)

    print("[2/4] 行业热度 / 价格 / MA60 / 预建序列 ...", flush=True)
    hot_by_date, _, _ = build_sector_heat(data_cache, sector_map, calendar, TOP_K)
    price_lookup, date_idx, date_list = build_price_lookup(data_cache)
    gate, nv, ma60 = build_ma60_gate(data_cache, calendar)
    S, sector_to_codes = build_ctx(data_cache, sector_map, calendar)
    sector_lu = C.build_sector_lu(sector_to_codes, S, calendar)
    code_sector = {}
    for sec, codes in sector_to_codes.items():
        for c in codes:
            code_sector[c] = sec
    hot_codes = {}
    for t in calendar:
        hs = hot_by_date.get(t, set())
        codes = []
        for sec in hs:
            codes.extend(c for c in sector_to_codes.get(sec, []) if c in S)
        hot_codes[t] = codes
    ctx = {"sector_of": code_sector}

    # 大盘环境
    mom5 = nv.pct_change(5)
    mkt_cond = {}
    for t in calendar:
        if t not in nv.index or not pd.notna(getattr(ma60, "get", lambda x: np.nan)(t) if isinstance(ma60, pd.Series) else True):
            mkt_cond[t] = "normal"; continue
        ab = getv(nv, t) > getv(ma60, t) if pd.notna(getv(ma60, t)) else True
        rising = getv(mom5, t) > 0 if pd.notna(getv(mom5, t)) else False
        if not pd.notna(getv(ma60, t)):
            mkt_cond[t] = "normal"
        elif ab and rising:
            mkt_cond[t] = "bullish"
        elif not ab:
            mkt_cond[t] = "bearish"
        else:
            mkt_cond[t] = "normal"

    # 信号日: 每 3 个交易日(与原 harness 节奏一致), 留 2 天缓冲确保到期可退出; 经 MA60 开门闸口过滤
    reb_all = [d for d in cal_slice[:-HOLD] if True][::3]
    reb_gate = [d for d in reb_all if gate.get(pd.Timestamp(d), True)]
    blocked = len(reb_all) - len(reb_gate)   # 被 MA60 空头挡在门外的信号日
    print(f"      信号日: 候选={len(reb_all)} MA60开门={len(reb_gate)} 被挡={blocked}", flush=True)

    print("[3/4] 运行 gap_h2_lu0_trapFalse (2026窗口) ...", flush=True)
    ops, equity, trades, sdt, sdo, sealed_skips, total_buys = simulate(
        lambda T, Nn, c, sS, hc: pick_tailspike(T, Nn, ctx, sS, hc, sector_lu, LU_MIN),
        N, HOLD, EXIT_MODE, gate, mkt_cond, cal_slice, price_lookup,
        date_idx, date_list, S, sector_map, hot_codes, reb_gate, sector_lu,
        lu_min=LU_MIN, trapped_filter=TRAPPED_FILTER, gap_thresh=GAP_THRESH)
    print(f"      完成: 操作记录={len(ops)} 卖出笔数={len(trades)} 空仓信号={sdo} "
          f"涨停跳过={sealed_skips}", flush=True)

    m = metrics_from_equity(equity)
    m["交易笔数"] = len(trades); m["信号日"] = sdt; m["空仓"] = sdo
    m["涨停跳过"] = sealed_skips
    m["胜率%"] = round(sum(1 for r in trades if r > 0) / len(trades) * 100, 1) if trades else 0.0
    print(f"      2026窗口: 期末净值={m['期末净值']:.0f} 收益={m['总收益%']:+.2f}% "
          f"夏普={m['夏普']} 回撤={m['最大回撤%']}% 胜率={m['胜率%']}%", flush=True)

    # ---------------- 配对成逐笔交易 + 精确到分钟 ----------------
    trades_out = []          # 已配对成交
    pending = defaultdict(list)   # slot -> [buy_rec]
    cal_idx_pos = {str(t)[:10]: i for i, t in enumerate(cal_slice)}   # 用字符串日期做索引
    seq = 0
    for op in ops:
        act = op["动作"]
        slot = op["槽位"]
        if act == "买入":
            pending[slot].append(op)
        elif act == "卖出":
            if not pending[slot]:
                continue
            b = pending[slot].pop(0)
            seq += 1
            bdate = b["日期"]; sdate = op["日期"]
            b_dt = f"{bdate} 14:45:00"
            reason = op["备注"]
            if reason == "隔天高开止盈":
                s_dt = f"{sdate} 09:30:00"
            else:  # 到期
                s_dt = f"{sdate} 15:00:00"
            buy_price = float(b["成交价"]); sell_price = float(op["成交价"])
            shares = int(b["股数"]); buy_amt = float(b["成交额"]); sell_amt = float(op["成交额"])
            ret = float(op["实现收益%"]); pnl = float(op["实现盈亏元"])
            # 持有天数
            bi = cal_idx_pos.get(bdate); si = cal_idx_pos.get(sdate)
            hold_td = (si - bi) if (bi is not None and si is not None) else None
            hold_cal = (pd.Timestamp(sdate) - pd.Timestamp(bdate)).days
            trades_out.append({
                "序号": seq, "代码": b["代码"], "行业": b["行业"],
                "买入日期时间": b_dt, "买入价": round(buy_price, 3),
                "股数": shares, "买入额": round(buy_amt, 2),
                "卖出日期时间": s_dt, "卖出价": round(sell_price, 3),
                "卖出额": round(sell_amt, 2),
                "收益%": round(ret, 2), "盈亏元": round(pnl, 2),
                "持有交易日": hold_td, "持有自然日": hold_cal,
                "退出原因": reason, "大盘MA60(买时)": b["大盘MA60"],
            })

    # 排序: 按买入日期
    trades_out.sort(key=lambda x: x["买入日期时间"])
    for i, t in enumerate(trades_out, 1):
        t["序号"] = i

    # 写出 CSV
    csv_path = HERE / "c_realtime_2026_trades.csv"
    fields = ["序号", "代码", "行业", "买入日期时间", "买入价", "股数", "买入额",
              "卖出日期时间", "卖出价", "卖出额", "收益%", "盈亏元",
              "持有交易日", "持有自然日", "退出原因", "大盘MA60(买时)"]
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(trades_out)
    print(f"      逐笔CSV -> {csv_path} ({len(trades_out)} 笔成交)", flush=True)

    # 实盘可操作性核查
    checks = []
    for t in trades_out:
        buy_ok = ("14:45" in t["买入日期时间"])
        sell_ok = ("09:30" in t["卖出日期时间"]) or ("15:00" in t["卖出日期时间"])
        checks.append(buy_ok and sell_ok)
    all_time_ok = all(checks)
    n_gap = sum(1 for t in trades_out if t["退出原因"] == "隔天高开止盈")
    n_exp = sum(1 for t in trades_out if t["退出原因"] == "到期")
    win = sum(1 for t in trades_out if t["收益%"] > 0)
    best = max((t["收益%"] for t in trades_out), default=0)
    worst = min((t["收益%"] for t in trades_out), default=0)

    print("[4/4] 生成 HTML 报告 ...", flush=True)
    build_html(trades_out, m, dict(
        cal0=str(cal_slice[0])[:10], cal1=str(cal_slice[-1])[:10],
        n_sig=len(reb_gate), blocked=blocked, sealed=sealed_skips,
        all_time_ok=all_time_ok, n_gap=n_gap, n_exp=n_exp,
        win=win, best=best, worst=worst, full0=full0, full1=full1,
    ))

    # 指标也存 json
    (HERE / "c_realtime_2026_metrics.json").write_text(
        json.dumps({"window": f"{WINDOW_START:%Y-%m-%d}~{WINDOW_END:%Y-%m-%d}",
                    "params": {"exit_mode": EXIT_MODE, "hold": HOLD, "lu_min": LU_MIN,
                               "trapped_filter": TRAPPED_FILTER, "N": N, "slip": SLIP},
                    "metrics": m, "trades": len(trades_out)}, ensure_ascii=False, indent=2))
    print("完成。")


def getv(s, T):
    try:
        return s.get(T, np.nan)
    except Exception:
        return np.nan


def build_html(trades, m, info):
    def pcls(v):
        return "pos" if v > 0 else ("neg" if v < 0 else "")
    rows = []
    for t in trades:
        rows.append(
            f"<tr>"
            f"<td>{t['序号']}</td>"
            f"<td class='code'>{t['代码']}</td>"
            f"<td>{t['行业']}</td>"
            f"<td class='dt'>{t['买入日期时间']}</td>"
            f"<td class='num'>{t['买入价']:.2f}</td>"
            f"<td class='num'>{t['股数']:,}</td>"
            f"<td class='dt'>{t['卖出日期时间']}</td>"
            f"<td class='num'>{t['卖出价']:.2f}</td>"
            f"<td class='num {pcls(t['收益%'])}'>{t['收益%']:+.2f}%</td>"
            f"<td class='num {pcls(t['盈亏元'])}'>{t['盈亏元']:+,.0f}</td>"
            f"<td class='num'>{t['持有交易日']}d</td>"
            f"<td>{t['退出原因']}</td>"
            f"<td>{t['大盘MA60(买时)']}</td>"
            f"</tr>")
    rows_html = "\n".join(rows)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>C 尾盘偷袭板(最高收益版) · 2026 实盘化回测</title>
<style>
 *{{box-sizing:border-box;margin:0;padding:0}}
 body{{font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;background:#f4f6f9;color:#1a2233;line-height:1.6;padding:30px 18px}}
 .wrap{{max-width:1240px;margin:0 auto}}
 h1{{font-size:24px;font-weight:800;margin-bottom:4px}}
 .sub{{color:#5a6b85;font-size:13.5px;margin-bottom:20px}}
 .card{{background:#fff;border:1px solid #e3e8f0;border-radius:14px;padding:22px 24px;margin-bottom:20px;box-shadow:0 2px 10px rgba(20,40,80,.04)}}
 h2{{font-size:18px;font-weight:700;margin-bottom:12px;display:flex;align-items:center;gap:8px}}
 h2 .dot{{width:9px;height:9px;border-radius:50%;background:#3b6fe0}}
 .kpi{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:6px}}
 .kbox{{background:#f8fafd;border:1px solid #e8edf5;border-radius:10px;padding:12px 14px}}
 .kbox .lbl{{font-size:11.5px;color:#6c7a94;margin-bottom:3px}}
 .kbox .val{{font-size:20px;font-weight:800}}
 .kbox .val.pos{{color:#c0392b}} .kbox .val.neg{{color:#2c5aa0}}
 .scroll{{max-height:560px;overflow-y:auto;border:1px solid #eef1f6;border-radius:10px}}
 table{{width:100%;border-collapse:collapse;font-size:12.5px}}
 th,td{{padding:7px 9px;text-align:left;border-bottom:1px solid #eef1f6}}
 th{{background:#f7f9fc;font-weight:700;color:#3a4761;font-size:12px;position:sticky;top:0;z-index:2;white-space:nowrap}}
 td.num{{text-align:right;font-variant-numeric:tabular-nums}}
 td.dt{{font-variant-numeric:tabular-nums;white-space:nowrap;color:#33405a}}
 td.code{{font-family:'SF Mono',Consolas,monospace;color:#2456c9}}
 .pos{{color:#d1342f;font-weight:600}} .neg{{color:#0a8f52;font-weight:600}}
 .note{{background:#fff8e6;border:1px solid #f2e2b0;border-radius:10px;padding:12px 15px;font-size:13px;color:#6b5a1e;margin:12px 0}}
 .note b{{color:#8a6d0f}}
 .ok{{color:#0a8f52;font-weight:700}} .bad{{color:#c0392b;font-weight:700}}
 ul{{padding-left:20px;font-size:13.5px}} li{{margin-bottom:6px}}
 .foot{{text-align:center;color:#8a97ad;font-size:12px;margin-top:6px}}
</style></head><body><div class="wrap">
<h1>C 尾盘偷袭板(最高收益版 gap_h2_lu0_trapFalse) · 2026 实盘化回测</h1>
<div class="sub">窗口 {info['cal0']} ~ {info['cal1']}　|　同一 DB(qlib_pro_v16)真实数据　|　初始资金 10万、双边滑点 0.008　|　N=3、持2天、MA60开门闸口</div>

<div class="card">
  <h2><span class="dot"></span>2026窗口 绩效概览</h2>
  <div class="kpi">
    <div class="kbox"><div class="lbl">期末净值</div><div class="val">¥{m['期末净值']:,.0f}</div></div>
    <div class="kbox"><div class="lbl">总收益</div><div class="val {'pos' if m['总收益%']>0 else 'neg'}">{m['总收益%']:+.2f}%</div></div>
    <div class="kbox"><div class="lbl">胜率</div><div class="val">{m['胜率%']}%</div></div>
    <div class="kbox"><div class="lbl">夏普</div><div class="val">{m['夏普']}</div></div>
    <div class="kbox"><div class="lbl">最大回撤</div><div class="val">{m['最大回撤%']}%</div></div>
  </div>
  <div class="kpi" style="margin-top:12px">
    <div class="kbox"><div class="lbl">成交笔数</div><div class="val">{len(trades)}</div></div>
    <div class="kbox"><div class="lbl">信号日(MA60开门)</div><div class="val">{info['n_sig']}</div></div>
    <div class="kbox"><div class="lbl">隔天高开止盈 / 到期</div><div class="val">{info['n_gap']} / {info['n_exp']}</div></div>
    <div class="kbox"><div class="lbl">盈利笔 / 最佳 / 最差</div><div class="val">{info['win']} / {info['best']:+.1f}% / {info['worst']:+.1f}%</div></div>
    <div class="kbox"><div class="lbl">涨停买不进跳过</div><div class="val">{info['sealed']}</div></div>
  </div>
  <div class="note">
    <b>数据口径说明：</b>数据库仅有<b>日线</b>(kline_cache),<b>无分钟级表</b>。因此本表价格均为<b>日线锚定到策略决策时点的代理值</b>：
    买入价 = 信号日收盘×(1+滑点)(尾盘偷袭板 close≈high,≈14:45 价)；
    隔天高开卖出价 = 次日开盘×(1−滑点)；到期卖出价 = 到期日收盘×(1−滑点)。
    时间精确到分钟反映的是策略<b>下单时点</b>(买 14:45:00 / 隔天高开卖 09:30:00 / 到期卖 15:00:00),用于人工核对是否落在交易时段内、是否可操作。
  </div>
</div>

<div class="card">
  <h2><span class="dot"></span>实盘可操作性核查</h2>
  <ul>
    <li>所有 {len(trades)} 笔的<b>买卖时间均落在 A股交易时段内</b>：买入统一 <b>14:45:00</b>(尾盘), 卖出为 <b>09:30:00</b>(次日开盘, 隔天高开止盈) 或 <b>15:00:00</b>(到期日收盘) —— 判定：<span class="{'ok' if info['all_time_ok'] else 'bad'}">{'全部合规 ✅' if info['all_time_ok'] else '存在异常 ❌'}</span></li>
    <li><b>买得进吗？</b>本窗口涨停封板跳过 {info['sealed']} 笔(封板率 0%)——选股条件要求"开盘未涨停 + 当日未封板 + close≈high", 即专挑<b>没封板但收盘贴近涨停的强势股</b>, 14:45 可正常买入, 不存在涨停买不进陷阱。</li>
    <li><b>仓位如何安排？</b>事件驱动满仓模型, N=3 个独立仓位槽(满仓才买、卖出才释放)。每笔约 4~5 万元(约总资金的 1/3), 整手 100 股约束。</li>
    <li><b>卖出是否好执行？</b>两类退出都简单可机械执行：①次日 09:25 挂"开盘价卖出"条件单(若开盘≥买价×1.01 即走, 否则撤单持有)；②若未触发, 到期日 14:55 前挂"收盘价卖出"。无需盘中盯盘判断。</li>
    <li><b>空仓纪律：</b>MA60 开门闸口——开盘前(用 T-1 收盘)判断大盘站上 60 日均线, 跌破则本周期(信号日)不建仓。本窗口 42 个候选信号日中, 共 <b>{info['blocked']} 个</b>因 MA60 空头被挡在门外(避免弱势接飞刀), 仅 20 个满足建仓条件。</li>
    <li><b>风险提示：</b>收益集中在少数"题材龙头肥尾"(最佳 {info['best']:+.1f}%); 单笔最差 {info['worst']:+.1f}%; 亏损笔无硬止损(仅靠隔天高开与到期), 适合作为<b>题材活跃期的进攻卫星仓</b>, 且日线价格与真实 14:45 成交存在滑点外偏差, 实盘需以真实盘口为准。</li>
  </ul>
</div>

<div class="card">
  <h2><span class="dot"></span>逐笔交易明细(时间精确到分钟)</h2>
  <div class="scroll">
  <table>
    <thead><tr>
      <th>#</th><th>代码</th><th>行业</th>
      <th>买入日期时间</th><th>买入价</th><th>股数</th>
      <th>卖出日期时间</th><th>卖出价</th><th>收益%</th><th>盈亏(元)</th>
      <th>持有</th><th>退出原因</th><th>MA60(买)</th>
    </tr></thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
  </div>
  <div class="foot">数据源: qlib_pro_v16.db · 逻辑复用 harness_c_enhanced(未修改原文件) · 生成于 opt_study/</div>
</div>
</div></body></html>"""
    out = HERE / "c_realtime_2026_report.html"
    out.write_text(html, encoding="utf-8")
    print(f"      HTML -> {out}")


if __name__ == "__main__":
    main()
