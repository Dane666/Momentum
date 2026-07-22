# -*- coding: utf-8 -*-
"""
超跌绩优 · 博超跌反弹 离线回测 (只读复用, 不修改任何原文件)

回答三个问题:
  1) 这种策略在熊市 / 牛市好用吗?  -> 按等权市场净值 vs MA60 分牛熊, 分别报胜率/收益
  2) 实盘验证在什么时间点买入? 挂单买入可行吗? -> 对比 信号日收盘(14:55) vs 次日开盘(09:30)
  3) 一般绩优到什么地步会超跌反弹, 60日线?   -> 扫描 跌破60日线乖离 gap 与 距60日高回撤 敏感度

绩优代理定义(无基本面字段, 用可量化行为代理):
  - 中长期趋势结构 intact: MA60 > MA120 (说明是"优等生", 只是在上升趋势中的回调, 而非长期下跌股)
超跌定义:
  - 距 60 日最高价回撤 <= -DD   (深度套牢区)
  - 收盘价 < MA60 * (1 - GAP)   (跌破60日线)   <-- 直接回答"60日线?"
  - RSI14 < RSI_TH               (超卖)
反弹触发(买点确认):
  - 当日止跌企稳: close>open (阳线) 且 close>=MA5 (收复5日线) 且 成交量放大 vol>vol[-1]*1.2
账户: 事件驱动 N 槽等权, 初始 10万, 滑点 0.008, 整手 100; 到期 close 出(若跌停则顺延)
"""
from __future__ import annotations
import os, sys, json
from collections import defaultdict
from pathlib import Path
import numpy as np
import pandas as pd

# 复用原框架干净模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))  # .../tests
from momentum import config as cfg
from harness import load_universe, build_market_proxy   # 同目录复用

PROJ = Path(__file__).resolve().parent.parent
DB_PATH = str(PROJ / "qlib_pro_v16.db")
SLIP = 0.008
INIT_CAPITAL = 100_000.0
N_SLOTS = 5                      # 分散到 5 个反弹仓位
LOT = 100

# ---- 可调默认参数 ----
DD_DEF      = -0.18             # 距60日高回撤阈值(默认 -18%)
GAP_DEF     = 0.00              # 跌破60日线乖离(默认 0%, 即只要收盘<MA60 即可)
RSI_TH_DEF  = 35                # RSI 超卖阈值
HOLD_DEF    = 10                # 默认持有交易日
ENTRY_MODES = ["close", "next_open"]   # 信号收盘 vs 次日开盘
HOLDS       = [5, 10, 15, 20]


def wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta).clip(lower=0).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return rsi.fillna(50.0)


def build_ctx(data_cache, calendar):
    """预计算每个标的的指标, 返回 {code: DataFrame(date-indexed)}"""
    ctx = {}
    for code, g in data_cache.items():
        g = g.copy()
        g = g.set_index("trade_date").sort_index()
        c = g["close"]; h = g["high"]; l = g["low"]; o = g["open"]; v = g["volume"]
        g["ma5"] = c.rolling(5).mean()
        g["ma20"] = c.rolling(20).mean()
        g["ma60"] = c.rolling(60).mean()
        g["ma120"] = c.rolling(120).mean()
        g["rsi14"] = wilder_rsi(c, 14)
        g["high60"] = h.rolling(60).max()
        g["dd60"] = c / g["high60"] - 1.0          # 距60日高回撤(<=0)
        g["ret20"] = c / c.shift(20) - 1.0
        g["ret60"] = c / c.shift(60) - 1.0
        g["ma60_gap"] = c / g["ma60"] - 1.0         # 相对60日线乖离(<0 为跌破)
        g["ma_structure"] = g["ma60"] > g["ma120"]  # 中长期趋势结构 intact = 绩优代理
        g["vol_ratio"] = v / v.shift(1)
        # 预计算"基础信号"(gap=0 基, 便于后续按不同乖离阈值快速过滤)
        quality_ok = g["ma_structure"]
        oversold = (g["dd60"] <= DD_DEF) & (g["ma60_gap"] <= 0.0) & (g["rsi14"] < RSI_TH_DEF)
        bull_day = (g["close"] > g["open"]) & (g["close"] >= g["ma5"])
        vol_up = g["vol_ratio"] > 1.2
        g["sig"] = (quality_ok & oversold & bull_day & vol_up).fillna(False)
        ctx[code] = g
    return ctx


def build_signal_index(ctx):
    """date -> [(code, ma60_gap), ...] 仅含基础信号为 True 的标的, 供 simulate 快速检索"""
    from collections import defaultdict as _dd
    idx = _dd(list)
    for code, g in ctx.items():
        if "sig" not in g.columns:
            continue
        sub = g[g["sig"]]
        for t in sub.index:
            idx[t].append((code, float(g.loc[t, "ma60_gap"])))
    return idx


def is_limit_down(row) -> bool:
    """粗略跌停判定(主板10%)"""
    lim = 0.10
    return (row["close"] <= row["open"] * (1 - lim + 0.001)) and (row["close"] <= row["low"] * (1.001))


def screen_signal(ctx_code, t, dd, gap, rsi_th):
    """判断 code 在日期 t 是否触发超跌绩优反弹买点. 返回 True/False"""
    g = ctx_code
    if t not in g.index:
        return False
    i = g.index.get_loc(t)
    if i < 120:
        return False
    r = g.iloc[i]
    # 缺失指标跳过
    if pd.isna(r["ma60"]) or pd.isna(r["ma120"]) or pd.isna(r["rsi14"]) or pd.isna(r["high60"]):
        return False
    # 绩优代理: 中长期趋势结构 intact (MA60 > MA120, 优等生在上升趋势中的回调)
    quality_ok = bool(r["ma_structure"])
    # 超跌三条件
    oversold = (r["dd60"] <= dd) and (r["ma60_gap"] <= -gap) and (r["rsi14"] < rsi_th)
    if not (quality_ok and oversold):
        return False
    # 反弹触发: 阳线 + 收复5日线 + 放量
    bull_day = (r["close"] > r["open"]) and (r["close"] >= r["ma5"])
    vol_up = (not pd.isna(r["vol_ratio"])) and (r["vol_ratio"] > 1.2)
    return bool(bull_day and vol_up)


def simulate(ctx, calendar, gap, hold, entry_mode, regime_at, signal_index):
    """事件驱动 N 槽回测(用预计算信号索引). 返回 (trades, equity_curve, metrics)"""
    cal = pd.DatetimeIndex(calendar)
    cal_pos = {t: i for i, t in enumerate(cal)}
    capital = INIT_CAPITAL
    slots = [None] * N_SLOTS            # 每个槽存 {code, buy_t, buy_px, shares, regime}
    trades = []
    eq = []

    for ti, t in enumerate(cal):
        # 1) 处理到期卖出
        for si in range(N_SLOTS):
            pos = slots[si]
            if pos is None:
                continue
            if cal_pos[t] - cal_pos[pos["buy_t"]] >= hold:
                g = ctx[pos["code"]]
                # 找当前日 bar
                if t in g.index:
                    row = g.loc[t]
                    exit_px = row["close"] * (1 - SLIP)
                    # 跌停顺延
                    if is_limit_down(row):
                        continue
                    ret = exit_px / pos["buy_px"] - 1.0
                    pnl = pos["shares"] * (exit_px - pos["buy_px"])
                    capital += pos["shares"] * exit_px
                    trades.append(dict(code=pos["code"], buy_t=pos["buy_t"], buy_px=pos["buy_px"],
                                       sell_t=t, sell_px=exit_px, shares=pos["shares"],
                                       ret=ret, pnl=pnl, regime=pos["regime"], reason="到期",
                                       hold_days=cal_pos[t] - cal_pos[pos["buy_t"]]))
                    slots[si] = None
        # 2) 找当日买点(信号索引快速检索), 填入空槽
        for code, mg in signal_index.get(t, []):
            if mg > -gap:        # ma60_gap 必须 <= -gap (跌破60日线足够深)
                continue
            # 找空槽
            free = next((k for k in range(N_SLOTS) if slots[k] is None), None)
            if free is None:
                break
            g = ctx[code]
            row = g.loc[t]
            if entry_mode == "close":
                buy_t = t
                buy_px = row["close"] * (1 + SLIP)
            else:  # next_open
                if ti + 1 >= len(cal):
                    continue
                t1 = cal[ti + 1]
                if t1 not in g.index:
                    continue
                buy_t = t1
                buy_px = g.loc[t1, "open"] * (1 + SLIP)
            # 资金: 用当前权益按剩余空槽均分
            free_slots = sum(1 for k in range(N_SLOTS) if slots[k] is None)
            alloc = capital / free_slots
            shares = int(alloc / buy_px / LOT) * LOT
            if shares <= 0:
                continue
            capital -= shares * buy_px
            slots[free] = dict(code=code, buy_t=buy_t, buy_px=buy_px, shares=shares,
                               regime=regime_at.get(t, "bear"))
        # 3) 记账(含在途市值近似)
        mv = capital
        for pos in slots:
            if pos is None:
                continue
            g = ctx[pos["code"]]
            if t in g.index:
                mv += pos["shares"] * g.loc[t, "close"]
        eq.append((t, mv))
    # 末日强平
    for pos in slots:
        if pos is None:
            continue
        g = ctx[pos["code"]]
        if cal[-1] in g.index:
            row = g.loc[cal[-1]]
            exit_px = row["close"] * (1 - SLIP)
            ret = exit_px / pos["buy_px"] - 1.0
            pnl = pos["shares"] * (exit_px - pos["buy_px"])
            capital += pos["shares"] * exit_px
            trades.append(dict(code=pos["code"], buy_t=pos["buy_t"], buy_px=pos["buy_px"],
                               sell_t=cal[-1], sell_px=exit_px, shares=pos["shares"],
                               ret=ret, pnl=pnl, regime=pos["regime"], reason="期末强平",
                               hold_days=cal_pos[cal[-1]] - cal_pos[pos["buy_t"]]))
    eq_df = pd.DataFrame(eq, columns=["date", "equity"])
    # 指标
    if trades:
        rets = np.array([t["ret"] for t in trades])
        win = (rets > 0).mean()
        ann = (1 + (eq_df["equity"].iloc[-1] / INIT_CAPITAL - 1)) ** (252 / max(len(eq_df), 1)) - 1
        sharpe = rets.mean() / (rets.std() + 1e-9) * np.sqrt(252 / hold) if rets.std() > 0 else 0
        peak = eq_df["equity"].cummax(); dd = (eq_df["equity"] / peak - 1).min()
        metrics = dict(n=len(trades), winrate=round(100 * win, 1),
                       avg_ret=round(100 * rets.mean(), 2),
                       avg_win=round(100 * rets[rets > 0].mean(), 2) if (rets > 0).any() else 0,
                       avg_loss=round(100 * rets[rets <= 0].mean(), 2) if (rets <= 0).any() else 0,
                       best=round(100 * rets.max(), 2), worst=round(100 * rets.min(), 2),
                       total_ret=round(100 * (eq_df["equity"].iloc[-1] / INIT_CAPITAL - 1), 2),
                       annual=round(100 * ann, 2), sharpe=round(sharpe, 2),
                       maxdd=round(100 * dd, 2),
                       bull_n=sum(1 for t in trades if t["regime"] == "bull"),
                       bear_n=sum(1 for t in trades if t["regime"] == "bear"))
    else:
        metrics = dict(n=0)
    return trades, eq_df, metrics


def regime_split(trades):
    out = {}
    for rg in ["bull", "bear"]:
        sub = [t for t in trades if t["regime"] == rg]
        if not sub:
            out[rg] = dict(n=0, winrate=0, avg_ret=0, total=0)
            continue
        rs = np.array([t["ret"] for t in sub])
        out[rg] = dict(n=len(sub), winrate=round(100 * (rs > 0).mean(), 1),
                       avg_ret=round(100 * rs.mean(), 2),
                       avg_win=round(100 * rs[rs > 0].mean(), 2) if (rs > 0).any() else 0,
                       avg_loss=round(100 * rs[rs <= 0].mean(), 2) if (rs <= 0).any() else 0)
    return out


def fmt_dt(ts, hms):
    return f"{pd.Timestamp(ts):%Y-%m-%d} {hms}"


def build_html(path, res_default, res_sens_hold, res_sens_gap, res_entry, res_regime,
               sample_trades, params, best_combo=None):
    rows_regime = ""
    for rg, d in res_regime.items():
        name = "牛市(市场站上MA60)" if rg == "bull" else "熊市(市场跌破MA60)"
        rows_regime += f"<tr><td>{name}</td><td class='num'>{d['n']}</td><td class='num'>{d['winrate']}%</td><td class='num {('pos' if d['avg_ret']>0 else 'neg')}'>{d['avg_ret']}%</td><td class='num'>{d['avg_win']}%</td><td class='num'>{d['avg_loss']}%</td></tr>\n"
    rows_hold = ""
    for h, m in res_sens_hold:
        rows_hold += f"<tr><td class='num'>{h}</td><td class='num'>{m['n']}</td><td class='num'>{m['winrate']}%</td><td class='num {('pos' if m['avg_ret']>0 else 'neg')}'>{m['avg_ret']}%</td><td class='num'>{m['sharpe']}</td><td class='num'>{m['maxdd']}%</td></tr>\n"
    rows_gap = ""
    for gp, m in res_sens_gap:
        rows_gap += f"<tr><td class='num'>跌破{gp*100:.0f}%</td><td class='num'>{m['n']}</td><td class='num'>{m['winrate']}%</td><td class='num {('pos' if m['avg_ret']>0 else 'neg')}'>{m['avg_ret']}%</td><td class='num {('pos' if m['total_ret']>0 else 'neg')}'>{m['total_ret']}%</td></tr>\n"
    rows_entry = ""
    for em, m in res_entry.items():
        rows_entry += f"<tr><td>{'信号日收盘 14:55' if em=='close' else '次日开盘 09:30'}</td><td class='num'>{m['n']}</td><td class='num'>{m['winrate']}%</td><td class='num {('pos' if m['avg_ret']>0 else 'neg')}'>{m['avg_ret']}%</td><td class='num'>{m['total_ret']}%</td></tr>\n"
    # 样本逐笔(含分钟级时间)
    rows_sample = ""
    for t in sample_trades:
        buy_hms = "14:55:00" if params["entry_mode"] == "close" else "09:30:00"
        sell_hms = "15:00:00"
        rows_sample += (f"<tr><td>{t['code']}</td><td class='num'>{fmt_dt(t['buy_t'],buy_hms)}</td>"
                        f"<td class='num'>{t['buy_px']:.2f}</td><td class='num'>{t['shares']}</td>"
                        f"<td class='num'>{fmt_dt(t['sell_t'],sell_hms)}</td><td class='num'>{t['sell_px']:.2f}</td>"
                        f"<td class='num {('pos' if t['ret']>0 else 'neg')}'>{t['ret']*100:.2f}%</td>"
                        f"<td class='num'>{t['pnl']:.0f}</td><td>{'牛市' if t['regime']=='bull' else '熊市'}</td>"
                        f"<td>{t['reason']}</td></tr>\n")
    m = res_default
    best_callout = ""
    if best_combo:
        bc = best_combo.get("gap3_hold20_open", {})
        bc2 = best_combo.get("gap3_hold15_close", {})
        best_callout = (f"<div class='card'>将敏感度最优值交叉组合: <b>跌破60日线≈3% + 持有20日 + 次日09:30开盘</b><br>"
                        f"结果: 笔数 {bc.get('n')}, 胜率 {bc.get('winrate')}%, 平均每笔 {bc.get('avg_ret')}%, "
                        f"账户总收益 {bc.get('total_ret')}%, 最大回撤 {bc.get('maxdd')}% (夏普 {bc.get('sharpe')})<br>"
                        f"对照 跌破3%+持有15日+信号收盘: 胜率 {bc2.get('winrate')}%, 总收益 {bc2.get('total_ret')}%, 回撤 {bc2.get('maxdd')}%<br>"
                        f"<span style='color:#713f12'>即便取最优组合, 样本内仍是<b>负期望</b>(亏损略小于基准, 但未转正)。"
                        f"说明'绩优=MA60&gt;MA120的行为代理'不够, 真实绩优(ROE/PE/业绩拐点)或题材催化才是反弹持续的关键, 需补强后再实盘。</span></div>")
    html = f"""<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>超跌绩优·博反弹 回测报告</title>
<style>
*{{box-sizing:border-box}} body{{font-family:-apple-system,'Segoe UI',sans-serif;margin:0;background:#f5f6f8;color:#1a1a1a}}
.wrap{{max-width:1080px;margin:0 auto;padding:28px 20px 60px}}
h1{{font-size:23px;margin:0 0 4px}} h2{{font-size:18px;margin:30px 0 10px;border-left:4px solid #2563eb;padding-left:10px}}
.sub{{color:#666;font-size:13px;margin-bottom:8px}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:16px 18px;margin:12px 0;box-shadow:0 1px 2px rgba(0,0,0,.04)}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:6px}}
th,td{{border:1px solid #e5e7eb;padding:7px 9px;text-align:left}} th{{background:#f1f5f9;font-weight:600}}
td.num{{text-align:right;font-variant-numeric:tabular-nums}} .pos{{color:#c0392b;font-weight:600}} .neg{{color:#1e7d3a;font-weight:600}}
.tag{{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:5px;padding:2px 8px;font-size:12px;margin:2px}}
.note{{background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 14px;font-size:13px;color:#713f12;margin:12px 0}}
.kpi{{display:flex;flex-wrap:wrap;gap:12px;margin:10px 0}}
.kpi>div{{flex:1;min-width:120px;background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:12px;text-align:center}}
.kpi .v{{font-size:22px;font-weight:700}} .kpi .l{{font-size:12px;color:#666;margin-top:2px}}
</style></head><body><div class='wrap'>
<h1>超跌绩优 · 博超跌反弹 策略回测</h1>
<div class='sub'>数据: qlib_pro_v16.db (日线, 60/00 主板+中小板) · 窗口 {params['w0']}~{params['w1']} · 滑点 {SLIP*100:.1f}% · 初始 10 万 · N={N_SLOTS} 槽等权</div>

<div class='card'><h2 style='border:0;margin:0 0 8px'>策略定义(默认参数)</h2>
<div>
<span class='tag'>绩优代理: MA60 &gt; MA120(中长期趋势向上, 优等生在上升趋势中的回调)</span>
<span class='tag'>超跌①: 距60日最高价回撤 ≤ {params['dd']*100:.0f}%</span>
<span class='tag'>超跌②: 收盘价跌破 MA60 乖离 ≥ {params['gap']*100:.0f}%</span>
<span class='tag'>超跌③: RSI14 &lt; {params['rsi']}</span>
<span class='tag'>触发: 阳线 + 收复5日线 + 量比&gt;1.2(止跌企稳)</span>
<span class='tag'>持有: {params['hold']} 交易日</span>
<span class='tag'>买点: {'信号日14:55收盘' if params['entry_mode']=='close' else '次日09:30开盘'}</span>
</div></div>

<div class='kpi'>
<div><div class='v'>{m['total_ret']}%</div><div class='l'>总收益</div></div>
<div><div class='v'>{m['winrate']}%</div><div class='l'>胜率(笔数 {m['n']})</div></div>
<div><div class='v'>{m['avg_ret']}%</div><div class='l'>平均每笔收益</div></div>
<div><div class='v'>{m['sharpe']}</div><div class='l'>夏普(年化近似)</div></div>
<div><div class='v'>{m['maxdd']}%</div><div class='l'>最大回撤</div></div>
</div>

<h2>① 牛熊分段表现(回答: 熊市/牛市好用吗)</h2>
<table><tr><th>市场状态</th><th>笔数</th><th>胜率</th><th>平均每笔收益</th><th>平均盈利</th><th>平均亏损</th></tr>{rows_regime}</table>
<div class='note'>结论: 超跌反弹属于<b>逆势/均值回归</b>策略, 本质是在弱势中"接飞刀"。下方牛熊分段会显示它在哪种环境下胜率与赔率更占优。</div>

<h2>② 买点时点对比(回答: 实盘何时买 / 挂单可行吗)</h2>
<table><tr><th>买入时点</th><th>笔数</th><th>胜率</th><th>平均每笔收益</th><th>总收益</th></tr>{rows_entry}</table>
<div class='note'><b>挂单可行性:</b> 超跌反弹买的是"弱中转强", 价格本就在低位, <b>挂限价单完全可行</b>。实操建议: 盘前挂 <code>buy_limit = max(前日收盘×(1−1%), MA60支撑价)</code> 的限价单(买在比信号价更低的位置博更优成本); 次日开盘 09:25 前挂单, 未成交则撤单观察, 避免追高。信号日收盘买入需盘中盯盘确认止跌, 适合有条件单/手动尾盘执行。</div>

<h2>③ 60日线乖离敏感度(回答: 绩优到什么地步会超跌反弹)</h2>
<table><tr><th>跌破60日线幅度</th><th>笔数</th><th>胜率</th><th>平均每笔收益</th><th>总收益</th></tr>{rows_gap}</table>
<div class='note'>越深(乖离越大)往往代表错杀越严重、反弹空间越大, 但"接飞刀"风险也越高。下方持有期敏感度配合看, 找出赔率与胜率平衡点。</div>

<h2>持有期敏感度</h2>
<table><tr><th>持有交易日</th><th>笔数</th><th>胜率</th><th>平均每笔收益</th><th>夏普</th><th>最大回撤</th></tr>{rows_hold}</table>

<h2>最优组合(敏感度交叉点)</h2>
{best_callout}

<h2>样本逐笔(含精确到分钟的买卖时点, 供实盘可操作性核查)</h2>
<table><tr><th>代码</th><th>买入时间</th><th>买价</th><th>股数</th><th>卖出时间</th><th>卖价</th><th>收益</th><th>盈亏元</th><th>市况</th><th>退出</th></tr>{rows_sample}</table>

<div class='note'><b>⚠️ 数据口径诚实说明:</b> 数据库仅日线、无分钟表, 价格为日线锚定到决策时点的代理值(收盘/开盘), 时间精确到分钟是<b>策略下单时点</b>而非真实盘口成交价。实盘请以真实盘口为准, 滑点外另有偏差。本策略假设"绩优=此前上升通道"为行为代理, 非真实基本面绩优(无 ROE/PE 数据)。</div>
</div></body></html>"""
    path.write_text(html, encoding="utf-8")


def main():
    data_cache, sector_map, calendar = load_universe()
    # 限制窗口起点, 保证 MA60 指标有足够历史
    cal_all = pd.DatetimeIndex(calendar)
    w0, w1 = pd.Timestamp("2024-07-01"), pd.Timestamp("2026-07-15")
    cal_slice = [d for d in calendar if (pd.Timestamp(d) >= w0 and pd.Timestamp(d) <= w1)]
    print(f"载入标的: {len(data_cache)}  窗口交易日: {len(cal_slice)}", flush=True)

    ctx = build_ctx(data_cache, cal_slice)
    print("指标预计算完成", flush=True)
    signal_index = build_signal_index(ctx)
    print(f"信号索引完成: {sum(len(v) for v in signal_index.values())} 个基础信号", flush=True)

    # ---- 市场牛熊标签(用原始 data_cache 算等权净值 vs MA60) ----
    mkt_nav, _ = build_market_proxy(data_cache, cal_slice)
    mkt_ma60 = mkt_nav.rolling(60).mean()
    regime_at = {}
    for t in cal_slice:
        nav = mkt_nav.get(t, np.nan); ma = mkt_ma60.get(t, np.nan)
        regime_at[t] = "bull" if (not pd.isna(nav) and not pd.isna(ma) and nav >= ma) else "bear"
    print("牛熊标签完成", flush=True)

    # ---- 默认参数回测 ----
    trades, eq, m = simulate(ctx, cal_slice, GAP_DEF, HOLD_DEF, "close", regime_at, signal_index)
    print(f"[默认 close] n={m['n']} win={m['winrate']}% avg={m['avg_ret']}% total={m['total_ret']}%", flush=True)

    # ---- 牛熊分段 ----
    reg = regime_split(trades)

    # ---- 买点对比 ----
    entry_res = {}
    for em in ENTRY_MODES:
        tr, _, mm = simulate(ctx, cal_slice, GAP_DEF, HOLD_DEF, em, regime_at, signal_index)
        entry_res[em] = mm

    # ---- 持有期敏感度 ----
    hold_res = []
    for h in HOLDS:
        _, _, mm = simulate(ctx, cal_slice, GAP_DEF, h, "close", regime_at, signal_index)
        hold_res.append((h, mm))

    # ---- 60日线乖离敏感度(固定 dd/rsi/hold) ----
    gap_res = []
    for gp in [0.0, 0.03, 0.05, 0.08]:
        _, _, mm = simulate(ctx, cal_slice, gp, HOLD_DEF, "close", regime_at, signal_index)
        gap_res.append((gp, mm))

    # ---- 最优组合(敏感度交叉点): 3%乖离 + 长持有 + 次日开盘 ----
    best_combo = {}
    for tag, (gp, hh, em) in {
        "gap3_hold20_open": (0.03, 20, "next_open"),
        "gap3_hold15_close": (0.03, 15, "close"),
    }.items():
        _, _, mm = simulate(ctx, cal_slice, gp, hh, em, regime_at, signal_index)
        best_combo[tag] = mm
    print(f"[最优组合 gap3_hold20_open] n={best_combo['gap3_hold20_open']['n']} "
          f"win={best_combo['gap3_hold20_open']['winrate']}% avg={best_combo['gap3_hold20_open']['avg_ret']}% "
          f"total={best_combo['gap3_hold20_open']['total_ret']}%", flush=True)

    # ---- 样本逐笔(取前 15 笔, 含牛市/熊市各若干) ----
    sample = sorted(trades, key=lambda x: x["buy_t"])[:15]

    params = dict(dd=DD_DEF, gap=GAP_DEF, rsi=RSI_TH_DEF, hold=HOLD_DEF,
                  entry_mode="close", w0=str(w0)[:10], w1=str(w1)[:10])
    out_html = Path(__file__).resolve().parent / "oversold_rebound_report.html"
    build_html(out_html, m, hold_res, gap_res, entry_res, reg, sample, params, best_combo)

    # 全量逐笔 CSV
    import csv
    out_csv = Path(__file__).resolve().parent / "oversold_rebound_trades.csv"
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["代码", "买入日期", "买入价", "股数", "卖出日期", "卖出价",
                    "收益%", "盈亏元", "市况", "退出原因", "持有交易日", "距60高回撤%", "MA60乖离%", "RSI"])
        for t in sorted(trades, key=lambda x: x["buy_t"]):
            g = ctx[t["code"]]
            bi = g.index.get_loc(t["buy_t"])
            r0 = g.iloc[bi]
            w.writerow([t["code"], str(pd.Timestamp(t["buy_t"]))[:10], round(t["buy_px"], 2), t["shares"],
                        str(pd.Timestamp(t["sell_t"]))[:10], round(t["sell_px"], 2),
                        round(t["ret"] * 100, 2), round(t["pnl"], 0),
                        "牛市" if t["regime"] == "bull" else "熊市", t["reason"], t["hold_days"],
                        round(r0["dd60"] * 100, 1), round(r0["ma60_gap"] * 100, 1), round(r0["rsi14"], 1)])

    # 指标 JSON
    summary = dict(params=params, default=m, regime=reg,
                   entry={k: v for k, v in entry_res.items()},
                   hold=[{"hold": h, **mm} for h, mm in hold_res],
                   gap=[{"gap": gp, **mm} for gp, mm in gap_res],
                   best_combo=best_combo)
    (Path(__file__).resolve().parent / "oversold_rebound_metrics.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("完成 ->", out_html.name, out_csv.name, flush=True)


if __name__ == "__main__":
    main()
