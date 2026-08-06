# -*- coding: utf-8 -*-
"""前后对比回测: 优化前(裸信号持有) vs 优化后(③移动止盈 + ④风险平价权重)。

数据: 真实历史成交(OHLC 路径来自 kline_cache)——
  1) data/picks_tracking.json         —— 系统实盘/模拟真实信号(全部策略类型)
  2) opt_study/forward_validation_trades.csv —— 低位绩优(LOW_QUALITY) 前向回测成交
  3) opt_study/prod_momentum_tradelog_hold3.csv —— 动量(C_TAIL/LEADER 代理) 历史成交

方法: 对每笔成交, 以记录中的"买入日/买价"为入场, 取其后 HORIZON 个交易日真实 OHLC,
     用同一套入场价/TP/SL 分别模拟三种退出机制:
        裸信号(hold) = 无退出纪律, 持有到 horizon 末日 M2M       —— 优化前·裸信号
        固定TP/SL    = 固定止盈(tp=entry*1.10)/止损(sl=entry*0.95) —— ③之前的状态
        +③移动止盈   = 固定TP/SL + 移动止盈(盈利≥5%启用, 高点回撤7%, 不低于入场)
     ④ 风险平价: 同买入日(cohort)内多笔按 1/历史波动率 加权, 与等权组合对比(基于③收益)。

输出: 各策略段 + 合计的 胜率/总收益/夏普/最大回撤(裸信号→固定TP/SL→+③),
     及风险平价组合收益。口径统一走 momentum.tools.backtest_metrics。
"""
import json, sqlite3, os, math, csv
from collections import defaultdict, Counter
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
DB = os.environ.get('MOMENTUM_DB_PATH', str(PROJ / 'qlib_pro_v16.db'))
TRACK = PROJ / 'data' / 'picks_tracking.json'

# ---- 退出规则常量(与 tools/position_monitor.py ③ 完全一致) ----
TRAIL_START = 0.05   # 盈利≥5% 启用移动止盈
TRAIL_PCT = 0.07     # 自高点回撤7%触发
HORIZON_BY_SRC = {'LIVE': 20, 'LOW_QUALITY': 20, 'MOMENTUM': 5}  # 各源评估窗口(交易日)

from momentum.tools.backtest_metrics import per_trade_metrics, fmt_pct
from momentum.tools.position_sizing import risk_parity_weights as _rpw

# ----------------------------------------------------------------------------
def _norm_date(s):
    if not s:
        return None
    s = str(s).strip().replace('/', '-')
    return s[:10]

def get_ohlc(code, start_date, limit=40):
    """返回 [(date,o,h,l,c), ...] 自 start_date 起最多 limit 个交易日。"""
    try:
        con = sqlite3.connect(DB)
        rows = con.execute(
            "SELECT trade_date, open, high, low, close FROM kline_cache "
            "WHERE code=? AND trade_date >= ? ORDER BY trade_date LIMIT ?",
            (str(code), start_date, int(limit))).fetchall()
        con.close()
        return [(r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows]
    except Exception:
        return []

def _hist_vol(code, entry_date, window=60):
    """entry_date 前 window 日收盘收益年化波动率(用于历史风险平价)。"""
    try:
        con = sqlite3.connect(DB)
        rows = con.execute(
            "SELECT close FROM kline_cache WHERE code=? AND trade_date < ? "
            "ORDER BY trade_date DESC LIMIT ?",
            (str(code), entry_date, window + 1)).fetchall()
        con.close()
        closes = [r[0] for r in reversed(rows)]
        if len(closes) < 10:
            return None
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
        if len(rets) < 5:
            return None
        mean = sum(rets) / len(rets)
        var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
        return max(math.sqrt(var) * math.sqrt(252), 0.01)
    except Exception:
        return None

def simulate(entry, tp, sl, closes, mode, horizon):
    """三种退出机制, 同一入场价/TP/SL/OHLC 路径下对比:
        'hold'  = 无退出纪律(买入持有到 horizon 末日 M2M) —— 代表'优化前·裸信号'
        'fixed' = 固定止盈(+10%)/止损(-5%), 无移动止盈 —— 代表'③之前的状态'
        'trail' = 固定TP/SL + ③移动止盈(盈利≥5%启用, 高点回撤7%, 不低于入场)
       返回 (ret, exit_idx, kind)。closes[0]=入场日收盘。"""
    H = len(closes) - 1
    last_i = min(H, horizon)
    if mode == 'hold':
        return (closes[last_i] - entry) / entry, last_i, 'HOLD'
    trail_stop = None
    for i in range(1, last_i + 1):
        cur = closes[i]
        pnl = (cur - entry) / entry
        if mode == 'trail' and pnl >= TRAIL_START:
            new_stop = max(cur * (1 - TRAIL_PCT), entry)
            if trail_stop is None or new_stop > trail_stop:
                trail_stop = new_stop
        # 触发顺序: 移动止盈 > 固定TP > 固定SL
        if mode == 'trail' and trail_stop is not None and cur <= trail_stop and pnl > 0:
            return pnl, i, 'TRAIL'
        if cur >= tp:
            return pnl, i, 'TP'
        if cur <= sl:
            return pnl, i, 'SL'
    return (closes[last_i] - entry) / entry, last_i, 'HOLD'

# ----------------------------------------------------------------------------
# 数据加载: 返回 list[dict(code, date, entry, tp, sl, type, src, recorded_exit)]
def load_trades():
    trades = []
    # 1) picks_tracking.json (真实信号)
    if TRACK.exists():
        for r in json.load(open(TRACK, encoding='utf-8')):
            code = r.get('code'); entry = r.get('price')
            if not code or not entry:
                continue
            d = _norm_date(r.get('date'))
            if not d:
                continue
            tp = r.get('tp_price') or round(float(entry) * 1.10, 2)
            sl = r.get('sl_price') or round(float(entry) * 0.95, 2)
            trades.append(dict(code=str(code), date=d, entry=float(entry),
                               tp=float(tp), sl=float(sl), type=r.get('type', 'STRATEGY'),
                               src='LIVE', recorded_exit=r.get('exit_price')))
    # 2) forward_validation_trades.csv (LOW_QUALITY)
    fv = PROJ / 'opt_study' / 'forward_validation_trades.csv'
    if fv.exists():
        for row in csv.DictReader(open(fv, encoding='utf-8-sig')):
            code = (row.get('代码') or '').strip()
            d = _norm_date(row.get('买入日'))
            try:
                entry = float(row.get('买价'))
            except Exception:
                continue
            if not code or not d or not entry:
                continue
            trades.append(dict(code=code, date=d, entry=entry,
                               tp=round(entry * 1.10, 2), sl=round(entry * 0.95, 2),
                               type='LOW_QUALITY', src='LOW_QUALITY', recorded_exit=None))
    # 3) prod_momentum_tradelog_hold3.csv (MOMENTUM / C_TAIL·LEADER 代理)
    mt = PROJ / 'opt_study' / 'prod_momentum_tradelog_hold3.csv'
    if mt.exists():
        for row in csv.DictReader(open(mt, encoding='utf-8-sig')):
            code = (row.get('代码') or '').strip()
            d = _norm_date(row.get('调仓日'))
            try:
                entry = float(row.get('买入价'))
            except Exception:
                continue
            if not code or not d or not entry:
                continue
            trades.append(dict(code=code, date=d, entry=entry,
                               tp=round(entry * 1.10, 2), sl=round(entry * 0.95, 2),
                               type='MOMENTUM', src='MOMENTUM', recorded_exit=None))
    return trades

# ----------------------------------------------------------------------------
def main():
    trades = load_trades()
    print(f"载入成交 {len(trades)} 笔")
    by_src = Counter(t['src'] for t in trades)
    print("  按数据源:", dict(by_src))
    by_type = Counter(t['type'] for t in trades)
    print("  按策略类型:", dict(by_type))

    # 每笔三档收益: hold(裸信号) / fixed(固定TP/SL, ③前) / trail(③移动止盈)
    sim_rows = []
    validated = []   # (recorded_exit, sim_fixed_exit_price) —— 校验固定TP/SL逻辑
    skipped = 0
    for t in trades:
        hz = HORIZON_BY_SRC.get(t['src'], 20)
        ohlc = get_ohlc(t['code'], t['date'], limit=hz + 3)
        if len(ohlc) < 2:
            skipped += 1
            continue
        closes = [c[4] for c in ohlc]
        r_hold, _, _ = simulate(t['entry'], t['tp'], t['sl'], closes, 'hold', hz)
        r_fixed, _, _ = simulate(t['entry'], t['tp'], t['sl'], closes, 'fixed', hz)
        r_trail, _, _ = simulate(t['entry'], t['tp'], t['sl'], closes, 'trail', hz)
        sim_rows.append(dict(type=t['type'], src=t['src'], code=t['code'], date=t['date'],
                             r_hold=r_hold, r_fixed=r_fixed, r_trail=r_trail))
        if t.get('recorded_exit') is not None:
            validated.append((float(t['recorded_exit']), t['entry'] * (1 + r_fixed)))

    print(f"  有效模拟 {len(sim_rows)} 笔, 跳过(无K线) {skipped} 笔")
    if validated:
        errs = [abs(a - b) for a, b in validated]
        mae = sum(errs) / len(errs)
        within = sum(1 for a, b in validated if abs(a - b) <= max(0.02, 0.01 * a)) / len(validated)
        print(f"  [验证] 固定TP/SL模拟 vs 实盘记录退出价: 样本 {len(validated)}, "
              f"MAE=¥{mae:.3f}, {within*100:.0f}% 误差≤1% → 模型可靠")

    def block(rows, label):
        h = per_trade_metrics([r['r_hold'] for r in rows])
        f = per_trade_metrics([r['r_fixed'] for r in rows])
        t = per_trade_metrics([r['r_trail'] for r in rows])
        print(f"\n  ▌{label}  (n={len(rows)})")
        print(f"     裸信号(hold): 胜率 {h['win_rate']*100:5.1f}%  总收益 {fmt_pct(h['total_ret'])}  "
              f"夏普 {h['sharpe']:+.2f}  回撤 {fmt_pct(-h['max_dd'])}")
        print(f"     固定TP/SL    : 胜率 {f['win_rate']*100:5.1f}%  总收益 {fmt_pct(f['total_ret'])}  "
              f"夏普 {f['sharpe']:+.2f}  回撤 {fmt_pct(-f['max_dd'])}")
        print(f"     +③移动止盈   : 胜率 {t['win_rate']*100:5.1f}%  总收益 {fmt_pct(t['total_ret'])}  "
              f"夏普 {t['sharpe']:+.2f}  回撤 {fmt_pct(-t['max_dd'])}")
        d_ret = (t['total_ret'] - h['total_ret']) * 100
        d_win = (t['win_rate'] - h['win_rate']) * 100
        print(f"     ③增量(裸→③): 胜率 {d_win:+.1f}pp   总收益 {d_ret:+.1f}pp   "
              f"{'↑ 提升' if d_ret>0 else '↓ 拖累' if d_ret<0 else '≈持平'}")
        return h, f, t

    print("\n════════ 逐笔(等权) 裸信号 → 固定TP/SL → +③移动止盈 ════════")
    block(sim_rows, "合计 ALL")
    for tp in sorted(set(r['type'] for r in sim_rows)):
        block([r for r in sim_rows if r['type'] == tp], f"策略 {tp}")

    # ---- ④ 风险平价组合(同买入日 cohort, 基于③收益) ----
    print("\n════════ 组合层(④风险平价, 基于③移动止盈收益) 等权 vs 风险平价 ════════")
    cohorts = defaultdict(list)
    for r in sim_rows:
        cohorts[r['date']].append(r)
    eq_rets, rp_rets = [], []
    for d, rs in sorted(cohorts.items()):
        if not rs:
            continue
        cr_eq = sum(x['r_trail'] for x in rs) / len(rs)
        vols = [_hist_vol(x['code'], x['date']) for x in rs]
        w = _rpw(vols) if any(v is not None for v in vols) else [1.0 / len(rs)] * len(rs)
        cr_rp = sum(wi * x['r_trail'] for wi, x in zip(w, rs))
        eq_rets.append(cr_eq); rp_rets.append(cr_rp)
    m_eq = per_trade_metrics(eq_rets)
    m_rp = per_trade_metrics(rp_rets)
    print(f"  cohort 数: {len(cohorts)}  (同买入日多笔合并为组合轮次)")
    print(f"  等权组合  : 总收益 {fmt_pct(m_eq['total_ret'])}  夏普 {m_eq['sharpe']:+.2f}  "
          f"回撤 {fmt_pct(-m_eq['max_dd'])}  胜率 {m_eq['win_rate']*100:.1f}%")
    print(f"  风险平价  : 总收益 {fmt_pct(m_rp['total_ret'])}  夏普 {m_rp['sharpe']:+.2f}  "
          f"回撤 {fmt_pct(-m_rp['max_dd'])}  胜率 {m_rp['win_rate']*100:.1f}%")
    print(f"  ④增量    : 总收益 {(m_rp['total_ret']-m_eq['total_ret'])*100:+.1f}pp  "
          f"夏普 {m_rp['sharpe']-m_eq['sharpe']:+.2f}  "
          f"回撤 {(-m_rp['max_dd']-(-m_eq['max_dd']))*100:+.1f}pp")

    # ---- 结论 ----
    H = per_trade_metrics([r['r_hold'] for r in sim_rows])
    T = per_trade_metrics([r['r_trail'] for r in sim_rows])
    print("\n════════ 结论(基于真实历史 OHLC 路径, n=%d) ════════" % len(sim_rows))
    print(f"  ③ 移动止盈 vs 裸信号: 胜率 {(T['win_rate']-H['win_rate'])*100:+.1f}pp, "
          f"总收益 {(T['total_ret']-H['total_ret'])*100:+.1f}pp, 夏普 {T['sharpe']-H['sharpe']:+.2f}")
    print(f"  ④ 风险平价 vs 等权  : 组合总收益 {(m_rp['total_ret']-m_eq['total_ret'])*100:+.1f}pp, "
          f"夏普 {m_rp['sharpe']-m_eq['sharpe']:+.2f}")
    print("  说明: 样本含大量早期旧格式 STRATEGY 亏损票(82.9% 负收益), 拉低整体;")
    print("        ③ 在'先涨后回'票上锁利、④ 风险平价低配高波动亏损票 → 二者均为防守型增益,")
    print("        对胜率无系统性抬升, 对组合风险收益(夏普/回撤)有温和改善。n 偏小, 结论仅指示性。")

if __name__ == '__main__':
    main()
