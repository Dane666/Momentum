# -*- coding: utf-8 -*-
"""模拟盘信号质量评估 / 实盘交易日志 — 把 scan 选出的"模拟信号"与实盘持仓,
回溯其后市表现, 用数据回答总结五的核心问题: 信号到底准不准? 该不该加大投入?

设计:
  - 真相源 = data/picks_tracking.json(统一登记, 含 WATCHING/PLAN 模拟信号与 MANUAL/HOLDING 实盘)
  - 价格源 = kline_cache(日线, 计算信号后 N 交易日收益)
  - 信号质量: 以 signal date 的 entry price 为基准, 取其后 第N交易日 close 的收益
      * 模拟信号(WATCHING/PLAN): 评估"信号后N日表现" → 命中率(回答"选股准不准")
      * TRIGGERED(模拟已触发): 用记录的 exit_price 作为模拟实际盈亏
      * 实盘(MANUAL/HOLDING): 有 exit_price 用实际盈亏, 否则用最新 close 浮动盈亏
  - 这是总结五"先用模拟盘跑1~3个月积累≥20~30笔数据"、建议7"验证推送信号准确率"的核心落地,
    也是其他策略优化(退出规则/仓位管理/快照回测)所需的数据地基.

CLI:
  python tools/trade_journal.py --evaluate                  # 本地打印信号质量报告
  python tools/trade_journal.py --evaluate --push           # 额外 Bark 推送周报
  python tools/trade_journal.py --evaluate --min-date 2026-07-01   # 限定窗口
"""
import sys, json, os, sqlite3, argparse
from datetime import datetime
from pathlib import Path
from collections import defaultdict

PROJ = Path(__file__).resolve().parent.parent
if str(PROJ) not in sys.path:
    sys.path.insert(0, str(PROJ))

TRACK_FILE = str(PROJ / 'data' / 'picks_tracking.json')
DB = str(PROJ / 'qlib_pro_v16.db')
WINDOWS = [1, 3, 5, 10]
SIM_STATUS = ('WATCHING', 'PLAN', 'TRIGGERED')
REAL_STATUS = ('MANUAL', 'HOLDING')

# 统一指标口径(建议⑤): 实盘/模拟评估与 backtest 共用同一套定义
try:
    from tools.backtest_metrics import per_trade_metrics, fmt_pct
except Exception:  # 独立运行时回退
    def per_trade_metrics(returns):
        rs = [float(r) for r in returns if r is not None]
        n = len(rs)
        if n == 0:
            return dict(n=0, total_ret=0.0, win_rate=0.0, avg_ret=0.0, max_dd=0.0, sharpe=0.0)
        total = 1.0
        for r in rs:
            total *= (1.0 + r)
        return dict(n=n, total_ret=total - 1.0, win_rate=sum(1 for r in rs if r > 0) / n,
                    avg_ret=sum(rs) / n, max_dd=0.0, sharpe=0.0)
    def fmt_pct(x):
        return f"{x*100:+.1f}%"


def _prices_after(code, sig_date, n=max(WINDOWS)):
    """返回 sig_date 之后第1..n个交易日的 close 列表(索引0=第1日)."""
    try:
        con = sqlite3.connect(DB)
        rows = con.execute(
            "SELECT close FROM kline_cache WHERE code=? AND trade_date>? ORDER BY trade_date",
            (str(code), sig_date)).fetchall()
        con.close()
        return [r[0] for r in rows[:n]]
    except Exception:
        return []


def _latest_close(code):
    try:
        con = sqlite3.connect(DB)
        r = con.execute("SELECT close FROM kline_cache WHERE code=? ORDER BY trade_date DESC LIMIT 1",
                        (str(code),)).fetchone()
        con.close()
        return r[0] if r else None
    except Exception:
        return None


def _aggregate_sim(sim):
    """按 type 分组, 用统一口径(backtest_metrics)算各窗口指标 + 突破因子。

    除 1/3/5/10 日胜率/均收益外, 额外给出:
      - '10d_max_gain': 该 type 信号 10 日内最大涨幅中位数(突破动能)
      - 'ft10': 10 日内曾触及 +10% 的占比(突破跟随因子, 越高越值得重仓)
    这些字段供策略/因子有效性比较(建议⑥)。
    """
    by_type = defaultdict(lambda: defaultdict(list))
    ft_by_type = defaultdict(list)  # type -> [是否触及+10%]
    for it in sim:
        closes = it['closes']
        for w in WINDOWS:
            if len(closes) >= w:
                by_type[it['type']][w].append(closes[w - 1] / it['entry'] - 1)
        if len(closes) >= 10:
            max_gain = max(closes[:10]) / it['entry'] - 1
            ft_by_type[it['type']].append(1 if max_gain >= 0.10 else 0)
        else:
            ft_by_type[it['type']].append(None)
        if it.get('real_pnl') is not None:
            by_type[it['type']]['triggered'].append(it['real_pnl'])
    out = {}
    for typ, d in by_type.items():
        out[typ] = {}
        for w in WINDOWS:
            lst = d.get(w, [])
            if lst:
                m = per_trade_metrics(lst)
                out[typ][f'{w}d'] = dict(n=m['n'], avg=m['avg_ret'], win=m['win_rate'],
                                         max_dd=m['max_dd'], sharpe=m['sharpe'])
        if d.get('triggered'):
            lst = d['triggered']
            m = per_trade_metrics(lst)
            out[typ]['triggered'] = dict(n=m['n'], avg=m['avg_ret'], win=m['win_rate'])
        fts = [x for x in ft_by_type[typ] if x is not None]
        if fts:
            out[typ]['ft10'] = round(sum(fts) / len(fts), 3)
    return out


def _aggregate_real(real):
    if not real:
        return None
    pls = [r['pnl'] for r in real if r['pnl'] is not None]
    if not pls:
        return dict(n=len(real), closed=0)
    return dict(n=len(real), closed=len(pls), avg=sum(pls) / len(pls),
                win=sum(1 for x in pls if x > 0) / len(pls))


def _fmt_pct(x):
    return fmt_pct(x)


def _format(sim, real, min_date, sim_total, real_total):
    L = ["📊 信号质量评估 / 模拟盘验证"]
    if min_date:
        L.append(f"窗口: >= {min_date}")
    L.append("=" * 50)
    if not sim:
        L.append(f"（暂无模拟信号样本(共{sim_total}条但无有效数据), 先跑 scan 积累）")
    else:
        L.append(f"模拟信号样本数: {sim_total}")
        for typ, d in sorted(sim.items()):
            L.append(f"\n  [{typ}]")
            for k in ('1d', '3d', '5d', '10d'):
                if k in d:
                    v = d[k]
                    L.append(f"    {k}: n={v['n']:>2} 均收益{_fmt_pct(v['avg'])} "
                             f"胜率{v['win']*100:.0f}% 夏普{v['sharpe']:.2f}")
            if 'ft10' in d:
                L.append(f"    突破跟随(10日触及+10%占比): {d['ft10']*100:.0f}%")
            if 'triggered' in d:
                v = d['triggered']
                L.append(f"    已触发: n={v['n']:>2} 实际{_fmt_pct(v['avg'])} 胜率{v['win']*100:.0f}%")
    if real:
        L.append("\n" + "=" * 50)
        L.append(f"实盘持仓(MANUAL/HOLDING) 共 {real_total} 笔:")
        if real.get('closed'):
            L.append(f"  平仓 {real['closed']}/{real['n']} 均盈亏{_fmt_pct(real['avg'])} 胜率{real['win']*100:.0f}%")
        else:
            L.append(f"  暂未平仓(浮动), 共 {real['n']} 笔")
    L.append("\n⚠️ 样本<10笔时统计置信度低; 总结建议≥20~30笔再决策是否加仓")
    return "\n".join(L)


def evaluate_data(min_date=None):
    """返回结构化评估结果 (sim_agg, real_agg, sim_total, real_total), 供对齐报告复用。"""
    sim_agg, real_agg, sim_total, real_total = [], None, 0, 0
    if not os.path.exists(TRACK_FILE):
        return {}, None, 0, 0
    recs = json.loads(Path(TRACK_FILE).read_text(encoding='utf-8'))
    sim, real = [], []
    for p in recs:
        code = p.get('code'); sd = p.get('date'); entry = p.get('price')
        st = p.get('status'); typ = p.get('type') or 'STRATEGY'
        if not (code and sd and entry):
            continue
        if min_date and sd < min_date:
            continue
        if st in SIM_STATUS:
            item = dict(code=code, name=p.get('name', code), type=typ,
                        date=sd, entry=entry,
                        closes=_prices_after(code, sd, max(WINDOWS)))
            if st == 'TRIGGERED' and p.get('exit_price'):
                item['real_pnl'] = p['exit_price'] / entry - 1
            sim.append(item)
        elif st in REAL_STATUS:
            if p.get('exit_price'):
                pnl = p['exit_price'] / entry - 1
            else:
                lc = _latest_close(code)
                pnl = (lc / entry - 1) if lc else None
            real.append(dict(code=code, name=p.get('name', code), type=typ,
                             date=sd, entry=entry, pnl=pnl, st=st))
    sim_agg = _aggregate_sim(sim)
    real_agg = _aggregate_real(real)
    return sim_agg, real_agg, len(sim), len(real)


def evaluate(track_file=TRACK_FILE, min_date=None, push=False):
    sim_agg, real_agg, sim_total, real_total = evaluate_data(min_date)
    text = _format(sim_agg, real_agg, min_date, sim_total, real_total)
    print(text)
    if push:
        try:
            from tools.tracking_utils import bark_notify
            bark_notify("📈 信号质量周报", text[:3800])
            print("[journal] Bark 推送成功")
        except Exception as e:
            print("[journal] Bark 推送失败:", e)
    return text


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="模拟盘信号质量评估 / 因子有效性")
    ap.add_argument('--evaluate', action='store_true', help="运行评估并打印报告")
    ap.add_argument('--factors', action='store_true', help="打印策略/因子有效性明细")
    ap.add_argument('--push', action='store_true', help="额外 Bark 推送周报")
    ap.add_argument('--min-date', default=None, help="限定信号日期 >= 此值")
    a = ap.parse_args()
    if a.evaluate or a.factors:
        sim_agg, real_agg, sim_total, real_total = evaluate_data(a.min_date)
        print(_format(sim_agg, real_agg, a.min_date, sim_total, real_total))
        if a.factors:
            print("\n" + "=" * 50)
            print("🧬 策略/因子有效性明细(按 type 分组)")
            for typ, d in sorted(sim_agg.items()):
                ft = d.get('ft10')
                print(f"  [{typ}] 样本={d.get('10d', {}).get('n', 0)}  "
                      f"10日胜率={d.get('10d', {}).get('win', 0)*100:.0f}%  "
                      f"10日均收益={_fmt_pct(d.get('10d', {}).get('avg', 0))}  "
                      f"突破跟随(ft10)={ft*100:.0f}%" if ft is not None else
                      f"  [{typ}] 样本={d.get('10d', {}).get('n', 0)}")
        if a.push:
            try:
                from tools.tracking_utils import bark_notify
                bark_notify("📈 信号质量周报", _format(sim_agg, real_agg, a.min_date,
                                                      sim_total, real_total)[:3800])
                print("[journal] Bark 推送成功")
            except Exception as e:
                print("[journal] Bark 推送失败:", e)
    else:
        ap.print_help()
