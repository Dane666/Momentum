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


def evaluate(track_file=TRACK_FILE, min_date=None, push=False):
    if not os.path.exists(track_file):
        print("无 picks_tracking.json, 先跑 scan 积累数据"); return None
    recs = json.loads(Path(track_file).read_text(encoding='utf-8'))
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
    text = _format(sim_agg, real_agg, min_date, sim_total=len(sim), real_total=len(real))
    print(text)
    if push:
        try:
            from tools.tracking_utils import bark_notify
            bark_notify("📈 信号质量周报", text[:3800])
            print("[journal] Bark 推送成功")
        except Exception as e:
            print("[journal] Bark 推送失败:", e)
    return text


def _aggregate_sim(sim):
    by_type = defaultdict(lambda: defaultdict(list))
    for it in sim:
        closes = it['closes']
        for w in WINDOWS:
            if len(closes) >= w:
                by_type[it['type']][w].append(closes[w - 1] / it['entry'] - 1)
        if it.get('real_pnl') is not None:
            by_type[it['type']]['triggered'].append(it['real_pnl'])
    out = {}
    for typ, d in by_type.items():
        out[typ] = {}
        for w in WINDOWS:
            lst = d.get(w, [])
            if lst:
                out[typ][f'{w}d'] = dict(n=len(lst), avg=sum(lst) / len(lst),
                                        win=sum(1 for x in lst if x > 0) / len(lst))
        if d.get('triggered'):
            lst = d['triggered']
            out[typ]['triggered'] = dict(n=len(lst), avg=sum(lst) / len(lst),
                                         win=sum(1 for x in lst if x > 0) / len(lst))
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
    return f"{x*100:+.1f}%"


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
                    L.append(f"    {k}: n={v['n']:>2} 均收益{_fmt_pct(v['avg'])} 胜率{v['win']*100:.0f}%")
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


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="模拟盘信号质量评估")
    ap.add_argument('--evaluate', action='store_true', help="运行评估并打印报告")
    ap.add_argument('--push', action='store_true', help="额外 Bark 推送周报")
    ap.add_argument('--min-date', default=None, help="限定信号日期 >= 此值")
    a = ap.parse_args()
    if a.evaluate:
        evaluate(min_date=a.min_date, push=a.push)
    else:
        ap.print_help()
