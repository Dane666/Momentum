# -*- coding: utf-8 -*-
"""实盘交易日志(评估报告建议⑥) — 从统一真相源 picks_tracking.json 聚合已平仓/持仓,
导出可复盘 CSV, 并给出按策略分组的盈亏统计。

与 trade_journal(信号质量/模拟盘) 的区别: 本模块聚焦"实盘持仓(MANUAL/HOLDING)与其
平仓结果(TRIGGERED/带 exit_price)", 是真正的交易台账; trade_journal 评估的是"信号准不准"。

真相源:
  - 已平仓: status==TRIGGERED(trigger_type∈TP/SL/TRAIL) 或 MANUAL/HOLDING 但带 exit_price
  - 持仓中: status∈(MANUAL, HOLDING) 且无 exit_price
  - 字段: entry=date/price, exit=trigger_time/exit_price, pnl_pct, hold_days, weight

CLI:
  python tools/trade_log.py                 # 打印实盘盈亏总结 + 当前持仓
  python tools/trade_log.py --export out.csv   # 导出已平仓台账 CSV
  python tools/trade_log.py --push           # 额外 Bark 推送
"""
import sys, json, os, sqlite3, argparse
from datetime import datetime
from pathlib import Path

PROJ = Path(__file__).resolve().parent.parent
if str(PROJ) not in sys.path:
    sys.path.insert(0, str(PROJ))
TRACK_FILE = str(PROJ / 'data' / 'picks_tracking.json')


def _hold_days(entry_date, exit_date):
    try:
        d0 = datetime.strptime(str(entry_date)[:10], '%Y-%m-%d')
        d1 = datetime.strptime(str(exit_date)[:10], '%Y-%m-%d')
        return (d1 - d0).days
    except Exception:
        return None


def collect(track_file=TRACK_FILE):
    """返回 (realized, open) 两个 list。

    realized: 已平仓交易(含 TRIGGERED 与带 exit_price 的 MANUAL/HOLDING)
    open:     当前仍持仓(无 exit_price)
    每条记录字段: code,name,type,entry_date,entry_price,exit_date,exit_price,
                  pnl_pct,hold_days,trigger_type,weight
    """
    if not os.path.exists(track_file):
        return [], []
    recs = json.loads(Path(track_file).read_text(encoding='utf-8'))
    realized, open_ = [], []
    for p in recs:
        code = p.get('code'); entry = p.get('price'); edate = p.get('date')
        st = p.get('status')
        if not (code and entry and edate):
            continue
        exit_price = p.get('exit_price')
        if st == 'TRIGGERED' or (st in ('MANUAL', 'HOLDING') and exit_price is not None):
            exd = p.get('trigger_time') or edate
            realized.append(dict(
                code=code, name=p.get('name', code), type=p.get('type', 'STRATEGY'),
                entry_date=edate, entry_price=round(float(entry), 2),
                exit_date=str(exd)[:10], exit_price=round(float(exit_price), 2),
                pnl_pct=round(p.get('pnl_pct') if p.get('pnl_pct') is not None else 0.0, 2),
                hold_days=_hold_days(edate, exd),
                trigger_type=p.get('trigger_type', 'EXIT'),
                weight=p.get('weight')))
        elif st in ('MANUAL', 'HOLDING') and exit_price is None:
            open_.append(dict(
                code=code, name=p.get('name', code), type=p.get('type', 'STRATEGY'),
                entry_date=edate, entry_price=round(float(entry), 2),
                weight=p.get('weight'),
                sl=round(p.get('sl_price'), 2) if p.get('sl_price') else None,
                tp=round(p.get('tp_price'), 2) if p.get('tp_price') else None))
    return realized, open_


def summarize(realized, open_):
    L = ["📒 实盘交易日志"]
    L.append("=" * 50)
    if not realized:
        L.append("（暂无已平仓实盘交易; 先用 add_manual_position 录入真实持仓, 触发后自动登记）")
    else:
        pls = [r['pnl_pct'] for r in realized]
        n = len(pls)
        wins = sum(1 for x in pls if x > 0)
        total = 1.0
        for x in pls:
            total *= (1.0 + x / 100.0)
        L.append(f"已平仓 {n} 笔 ｜ 胜率 {wins/n*100:.0f}% ｜ 平均 {sum(pls)/n:+.2f}% ｜ "
                 f"累计复利 { (total-1)*100:+.2f}%")
        # 按 type 分组
        from collections import defaultdict
        by = defaultdict(list)
        for r in realized:
            by[r['type']].append(r['pnl_pct'])
        for t, lst in sorted(by.items()):
            w = sum(1 for x in lst if x > 0)
            L.append(f"  [{t}] n={len(lst)} 胜率{w/len(lst)*100:.0f}% 平均{sum(lst)/len(lst):+.2f}%")
    L.append("-" * 50)
    if open_:
        L.append(f"当前持仓 {len(open_)} 笔:")
        for o in open_:
            w = f" 仓位{o['weight']*100:.0f}%" if o.get('weight') else ""
            L.append(f"  {o['code']} {o['name']} 入场¥{o['entry_price']} "
                     f"SL¥{o['sl']} TP¥{o['tp']}{w}")
    else:
        L.append("当前无持仓")
    return "\n".join(L)


def export_csv(realized, path):
    import csv
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(["代码", "名称", "类型", "入场日", "入场价", "平仓日", "平仓价",
                    "盈亏%", "持有日", "触发类型", "权重"])
        for r in realized:
            w.writerow([r['code'], r['name'], r['type'], r['entry_date'], r['entry_price'],
                        r['exit_date'], r['exit_price'], r['pnl_pct'], r['hold_days'],
                        r['trigger_type'], r['weight']])
    return path


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="实盘交易日志 / 台账导出")
    ap.add_argument('--export', default=None, help="导出已平仓台账 CSV 路径")
    ap.add_argument('--push', action='store_true', help="额外 Bark 推送")
    ap.add_argument('--track-file', default=TRACK_FILE)
    a = ap.parse_args()
    realized, open_ = collect(a.track_file)
    text = summarize(realized, open_)
    print(text)
    if a.export:
        p = export_csv(realized, a.export)
        print(f"\n已导出 {len(realized)} 笔平仓记录 → {p}")
    if a.push:
        try:
            from tools.tracking_utils import bark_notify
            bark_notify("📒 实盘交易日志", text[:3800])
            print("[trade_log] Bark 推送成功")
        except Exception as e:
            print("[trade_log] Bark 推送失败:", e)
