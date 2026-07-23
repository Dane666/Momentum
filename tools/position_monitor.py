# -*- coding: utf-8 -*-
"""持仓监控 — TP/SL触发 + 盈亏计算 + Bark分流通知 + 状态回写"""
import json, logging, os
from datetime import datetime
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('monitor')
TRACK_FILE = 'data/picks_tracking.json'

def load():
    try:
        with open(TRACK_FILE) as f: tracking = json.load(f)
        changed = False
        for p in tracking:
            if 'status' not in p:
                p['status'] = 'WATCHING'; changed = True
            if 'sl_price' not in p:
                p['sl_price'] = round(p['price'] * 0.95, 2); changed = True
            if 'tp_price' not in p:
                p['tp_price'] = round(p['price'] * 1.10, 2); changed = True
            if 'type' not in p:
                p['type'] = 'STRATEGY'; changed = True
        if changed:
            save(tracking)
            logger.info("Migrated old-format records")
        return tracking
    except: return []

def save(data):
    os.makedirs('data', exist_ok=True)
    with open(TRACK_FILE, 'w') as f: json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_price(code):
    try:
        from momentum.data import load_or_fetch_kline, fetch_kline_from_api
        df = load_or_fetch_kline(str(code), fetch_kline_from_api)
        if df is not None and not df.empty: return float(df['close'].iloc[-1])
    except: pass
    return None

def notify(title, msg):
    try:
        from momentum.notify.bark import send_bark
        send_bark(title, msg)
    except: pass

def run():
    logger.info("[Monitor] scanning...")
    tracking = load()
    updated = False; alerts = []
    for i, p in enumerate(tracking):
        if p.get('status') not in ('WATCHING', 'HOLDING', None): continue
        entry = p['price']; sl = p.get('sl_price', entry*0.95); tp = p.get('tp_price', entry*1.10)
        current = fetch_price(p['code'])
        if current is None: continue
        pnl = (current - entry) / entry * 100
        triggered = False; trigger_type = ''
        if current >= tp:
            tracking[i]['status'] = 'TRIGGERED'
            tracking[i]['exit_price'] = current
            tracking[i]['pnl_pct'] = round(pnl, 2)
            tracking[i]['trigger_type'] = 'TP'
            tracking[i]['trigger_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tracking[i]['pnl_ratio'] = round(pnl, 2)
            updated = True; triggered = True; trigger_type = 'TP'
        elif current <= sl:
            tracking[i]['status'] = 'TRIGGERED'
            tracking[i]['exit_price'] = current
            tracking[i]['pnl_pct'] = round(pnl, 2)
            tracking[i]['trigger_type'] = 'SL'
            tracking[i]['trigger_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tracking[i]['pnl_ratio'] = round(pnl, 2)
            updated = True; triggered = True; trigger_type = 'SL'

        if triggered:
            pick_type = p.get('type', 'STRATEGY')
            direction = '止盈' if trigger_type == 'TP' else '止损'
            if pick_type == 'MANUAL':
                line = (
                    f"⚠️ 【实际持仓警告】您的持仓 {p['name']}({p['code']})"
                    f" 已达{direction}点！\n"
                    f"当前价: ¥{current:.2f}  实际盈亏: {pnl:+.1f}%\n"
                    f"请速去券商手动操作！"
                )
            else:
                tlabel = {'LOW_QUALITY': '低位绩优', 'C_TAIL': 'C尾盘',
                          'LEADER': '龙头', 'STRATEGY': '策略'}.get(
                    pick_type, '策略')
                line = (
                    f"📊 【{tlabel}模拟提示】观察股 {p['name']}({p['code']})"
                    f" 已触发{direction}信号。\n"
                    f"当前价: ¥{current:.2f}  模拟盈亏: {pnl:+.1f}%"
                )
            alerts.append(line)
        else:
            logger.info(f"  {p['code']} {p['name']} ¥{current:.2f} {pnl:+.1f}% SL=¥{sl:.2f}")
    if updated:
        save(tracking)
        notify("💼 持仓提醒", "\n".join(alerts))
        logger.info(f"Saved {len(alerts)} triggers")
