# -*- coding: utf-8 -*-
"""持仓监控 — TP/SL触发 + 盈亏计算 + Bark + 状态回写"""
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
        if p.get('status') not in ('WATCHING', None): continue
        entry = p['price']; sl = p.get('sl_price', entry*0.95); tp = p.get('tp_price', entry*1.10)
        current = fetch_price(p['code'])
        if current is None: continue
        pnl = (current - entry) / entry * 100
        if current >= tp:
            tracking[i]['status'] = 'TP_TRIGGERED'
            tracking[i]['exit_price'] = current; tracking[i]['pnl_pct'] = round(pnl, 2)
            updated = True
            alerts.append(f"🎯 止盈 {p['code']} {p['name']}: ¥{current:.2f} (买入¥{entry:.2f}, +{pnl:.1f}%)")
        elif current <= sl:
            tracking[i]['status'] = 'SL_TRIGGERED'
            tracking[i]['exit_price'] = current; tracking[i]['pnl_pct'] = round(pnl, 2)
            updated = True
            alerts.append(f"🛑 止损 {p['code']} {p['name']}: ¥{current:.2f} (买入¥{entry:.2f}, {pnl:.1f}%)")
        else:
            logger.info(f"  {p['code']} {p['name']} ¥{current:.2f} {pnl:+.1f}% SL=¥{sl:.2f}")
    if updated:
        save(tracking)
        notify("💼 持仓提醒", "\n".join(alerts))
        logger.info(f"Saved {len(alerts)} triggers")
