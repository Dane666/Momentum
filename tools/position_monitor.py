# -*- coding: utf-8 -*-
"""盘中持仓监控 → Bark 实时提醒 (每30分钟检查)"""
import json, logging
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('monitor')

TP, SL = 0.10, 0.05
BIAS_LIMIT, RSI_DANGER = 0.20, 80.0
HOLD_DAYS = 5


def load_holdings():
    try:
        with open('data/picks_tracking.json') as f: tracking = json.load(f)
        cutoff = (datetime.now() - timedelta(days=HOLD_DAYS)).strftime('%Y-%m-%d')
        return [p for p in tracking if p['date'] >= cutoff]
    except: return []


def rsi(prices, p=14):
    if len(prices) < p + 1: return 50.0
    d = prices.diff()
    g, l = d.clip(lower=0), (-d).clip(lower=0)
    ag = g.rolling(p).mean().iloc[-1]
    al = l.rolling(p).mean().iloc[-1]
    return 100 - 100 / (1 + ag / al) if al > 0 else 100.0


def check(entry: dict) -> str:
    try:
        from momentum.data import load_or_fetch_kline, fetch_kline_from_api
        df = load_or_fetch_kline(str(entry['code']), fetch_kline_from_api)
        if df is None or df.empty or len(df) < 36:
            return f"{entry['code']} {entry['name']}: 数据不足"
        c, h, l = float(df['close'].iloc[-1]), float(df['high'].iloc[-1]), float(df['low'].iloc[-1])
        ep = float(entry['price'])
        pnl = (c - ep) / ep * 100
        ma5 = float(df['close'].tail(5).mean())
        ma20 = float(df['close'].tail(20).mean())
        bias = (c - ma20) / ma20 * 100
        rs = rsi(df['close'])
        holds = (datetime.now() - datetime.strptime(entry['date'], '%Y-%m-%d')).days

        if h >= ep * (1 + TP):
            return f"🎯 止盈! {entry['code']} {entry['name']}: 现{c:.2f} 高{h:.2f}≥止盈{ep*(1+TP):.2f} (+{pnl:+.1f}%)"
        if l <= ep * (1 - SL):
            return f"🛑 止损! {entry['code']} {entry['name']}: 现{c:.2f} 低{l:.2f}≤止损{ep*(1-SL):.2f} ({pnl:+.1f}%)"
        if c < ma5:
            return f"⚠️ 破MA5 {entry['code']} {entry['name']}: {c:.2f} MA5={ma5:.2f}"
        if bias > BIAS_LIMIT * 100:
            return f"📈 高乖离 {entry['code']} {entry['name']}: bias={bias:.1f}%"
        if rs > RSI_DANGER:
            return f"🔥 RSI超买 {entry['code']} {entry['name']}: RSI={rs:.0f}"
        if c < ma20:
            return f"💀 破MA20 {entry['code']} {entry['name']}: {c:.2f} MA20={ma20:.2f}"
        if holds >= HOLD_DAYS:
            return f"⏰ 到期 {entry['code']} {entry['name']}: D{holds} {pnl:+.1f}%"
        return f"✅ {entry['code']} {entry['name']}: {c:.2f} ({pnl:+.1f}%) D{holds}"
    except Exception as e:
        return f"{entry['code']} {entry['name']}: err {e}"


def run():
    logger.info("[Monitor] checking...")
    holdings = load_holdings()
    if not holdings: return
    alerts = [check(e) for e in holdings]
    text = "\n".join(alerts)
    print(text)
    # 只在有止盈止损信号时推送 Bark
    if '🎯' in text or '🛑' in text or '⚠️' in text:
        from momentum.notify.bark import send_bark
        send_bark('💼 持仓提醒', text)


if __name__ == '__main__':
    run()
