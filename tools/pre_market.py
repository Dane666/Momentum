# -*- coding: utf-8 -*-
"""
盘前外围与宏观监控 (每日 08:30 推送)
数据源: yfinance (美股/汇率/期货/国债)
"""
import sys, os, logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import requests
import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('pre_market')

US_INDICES = {
    '^GSPC': '标普500', '^IXIC': '纳斯达克', '^DJI': '道琼斯',
}
TECH_STOCKS = {
    'NVDA': '英伟达', 'AAPL': '苹果', 'TSLA': '特斯拉',
    'MSFT': '微软', 'AMD': 'AMD', '^SOX': '费城半导体',
}
MACRO = {
    'CNH=X': '离岸人民币(USDCNH)', 'DX-Y.NYB': '美元指数(DXY)', '^TNX': '美10Y国债收益率',
}


def assess_risk(data: Dict) -> Tuple[str, str]:
    """评估大盘风险"""
    warnings = []
    score = 0
    chex = data.get('CNH=X', {})
    val_cnh = float(chex.get('close', 7.25)) if isinstance(chex, dict) else 7.25
    if val_cnh > 7.35:
        warnings.append(f'CNH贬值至{val_cnh:.2f}')
        score += 1
    dxy = data.get('DX-Y.NYB', {})
    val_dxy = float(dxy.get('close', 100)) if isinstance(dxy, dict) else 100
    if val_dxy > 106:
        warnings.append(f'DXY走强至{val_dxy:.0f}')
        score += 1
    nasdaq = data.get('^IXIC', {})
    chg_nasdaq = float(nasdaq.get('change_pct', 0)) if isinstance(nasdaq, dict) else 0
    if chg_nasdaq < -0.015:
        warnings.append(f'纳指跌幅{abs(chg_nasdaq)*100:.1f}%')
        score += 1
    sp500 = data.get('^GSPC', {})
    chg_sp = float(sp500.get('change_pct', 0)) if isinstance(sp500, dict) else 0
    if chg_sp < -0.015:
        warnings.append(f'标普跌幅{abs(chg_sp)*100:.1f}%')
        score += 1

    if score >= 3:
        return '🔴 高风险', '; '.join(warnings)
    elif score >= 1:
        return '🟡 谨慎', '; '.join(warnings)
    return '🟢 平稳', ''


def fetch_ticker(ticker: str) -> Optional[Dict]:
    try:
        t = yf.Ticker(ticker)
        h = t.history(period='5d')
        if h.empty or len(h) < 2:
            return None
        latest = h.iloc[-1]
        prev = h.iloc[-2]
        change = float(latest['Close']) - float(prev['Close'])
        change_pct = change / float(prev['Close']) if float(prev['Close']) > 0 else 0
        return {
            'ticker': ticker, 'close': float(latest['Close']),
            'change': change, 'change_pct': change_pct,
            'date': h.index[-1].strftime('%Y-%m-%d'),
        }
    except Exception as e:
        logger.debug(f"{ticker}: {e}")
        return None


def fetch_all() -> Dict:
    all_tk = {}
    all_tk.update(US_INDICES)
    all_tk.update(TECH_STOCKS)
    all_tk.update(MACRO)
    results = {}
    for tk, nm in all_tk.items():
        d = fetch_ticker(tk)
        if d:
            d['name'] = nm
            results[tk] = d
    return results


def fmt_chg(v: float, is_pct: bool = True) -> str:
    sign = '+' if v >= 0 else ''
    if is_pct:
        return f"{sign}{v*100:.2f}%"
    return f"{sign}{v:.2f}"


def build_report(data: Dict) -> str:
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    risk_level, risk_detail = assess_risk(data)
    lines = [
        f"📊 盘前全景早报 | {now}",
        f"风险等级: {risk_level}",
    ]
    if risk_detail:
        lines.append(f"因子: {risk_detail}")
    lines.append("─" * 40)
    lines.append("🇺🇸 美股三大指数")
    for t, n in US_INDICES.items():
        d = data.get(t)
        if d:
            a = '🔺' if d['change_pct'] > 0 else '🔻'
            lines.append(f"  {a} {n}: {d['close']:.2f} ({fmt_chg(d['change_pct'])})")
    lines.append("")
    lines.append("💻 核心科技")
    for t, n in TECH_STOCKS.items():
        d = data.get(t)
        if d:
            a = '🔺' if d['change_pct'] > 0 else '🔻'
            lines.append(f"  {a} {n}: {d['close']:.2f} ({fmt_chg(d['change_pct'])})")
    lines.append("")
    lines.append("🌍 宏观三剑客")
    for t, n in MACRO.items():
        d = data.get(t)
        if d:
            if t == '^TNX':
                lines.append(f"  📌 {n}: {d['close']:.2f}% ({fmt_chg(d['change'], False)}%)")
            elif t == 'CNH=X':
                lines.append(f"  📌 {n}: {d['close']:.4f} ({fmt_chg(d['change_pct'])})")
            else:
                lines.append(f"  📌 {n}: {d['close']:.2f} ({fmt_chg(d['change_pct'])})")
    lines.append("")
    lines.append(f"📅 数据截止: {list(data.values())[0]['date'] if data else 'N/A'}")
    return '\n'.join(lines)


def send_feishu(text: str):
    from momentum import config as cfg
    url = getattr(cfg, 'FEISHU_WEBHOOK_URL', '').strip()
    if not url:
        logger.warning("No webhook configured")
        return
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        r = requests.post(url, json=payload, timeout=10)
        logger.info(f"Feishu: {r.status_code}")
    except Exception as e:
        logger.error(f"Feishu failed: {e}")


def run():
    logger.info("[PreMarket] Fetching...")
    data = fetch_all()
    if not data:
        logger.error("No data")
        return
    report = build_report(data)
    print(report)
    send_feishu(report)


if __name__ == '__main__':
    run()
