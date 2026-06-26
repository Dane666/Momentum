# -*- coding: utf-8 -*-
"""集合竞价全景透视 (每日 09:25)"""
import logging
from datetime import datetime
import requests
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('auction')


def fetch_auction():
    """获取竞价数据"""
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes(fs='沪深A股')
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning(f"efinance: {e}")
    try:
        from momentum.data import fetch_all_stock_codes
        from momentum.data.fetcher import fetch_quotes_sina
        codes = fetch_all_stock_codes()
        codes = [c for c in codes if c.startswith(('60','00'))]
        df = fetch_quotes_sina(codes[:800])
        if df is not None and len(df) > 100:
            return df
    except Exception as e:
        logger.warning(f"Sina fallback: {e}")
    return None


def analyze(df: pd.DataFrame) -> str:
    """分析竞价"""
    for c in ['涨跌幅','最新价','成交额']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    total = len(df)
    chg = df['涨跌幅'] if '涨跌幅' in df.columns else df.get('change_pct', pd.Series([0]*total))
    high_o = (chg > 0).sum()
    low_o = (chg < 0).sum()
    flat_o = (chg == 0).sum()
    total_amt = df['成交额'].sum() if '成交额' in df.columns else 0

    now = datetime.now().strftime('%H:%M:%S')
    lines = [
        f"🔔 集合竞价全景 | {now}",
        f"全市场: {total}只 | 高开: {high_o}只 | 低开: {low_o}只 | 平: {flat_o}只",
        f"高开率: {high_o/total*100:.1f}% | 成交额: {total_amt/1e8:.1f}亿",
        "─" * 40,
    ]

    if high_o / total > 0.7:
        lines.append("📈 情绪: 🔥 强势 (>70%高开)")
    elif low_o / total > 0.7:
        lines.append("📉 情绪: ❄️ 弱势 (>70%低开)")
    elif high_o > low_o:
        lines.append("📊 情绪: 😊 偏多")
    else:
        lines.append("📊 情绪: 😟 偏空")

    # 涨停/跌停
    if '涨跌幅' in df.columns and '股票代码' in df.columns:
        up = df[df['涨跌幅'] >= 9.5].head(6)
        if not up.empty:
            lines.append("\n🚀 涨停开盘 Top6:")
            for _, r in up.iterrows():
                lines.append(f"  {r['股票代码']} {r.get('股票名称','?')}: {r['最新价']:.2f} (+{r['涨跌幅']:.1f}%)")
        dn = df[df['涨跌幅'] <= -9.5].head(6)
        if not dn.empty:
            lines.append("\n⚠️ 跌停开盘:")
            for _, r in dn.iterrows():
                lines.append(f"  {r['股票代码']} {r.get('股票名称','?')}: {r['最新价']:.2f} ({r['涨跌幅']:.1f}%)")

    lines.append(f"\n⏰ 下次: 14:44 尾盘扫描")
    return '\n'.join(lines)


def send_feishu(text: str):
    from momentum import config as cfg
    url = getattr(cfg, 'FEISHU_WEBHOOK_URL', '').strip()
    if not url:
        return
    requests.post(url, json={"msg_type":"text","content":{"text":text}}, timeout=10)


def run():
    logger.info("[Auction] Fetching...")
    df = fetch_auction()
    if df is None or df.empty:
        logger.error("No data")
        return
    rpt = analyze(df)
    print(rpt)
    send_feishu(rpt)
