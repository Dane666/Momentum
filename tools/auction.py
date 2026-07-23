# -*- coding: utf-8 -*-
"""
集合竞价全景透视 + 主线赛道识别 (每日 09:25)
输出: 全市场情绪 + 封单排行 + 板块集中度 → 确认主线
"""
import logging
from datetime import datetime
from collections import Counter
import requests, pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('auction')


def fetch_auction():
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes(fs='沪深A股')
        if df is not None and not df.empty: return df
    except Exception as e: logger.warning(f'efinance: {e}')
    try:
        from momentum.data import fetch_all_stock_codes
        from momentum.data.fetcher import fetch_quotes_sina
        codes = [c for c in fetch_all_stock_codes() if c.startswith(('60','00'))]
        df = fetch_quotes_sina(codes[:800])
        if df is not None and len(df)>100: return df
    except Exception: pass
    return None


def get_sector(code: str) -> str:
    try:
        from momentum.data import fetch_stock_concept
        return fetch_stock_concept(code) or ''
    except Exception: return ''


def analyze(df: pd.DataFrame) -> str:
    for c in ['涨跌幅','最新价','成交额']:
        if c in df.columns: df[c]=pd.to_numeric(df[c],errors='coerce')

    total=len(df)
    chg=df['涨跌幅'] if '涨跌幅' in df.columns else pd.Series([0]*total)
    high_o=(chg>0).sum(); low_o=(chg<0).sum()

    now=datetime.now().strftime('%H:%M:%S')
    # MA60 闸口检查
    is_ma60_bull, ma60_msg, ma60_detail = check_ma60()

    lines=[f'🔔 集合竞价全景 | {now}',
           f'全市场: {total}只 | 高开: {high_o} | 低开: {low_o}',
           f'高开率: {high_o/total*100:.1f}%']
    if high_o/total>0.7: lines.append('📈 情绪: 🔥 强势')
    elif low_o/total>0.7: lines.append('📉 情绪: ❄️ 弱势')
    elif high_o>low_o: lines.append('📊 情绪: 😊 偏多')
    else: lines.append('📊 情绪: 😟 偏空')

    # MA60 多空判定
    if not is_ma60_bull and ma60_detail:
        diff = ma60_detail.get('diff_pct', 0)
        lines.append(f'⚠️ MA60闸口: 大盘跌破60日线 ({ma60_detail.get("close",0):.0f}/{ma60_detail.get("ma60",0):.0f} | {diff:+.1f}%)')
        lines.append('🛑 不适合交易 — 建议空仓观望')
    elif ma60_detail:
        diff = ma60_detail.get('diff_pct', 0)
        lines.append(f'✅ MA60闸口: 大盘站上60日线 (+{diff:.1f}%) — 适合择机交易')

    lines.append('─'*40)

    # 涨停开盘 Top10 + 板块识别
    if '涨跌幅' in df.columns and '股票代码' in df.columns:
        up=df[df['涨跌幅']>=9.5].head(10)
        if not up.empty:
            lines.append(f'\n🚀 涨停开盘 Top{len(up)}:')
            for _,r in up.iterrows():
                lines.append(f'  {r["股票代码"]} {r.get("股票名称","?")}: {r["最新价"]:.2f} (+{r["涨跌幅"]:.1f}%)')
            # 板块主线
            sector_counts=Counter()
            for _,r in up.iterrows():
                s=get_sector(str(r['股票代码']))
                if s:
                    for part in s.split(','):
                        part=part.strip()
                        if part: sector_counts[part]+=1
            top_sectors=sector_counts.most_common(5)
            if top_sectors:
                lines.append(f'\n🎯 竞价主线板块:')
                for s,c in top_sectors:
                    if c>=2: lines.append(f'  🔥 {s}: {c}只涨停')
                    else: lines.append(f'  📌 {s}: {c}只')
                # 判断主线
                if top_sectors[0][1]>=3:
                    lines.append(f'\n✅ 主线确认: {top_sectors[0][0]} ({top_sectors[0][1]}只涨停)')
                elif top_sectors[0][1]>=2:
                    lines.append(f'\n👀 潜在主线: {top_sectors[0][0]}')

    # 外部盘辅助: 碳酸锂期货 9:00-9:25 + 韩股早盘(至09:25) + 相关A股高开正反馈
    try:
        from tools.auction_extra import build_extra_sections
        extra = build_extra_sections(df)
        if extra:
            lines.append('─' * 40)
            lines.append(extra)
    except Exception as e:
        logger.warning(f'外部盘辅助段落失败: {e}')

    lines.append(f'\n⏰ 14:44 尾盘扫描见')
    return '\n'.join(lines)


def check_ma60():
    """检查大盘是否站上 MA60 (开盘前判定: T-1收盘 vs MA60[T-1])."""
    try:
        from momentum.factors.market import calc_ma60_gate_open
        is_bull, msg, detail = calc_ma60_gate_open('000001')
        return is_bull, msg, detail
    except Exception as e:
        logger.warning(f"MA60检查失败: {e}")
        return True, "MA60检查失败", {}


def send_notify(text:str):
    from momentum.notify.bark import send_bark
    send_bark('竞价扫描', text)


def run():
    logger.info('[Auction]')
    df=fetch_auction()
    if df is None or df.empty: return logger.error('no data')
    rpt=analyze(df); print(rpt); send_notify(rpt)
