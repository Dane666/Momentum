# -*- coding: utf-8 -*-
"""
集合竞价全景透视 + 主线赛道识别 (每日 09:25)
输出: 全市场情绪 + 封单排行 + 板块集中度 → 确认主线
"""
import json, sqlite3, logging
from datetime import datetime
from pathlib import Path
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

    # 合并: LightGBM 候选开盘核验(盘前弱市撤单预警) — 并入竞价扫描同一条 Bark
    try:
        cand_sec = build_model_candidate_section()
        if cand_sec:
            lines.append('─' * 40)
            lines.append(cand_sec)
    except Exception as e:
        logger.warning(f'模型候选核验段落失败: {e}')

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


def build_model_candidate_section(today: str = None) -> str | None:
    """读取夜盘持久化的 LightGBM 候选清单, 对比今日开盘, 生成「盘前弱市撤单预警」段落。

    并入竞价扫描报告(同一 Bark 推送), 不再单独发推送。
    数据来源: data/push_candidates.json(夜盘 daily_inference 持久化) + 今日 kline_cache.open。
    返回 None 表示无候选/今日开盘尚未入库, 调用方跳过(不影响竞价扫描主报告)。
    """
    try:
        cp = Path('data/push_candidates.json')
        if not cp.exists():
            logger.info('[候选核验] 无 data/push_candidates.json, 跳过')
            return None
        cand = json.loads(cp.read_text(encoding='utf-8'))
        picks = cand.get('picks', [])
        if not picks:
            return None
        if today is None:
            today = datetime.now().strftime('%Y-%m-%d')
        con = sqlite3.connect('qlib_pro_v16.db')
        rows = {r[0]: r[1] for r in con.execute(
            "SELECT code, open FROM kline_cache WHERE trade_date=?", (today,)).fetchall()}
        con.close()
        if not rows:
            logger.info('[候选核验] 今日 K 线尚未入库, 跳过')
            return None

        def lr(c): return 0.20 if str(c).startswith(('30', '68')) else 0.10
        weak, toprisk, normal = [], [], []
        for pk in picks:
            c = pk['code']; ref = pk.get('close'); o = rows.get(c)
            if not ref or o is None:
                weak.append((pk.get('name', c), c, None)); continue
            gap = o / float(ref) - 1
            lu = float(ref) * (1 + lr(c))
            if o >= lu * 0.98:
                toprisk.append((pk.get('name', c), c, round(gap * 100, 2)))
            elif gap < -0.03:
                weak.append((pk.get('name', c), c, round(gap * 100, 2)))
            else:
                normal.append((pk.get('name', c), c, round(gap * 100, 2)))

        n = len(picks)
        lines = [f'🤖 LightGBM 候选开盘核验 ({n}只, 信号日 {cand.get("date")}) — 盘前弱市撤单预警:']
        if toprisk:
            lines += ['', '⚠️ 开盘近涨停·高位接盘/炸板风险(已成交者重点盯 -8% 止损):']
            for nm, c, g in toprisk:
                lines.append(f'  • {nm}({c}) 高开{g:+.1f}%')
        if weak:
            lines += ['', '📉 弱开(低于信号价>3%, 已成交者重点盯 -8% 止损):']
            for nm, c, g in weak:
                tag = ' (无开盘数据)' if g is None else f'低开{g:+.1f}%'
                lines.append(f'  • {nm}({c}) {tag}')
        if normal:
            lines += ['', f'✅ 开盘正常 {len(normal)} 只(高开/平开<3%)']
        lines += ['', '⏰ 注: 本核验为 09:25 开盘后确认 (9:20 后单已锁定不可撤); 弱开/高位接盘由 -8% 止损 + 5票分散兜住']
        return '\n'.join(lines)
    except Exception as e:
        logger.warning(f'[候选核验] 段落生成失败: {e}')
        return None


def run():
    logger.info('[Auction]')
    df=fetch_auction()
    if df is None or df.empty: return logger.error('no data')
    rpt=analyze(df); print(rpt); send_notify(rpt)
