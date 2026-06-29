# -*- coding: utf-8 -*-
"""盘后数据归档与初筛 (每日 16:00)"""
import logging
from datetime import datetime
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('eod')


def fetch_limit_board():
    try:
        import efinance as ef
        df = ef.stock.get_realtime_quotes(fs='沪深A股')
        if df is not None and not df.empty:
            for c in ['涨跌幅']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            return df[df['涨跌幅'] >= 9.5].copy()
    except Exception as e:
        logger.warning(f"limit board: {e}")
    return pd.DataFrame()


def fetch_dragon_tiger():
    try:
        import efinance as ef
        return ef.stock.get_daily_billboard()
    except Exception:
        return pd.DataFrame()


def check_positions(holdings: dict):
    lines = []
    for code, entry in holdings.items():
        try:
            from momentum.data import load_or_fetch_kline, fetch_kline_from_api
            df = load_or_fetch_kline(str(code), fetch_kline_from_api)
            if df is None or df.empty or len(df) < 60:
                continue
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            close = float(df['close'].iloc[-1])
            ma5 = float(df['close'].tail(5).mean())
            ma20 = float(df['close'].tail(20).mean())
            ma60 = float(df['close'].tail(60).mean())
            low20 = float(df['low'].tail(20).min())
            pct = (close - entry) / entry * 100 if entry > 0 else 0

            status, warns = '✅', []
            if close < ma5: warns.append('MA5'); status = '🟡'
            if close < ma20: warns.append('MA20'); status = '🟠'
            if close < ma60: warns.append('MA60'); status = '🔴'
            if close < low20: warns.append('前低'); status = '🔴'
            if pct < -10: status = '🔴'

            lines.append(f"  {status} {code}: {close:.2f} (成本{entry:.2f}|{pct:+.1f}%)")
            if warns:
                lines.append(f"      ⚠️ 破{'/'.join(warns)} | MA5={ma5:.2f} MA20={ma20:.2f} MA60={ma60:.2f}")
        except Exception as e:
            logger.debug(f"check {code}: {e}")
    return lines


def send_feishu(text: str):
    from momentum import config as cfg
    url = getattr(cfg, 'FEISHU_WEBHOOK_URL', '').strip()
    if not url:
        return
    requests.post(url, json={"msg_type":"text","content":{"text":text}}, timeout=10)


def run():
    logger.info("[EOD] Running...")
    now = datetime.now()

    # 从龙虎榜提取实际数据日期
    data_date = now.strftime('%Y-%m-%d')
    dt = fetch_dragon_tiger()
    if dt is not None and not dt.empty and '上榜日期' in dt.columns:
        dd = str(dt['上榜日期'].iloc[0])[:10]
        if dd: data_date = dd

    # 重复检测: 如果今天已经跑过同一天数据的报告，跳过
    lock_file = 'data/eod_last_date.txt'
    skip = False
    try:
        import os
        if os.path.exists(lock_file):
            with open(lock_file) as f:
                if f.read().strip() == data_date:
                    logger.info(f"[EOD] Already reported for {data_date}, skipping")
                    return
    except: pass

    lines = [f"📋 盘后归档 | {data_date} {now.strftime('%H:%M')}", "─" * 40]

    # 涨停板
    up = fetch_limit_board()
    if not up.empty:
        lines.append(f"\n🚀 今日涨停: {len(up)} 只")
        if '成交额' in up.columns:
            up['成交额'] = pd.to_numeric(up['成交额'], errors='coerce')
            top = up.nlargest(5, '成交额')
            for _, r in top.iterrows():
                code = r.get('股票代码','?')
                name = r.get('股票名称','?')
                lines.append(f"  {code} {name}: {r.get('最新价',0):.2f} ({r.get('涨跌幅',0):+.1f}%)")

    # 龙虎榜
    dt = fetch_dragon_tiger()
    if dt is not None and not dt.empty:
        dt['龙虎榜净买额'] = pd.to_numeric(dt['龙虎榜净买额'], errors='coerce')
        # 过滤 ST/退市，选正常股
        normal = dt[~dt['股票名称'].astype(str).str.contains('ST|退', na=False)]
        # 净买入 Top5 (去重)
        normal['_code_dedup'] = normal['股票代码'].astype(str)
        top_buy = normal.drop_duplicates('_code_dedup').nlargest(5, '龙虎榜净买额')
        lines.append(f"\n🐉 龙虎榜 净买入 Top5 (共{len(dt)}条):")
        for _, r in top_buy.iterrows():
            code = r.get('股票代码','?')
            name = r.get('股票名称','?')
            net = r['龙虎榜净买额'] / 1e4  # 转万元
            reason = str(r.get('解读',''))[:15]
            lines.append(f"  {code} {name}: +{net:.0f}万 | {reason}")
        # 机构买入汇总
        inst = dt[dt['解读'].astype(str).str.contains('机构', na=False)]
        if not inst.empty:
            lines.append(f"\n🏦 机构上榜: {len(inst)} 只")
            inst_top = inst.nlargest(3, '龙虎榜净买额')
            for _, r in inst_top.iterrows():
                code = r.get('股票代码','?')
                name = r.get('股票名称','?')
                net = r['龙虎榜净买额'] / 1e4
                lines.append(f"  {code} {name}: {net:+.0f}万")

    # 持仓
    try:
        from momentum.config import HOLDINGS_DEFAULT
        holds = {}
        for k, v in (HOLDINGS_DEFAULT or {}).items():
            holds[k] = v
        pos = check_positions(holds)
        if pos:
            lines.append("\n💼 持仓体检:")
            lines.extend(pos)
    except Exception:
        pass

    # 选股跟踪: 近5日收益
    try:
        import json, os
        track_file = 'data/picks_tracking.json'
        if os.path.exists(track_file):
            with open(track_file) as f: tracking = json.load(f)
            recent = [p for p in tracking if (datetime.now()-datetime.strptime(p['date'],'%Y-%m-%d')).days <= 5]
            if recent:
                lines.append('\n📈 近期选股跟踪:')
                for p in sorted(recent, key=lambda x: x['date'], reverse=True)[:8]:
                    code, entry, d = p['code'], p['price'], p['date']
                    perf = ''
                    try:
                        from momentum.data import load_or_fetch_kline, fetch_kline_from_api
                        df = load_or_fetch_kline(str(code), fetch_kline_from_api)
                        if df is not None and not df.empty:
                            df['close'] = pd.to_numeric(df['close'], errors='coerce')
                            df['high'] = pd.to_numeric(df['high'], errors='coerce')
                            df = df[df['trade_date'] >= d].reset_index(drop=True)
                            for i, (label, idx) in enumerate([('当日',0),('次日',1),('D2',2),('D3',3)]):
                                if idx < len(df):
                                    c = float(df['close'].iloc[idx]); h = float(df['high'].iloc[idx])
                                    rc = (c-entry)/entry*100; rh = (h-entry)/entry*100
                                    perf += f' {label}: {rc:+.1f}%高{rh:+.1f}%'
                    except: pass
                    lines.append(f'  {d} {code} {p["name"]}: ¥{entry:.2f}{perf}')
    except Exception:
        pass

    rpt = '\n'.join(lines)
    print(rpt)
    send_feishu(rpt)

    # 标记已推送 (防止重复)
    import os as _os
    try: _os.makedirs('data', exist_ok=True)
    except: pass
    try:
        with open(lock_file, 'w') as f: f.write(data_date)
    except: pass
