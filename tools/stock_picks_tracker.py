# -*- coding: utf-8 -*-
"""选股后验跟踪 (D0-D3) — 每日 16:00 盘后更新

功能:
  1. sync_picks_to_db()  — picks_tracking.json → stock_picks 表同步 (增量)
  2. update_tracking_stocks()  — 获取 K 线，计算 T+1/T+2/T+3 涨跌幅与最高涨幅
  3. generate_report()  — 生成 Bark 兼容的文本报告
  4. run()  — 入口，串联上述步骤并推送
"""
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('stock_picks_tracker')

DB_PATH = None


def _get_db():
    """获取 SQLite 连接，复用已有数据库路径."""
    global DB_PATH
    if DB_PATH is None:
        try:
            from momentum import config as cfg
            DB_PATH = cfg.DB_PATH
        except Exception:
            DB_PATH = 'qlib_pro_v16.db'
    return sqlite3.connect(DB_PATH)


def _trading_days_after(start_date: str, max_days: int = 3):
    """返回 start_date 之后的交易日列表 (最多 max_days 个)."""
    try:
        import chinese_calendar as cc
        from datetime import date as _date
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        result = []
        d = start + timedelta(days=1)
        while len(result) < max_days and d <= datetime.now().date():
            if cc.is_workday(d):
                result.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        return result
    except ImportError:
        # 降级: 简单按自然日 +1/+2/+3 (跳过周六日)
        start = datetime.strptime(start_date, '%Y-%m-%d')
        result = []
        d = start + timedelta(days=1)
        while len(result) < max_days and d <= datetime.now():
            if d.weekday() < 5:
                result.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        return result


def sync_picks_to_db():
    """从 picks_tracking.json 增量同步到 stock_picks 表 (按 date+code 去重)."""
    track_file = 'data/picks_tracking.json'
    if not os.path.exists(track_file):
        logger.warning(f"Tracking file not found: {track_file}")
        return 0

    with open(track_file, 'r', encoding='utf-8') as f:
        picks = json.load(f)

    if not picks:
        return 0

    conn = _get_db()
    cursor = conn.cursor()
    inserted = 0

    for p in picks:
        date = p.get('date', '')
        code = p.get('code', '')
        if not date or not code:
            continue

        # 按 date + code 去重
        cursor.execute(
            'SELECT id FROM stock_picks WHERE date=? AND code=?',
            (date, code)
        )
        if cursor.fetchone():
            continue

        cursor.execute('''
            INSERT INTO stock_picks
            (date, code, name, price, status, sl_price, tp_price, type,
             exit_price, pnl_pct, trigger_type, trigger_time, pnl_ratio,
             track_status, track_count, day1_pnl, day2_pnl, day3_pnl, max_pnl_3d)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            date,
            code,
            p.get('name', ''),
            p.get('price', 0),
            p.get('status', 'WATCHING'),
            p.get('sl_price'),
            p.get('tp_price'),
            p.get('type', 'STRATEGY'),
            p.get('exit_price'),
            p.get('pnl_pct'),
            p.get('trigger_type'),
            p.get('trigger_time'),
            p.get('pnl_ratio'),
            'TRACKING',   # track_status
            0,            # track_count
            None,         # day1_pnl
            None,         # day2_pnl
            None,         # day3_pnl
            0.0,          # max_pnl_3d
        ))
        inserted += 1

    conn.commit()
    conn.close()
    if inserted:
        logger.info(f"[Tracker] Synced {inserted} new picks from picks_tracking.json")
    return inserted


def update_tracking_stocks():
    """更新所有 TRACKING 中股票的 D0-D3 跟踪指标."""
    conn = _get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''SELECT id, code, date, price, track_count, max_pnl_3d
           FROM stock_picks WHERE track_status = 'TRACKING' ORDER BY date'''
    )
    rows = cursor.fetchall()

    if not rows:
        conn.close()
        logger.info("[Tracker] No stocks in TRACKING status")
        return 0

    updated = 0
    for row in rows:
        try:
            db_id, code, pick_date, base_price, current_count, current_max = row
            current_max = current_max or 0.0

            # 获取选股日之后的交易日列表
            trading_days = _trading_days_after(pick_date, 3)
            if not trading_days:
                continue

            # 获取 K 线数据
            try:
                from momentum.data import load_or_fetch_kline, fetch_kline_from_api
                df = load_or_fetch_kline(str(code), fetch_kline_from_api)
            except Exception:
                from data.cache import load_or_fetch_kline
                from data.fetcher import fetch_kline_from_api
                df = load_or_fetch_kline(str(code), fetch_kline_from_api)

            if df is None or df.empty:
                logger.debug(f"[Tracker] No K-line data for {code}")
                continue

            import pandas as pd
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df = df[df['trade_date'].isin(trading_days)].sort_values('trade_date').reset_index(drop=True)

            if df.empty:
                continue

            max_high_pnl = current_max
            day1_pnl = day2_pnl = day3_pnl = None

            for i in range(min(len(df), 3)):
                row_data = df.iloc[i]
                high_val = float(row_data['high'])
                close_val = float(row_data['close'])

                high_pnl = (high_val - base_price) / base_price * 100
                if high_pnl > max_high_pnl:
                    max_high_pnl = high_pnl

                close_pnl = (close_val - base_price) / base_price * 100
                if i == 0:
                    day1_pnl = round(close_pnl, 2)
                elif i == 1:
                    day2_pnl = round(close_pnl, 2)
                elif i == 2:
                    day3_pnl = round(close_pnl, 2)

            new_count = len(df)
            status = 'FINISHED' if new_count >= 3 else 'TRACKING'

            cursor.execute('''
                UPDATE stock_picks
                SET track_count=?, track_status=?, day1_pnl=?, day2_pnl=?, day3_pnl=?, max_pnl_3d=?
                WHERE id=?
            ''', (new_count, status, day1_pnl, day2_pnl, day3_pnl, round(max_high_pnl, 2), db_id))
            updated += 1
            logger.debug(
                f"[Tracker] {code} {pick_date}: count={new_count} "
                f"day1={day1_pnl}% day2={day2_pnl}% day3={day3_pnl}% max={max_high_pnl:.2f}%"
            )

        except Exception as e:
            logger.warning(f"[Tracker] Failed to update {code}: {e}")
            continue

    conn.commit()
    conn.close()
    if updated:
        logger.info(f"[Tracker] Updated {updated} tracking records")
    return updated


def generate_report():
    """生成 Bark 兼容的文本报告."""
    conn = _get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''SELECT code, name, date, price, track_count, track_status,
                  day1_pnl, day2_pnl, day3_pnl, max_pnl_3d
           FROM stock_picks
           WHERE track_count > 0
           ORDER BY date DESC, code'''
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    lines = ['📈 选股跟踪 (D0-D3):', '─' * 40]
    for r in rows:
        code, name, date, price, count, status, d1, d2, d3, mx = r

        pnl_str = ''
        if d1 is not None:
            pnl_str += f' T+1: {d1:+.1f}%'
        if d2 is not None:
            pnl_str += f' T+2: {d2:+.1f}%'
        if d3 is not None:
            pnl_str += f' T+3: {d3:+.1f}%'

        max_str = f' 3D最高: {mx:+.1f}%' if mx else ''
        done = ' ✅' if status == 'FINISHED' else ''

        lines.append(f'  {date} {code} {name} ¥{price:.2f}{done}')
        lines.append(f'     {pnl_str}{max_str}')

    return lines


def run():
    """主入口: 同步 + 更新 + 返回报告文本行列表.

    Returns:
        list[str]: 报告文本行 (可直接拼接到 Bark 推送).
    """
    logger.info("[Tracker] Starting stock picks D0-D3 tracking...")

    # 1. 增量同步 picks_tracking.json → stock_picks 表
    sync_picks_to_db()

    # 2. 更新 TRACKING 中的股票指标
    update_tracking_stocks()

    # 3. 生成报告
    report_lines = generate_report()
    if report_lines:
        print('\n'.join(report_lines))
    else:
        logger.info("[Tracker] No tracking data to report")

    return report_lines


if __name__ == '__main__':
    run()
