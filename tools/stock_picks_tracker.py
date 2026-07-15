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
import sys
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# 引导 momentum 包别名 (兼容直接运行和 Actions 环境)
_PROJ = Path(__file__).resolve().parent.parent
_sys_parent = str(_PROJ.parent)
if _sys_parent not in sys.path:
    sys.path.insert(0, _sys_parent)
_sys_grandparent = str(_PROJ.parent.parent)
if _sys_grandparent not in sys.path:
    sys.path.insert(0, _sys_grandparent)
try:
    import momentum as _m  # noqa: F401
except ImportError:
    import importlib.util
    _init_file = _PROJ / '__init__.py'
    _spec = importlib.util.spec_from_file_location('momentum', _init_file, submodule_search_locations=[str(_PROJ)])
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        sys.modules['momentum'] = _mod
        _spec.loader.exec_module(_mod)

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


def _init_tables():
    """确保 stock_picks 表存在 (兼容旧缓存 DB 无此表)."""
    conn = _get_db()
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_picks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, code TEXT, name TEXT,
        price REAL, status TEXT, sl_price REAL, tp_price REAL, type TEXT,
        exit_price REAL, pnl_pct REAL, trigger_type TEXT, trigger_time TEXT,
        pnl_ratio REAL, track_status TEXT DEFAULT 'TRACKING',
        track_count INTEGER DEFAULT 0, day1_pnl REAL, day2_pnl REAL,
        day3_pnl REAL, max_pnl_3d REAL DEFAULT 0.0,
        sl_triggered INTEGER DEFAULT 0,
        sl_recovery REAL)''')
    # 兼容旧表: 新增列 (SQLite 不支持 ADD COLUMN IF NOT EXISTS, 用 try-except 兜底)
    for col, col_type in [('sl_triggered', 'INTEGER DEFAULT 0'), ('sl_recovery', 'REAL')]:
        try:
            conn.execute(f'ALTER TABLE stock_picks ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass  # 列已存在
    conn.commit()
    conn.commit()
    conn.close()


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
                df = None

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
            sl_triggered = 0
            sl_recovery = None

            for i in range(min(len(df), 3)):
                row_data = df.iloc[i]
                high_val = float(row_data['high'])
                low_val = float(row_data['low'])
                close_val = float(row_data['close'])

                high_pnl = (high_val - base_price) / base_price * 100
                if high_pnl > max_high_pnl:
                    max_high_pnl = high_pnl

                # 检测是否触发 -5% 止损 (盘中最低价触及)
                low_pnl = (low_val - base_price) / base_price * 100
                if low_pnl <= -5.0:
                    sl_triggered = 1

                close_pnl = (close_val - base_price) / base_price * 100
                if i == 0:
                    day1_pnl = round(close_pnl, 2)
                elif i == 1:
                    day2_pnl = round(close_pnl, 2)
                elif i == 2:
                    day3_pnl = round(close_pnl, 2)

            # 计算止损后恢复: 触发过止损且最终收盘价高于买入价时为"洗盘后涨"
            if sl_triggered:
                sl_recovery = round(max_high_pnl, 2)

            new_count = len(df)
            status = 'FINISHED' if new_count >= 3 else 'TRACKING'

            cursor.execute('''
                UPDATE stock_picks
                SET track_count=?, track_status=?, day1_pnl=?, day2_pnl=?, day3_pnl=?, max_pnl_3d=?,
                    sl_triggered=?, sl_recovery=?
                WHERE id=?
            ''', (new_count, status, day1_pnl, day2_pnl, day3_pnl, round(max_high_pnl, 2),
                  sl_triggered, sl_recovery, db_id))
            updated += 1
            logger.debug(
                f"[Tracker] {code} {pick_date}: count={new_count} "
                f"day1={day1_pnl}% day2={day2_pnl}% day3={day3_pnl}% max={max_high_pnl:.2f}% "
                f"sl={sl_triggered} rec={sl_recovery}"
            )

        except Exception as e:
            logger.warning(f"[Tracker] Failed to update {code}: {e}")
            continue

    conn.commit()
    conn.close()
    if updated:
        logger.info(f"[Tracker] Updated {updated} tracking records")
    return updated


def _display_width(s: str) -> int:
    """计算字符串显示宽度 (CJK/全角字符按 2 列宽)."""
    w = 0
    for c in str(s):
        cp = ord(c)
        if (0x1100 <= cp <= 0x115F or 0x2329 <= cp <= 0x232A or
            0x2E80 <= cp <= 0xA4CF or 0xA960 <= cp <= 0xA97C or
            0xAC00 <= cp <= 0xD7A3 or 0xF900 <= cp <= 0xFAFF or
            0xFE10 <= cp <= 0xFE19 or 0xFE30 <= cp <= 0xFE6F or
            0xFF01 <= cp <= 0xFF60 or 0xFFE0 <= cp <= 0xFFE6 or
            0x1F300 <= cp <= 0x1F64F or 0x1F680 <= cp <= 0x1F6FF or
            0x2600 <= cp <= 0x26FF or 0x2700 <= cp <= 0x27BF):
            w += 2
        else:
            w += 1
    return w


def _pad(s: str, width: int) -> str:
    """填充字符串到指定显示宽度."""
    dw = _display_width(str(s))
    if dw >= width:
        return str(s)
    return str(s) + ' ' * (width - dw)


def generate_report():
    """生成优化排版报告 — 去除代码字段, 增加类型标识, 单独卡片推送."""

    def _pad_str(s, width):
        """中英文混排填充到指定显示宽度."""
        w = _display_width(str(s))
        return str(s) + ' ' * (width - w) if w < width else str(s)

    def _pct_fmt(v, width=7):
        """格式化百分比 (None → '  -   ')."""
        if v is None:
            return _pad_str('  -   ', width)
        return _pad_str(f'{v:+.1f}%', width)

    conn = _get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''SELECT code, name, date, price, track_count, track_status,
                  day1_pnl, day2_pnl, day3_pnl, max_pnl_3d,
                  sl_triggered, sl_recovery, type
           FROM stock_picks
           WHERE track_count > 0
           ORDER BY date DESC, code'''
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    # 类型标识映射
    TYPE_ICON = {'STRATEGY': '🎯', 'MANUAL': '📋'}

    SEP = '━' * 56
    lines = ['📊 策略表现监控日报', SEP,
             f'{_pad_str("名称", 16)}{_pad_str("标识", 4)}{_pad_str("T+1", 8)}{_pad_str("T+2", 8)}{_pad_str("T+3", 8)}{_pad_str("3D最高", 8)}{_pad_str("止损", 6)}状态',
             SEP]

    up_count = 0
    sl_count = 0
    sl_recover_count = 0
    total = 0
    for r in rows:
        code, name, date, price, count, status, d1, d2, d3, mx, sl, sl_rec, ptype = r
        total += 1
        done = ' ✅' if status == 'FINISHED' else '    '
        mx_val = mx if mx and mx != 0 else None

        if mx_val and mx_val > 0:
            up_count += 1

        # 类型图标
        icon = TYPE_ICON.get(ptype, '📌')

        # 止损状态
        if sl:
            sl_count += 1
            if sl_rec is not None and sl_rec > 0:
                sl_recover_count += 1
                sl_mark = ' 📈↑'
            else:
                sl_mark = ' 📉✗'
        else:
            sl_mark = '   - '

        line = (f'{_pad_str(name, 16)}{_pad_str(icon, 4)}'
                f'{_pct_fmt(d1)}{_pct_fmt(d2)}{_pct_fmt(d3)}{_pct_fmt(mx_val)}{_pad_str(sl_mark, 6)}{done}')
        lines.append(line)

    rate = up_count / total * 100 if total > 0 else 0
    lines.append(SEP)
    lines.append(f'💹 3D最高为正: {up_count}/{total} ({rate:.0f}%)')
    if sl_count > 0:
        lines.append(f'🛡 止损触发: {sl_count}/{total}  📈 洗盘后涨: {sl_recover_count}/{sl_count}')
    lines.append(f'🎯 策略选股 ｜ 📋 模拟选股')

    return lines


def run():
    """主入口: 同步 + 更新 + 返回报告文本行列表.

    Returns:
        list[str]: 报告文本行 (可直接拼接到 Bark 推送).
    """
    logger.info("[Tracker] Starting stock picks D0-D3 tracking...")

    # 0. 确保表结构存在 (兼容旧缓存 DB)
    _init_tables()

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
