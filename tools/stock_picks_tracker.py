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

# ---- 日报裁剪 / 归档 (2026-11-13) ----
# 诉求: 日报只列最新的一小段, 其余记录移出活动表并归档统计, 整份报告落进一条 Bark。
#   保留条数 N = min(REPORT_MAX_ROWS, 最近 REPORT_WINDOW_DATES 个选股日的记录数)
#   其余 → stock_picks_archive(归档表) + data/monitor_archive_stats.json(归档统计)
REPORT_MAX_ROWS = int(os.environ.get('MONITOR_REPORT_MAX_ROWS', '10'))
REPORT_WINDOW_DATES = int(os.environ.get('MONITOR_REPORT_WINDOW_DATES', '5'))
# Bark 单条上限 3500 字节(见 notify/bark.MAX_BODY_BYTES), 留 200 字节余量给标题/分隔
REPORT_MAX_BYTES = int(os.environ.get('MONITOR_REPORT_MAX_BYTES', '3300'))
# 仍在跟踪中(未攒满 D+3)的记录不归档, 否则这几只的 T+1~T+3 统计会永久缺失
TRACKING_GRACE_DAYS = int(os.environ.get('MONITOR_TRACKING_GRACE_DAYS', '10'))
ARCHIVE_STATS_FILE = _PROJ / 'data' / 'monitor_archive_stats.json'

# 归档表列顺序(与 stock_picks 同名同序), 迁移时按此显式列举, 避免依赖 SELECT *
ARCHIVE_COLS = (
    'date', 'code', 'name', 'price', 'status', 'sl_price', 'tp_price', 'type',
    'exit_price', 'pnl_pct', 'trigger_type', 'trigger_time', 'pnl_ratio',
    'track_status', 'track_count', 'day1_pnl', 'day2_pnl', 'day3_pnl',
    'max_pnl_3d', 'sl_triggered', 'sl_recovery',
)


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


# ===================== 窗口裁剪 + 归档 =====================

def _ensure_archive_table(conn):
    """建归档表(结构与 stock_picks 同构 + archived_at)。幂等。"""
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_picks_archive'"
    ).fetchone()
    if not exists:
        # 用 0 行拷贝继承列定义, 避免手写 DDL 与主表漂移
        conn.execute('CREATE TABLE stock_picks_archive AS SELECT * FROM stock_picks WHERE 0')
        conn.execute('ALTER TABLE stock_picks_archive ADD COLUMN archived_at TEXT')
        conn.commit()


def _report_selection(cur):
    """决定报告保留集。

    Returns:
        dict(window_dates=list, window_rows=int, keep=list[Row], tracked_total=int)
        keep 已是「最新在前」且 <= REPORT_MAX_ROWS。
    """
    window_dates = [r[0] for r in cur.execute(
        'SELECT DISTINCT date FROM stock_picks WHERE track_count > 0 '
        'ORDER BY date DESC LIMIT ?', (REPORT_WINDOW_DATES,))]
    rows = cur.execute(
        'SELECT id, code, name, date, price, track_count, track_status, '
        'day1_pnl, day2_pnl, day3_pnl, max_pnl_3d, sl_triggered, sl_recovery, type '
        'FROM stock_picks WHERE track_count > 0 ORDER BY date DESC, code'
    ).fetchall()
    window = set(window_dates)
    in_window = [r for r in rows if r[3] in window]
    return dict(window_dates=window_dates,
                window_rows=len(in_window),
                keep=in_window[:REPORT_MAX_ROWS],
                tracked_total=len(rows))


def _still_tracking(row_date, track_status, track_count):
    """未攒满 D+3 且在宽限期内的记录: 必须留在活动表继续算完, 只是不列进报告。"""
    if track_status != 'TRACKING' or (track_count or 0) >= 3:
        return False
    try:
        d0 = datetime.strptime(str(row_date)[:10], '%Y-%m-%d').date()
    except Exception:
        return False
    return (datetime.now().date() - d0).days <= TRACKING_GRACE_DAYS


def _archive_stats(moved):
    """归档批次的统计口径(与报告口径一致: 3D最高为正 / 均值 / 止损后恢复)。"""
    n = len(moved)
    if not n:
        return dict(n=0)
    mx = [m['max_pnl_3d'] for m in moved if m['max_pnl_3d'] is not None]
    d3 = [m['day3_pnl'] for m in moved if m['day3_pnl'] is not None]
    win = sum(1 for v in mx if v > 0)
    sl = sum(1 for m in moved if m['sl_triggered'])
    sl_rec = sum(1 for m in moved if m['sl_triggered'] and (m['sl_recovery'] or 0) > 0)
    dates = sorted({str(m['date'])[:10] for m in moved if m['date']})
    return dict(
        n=n,
        date_from=dates[0] if dates else '',
        date_to=dates[-1] if dates else '',
        win=win,
        avg_max3d=round(sum(mx) / len(mx), 2) if mx else None,
        avg_d3=round(sum(d3) / len(d3), 2) if d3 else None,
        sl=sl,
        sl_recover=sl_rec,
        with_3d=len(mx),
    )


def _read_archive_stats():
    """读取累积归档统计(跨 DB 缓存丢失仍然有效)。"""
    try:
        with open(ARCHIVE_STATS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _append_archive_stats(stats, run_date=None):
    """把本次归档统计追加到 data/monitor_archive_stats.json 并回写(按 run_date 去重合并)。"""
    if not stats or not stats.get('n'):
        return
    run_date = run_date or datetime.now().strftime('%Y-%m-%d')
    entries = _read_archive_stats()
    entry = dict(stats, run_date=run_date)
    # 同日多次运行: 合并为一条(累加条数/加权均值)
    for i, e in enumerate(entries):
        if e.get('run_date') == run_date:
            total = (e.get('n') or 0) + entry['n']
            if total:
                for k, w in (('avg_max3d', 'with_3d'), ('avg_d3', 'with_3d')):
                    a = (e.get(k) or 0) * (e.get(w) or 0)
                    b = (entry.get(k) or 0) * (entry.get(w) or 0)
                    entry[k] = round((a + b) / total, 2) if total else None
            for k in ('n', 'win', 'sl', 'sl_recover', 'with_3d'):
                entry[k] = (e.get(k) or 0) + (entry.get(k) or 0)
            entry['date_from'] = min(filter(None, [e.get('date_from'), entry.get('date_from')]),
                                     default='')
            entry['date_to'] = max(filter(None, [e.get('date_to'), entry.get('date_to')]),
                                   default='')
            entries[i] = entry
            break
    else:
        entries.append(entry)
    try:
        ARCHIVE_STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(ARCHIVE_STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning('[Tracker] 归档统计写入失败: %s', e)


def archive_old_picks():
    """把不在报告保留集内的记录移入 stock_picks_archive 并登记归档统计。

    保留: 报告列出的 keep 集 + 仍在跟踪中的少量记录(见 _still_tracking)
    其余: 迁移到归档表后从活动表删除(数据不丢, 只是不再进入日报)
    """
    conn = _get_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    _ensure_archive_table(conn)

    sel = _report_selection(cur)
    keep_ids = {r['id'] for r in sel['keep']}
    rows = cur.execute(
        f"SELECT id, {', '.join(ARCHIVE_COLS)} FROM stock_picks "
        "WHERE track_count > 0 ORDER BY date DESC, code").fetchall()

    moved, kept_tracking = [], []
    for r in rows:
        if r['id'] in keep_ids:
            continue
        if _still_tracking(r['date'], r['track_status'], r['track_count']):
            kept_tracking.append(r['code'])
            continue
        moved.append(r)

    if not moved:
        conn.close()
        logger.info('[Tracker] 无需归档(活动表 %d 条, 报告保留 %d 条)',
                    sel['tracked_total'], len(sel['keep']))
        return dict(n=0)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    placeholders = ','.join('?' * (len(ARCHIVE_COLS) + 1))
    cur.executemany(
        f"INSERT INTO stock_picks_archive ({', '.join(ARCHIVE_COLS)}, archived_at) "
        f"VALUES ({placeholders})",
        [tuple(r[c] for c in ARCHIVE_COLS) + (now,) for r in moved])
    cur.executemany('DELETE FROM stock_picks WHERE id=?', [(r['id'],) for r in moved])
    conn.commit()

    stats = _archive_stats(moved)
    _append_archive_stats(stats)
    conn.close()
    logger.info('[Tracker] 归档 %d 条(%s~%s) → stock_picks_archive; 活动表余 %d 条, '
                '其中跟踪中未满 D+3 保留 %d 条',
                stats['n'], stats['date_from'], stats['date_to'],
                sel['tracked_total'] - stats['n'], len(kept_tracking))
    return stats


def _archive_footer_line():
    """归档累计统计一行(来自 data/monitor_archive_stats.json, 不依赖 DB 缓存存活)。"""
    entries = _read_archive_stats()
    n = sum(e.get('n') or 0 for e in entries)
    if not n:
        return None
    win = sum(e.get('win') or 0 for e in entries)
    with_3d = sum(e.get('with_3d') or 0 for e in entries)
    sl = sum(e.get('sl') or 0 for e in entries)
    sl_rec = sum(e.get('sl_recover') or 0 for e in entries)
    # 加权平均 3D 最高涨幅
    tot_mx = sum((e.get('avg_max3d') or 0) * (e.get('with_3d') or 0) for e in entries)
    avg_mx = tot_mx / with_3d if with_3d else 0
    d_from = min((e.get('date_from') for e in entries if e.get('date_from')), default='')
    d_to = max((e.get('date_to') for e in entries if e.get('date_to')), default='')
    span = f'{d_from[5:]}~{d_to[5:]}' if d_from and d_to else ''
    win_rate = win / with_3d * 100 if with_3d else 0
    line = (f'🗄 归档 {n} 条{("(" + span + ")") if span else ""}: '
            f'3D胜率 {win_rate:.0f}% 均高 {avg_mx:+.1f}%')
    if sl:
        line += f' | 止损 {sl} 只(洗盘后涨 {sl_rec / sl * 100:.0f}%)'
    return line


def generate_report(selection=None):
    """生成监控日报 — 只列最新 N 条, 其余归档统计, 整体保证单条 Bark 可容纳。

    保留条数 N = min(REPORT_MAX_ROWS, 最近 REPORT_WINDOW_DATES 个选股日的记录数)。
    超出 N 条的部分不再逐条列出, 只体现在归档统计行里。

    Args:
        selection: 归档前的 _report_selection() 结果(用于如实展示窗口口径);
                   为空则就地重算(此时活动表已被归档, 窗口信息会偏小)。
    """

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
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sel = selection or _report_selection(cur)
    rows = sel['keep']
    conn.close()

    if not rows:
        return []

    # 类型标识映射
    TYPE_ICON = {'STRATEGY': '🎯', 'MANUAL': '📋', 'LEADER': '🐉',
                 'LOW_QUALITY': '📉', 'C_TAIL': '🆑'}

    today = datetime.now().strftime('%m-%d')
    sep = '━' * 56
    head = [f'📊 策略表现监控日报 | {today}',
            f'近{len(sel["window_dates"])}个选股日 {sel["window_rows"]} 条 → 列最新 {len(rows)} 条', sep,
            f'{_pad_str("名称", 16)}{_pad_str("标识", 6)}{_pad_str("T+1", 8)}'
            f'{_pad_str("T+2", 8)}{_pad_str("T+3", 8)}{_pad_str("3D最高", 8)}'
            f'{_pad_str("止损", 6)}状态', sep]

    body, up_count, sl_count, sl_recover_count = [], 0, 0, 0
    for r in rows:
        name, status = r['name'], r['track_status']
        d1, d2, d3, mx = r['day1_pnl'], r['day2_pnl'], r['day3_pnl'], r['max_pnl_3d']
        sl, sl_rec = r['sl_triggered'], r['sl_recovery']
        done = ' ✅' if status == 'FINISHED' else '    '
        mx_val = mx if mx and mx != 0 else None
        if mx_val and mx_val > 0:
            up_count += 1
        icon = TYPE_ICON.get(r['type'], '📌')
        if sl:
            sl_count += 1
            if sl_rec is not None and sl_rec > 0:
                sl_recover_count += 1
                sl_mark = ' 📈↑'
            else:
                sl_mark = ' 📉✗'
        else:
            sl_mark = '   - '
        body.append(f'{_pad_str(name, 16)}{_pad_str(icon, 6)}'
                    f'{_pct_fmt(d1)}{_pct_fmt(d2)}{_pct_fmt(d3)}{_pct_fmt(mx_val)}'
                    f'{_pad_str(sl_mark, 6)}{done}')

    total = len(body)
    rate = up_count / total * 100 if total else 0
    tail = [sep, f'💹 3D最高为正: {up_count}/{total} ({rate:.0f}%)']
    if sl_count:
        tail.append(f'🛡 止损触发: {sl_count}/{total}  📈 洗盘后涨: {sl_recover_count}/{sl_count}')
    arch = _archive_footer_line()
    if arch:
        tail.append(arch)

    # ---- 单条消息预算: 从最新往最旧贪心装填, 装不下的不再列出 ----
    # 预留 80 字节给"另 N 条未列"提示, 保证加了提示也不会顶破上限。
    legend = '🎯 策略 ｜ 📉 低位绩优 ｜ 🆑 C尾盘 ｜ 🐉 龙头 ｜ 📋 手动持仓'
    fixed = head + tail + [legend]

    def _size(rows, reserve=0):
        return len('\n'.join(fixed + rows).encode('utf-8')) + reserve

    kept = []
    for i, line in enumerate(body):
        remaining = len(body) - i - 1
        if _size(kept + [line], reserve=80 if remaining else 0) > REPORT_MAX_BYTES:
            break
        kept.append(line)
    dropped = len(body) - len(kept)
    if dropped:
        kept.append(f'…另 {dropped} 条超单条消息上限未列(已归档统计)')
    lines = head + kept + tail + [legend]
    body = kept

    nbytes = len('\n'.join(lines).encode('utf-8'))
    logger.info('[Tracker] 报告 %d 行 / %d 字节 (上限 %d, 列出 %d/%d 条)',
                len(lines), nbytes, REPORT_MAX_BYTES, total - dropped, sel['tracked_total'])
    if nbytes > 3500:
        logger.warning('[Tracker] 报告仍超 Bark 单条上限(%d>3500), 将被拆条', nbytes)
    return lines


def run(archive=True):
    """主入口: 同步 + 更新 + 归档裁剪 + 返回报告文本行列表.

    Args:
        archive: 是否把窗口外记录迁入归档表(默认 True)。

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

    # 3. 归档裁剪: 报告窗口外的记录移出活动表(数据进归档表, 不再逐条列出)
    sel = None
    if archive:
        try:
            # 先按归档前的数据算出窗口口径, 报告里才能如实写"N 个选股日 X 条 → 列 N 条"
            _conn = _get_db()
            _conn.row_factory = sqlite3.Row
            sel = _report_selection(_conn.cursor())
            _conn.close()
            archive_old_picks()
        except Exception as e:
            logger.warning("[Tracker] 归档失败(继续生成报告): %s", e)

    # 4. 生成报告
    report_lines = generate_report(selection=sel)
    if report_lines:
        print('\n'.join(report_lines))
    else:
        logger.info("[Tracker] No tracking data to report")

    return report_lines


def push_report(sender=None):
    """生成并推送监控日报 — 全流程只发一条 Bark 消息。

    报告已按 REPORT_MAX_BYTES 裁剪, 正常情况下只会发出 1 条;
    若真超过 Bark 单条上限会记 WARNING 并拆条(兜底, 不应发生)。

    Args:
        sender: 推送函数 (title, content) -> None。默认用 momentum.notify.bark.send_bark;
                测试可注入桩函数(避免依赖 momentum/tools 两套包命名空间的模块同一性)。
    """
    lines = run()
    if not lines:
        logger.info("[Tracker] 无监控日报内容, 跳过推送")
        return False
    text = '\n'.join(lines)
    if sender is None:
        from momentum.notify.bark import send_bark as sender
    sender('策略表现监控日报', text)
    logger.info("[Tracker] 监控日报已推送: %d 行 / %d 字节", len(lines),
                len(text.encode('utf-8')))
    return True


if __name__ == '__main__':
    run()
