# -*- coding: utf-8 -*-
"""监控日报窗口裁剪 + 归档 单元测试

背景: 策略表现监控日报长期把所有历史记录一次性列出, 记录越积越多后单条 Bark
     装不下(被拆成 2~3 条, 甚至 PayloadTooLarge 整条丢失)。改造口径:
        只列最新 N 条, N = min(10, 最近 5 个选股日的记录数);
        其余移入 stock_picks_archive 且只在报告里留一行累计统计;
        整份报告必须落在一条 Bark 消息内。
这里锁死这四条行为, 防止以后回退成"全量罗列"。
"""
import json

import pytest

from ..tools import stock_picks_tracker as ST
from ..notify.bark import _split_utf8


def _mk_db(path, days, per_day=3, start_day=1):
    """构造 stock_picks 测试库: days 个选股日 × per_day 只(全部已攒满 D+3)。"""
    from datetime import date, timedelta
    ST.DB_PATH = str(path)
    ST._init_tables()
    import sqlite3
    conn = sqlite3.connect(str(path))
    base = date(2026, 10, 1)
    for d in range(days):
        date_s = (base + timedelta(days=d)).strftime('%Y-%m-%d')
        for k in range(per_day):
            conn.execute(
                '''INSERT INTO stock_picks
                   (date, code, name, price, status, type, track_status, track_count,
                    day1_pnl, day2_pnl, day3_pnl, max_pnl_3d, sl_triggered, sl_recovery)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (date_s, f'{600000 + d * 10 + k:06d}', f'股{d}-{k}', 10.0, 'WATCHING',
                 'STRATEGY', 'FINISHED', 3, 1.0, 2.0, 3.0, 5.0 + k, k % 2, 0.0))
    conn.commit()
    conn.close()


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(ST, 'ARCHIVE_STATS_FILE', tmp_path / 'archive_stats.json')
    monkeypatch.setattr(ST, 'DB_PATH', str(tmp_path / 'test.db'))
    return tmp_path


def test_window_and_cap(env):
    """12 个选股日×3 只: 窗口只取最近 5 日(15 条), 报告只列 10 条。"""
    _mk_db(env / 'test.db', days=12, per_day=3)
    import sqlite3
    conn = sqlite3.connect(ST.DB_PATH)
    conn.row_factory = sqlite3.Row
    sel = ST._report_selection(conn.cursor())
    conn.close()
    assert len(sel['window_dates']) == 5
    assert sel['window_rows'] == 15          # 5 日 × 3 只
    assert len(sel['keep']) == 10            # 上限 10
    assert sel['tracked_total'] == 36


def test_window_smaller_than_cap(env):
    """只有 2 个选股日×3 只: 窗口内 6 条 < 10, 以数量少的为准。"""
    _mk_db(env / 'test.db', days=2, per_day=3)
    import sqlite3
    conn = sqlite3.connect(ST.DB_PATH)
    conn.row_factory = sqlite3.Row
    sel = ST._report_selection(conn.cursor())
    conn.close()
    assert sel['window_rows'] == 6
    assert len(sel['keep']) == 6


def test_archive_moves_out_of_window(env):
    """窗口外的记录被迁到归档表, 活动表只剩保留集。"""
    _mk_db(env / 'test.db', days=12, per_day=3)
    stats = ST.archive_old_picks()
    assert stats['n'] == 26                   # 36 - 10 保留
    import sqlite3
    conn = sqlite3.connect(ST.DB_PATH)
    left = conn.execute('SELECT COUNT(*) FROM stock_picks WHERE track_count>0').fetchone()[0]
    arch = conn.execute('SELECT COUNT(*) FROM stock_picks_archive').fetchone()[0]
    conn.close()
    assert left == 10 and arch == 26

    entries = json.loads(ST.ARCHIVE_STATS_FILE.read_text(encoding='utf-8'))
    assert len(entries) == 1
    e = entries[0]
    assert e['n'] == 26 and e['with_3d'] == 26
    assert 0 <= e['win'] <= 26
    assert e['date_from'] <= e['date_to']


def test_tracking_incomplete_is_protected(env):
    """未攒满 D+3 的近期记录不归档(否则 T+1~T+3 统计永久缺失)。"""
    _mk_db(env / 'test.db', days=12, per_day=3)
    import sqlite3
    from datetime import datetime
    conn = sqlite3.connect(ST.DB_PATH)
    conn.execute(
        '''INSERT INTO stock_picks (date, code, name, price, type, track_status, track_count,
                                    day1_pnl, max_pnl_3d)
           VALUES (?,?,?,?,?,?,?,?,?)''',
        (datetime.now().strftime('%Y-%m-%d'), '000001', '跟踪中', 10.0, 'STRATEGY',
         'TRACKING', 1, 0.5, 0.5))
    conn.commit()
    conn.close()

    ST.archive_old_picks()
    conn = sqlite3.connect(ST.DB_PATH)
    in_active = conn.execute("SELECT COUNT(*) FROM stock_picks WHERE code='000001'").fetchone()[0]
    in_archive = conn.execute(
        "SELECT COUNT(*) FROM stock_picks_archive WHERE code='000001'").fetchone()[0]
    conn.close()
    assert in_active == 1 and in_archive == 0


def test_report_fits_single_bark(env):
    """大量记录下报告仍不超过单条上限, 且只列上限内条数。"""
    _mk_db(env / 'test.db', days=40, per_day=8)
    ST.archive_old_picks()
    lines = ST.generate_report()
    text = '\n'.join(lines)
    assert len(text.encode('utf-8')) <= 3500
    assert len(_split_utf8(text)) == 1, '监控日报必须能装进一条 Bark 消息'
    assert '归档' in text


def test_report_size_budget_trims_rows(env, monkeypatch):
    """把预算压到很小: 应裁掉最旧的条目并给出说明, 不允许超限。"""
    _mk_db(env / 'test.db', days=3, per_day=6)
    monkeypatch.setattr(ST, 'REPORT_MAX_BYTES', 1100)
    lines = ST.generate_report()
    text = '\n'.join(lines)
    assert len(text.encode('utf-8')) <= 1100
    assert '超单条消息上限未列' in text


def test_empty_table_returns_no_report(env):
    """无任何 tracking 记录 → 空列表(调用方据此跳过推送)。"""
    _mk_db(env / 'test.db', days=0)
    assert ST.generate_report() == []


def test_archive_is_idempotent_against_resurrection(env):
    """归档后再被同步回来的同一条记录: 只删除, 不重复计入归档统计。"""
    _mk_db(env / 'test.db', days=12, per_day=3)
    first = ST.archive_old_picks()
    assert first['n'] == 26
    entries = json.loads(ST.ARCHIVE_STATS_FILE.read_text(encoding='utf-8'))
    assert sum(e['n'] for e in entries) == 26

    # 模拟 json 把这些老记录又同步回活动表
    import sqlite3
    conn = sqlite3.connect(ST.DB_PATH)
    rows = conn.execute('SELECT * FROM stock_picks_archive').fetchall()
    cols = [d[0] for d in conn.execute('SELECT * FROM stock_picks_archive LIMIT 0').description]
    idx = {c: i for i, c in enumerate(cols)}
    for r in rows[:5]:
        conn.execute(
            '''INSERT INTO stock_picks (date, code, name, price, status, type,
                                        track_status, track_count, max_pnl_3d)
               VALUES (?,?,?,?,?,?,?,?,?)''',
            (r[idx['date']], r[idx['code']], r[idx['name']], r[idx['price']],
             'WATCHING', r[idx['type']], 'FINISHED', 3, 5.0))
    conn.commit()
    conn.close()

    second = ST.archive_old_picks()
    assert second['n'] == 0, '回流记录不应再次计入归档统计'
    entries = json.loads(ST.ARCHIVE_STATS_FILE.read_text(encoding='utf-8'))
    assert sum(e['n'] for e in entries) == 26, '归档统计被重复累加'

    conn = sqlite3.connect(ST.DB_PATH)
    left = conn.execute("SELECT COUNT(*) FROM stock_picks WHERE code IN "
                        "(SELECT code FROM stock_picks_archive)").fetchone()[0]
    conn.close()
    assert left == 0, '回流记录应从活动表删除'


def test_sync_skips_already_archived(env, monkeypatch):
    """picks_tracking.json 里残留的已归档记录不再被同步回活动表。"""
    _mk_db(env / 'test.db', days=12, per_day=3)
    ST.archive_old_picks()
    import sqlite3
    conn = sqlite3.connect(ST.DB_PATH)
    archived = conn.execute(
        'SELECT date, code, name FROM stock_picks_archive LIMIT 3').fetchall()
    conn.close()

    monkeypatch.chdir(env)
    (env / 'data').mkdir(exist_ok=True)
    payload = [{'date': d, 'code': c, 'name': n, 'price': 10.0, 'type': 'STRATEGY'}
               for d, c, n in archived]
    (env / 'data' / 'picks_tracking.json').write_text(
        json.dumps(payload, ensure_ascii=False), encoding='utf-8')

    assert ST.sync_picks_to_db() == 0
    conn = sqlite3.connect(ST.DB_PATH)
    back = conn.execute(
        f"SELECT COUNT(*) FROM stock_picks WHERE code IN ({','.join('?' * len(archived))})",
        [c for _, c, _ in archived]).fetchone()[0]
    conn.close()
    assert back == 0


def test_push_report_sends_exactly_one_bark(env, monkeypatch):
    """push_report 只调用一次推送(消息不拆分), 内容不超过单条上限。"""
    _mk_db(env / 'test.db', days=40, per_day=8)
    monkeypatch.setattr(ST, 'sync_picks_to_db', lambda: 0)
    monkeypatch.setattr(ST, 'update_tracking_stocks', lambda: 0)

    calls = []
    assert ST.push_report(sender=lambda title, content: calls.append((title, content))) is True
    assert len(calls) == 1, f'应为 1 条 Bark, 实际 {len(calls)} 条'
    title, content = calls[0]
    assert title == '策略表现监控日报'
    assert len(content.encode('utf-8')) <= 3500
    assert len(_split_utf8(content)) == 1
