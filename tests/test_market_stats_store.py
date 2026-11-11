# -*- coding: utf-8 -*-
"""
market_stats_store 单元测试

背景: 小盘倾斜依赖 market_stats, 该表历史上从未入库分发, CI 基准库缺表导致
      "小盘倾斜静默失效数月"(2026-11-11)。这里锁死三条防线:
        1. data/market_stats.csv.gz 必须随仓库分发且可解析(缺文件=CI 必定失效);
        2. 空库可自举建表, 且幂等(重复调用不重复导入);
        3. 无源文件时必须返回明确失败原因(不静默)。
"""
import csv
import gzip
import sqlite3

import pytest

from ..tools import market_stats_store as MSS


@pytest.fixture(scope="module")
def csv_path():
    p = MSS.DEFAULT_CSV
    if not p.exists():
        pytest.fail(f"缺少随仓库分发的市值快照 {p} —— CI 的小盘倾斜会重新静默失效")
    return p


def test_csv_snapshot_available_and_parseable(csv_path):
    """快照文件存在、列齐全、覆盖月频多期且最新一期足够新(防止悄悄腐化)。"""
    rows = list(MSS._read_csv_rows(csv_path))
    assert len(rows) > 100_000, f"行数异常偏少: {len(rows)}"
    assert all(r[1] and len(r[1]) == 10 for r in rows[:1000]), "trade_date 格式应为 YYYY-MM-DD"

    snaps = sorted({r[1] for r in rows})
    assert len(snaps) >= 24, f"月频快照仅 {len(snaps)} 期, 疑似被截断"
    # 最新快照距今不超过 400 天(超过说明 CSV 长期未刷新, 需 --refresh 后重新导出)
    from datetime import date, datetime
    age = (date.today() - datetime.strptime(snaps[-1], "%Y-%m-%d").date()).days
    assert age <= 400, f"最新快照 {snaps[-1]} 距今 {age} 天, 需刷新并重新导出 CSV"

    circ = [r[3] for r in rows if r[3]]
    assert circ, "circ_mv 全为空"
    assert min(circ) > 0 and max(circ) > 1e8, "circ_mv 量纲异常(应为元)"


def test_bootstrap_into_empty_db(tmp_path, csv_path):
    """空库(无 market_stats 表)应能从 CSV 自举, 且二次调用幂等。"""
    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()

    assert MSS.count_rows(str(db)) == 0
    r1 = MSS.ensure_market_stats(str(db))
    assert r1["ok"] and r1["imported"], r1
    assert r1["rows"] > 100_000

    r2 = MSS.ensure_market_stats(str(db))
    assert r2["ok"] and r2["imported"] is False, "重复导入说明幂等被破坏"

    st = MSS.status(str(db))
    assert st["available"] and st["snapshots"] >= 24
    assert st["latest"] >= "2026-07-01"

    # 建表结构需与 harness 读取口径一致
    con = sqlite3.connect(str(db))
    cols = {r[1] for r in con.execute("PRAGMA table_info(market_stats)")}
    con.close()
    assert {"code", "trade_date", "total_mv", "circ_mv", "dividend_yield"} == cols


def test_missing_source_fails_explicitly(tmp_path):
    """表缺失且无快照文件时, 必须返回可读失败原因, 而不是抛异常/静默通过。"""
    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()
    r = MSS.ensure_market_stats(str(db), csv_file=str(tmp_path / "nope.csv"))
    assert r["ok"] is False
    assert "表缺失" in r["msg"] and "csv" in r["msg"].lower()


def test_status_on_missing_table_is_safe(tmp_path):
    """表不存在时 status()/count_rows() 不得抛异常(CI 冷启动路径)。"""
    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()
    assert MSS.count_rows(str(db)) == 0
    st = MSS.status(str(db))
    assert st["available"] is False and st["rows"] == 0


def test_export_roundtrip(tmp_path, csv_path):
    """DB -> CSV(.gz) -> DB 往返无损(保证分发文件可重建)。"""
    db = tmp_path / "bare.db"
    sqlite3.connect(str(db)).close()
    MSS.ensure_market_stats(str(db))
    before = MSS.status(str(db))

    out = tmp_path / "dump.csv.gz"
    r = MSS.export_csv(str(out), db=str(db))
    assert r["ok"] and out.exists()
    with gzip.open(out, "rt", encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))
    assert header == ["code", "trade_date", "total_mv", "circ_mv", "dividend_yield"]

    db2 = tmp_path / "bare2.db"
    sqlite3.connect(str(db2)).close()
    r2 = MSS.ensure_market_stats(str(db2), csv_file=str(out))
    assert r2["ok"] and r2["rows"] == before["rows"]
