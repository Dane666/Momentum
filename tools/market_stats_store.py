# -*- coding: utf-8 -*-
"""
market_stats_store.py — 市值/股息率快照的"随仓库分发 + 自举 + 刷新"层
=====================================================================
背景(2026-11-11 事故)
----------------------
`tools/low_quality_scan.py` 的小盘倾斜依赖 `market_stats` 表(月频 流通市值分位)。
该表历史上只由 `opt_study/market_stats.py --backfill` 在本地生成, **从未入库分发**,
而 CI 用的基准库 `db-baseline-v1`(2026-07-23 打包)不含此表 → CI 每日 scanner 抛
`no such table: market_stats`, 被 `except` 吞成一行 warning, **小盘倾斜静默失效**
(报告里既无加分也无提示, 完全不可见)。本地因为库里有表, 一直是好的, 所以长期未察觉。

本模块解决三件事
----------------
1. **自举**: 表缺失时从随仓库分发的 `data/market_stats.csv.gz` 建表导入(幂等)。
   CI 每个 job 的库都是 release 下载 + actions/cache, 只要首次导入一次即可长期存在。
2. **自检**: `status()` 给出覆盖区间/最新快照/年龄(天), 供扫描报告暴露"快照是否偏旧"。
3. **刷新**: `refresh_from_tencent()` 用腾讯批量行情(每批 60 只, 全市场约 90 请求)
   追加/覆盖当日快照。腾讯字段 [44]=流通市值(亿) [45]=总市值(亿), 与 akshare
   口径交叉校验一致(002765: 43.08亿@07-01 × 7.80/7.23 = 46.5亿 ≈ 腾讯 46.48亿)。

用法
----
    python -m tools.market_stats_store --import          # 幂等导入(CI 用)
    python -m tools.market_stats_store --status          # 覆盖率/新鲜度
    python -m tools.market_stats_store --refresh         # 追加当日快照(腾讯)
    python -m tools.market_stats_store --refresh-if-stale 45   # 仅在偏旧时刷新
    python -m tools.market_stats_store --export-csv data/market_stats.csv.gz
"""
from __future__ import annotations

import csv
import gzip
import io
import logging
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("market_stats_store")

_PROJ = Path(__file__).resolve().parent.parent
DEFAULT_CSV = _PROJ / "data" / "market_stats.csv.gz"
# 快照年龄超过该天数即在报告/日志中提示"偏旧"(月频数据 + 1 个月容差)
STALE_DAYS = int(os.environ.get("MARKET_STATS_STALE_DAYS", "45"))

CREATE_SQL = """CREATE TABLE IF NOT EXISTS market_stats (
    code        TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    total_mv    REAL,
    circ_mv     REAL,
    dividend_yield REAL,
    PRIMARY KEY (code, trade_date)
)"""


# --------------------------------------------------------------------------- #
# 路径 / 基础查询
# --------------------------------------------------------------------------- #
def db_path(path: Optional[str] = None) -> str:
    if path:
        return str(path)
    env = os.environ.get("MOMENTUM_DB_PATH")
    if env:
        return env
    return str(_PROJ / "qlib_pro_v16.db")


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def count_rows(db: Optional[str] = None) -> int:
    """表不存在时返回 0(不抛异常), 便于上层判定是否需要自举。"""
    p = db_path(db)
    if not os.path.exists(p):
        return 0
    con = sqlite3.connect(p)
    try:
        if not _table_exists(con, "market_stats"):
            return 0
        return int(con.execute("SELECT COUNT(*) FROM market_stats").fetchone()[0])
    finally:
        con.close()


def status(db: Optional[str] = None) -> dict:
    """覆盖区间 / 最新快照 / 年龄(天)。无表或无数据时 available=False。"""
    p = db_path(db)
    out = dict(db=p, available=False, rows=0, codes=0, snapshots=0,
               first=None, latest=None, age_days=None, stale=True)
    if not os.path.exists(p):
        return out
    con = sqlite3.connect(p)
    try:
        if not _table_exists(con, "market_stats"):
            return out
        r = con.execute(
            "SELECT MIN(trade_date), MAX(trade_date), COUNT(*), "
            "COUNT(DISTINCT code), COUNT(DISTINCT trade_date) FROM market_stats"
        ).fetchone()
        if not r or not r[2]:
            return out
        first, latest, rows, codes, snaps = r
        age = (date.today() - datetime.strptime(latest, "%Y-%m-%d").date()).days
        out.update(available=True, rows=int(rows), codes=int(codes),
                   snapshots=int(snaps), first=first, latest=latest,
                   age_days=age, stale=age > STALE_DAYS)
        return out
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 1) 自举: CSV -> DB
# --------------------------------------------------------------------------- #
def _open_csv(path: Path):
    if str(path).endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return open(path, "r", encoding="utf-8", newline="")


def _read_csv_rows(path: Path) -> Iterable[Tuple]:
    with _open_csv(path) as f:
        rd = csv.DictReader(f)
        need = {"code", "trade_date"}
        if not need.issubset({(c or "").strip() for c in (rd.fieldnames or [])}):
            raise ValueError(f"CSV 缺少必需列 {need}: fieldnames={rd.fieldnames}")
        for r in rd:
            code = (r.get("code") or "").strip()
            td = (r.get("trade_date") or "").strip()
            if not code or not td:
                continue

            def num(k):
                v = (r.get(k) or "").strip()
                return float(v) if v else None

            yield (code, td, num("total_mv"), num("circ_mv"), num("dividend_yield"))


def ensure_market_stats(db: Optional[str] = None,
                        csv_file: Optional[str] = None,
                        force: bool = False) -> dict:
    """确保 market_stats 表存在且有数据。幂等: 表已有数据直接返回, 不重复导入。

    Returns: dict(ok, imported, rows, msg)
    """
    p = db_path(db)
    csv_path = Path(csv_file or os.environ.get("MARKET_STATS_CSV") or DEFAULT_CSV)
    res = dict(ok=False, imported=False, rows=0, msg="")

    if not os.path.exists(p):
        res["msg"] = f"DB 不存在: {p}"
        return res

    con = sqlite3.connect(p)
    try:
        if _table_exists(con, "market_stats"):
            n = int(con.execute("SELECT COUNT(*) FROM market_stats").fetchone()[0])
            if n and not force:
                res.update(ok=True, rows=n, msg=f"已存在 {n} 行")
                return res
        if not csv_path.exists():
            res["msg"] = (f"表缺失且无快照文件({csv_path}); "
                          f"请运行 python -m tools.market_stats_store --refresh "
                          f"或 opt_study/market_stats.py --backfill")
            return res

        con.execute(CREATE_SQL)
        if force:
            con.execute("DELETE FROM market_stats")
        buf: List[Tuple] = []
        imported = 0
        for rec in _read_csv_rows(csv_path):
            buf.append(rec)
            if len(buf) >= 20000:
                con.executemany(
                    "INSERT OR IGNORE INTO market_stats"
                    "(code,trade_date,total_mv,circ_mv,dividend_yield)"
                    " VALUES (?,?,?,?,?)", buf)
                imported += len(buf)
                buf.clear()
        if buf:
            con.executemany(
                "INSERT OR IGNORE INTO market_stats"
                "(code,trade_date,total_mv,circ_mv,dividend_yield)"
                " VALUES (?,?,?,?,?)", buf)
            imported += len(buf)
        con.commit()
        n = int(con.execute("SELECT COUNT(*) FROM market_stats").fetchone()[0])
        res.update(ok=n > 0, imported=imported, rows=n,
                   msg=f"从 {csv_path.name} 导入 {imported} 行(表内 {n} 行)")
        return res
    except Exception as e:  # noqa: BLE001 — 自举失败必须显式上报, 不静默
        res["msg"] = f"导入失败: {type(e).__name__}: {e}"
        return res
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 2) 刷新: 腾讯批量行情 -> 当日快照
# --------------------------------------------------------------------------- #
def _tx_symbol(code: str) -> Optional[str]:
    if code.startswith(("60", "68", "9")):
        return f"sh{code}"
    if code.startswith(("00", "30", "20")):
        return f"sz{code}"
    if code.startswith(("8", "4")):
        return f"bj{code}"
    return None


def _codes_from_db(db: str, limit: Optional[int] = None) -> List[str]:
    con = sqlite3.connect(db)
    try:
        rows = con.execute("SELECT DISTINCT code FROM kline_cache").fetchall()
    finally:
        con.close()
    codes = [r[0] for r in rows if r[0] and r[0][:2] in
             ("60", "68", "00", "30", "20")]
    codes.sort()
    return codes[:limit] if limit else codes


def _parse_tx_line(line: str) -> Optional[Tuple[str, str, float, float]]:
    """v_sh600000="1~浦发银行~600000~10.17~..."; -> (code, name, circ_mv, total_mv) 单位: 元"""
    if '="' not in line:
        return None
    body = line.split('="', 1)[1].rstrip('";\n ')
    parts = body.split("~")
    if len(parts) < 46:
        return None
    code = parts[2].strip()
    if not code:
        return None
    try:
        circ = float(parts[44]) * 1e8   # 亿元 -> 元
        total = float(parts[45]) * 1e8
    except (TypeError, ValueError):
        return None
    if circ <= 0 and total <= 0:
        return None
    return code, parts[1].strip(), circ, total


def refresh_from_tencent(db: Optional[str] = None,
                         trade_date: Optional[str] = None,
                         codes: Optional[List[str]] = None,
                         batch: int = 60,
                         limit: Optional[int] = None) -> dict:
    """拉取腾讯实时行情中的总/流通市值, 写入 trade_date(默认今日) 快照。

    腾讯 `qt.gtimg.cn` 稳定且支持批量(60 只/请求), 全市场约 90 请求 / ~30s。
    与 akshare(stock_value_em) 口径一致, 已交叉校验。
    """
    import requests

    p = db_path(db)
    td = trade_date or date.today().strftime("%Y-%m-%d")
    res = dict(ok=False, date=td, written=0, requested=0, msg="")

    if codes is None:
        if not os.path.exists(p):
            res["msg"] = f"DB 不存在: {p}"
            return res
        codes = _codes_from_db(p, limit)
    res["requested"] = len(codes)

    # 本地代理环境变量会让 qt.gtimg.cn 超时; 显式禁用
    try:
        from momentum.data.proxy_guard import disable_proxy
        disable_proxy()
    except Exception:
        for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
                  "all_proxy", "ALL_PROXY"):
            os.environ.pop(k, None)

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
    rows: List[Tuple] = []
    fails = 0
    for i in range(0, len(codes), batch):
        chunk = [s for s in (_tx_symbol(c) for c in codes[i:i + batch]) if s]
        if not chunk:
            continue
        try:
            r = sess.get("https://qt.gtimg.cn/q=" + ",".join(chunk), timeout=15)
            text = r.content.decode("gbk", "replace")
        except Exception:
            fails += 1
            continue
        for line in text.split("\n"):
            parsed = _parse_tx_line(line)
            if not parsed:
                continue
            code, _name, circ, total = parsed
            rows.append((code, td, total, circ, None))
    res["written"] = len(rows)
    res["failed_batches"] = fails
    if not rows:
        res["msg"] = f"腾讯未返回有效市值({len(codes)} 只 / {fails} 批失败)"
        return res
    if not os.path.exists(p):
        res["msg"] = f"DB 不存在: {p}"
        return res
    con = sqlite3.connect(p)
    try:
        con.execute(CREATE_SQL)
        con.executemany(
            "INSERT OR REPLACE INTO market_stats"
            "(code,trade_date,total_mv,circ_mv,dividend_yield) VALUES (?,?,?,?,?)",
            rows)
        con.commit()
    finally:
        con.close()
    res.update(ok=True, msg=f"{td} 写入 {len(rows)} 只 (失败批 {fails})")
    return res


# --------------------------------------------------------------------------- #
# 3) 导出: DB -> CSV(.gz)
# --------------------------------------------------------------------------- #
def export_csv(out: Optional[str] = None, db: Optional[str] = None) -> dict:
    p = db_path(db)
    target = Path(out or DEFAULT_CSV)
    target.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p)
    try:
        if not _table_exists(con, "market_stats"):
            return dict(ok=False, rows=0, path=str(target), msg="表不存在")
        rows = con.execute(
            "SELECT code,trade_date,total_mv,circ_mv,dividend_yield "
            "FROM market_stats ORDER BY trade_date, code").fetchall()
    finally:
        con.close()
    if target.suffix == ".gz":
        fh = gzip.open(target, "wt", encoding="utf-8", newline="")
    else:
        fh = open(target, "w", encoding="utf-8", newline="")
    with fh:
        w = csv.writer(fh)
        w.writerow(["code", "trade_date", "total_mv", "circ_mv", "dividend_yield"])
        for r in rows:
            w.writerow([r[0], r[1],
                        "" if r[2] is None else f"{r[2]:.2f}",
                        "" if r[3] is None else f"{r[3]:.2f}",
                        "" if r[4] is None else r[4]])
    size = target.stat().st_size / 1e6
    return dict(ok=True, rows=len(rows), path=str(target),
                msg=f"导出 {len(rows)} 行 -> {target.name} ({size:.2f} MB)")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    import argparse

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="market_stats 快照自举/刷新/导出")
    ap.add_argument("--db", default=None)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--import", dest="do_import", action="store_true",
                   help="表缺失时从 data/market_stats.csv.gz 导入")
    g.add_argument("--force-import", action="store_true", help="清空后重新导入")
    g.add_argument("--status", action="store_true", help="打印覆盖率/新鲜度")
    g.add_argument("--refresh", action="store_true", help="腾讯拉取当日快照")
    g.add_argument("--refresh-if-stale", type=int, metavar="DAYS",
                   help="仅当最新快照超过 DAYS 天时刷新")
    g.add_argument("--export-csv", nargs="?", const=str(DEFAULT_CSV),
                   help="导出 CSV(gz)")
    ap.add_argument("--date", default=None, help="刷新写入的日期(默认今天)")
    ap.add_argument("--limit", type=int, default=None, help="调试: 只刷前 N 只")
    a = ap.parse_args(argv)

    if a.status:
        st = status(a.db)
        if not st["available"]:
            print(f"[status] 不可用: {st['db']}(表缺失或无数据)")
            return 1
        print(f"[status] {st['rows']} 行 / {st['codes']} 只 / {st['snapshots']} 期 "
              f"({st['first']} ~ {st['latest']}), 最新快照 {st['age_days']} 天前"
              f"{' ⚠️ 偏旧' if st['stale'] else ' ✅'}")
        return 0
    if a.do_import or a.force_import:
        r = ensure_market_stats(a.db, force=bool(a.force_import))
        print(f"[import] {'OK' if r['ok'] else 'FAIL'}: {r['msg']}")
        return 0 if r["ok"] else 1
    if a.refresh_if_stale is not None:
        st = status(a.db)
        if st["available"] and st["age_days"] is not None \
                and st["age_days"] <= a.refresh_if_stale:
            print(f"[refresh] 跳过: 最新快照 {st['latest']}({st['age_days']} 天)")
            return 0
        r = refresh_from_tencent(a.db, a.date, limit=a.limit)
        print(f"[refresh] {'OK' if r['ok'] else 'FAIL'}: {r['msg']}")
        return 0 if r["ok"] else 1
    if a.refresh:
        r = refresh_from_tencent(a.db, a.date, limit=a.limit)
        print(f"[refresh] {'OK' if r['ok'] else 'FAIL'}: {r['msg']}")
        return 0 if r["ok"] else 1
    if a.export_csv:
        r = export_csv(a.export_csv, a.db)
        print(f"[export] {'OK' if r['ok'] else 'FAIL'}: {r['msg']}")
        return 0 if r["ok"] else 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
