# -*- coding: utf-8 -*-
"""
market_stats.py — A 股市值 / 股息率数据落库与读取

背景
----
原 qlib_pro_v16.db 的 `fundamentals` 仅含 ROE/净利/营收/EPS/BVPS/行业, 缺
market_cap 与 dividend_yield, 导致因子倾斜(小盘 / 红利 tilt)不可测。

数据源(AKShare, 经代理可达)
- `stock_value_em(symbol)` : 个股历史每日估值, 含 总市值/流通市值/总股本/流通股本/PE/PB.
  单位: 元. 历史每日, 覆盖到最新交易日.  -> 市值(权威)
- `stock_history_dividend()` : 全市场分红汇总(每票 累计股息/年均股息/分红次数/上市日期).
  经已知票核验(茅台累计38.9/年均1.62, 工行累计86.3/年均4.54, 海油年均7.4):
  年均股息 = 上市以来**平均年度股息率(%)**(非每股分红元).  -> 直接作为股息率近似落库.
  (精确的 股息率-TTM 实时端点被代理拦截 push2 主机, 故用长期均值股息率近似;
   该值反映"历史分红慷慨度", 用于因子倾斜的相对排序, 绝对水平非当下 TTM)

落库
- `market_stats(code, trade_date[月首], total_mv, circ_mv, dividend_yield)`  月频, 点态
- `dividend_stats(code, avg_annual_div, cum_div, div_count, list_date)`        每票静态

用法
- 回填:  python market_stats.py --backfill [--start 2024-04-01 --end 2026-07-31 --limit N]
- 读取:  from market_stats import load_market_stats;  d = load_market_stats(codes=[...])
"""
from __future__ import annotations
import os
import sys
import time
import sqlite3
import argparse
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ---------- DB 路径解析 (与 harness 同口径) ----------
def _default_db() -> str:
    # 优先环境变量(与 CI / harness 一致), 否则取脚本上级目录(tests/momentum)的 qlib_pro_v16.db
    env = os.environ.get("MOMENTUM_DB_PATH")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.normpath(os.path.join(here, "..", "qlib_pro_v16.db"))
    return cand if os.path.exists(cand) else "qlib_pro_v16.db"


DB_PATH = os.environ.get("MOMENTUM_DB_PATH") or _default_db()
DEFAULT_START = "2024-04-01"
DEFAULT_END = "2026-07-31"


# ---------- 表结构 ----------
def ensure_tables(con: sqlite3.Connection) -> None:
    con.execute(
        """CREATE TABLE IF NOT EXISTS market_stats (
               code        TEXT NOT NULL,
               trade_date  TEXT NOT NULL,
               total_mv    REAL,
               circ_mv     REAL,
               dividend_yield REAL,
               PRIMARY KEY (code, trade_date)
           )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS dividend_stats (
               code           TEXT PRIMARY KEY,
               name           TEXT,
               list_date      TEXT,
               avg_annual_div REAL,
               cum_div        REAL,
               div_count      INTEGER
           )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS market_stats_failed (
               code    TEXT PRIMARY KEY,
               reason  TEXT
           )"""
    )
    con.commit()


# ---------- 抓取层 ----------
def _month_key(d) -> str:
    # 接受 datetime.date / Timestamp / str
    if isinstance(d, str):
        return d[:7]
    return str(pd.Timestamp(d).strftime("%Y-%m"))


def fetch_value_em_monthly(symbol: str, start: str, end: str) -> List[Tuple[str, float, float]]:
    """返回该票月首快照 [(date'YYYY-MM-DD', total_mv, circ_mv)]; 失败抛异常由调用方处理."""
    import akshare as ak

    df = ak.stock_value_em(symbol=symbol)
    if df is None or df.empty:
        return []
    # 列: 数据日期, 当日收盘价, 当日涨跌幅, 总市值, 流通市值, 总股本, 流通股本, ...
    df = df.rename(columns={"数据日期": "date", "总市值": "total_mv", "流通市值": "circ_mv"})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df[(df["date"] >= start) & (df["date"] <= end)]
    if df.empty:
        return []
    df = df.sort_values("date")
    # 取每月第一个交易日
    df["mk"] = df["date"].str[:7]
    first = df.groupby("mk", as_index=False).first()
    out = []
    for _, r in first.iterrows():
        tm = r.get("total_mv")
        cm = r.get("circ_mv")
        if pd.isna(tm) or pd.isna(cm):
            continue
        out.append((r["date"], float(tm), float(cm)))
    return out


def fetch_dividend_summary() -> Dict[str, dict]:
    """批量分红汇总 -> {code: {avg_annual_div, cum_div, div_count, list_date, name}}."""
    import akshare as ak

    df = ak.stock_history_dividend()
    out = {}
    for _, r in df.iterrows():
        code = str(r.get("代码", "")).strip()
        if not code:
            continue
        out[code] = dict(
            name=str(r.get("名称", "")),
            list_date=str(r.get("上市日期", "")) if pd.notna(r.get("上市日期")) else None,
            avg_annual_div=float(r["年均股息"]) if pd.notna(r.get("年均股息")) else None,
            cum_div=float(r["累计股息"]) if pd.notna(r.get("累计股息")) else None,
            div_count=int(r["分红次数"]) if pd.notna(r.get("分红次数")) else 0,
        )
    return out


# ---------- 读取层 ----------
def load_market_stats(
    codes: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """读取 market_stats. 返回 {code: DataFrame[trade_date(idx), total_mv, circ_mv, dividend_yield]}."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    q = "SELECT code, trade_date, total_mv, circ_mv, dividend_yield FROM market_stats WHERE 1=1"
    params: list = []
    if start:
        q += " AND trade_date>=?"; params.append(start)
    if end:
        q += " AND trade_date<=?"; params.append(end)
    if codes:
        ph = ",".join("?" * len(codes))
        q += f" AND code IN ({ph})"
        params.extend(codes)
    q += " ORDER BY code, trade_date"
    rows = cur.execute(q, params).fetchall()
    con.close()
    res: Dict[str, list] = {}
    for code, td, tm, cm, dy in rows:
        res.setdefault(code, []).append((td, tm, cm, dy))
    out = {}
    for code, lst in res.items():
        out[code] = pd.DataFrame(lst, columns=["trade_date", "total_mv", "circ_mv", "dividend_yield"]).set_index("trade_date").sort_index()
    return out


def load_dividend_stats(codes: Optional[List[str]] = None) -> Dict[str, dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    q = "SELECT code, name, list_date, avg_annual_div, cum_div, div_count FROM dividend_stats"
    if codes:
        ph = ",".join("?" * len(codes)); q += f" WHERE code IN ({ph})"; params = codes
    else:
        params = []
    rows = cur.execute(q, params).fetchall()
    con.close()
    return {r[0]: dict(name=r[1], list_date=r[2], avg_annual_div=r[3], cum_div=r[4], div_count=r[5]) for r in rows}


# ---------- 回填编排 ----------
def _active_universe(con: sqlite3.Connection, start: str, end: str) -> List[str]:
    # 排除基金/债券代码(15/51/56/58 ETF/LOF, 11/12/13/14 债券): 它们不是股票市值 tilt 对象,
    # 且东方财富 stock_value_em 对 ETF 返回 null(正确行为). 仅保留股票代码(00/30/60/68/8/4/9 开头).
    rows = con.execute(
        """SELECT DISTINCT code FROM kline_cache
           WHERE trade_date>=? AND trade_date<=?
             AND code NOT LIKE '15%' AND code NOT LIKE '51%'
             AND code NOT LIKE '56%' AND code NOT LIKE '58%'
             AND code NOT LIKE '11%' AND code NOT LIKE '12%'
             AND code NOT LIKE '13%' AND code NOT LIKE '14%'
           ORDER BY code""",
        (start, end),
    ).fetchall()
    return [r[0] for r in rows]


def _already_codes(con: sqlite3.Connection) -> set:
    rows = con.execute("SELECT DISTINCT code FROM market_stats").fetchall()
    return {r[0] for r in rows}


def _month_start_close(con: sqlite3.Connection, code: str, dates: List[str]) -> Dict[str, float]:
    """从 kline_cache 取给定日期(月首)的收盘价; 缺失则用向前最近一个交易日."""
    out: Dict[str, float] = {}
    if not dates:
        return out
    for d in dates:
        row = con.execute(
            "SELECT close FROM kline_cache WHERE code=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
            (code, d),
        ).fetchone()
        out[d] = float(row[0]) if row else None
    return out


def backfill(
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
    limit: Optional[int] = None,
    throttle: float = 0.06,
    max_retry: int = 3,
) -> dict:
    import akshare  # 确保可用

    ensure_tables(con := sqlite3.connect(DB_PATH))
    print(f"[backfill] DB={DB_PATH}")
    print("[backfill] 抓取全市场分红汇总 ...")
    div = fetch_dividend_summary()
    drows = [(c, v["name"], v["list_date"], v["avg_annual_div"], v["cum_div"], v["div_count"]) for c, v in div.items()]
    con.executemany(
        "INSERT OR REPLACE INTO dividend_stats(code,name,list_date,avg_annual_div,cum_div,div_count) VALUES(?,?,?,?,?,?)",
        drows,
    )
    con.commit()
    print(f"[backfill] dividend_stats 落地 {len(drows)} 票")

    universe = _active_universe(con, start, end)
    done = _already_codes(con)
    failed = {r[0] for r in con.execute("SELECT code FROM market_stats_failed").fetchall()}
    done = done | failed
    todo = [c for c in universe if c not in done]
    if limit:
        todo = todo[:limit]
    print(f"[backfill] 活跃 universe={len(universe)} 已落={len(done)-len(failed)} 永久失败={len(failed)} 待抓={len(todo)}")

    ok = skip = fail = 0
    for i, code in enumerate(todo):
        monthly = None
        last_err = None
        for attempt in range(max_retry):
            try:
                monthly = fetch_value_em_monthly(code, start, end)
                break
            except Exception as e:
                last_err = e
                if attempt < max_retry - 1:
                    time.sleep(0.5 * (attempt + 1))
                else:
                    print(f"  [fail] {code}: {type(e).__name__} {str(e)[:60]}")
        if not monthly:
            con.execute("INSERT OR IGNORE INTO market_stats_failed(code,reason) VALUES(?,?)",
                        (code, f"{type(last_err).__name__}: {str(last_err)[:50]}" if last_err else "empty"))
            con.commit()
            skip += 1
            continue
        dates = [m[0] for m in monthly]
        divinfo = div.get(code, {})
        # 年均股息 已验证为"上市以来平均年度股息率(%)" -> 直接作为股息率近似落库
        avg_div = divinfo.get("avg_annual_div")
        dy_val = round(float(avg_div), 4) if avg_div is not None else None
        rows = []
        for d, tm, cm in monthly:
            rows.append((code, d, tm, cm, dy_val))
        con.executemany(
            "INSERT OR REPLACE INTO market_stats(code,trade_date,total_mv,circ_mv,dividend_yield) VALUES(?,?,?,?,?)",
            rows,
        )
        con.commit()
        ok += 1
        if (i + 1) % 100 == 0:
            print(f"  progress {i+1}/{len(todo)} ok={ok} skip={skip} fail={fail}")
        time.sleep(throttle)
    con.close()
    summary = dict(total_universe=len(universe), done=ok, skip=skip, fail=fail, dividend_stocks=len(drows))
    print("[backfill] 完成:", summary)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--throttle", type=float, default=0.06)
    a = ap.parse_args()
    if a.backfill:
        backfill(start=a.start, end=a.end, limit=a.limit, throttle=a.throttle)
    else:
        print("use --backfill to fetch & store market cap / dividend yield")
