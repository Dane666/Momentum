# -*- coding: utf-8 -*-
"""
factor_tilt_smoke.py — 因子倾斜(小盘 / 红利)可行性烟雾测试

验证 market_stats 落库数据可被 harness 以 point-in-time 口径读取,
并在逐月截面上演示: 按 流通市值 分小盘/大盘, 按 dividend_yield 分高红利/低红利.

用法: python factor_tilt_smoke.py
"""
import os, sys, importlib.util as _ilu
from pathlib import Path as _P
import pandas as pd

ROOT = str(_P(__file__).resolve().parent.parent)  # tests/momentum
_spec = _ilu.spec_from_file_location("momentum", str(_P(ROOT) / "__init__.py"),
                                     submodule_search_locations=[ROOT])
_mm = _ilu.module_from_spec(_spec); sys.modules["momentum"] = _mm
_spec.loader.exec_module(_mm)

import harness_oversold_quality as HOQ
HOQ.DB = os.path.join(ROOT, "qlib_pro_v16.db")
HOQ.ROOT = ROOT

def main():
    mmap = HOQ.load_market_stats()
    print(f"[smoke] 落库股票数: {len(mmap)}")
    if not mmap:
        print("[smoke] 无数据 — 请先运行 market_stats.py --backfill"); return

    # 汇总每个月末可参与因子分组的股票数(覆盖率)
    dates = sorted({rec["trade_date"] for recs in mmap.values() for rec in recs})
    print(f"[smoke] 月末快照数: {len(dates)}  ({dates[0]} ~ {dates[-1]})")

    # 月度市值覆盖率(直接查 market_stats)
    import sqlite3
    con = sqlite3.connect(HOQ.DB)
    mc = con.execute(
        "SELECT trade_date, COUNT(DISTINCT code) FROM market_stats GROUP BY trade_date ORDER BY trade_date"
    ).fetchall()
    failed = con.execute("SELECT COUNT(*) FROM market_stats_failed").fetchone()[0]
    con.close()
    print(f"[smoke] 月度市值覆盖: 首月{mc[0]} 末月{mc[-1]}  范围 {min(c for _,c in mc)}~{max(c for _,c in mc)} 只/月")
    print(f"[smoke] 永久失败(接口null,退市/停牌): {failed} 只 (已记入 market_stats_failed, 重跑跳过)")

    # 选最近一个可用月末做演示
    d = dates[-1]
    rows = []
    for code, recs in mmap.items():
        rec = HOQ.market_stats_at(mmap, code, d)
        if rec and rec["circ_mv"] and rec["dividend_yield"] is not None:
            rows.append((code, rec["total_mv"], rec["circ_mv"], rec["dividend_yield"]))
    df = pd.DataFrame(rows, columns=["code", "total_mv", "circ_mv", "div_yield"])
    print(f"\n[smoke] {d} 可分组股票数(有市值且有股息率): {len(df)}")

    # 小盘 tilt: 流通市值最小 20% vs 最大 20%
    df = df.sort_values("circ_mv")
    n = len(df); k = max(1, n // 5)
    small = df.head(k); large = df.tail(k)
    print(f"[smoke] 小盘组({k}只) 中位流通市值={small.circ_mv.median()/1e8:.1f}亿  红利中位={small.div_yield.median():.2f}%")
    print(f"[smoke] 大盘组({k}只) 中位流通市值={large.circ_mv.median()/1e8:.1f}亿  红利中位={large.div_yield.median():.2f}%")

    # 红利 tilt: 股息率最高 20% vs 最低 20%
    dh = df.sort_values("div_yield").tail(k); dl = df.sort_values("div_yield").head(k)
    print(f"[smoke] 高红利组({k}只) 股息率中位={dh.div_yield.median():.2f}%  中位流通市值={dh.circ_mv.median()/1e8:.1f}亿")
    print(f"[smoke] 低红利组({k}只) 股息率中位={dl.div_yield.median():.2f}%  中位流通市值={dl.circ_mv.median()/1e8:.1f}亿")

    print("\n[smoke] 因子倾斜可行: 市值/股息率字段已可按 point-in-time 月度截面读取与分组.")

if __name__ == "__main__":
    main()
