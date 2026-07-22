# -*- coding: utf-8 -*-
"""
抓取真实基本面(ROE / 净利润同比(业绩拐点) / 营收同比 / EPS / 每股净资产) 并写入
qlib_pro_v16.db 的 fundamentals 表, 供超跌绩优反弹回测按交易日期取"当时可得"财报。

数据来源: akshare.stock_yjbb_em (东方财富 datacenter, 全A批量)
PE/PB 不在该接口, 由回测用真实 EPS + 当日收盘价 现场计算(point-in-time, 比陈旧快照更严谨)。

可用性规则(防未来函数): 报告期 period_end 的财报, 自 period_end+1 日起可用。
"""
import os, sys, time, sqlite3
import pandas as pd

DB = "/Users/admin/Documents/codeHub/adata-main/tests/momentum/qlib_pro_v16.db"
PERIODS = [
    "20240331", "20240630", "20240930", "20241231",
    "20250331", "20250630", "20250930", "20251231", "20260331",
]

def avail_from(period):
    y, m, d = int(period[:4]), int(period[4:6]), int(period[6:8])
    from datetime import date, timedelta
    # 报告期末次日可用(保守, 避免未来函数)
    dt = date(y, m, d) + timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

def annualize_factor(period):
    mm = period[4:6]
    return {"03": 4.0, "06": 2.0, "09": 4/3, "12": 1.0}[mm]

def fetch_one(period, ak):
    df = ak.stock_yjbb_em(date=period)
    rows = []
    fac = annualize_factor(period)
    for _, r in df.iterrows():
        try:
            code = str(r["股票代码"]).zfill(6)
        except Exception:
            continue
        def num(x):
            try:
                if pd.isna(x): return None
                return float(x)
            except Exception:
                return None
        roe = num(r.get("净资产收益率"))
        np_yoy = num(r.get("净利润-同比增长"))
        rev_yoy = num(r.get("营业总收入-同比增长"))
        eps = num(r.get("每股收益"))
        bvps = num(r.get("每股净资产"))
        ind = r.get("所处行业")
        ind = None if (ind is None or (isinstance(ind, float))) else str(ind)
        # 年化EPS(用于 point-in-time PE)
        eps_ann = eps * fac if eps is not None else None
        rows.append((code, period, avail_from(period), roe, np_yoy, rev_yoy, eps, eps_ann, bvps, ind))
    return rows

def main():
    import akshare as ak
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS fundamentals (
        code TEXT, period_end TEXT, avail_from TEXT,
        roe REAL, net_profit_yoy REAL, revenue_yoy REAL,
        eps REAL, eps_annualized REAL, bvps REAL, industry TEXT,
        PRIMARY KEY(code, period_end))""")
    total = 0
    for p in PERIODS:
        try:
            rows = fetch_one(p, ak)
        except Exception as e:
            print(f"  [WARN] {p} 抓取失败: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(2)
            continue
        con.executemany(
            "INSERT OR REPLACE INTO fundamentals VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
        total += len(rows)
        print(f"  {p}: {len(rows)} 行 (累计 {total})", flush=True)
        time.sleep(1)
    # 统计
    n = con.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
    nc = con.execute("SELECT COUNT(DISTINCT code) FROM fundamentals").fetchone()[0]
    print(f"完成: fundamentals 共 {n} 行, 覆盖 {nc} 只股票")
    con.close()

if __name__ == "__main__":
    main()
