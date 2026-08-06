# -*- coding: utf-8 -*-
"""
factor_pipeline.py — 因子面板统一调度
====================================
读取 kline_cache 全市场 OHLCV + turnover_ratio, 逐股计算 4 大类共 21 个因子,
输出因子面板宽表。

产出:
- output/factors_panel_full.parquet : 全历史 (code, trade_date, close, 21因子列)
- output/factors_YYYYMMDD.parquet  : 最新交易日截面 (每日运行产物, 含>=15因子列)

用法:
    python tasks/factor_engineering/factor_pipeline.py [--db qlib_pro_v16.db]
"""
import argparse
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent  # momentum 仓库根
sys.path.insert(0, str(ROOT))

DB_DEFAULT = "qlib_pro_v16.db"
COLS = ["code", "trade_date", "open", "high", "low", "close",
        "volume", "amount", "turnover_ratio"]


def _load_mod(name: str):
    """按文件名(可含数字前缀)加载模块。"""
    p = HERE / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# 各模块 -> 批量函数名
MODULE_FACTORIES = {
    "01_add_volatility_factors": "all_volatility_factors",
    "02_add_turnover_factors": "all_turnover_factors",
    "03_add_money_flow_factors": "all_money_flow_factors",
    "04_add_technical_factors": "all_technical_factors",
}


def load_factor_blocks():
    blocks = {}
    for mod_name, fn_name in MODULE_FACTORIES.items():
        mod = _load_mod(mod_name)
        blocks[mod_name] = getattr(mod, fn_name)
    return blocks


def compute_stock_factors(g: pd.DataFrame, blocks) -> pd.DataFrame:
    """对单只股票计算全部因子。g 含 COLS 列。"""
    g = g.sort_values("trade_date").copy()
    g["trade_date"] = pd.to_datetime(g["trade_date"])
    g = g.set_index("trade_date")
    out = pd.DataFrame(index=g.index)
    out["close"] = g["close"]
    for mod_name, fn in blocks.items():
        try:
            sub = fn(g)
            for c in sub.columns:
                out[c] = sub[c]
        except Exception as e:  # 单模块失败不影响整体
            print(f"  [warn] {mod_name} 失败: {e}")
    out = out.reset_index()  # trade_date 由索引还原为列
    return out


def build_panel(db_path: str, blocks) -> pd.DataFrame:
    import sqlite3
    con = sqlite3.connect(db_path)
    df = pd.read_sql(
        f"SELECT {','.join(COLS)} FROM kline_cache", con
    )
    con.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    print(f"[info] 加载 {len(df):,} 行 / {df['code'].nunique():,} 只股票")

    frames = []
    n = df["code"].nunique()
    for i, (code, g) in enumerate(df.groupby("code"), 1):
        fg = compute_stock_factors(g, blocks)
        fg.insert(0, "code", code)
        frames.append(fg)
        if i % 500 == 0:
            print(f"  ... {i:,}/{n:,}")
    panel = pd.concat(frames, ignore_index=True)
    return panel


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--outdir", default=str(HERE / "output"))
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    blocks = load_factor_blocks()
    panel = build_panel(args.db, blocks)

    factor_cols = [c for c in panel.columns if c not in ("code", "trade_date", "close")]
    print(f"[info] 因子列数 = {len(factor_cols)}")
    print(f"[info] 面板形状 = {panel.shape}")

    full_path = outdir / "factors_panel_full.parquet"
    panel.to_parquet(full_path, index=False)
    print(f"[ok] 全历史面板 -> {full_path}")

    latest = panel["trade_date"].max()
    sub = panel[panel["trade_date"] == latest].copy()
    date_str = pd.Timestamp(latest).strftime("%Y%m%d")
    daily_path = outdir / f"factors_{date_str}.parquet"
    sub.to_parquet(daily_path, index=False)
    print(f"[ok] 最新交易日 {date_str} 截面 ({len(sub):,} 只) -> {daily_path}")

    # 简要统计
    cov = panel[factor_cols].notna().mean().sort_values(ascending=False)
    print("\n[info] 因子非空覆盖率(全样本):")
    for c, v in cov.items():
        print(f"  {c:18s} {v*100:5.1f}%")


if __name__ == "__main__":
    main()
