# -*- coding: utf-8 -*-
"""
05_factor_validation.py — 因子有效性验证
========================================
加载因子面板, 计算:
1. 前向收益 (5/10/20 日)
2. 横截面 Rank IC / IR (按交易日平均的 Spearman 相关性)
3. 因子相关性矩阵 (横截面去均值后的 pooled 相关, 用于冗余筛查)

产出:
- output/factor_ic_ir.csv
- output/factor_corr_matrix.csv
- output/factor_validation_report.md
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"


def load_panel(path: str) -> pd.DataFrame:
    panel = pd.read_parquet(path)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    return panel.sort_values(["code", "trade_date"]).reset_index(drop=True)


def add_forward_returns(panel: pd.DataFrame, horizons=(5, 10, 20)) -> pd.DataFrame:
    for h in horizons:
        panel[f"fwd{h}"] = panel.groupby("code")["close"].transform(
            lambda s: s.shift(-h) / s - 1.0
        )
    return panel


def compute_ic_ir(panel: pd.DataFrame, factor_cols, horizons=(5, 10, 20)):
    """逐交易日计算因子与前向收益的 Rank IC, 返回均值(IC)与 IR=mean/std。"""
    dates = panel["trade_date"].unique()
    recs = []
    for h in horizons:
        fwd = f"fwd{h}"
        # 透视: 行=交易日, 列=code
        fw = panel.pivot(index="trade_date", columns="code", values=fwd)
        ic_per_factor = {f: [] for f in factor_cols}
        for d in dates:
            row = panel[panel["trade_date"] == d]
            if row[fwd].notna().sum() < 30:
                continue
            sub = row[["code"] + factor_cols + [fwd]].dropna()
            if len(sub) < 30:
                continue
            corr = sub[factor_cols + [fwd]].corr(method="spearman")
            for f in factor_cols:
                v = corr.loc[f, fwd]
                if pd.notna(v):
                    ic_per_factor[f].append(v)
        for f in factor_cols:
            arr = np.array(ic_per_factor[f])
            if len(arr) > 0:
                ic = arr.mean()
                ir = arr.mean() / (arr.std() + 1e-9)
                recs.append({
                    "factor": f, "horizon": h,
                    "IC": ic, "IR": ir,
                    "IC_std": arr.std(), "n_days": len(arr),
                    "IC_pos_ratio": float((arr > 0).mean()),
                })
    return pd.DataFrame(recs)


def compute_corr_matrix(panel: pd.DataFrame, factor_cols):
    """横截面去均值后 pooled 相关性(冗余筛查)。"""
    df = panel[["trade_date"] + factor_cols].copy()
    # 每个交易日横截面去均值
    df[factor_cols] = df.groupby("trade_date")[factor_cols].transform(
        lambda s: s - s.mean()
    )
    return df[factor_cols].corr()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default=str(OUT / "factors_panel_full.parquet"))
    args = ap.parse_args()

    panel = load_panel(args.panel)
    factor_cols = [c for c in panel.columns
                   if c not in ("code", "trade_date", "close")
                   and not c.startswith("fwd")]
    print(f"[info] 因子列 {len(factor_cols)} 个, 样本 {len(panel):,}")

    panel = add_forward_returns(panel)
    print("[info] 前向收益已计算")

    icir = compute_ic_ir(panel, factor_cols)
    icir.to_csv(OUT / "factor_ic_ir.csv", index=False)
    print(f"[ok] IC/IR -> {OUT/'factor_ic_ir.csv'}")

    corr = compute_corr_matrix(panel, factor_cols)
    corr.to_csv(OUT / "factor_corr_matrix.csv")
    print(f"[ok] 相关性矩阵 -> {OUT/'factor_corr_matrix.csv'}")

    # 报告
    best = icir.reindex(
        icir.groupby("horizon")["IC"].apply(lambda s: s.abs()).sort_values(ascending=False).index
    )
    lines = []
    lines.append("# 因子有效性验证报告\n")
    lines.append(f"- 样本: {len(panel):,} 行 / {panel['code'].nunique():,} 只 / "
                 f"{panel['trade_date'].nunique()} 交易日")
    lines.append(f"- 因子数: {len(factor_cols)}\n")
    lines.append("## Rank IC / IR (按 20 日收益排序取 Top10)\n")
    top20 = icir[icir.horizon == 20].reindex(
        icir[icir.horizon == 20]["IC"].abs().sort_values(ascending=False).index
    ).head(10)
    lines.append(top20.round(4).to_markdown(index=False))
    lines.append("\n## 高相关因子对 (|r|>0.7, 冗余预警)\n")
    pairs = []
    c = corr.values
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            if abs(c[i, j]) > 0.7:
                pairs.append((cols[i], cols[j], round(float(c[i, j]), 3)))
    if pairs:
        for a, b, r in sorted(pairs, key=lambda x: -abs(x[2])):
            lines.append(f"- {a} ~ {b} : r={r}")
    else:
        lines.append("- (无 |r|>0.7 的强冗余对)")
    rep = "\n".join(lines)
    (OUT / "factor_validation_report.md").write_text(rep, encoding="utf-8")
    print(f"[ok] 报告 -> {OUT/'factor_validation_report.md'}")

    # 控制台摘要
    print("\n=== 20日收益 Top 因子 (按 |IC|) ===")
    print(top20[["factor", "IC", "IR", "IC_pos_ratio", "n_days"]].round(4).to_string(index=False))


if __name__ == "__main__":
    main()
