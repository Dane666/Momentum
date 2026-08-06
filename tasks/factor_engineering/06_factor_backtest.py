# -*- coding: utf-8 -*-
"""
06_factor_backtest.py — 因子预测力分位回测
============================================
对高 |IC| 因子做横截面分位(quintile)排序, 计算各分位前向收益与
Q5(高)-Q1(低) 多空组合收益及胜率, 作为"因子是否带来胜率/收益提升"的证据。

用法:
    python tasks/factor_engineering/06_factor_backtest.py [--panel ...] [--topk 8]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"


def load_panel(path: str):
    panel = pd.read_parquet(path)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"])
    return panel.sort_values(["code", "trade_date"]).reset_index(drop=True)


def quintile_test(panel: pd.DataFrame, factor: str, fwd: str, q: int = 5):
    """返回 (各分位均值Series, 多空收益, 多空胜率, 每日Q1收益, 每日Q5收益)。"""
    df = panel[[factor, fwd, "trade_date"]].dropna()
    df = df.dropna(subset=[factor, fwd])
    # 按交易日横截面分位
    df["q"] = df.groupby("trade_date")[factor].transform(
        lambda s: pd.qcut(s.rank(method="first"), q, labels=False,
                          duplicates="drop")
    )
    grp = df.groupby("q")[fwd].agg(["mean", "std", "count"])
    # 每日 Q1(低)/Q5(高) 组内均值收益
    q1_per_date = df[df["q"] == 0].groupby("trade_date")[fwd].mean()
    q5_per_date = df[df["q"] == q - 1].groupby("trade_date")[fwd].mean()
    # 多空(每日期 Qtop - Qbottom): 默认 Q5-Q1
    ls_per_date = q5_per_date - q1_per_date
    ls = ls_per_date.mean()
    win = float((ls_per_date > 0).mean()) if len(ls_per_date) else np.nan
    return grp, ls, win, q1_per_date, q5_per_date


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default=str(OUT / "factors_panel_full.parquet"))
    ap.add_argument("--ic", default=str(OUT / "factor_ic_ir.csv"))
    ap.add_argument("--topk", type=int, default=8)
    ap.add_argument("--horizon", type=int, default=20)
    args = ap.parse_args()

    panel = load_panel(args.panel)
    for h in (5, 10, 20):
        panel[f"fwd{h}"] = panel.groupby("code")["close"].transform(
            lambda s: s.shift(-h) / s - 1.0
        )
    factor_cols = [c for c in panel.columns
                   if c not in ("code", "trade_date", "close")
                   and not c.startswith("fwd")]

    ic = pd.read_csv(args.ic)
    fwd = f"fwd{args.horizon}"
    top = ic[ic.horizon == args.horizon].reindex(
        ic[ic.horizon == args.horizon]["IC"].abs().sort_values(ascending=False).index
    ).head(args.topk)["factor"].tolist()

    print(f"[info] 对 {args.horizon}日收益 Top{args.topk} 因子做分位回测\n")
    print(f"{'factor':18s} {'IC':>7s} {'Q_low':>7s} {'Q_high':>7s} "
          f"{'可交易LS':>9s} {'日胜率':>7s}  方向")
    rows = []
    ic_map = dict(zip(ic[ic.horizon == args.horizon]["factor"],
                      ic[ic.horizon == args.horizon]["IC"]))
    for f in top:
        grp, ls, win, q1d, q5d = quintile_test(panel, f, fwd, q=5)
        q1 = grp.loc[0, "mean"] if 0 in grp.index else np.nan
        q5 = grp.loc[4, "mean"] if 4 in grp.index else np.nan
        ic_v = ic_map.get(f, 0.0)
        # 可交易方向: IC<0 -> 做多低分位(Q1); IC>0 -> 做多高分位(Q5)
        if ic_v < 0:
            actionable = (q1d - q5d)  # long low
            direction = "做多低值(Q1)"
        else:
            actionable = (q5d - q1d)  # long high
            direction = "做多高值(Q5)"
        a_ls = actionable.mean()
        a_win = float((actionable > 0).mean())
        rows.append({
            "factor": f, "IC": ic_v,
            "Q_low_mean": q1, "Q_high_mean": q5,
            "actionable_LS": a_ls, "actionable_win_rate": a_win,
            "direction": direction,
        })
        print(f"{f:18s} {ic_v:7.3f} {q1*100:6.2f}% {q5*100:6.2f}% "
              f"{a_ls*100:8.2f}% {a_win*100:6.1f}%  {direction}")

    res = pd.DataFrame(rows)
    res.to_csv(OUT / "factor_quintile_backtest.csv", index=False)
    print(f"\n[ok] -> {OUT/'factor_quintile_backtest.csv'}")

    # 报告
    lines = ["# 因子分位回测 (可交易方向多空)\n"]
    lines.append(f"- 前向收益: {args.horizon} 日 | 分位数: 5")
    lines.append("- 可交易方向: IC<0 做多低分位, IC>0 做多高分位\n")
    lines.append(res.round(4).to_markdown(index=False))
    lines.append("\n## 解读")
    lines.append("- actionable_LS>0 且 日胜率>50% : 因子有可交易选股能力")
    lines.append("- 波动率因子 IC 为负 -> 低波动股跑赢(经典低波动异象)")
    (OUT / "factor_quintile_report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] -> {OUT/'factor_quintile_report.md'}")


if __name__ == "__main__":
    main()
