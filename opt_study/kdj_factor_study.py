# -*- coding: utf-8 -*-
"""
KDJ 底部金叉 是否能提升「低位绩优股」胜率 —— 因子回溯测试
=========================================================
复用 harness_oversold_quality 的真实口径:
  - 低位 : H.base_signal(深度超跌 dd<=-15% & RSI<35 & 跌破60日线)
  - 绩优 : H.quality_ok(ROE≥8% & 净利>0 & PE≤50 & PB≤10, point-in-time)
候选日 = 上述两者同时成立的那一天(即 low_quality_scan 实际会命中的标的日)。

对每个候选日:
  1) 计算 KDJ(9,3,3), 判定"底部金叉" = K 上穿 D 且 D 处于超卖区(D<30, 另记 D<20)
  2) 记录 N 日正向收益(close[t+N]/close[t]-1), N=5/10/20/30
分组对比:
  ALL         : 全部低位绩优候选
  WITH_KDJ30  : 候选日发生 底部金叉(D<30)
  WITHOUT_KDJ : 候选日未发生金叉
  WITH_KDJ20  : 候选日发生 深度底部金叉(D<20) [参考]

核心指标: 胜率(正向收益占比)、均值、中位数、样本量; 并对 20 日收益做 Welch t 检验。
不修改任何原有文件; 结果打印 + 落盘 kdj_factor_study_result.json。
"""
import sys, os, json
from pathlib import Path
import numpy as np
import pandas as pd
import importlib.util

_PROJ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJ))

spec = importlib.util.spec_from_file_location(
    'harness_oversold_quality', _PROJ / 'opt_study' / 'harness_oversold_quality.py')
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)

# 复用真实扫描口径(对齐 low_quality_scan.SCAN_CFG)
H.DB = os.path.join(str(_PROJ), 'qlib_pro_v16.db')
H.WINDOW_START = "2024-07-01"
H.WINDOW_END = "2026-07-15"

SCAN_CFG = dict(mode="deep", dd=-0.15, gap=0.03, rsi_th=35,
                ma60_rising=False, vol_confirm=False, macd_rsi=False,
                hot_on=True, pe_pb_on=True, quality_on=True)

HORIZONS = [5, 10, 20, 30]


def compute_kdj(g, n=9, k_smooth=3, d_smooth=3):
    low_n = g['low'].rolling(n, min_periods=n).min()
    high_n = g['high'].rolling(n, min_periods=n).max()
    rng = (high_n - low_n).replace(0, np.nan)
    rsv = ((g['close'] - low_n) / rng * 100).fillna(50)
    k = rsv.ewm(alpha=1 / k_smooth, adjust=False).mean()
    d = k.ewm(alpha=1 / d_smooth, adjust=False).mean()
    j = 3 * k - 2 * d
    return k, d, j


def golden_cross(k, d):
    return (k > d) & (k.shift(1) <= d.shift(1).fillna(-999))


def summarize(sub, label):
    row = {'group': label, 'n': int(len(sub))}
    for h in HORIZONS:
        col = f'f{h}'
        vals = sub[col].dropna().values
        if len(vals) == 0:
            row[f'win{h}'] = None
            row[f'mean{h}'] = None
            row[f'med{h}'] = None
            continue
        win = float(np.mean(vals > 0) * 100)
        row[f'win{h}'] = round(win, 1)
        row[f'mean{h}'] = round(float(np.mean(vals)) * 100, 2)
        row[f'med{h}'] = round(float(np.median(vals)) * 100, 2)
    return row


def main():
    print("加载K线...", flush=True)
    ctx = H.load_kline()
    print(f"  标的数={len(ctx)}", flush=True)
    ctx = H.build_ctx(ctx)
    print("计算 KDJ...", flush=True)
    for code, g in ctx.items():
        k, d, _ = compute_kdj(g)
        g['kdj_k'] = k
        g['kdj_d'] = d
        g['kdj_cross'] = golden_cross(k, d)
    fmap = H.load_fundamentals()
    print(f"  基本面覆盖 {len(fmap)} 只", flush=True)

    last_t = pd.Timestamp("2026-06-01")  # 留足 30 日前瞻
    records = []
    for code, g in ctx.items():
        if g.shape[0] < 120:
            continue
        for t in g.index:
            if t < pd.Timestamp(H.WINDOW_START) or t > last_t:
                continue
            if not H.base_signal(g, t, SCAN_CFG):
                continue
            close = float(g.loc[t, 'close'])
            ok, *_ = H.quality_ok(fmap, code, str(t)[:10], close, True)
            if not ok:
                continue
            pos = g.index.get_loc(t)
            cross = bool(g.loc[t, 'kdj_cross']) if not pd.isna(g.loc[t, 'kdj_cross']) else False
            dval = g.loc[t, 'kdj_d']
            dval = float(dval) if not pd.isna(dval) else np.nan
            rec = dict(code=code, t=str(t)[:10], cross=cross,
                       dval=round(dval, 1) if not pd.isna(dval) else None)
            for h in HORIZONS:
                rec[f'f{h}'] = (g['close'].iloc[pos + h] / close - 1.0) if (pos + h < g.shape[0]) else np.nan
            records.append(rec)
    df = pd.DataFrame(records)
    print(f"低位绩优候选样本数 = {len(df)}", flush=True)
    if df.empty:
        print("无候选样本, 退出")
        return

    with_kdj30 = df[(df['cross']) & (df['dval'] < 30)]
    with_kdj20 = df[(df['cross']) & (df['dval'] < 20)]
    without = df[~df['cross']]

    rows = [
        summarize(df, "ALL(全部低位绩优候选)"),
        summarize(with_kdj30, "WITH_KDJ30(底部金叉 D<30)"),
        summarize(without, "WITHOUT_KDJ(无金叉)"),
        summarize(with_kdj20, "WITH_KDJ20(深度底部金叉 D<20)"),
    ]

    # Welch t 检验: WITH_KDJ30 vs WITHOUT, 针对 20 日收益
    try:
        from scipy import stats
        a = with_kdj30['f20'].dropna().values
        b = without['f20'].dropna().values
        if len(a) > 1 and len(b) > 1:
            tt, pp = stats.ttest_ind(a, b, equal_var=False)
            rows.append({'group': 'T_TEST(WITH30 vs WITHOUT, 20d)',
                         'n': f'a={len(a)},b={len(b)}',
                         'win20': None, 'mean20': round(float(np.mean(a)) * 100, 2),
                         'med20': round(float(np.median(a)) * 100, 2),
                         't_stat': round(float(tt), 3), 'p_value': round(float(pp), 4)})
    except Exception as e:
        print("scipy 不可用, 跳过显著性检验:", e)

    # 打印
    hdr = f"{'group':36s} {'n':>7s} | " + " ".join(
        f"胜{h:>3d}/均{h:>3d}" for h in HORIZONS)
    print("\n" + hdr)
    for r in rows:
        if 't_stat' in r:
            print(f"{r['group']:36s} {str(r['n']):>7s} | t={r['t_stat']} p={r['p_value']} "
                  f"(20d均值 WITH={r['mean20']}% vs WITHOUT 见上)")
            continue
        cells = []
        for h in HORIZONS:
            w = r.get(f'win{h}'); m = r.get(f'mean{h}')
            cells.append(f"{(str(w)+'%' if w is not None else ' - '):>5s}/"
                         f"{(str(m)+'%' if m is not None else ' - '):>6s}")
        print(f"{r['group']:36s} {r['n']:>7d} | " + " ".join(cells))

    out = dict(window=[H.WINDOW_START, H.WINDOW_END], horizons=HORIZONS,
               scan_cfg=SCAN_CFG, rows=rows,
               samples=dict(total=len(df), with_kdj30=len(with_kdj30),
                            with_kdj20=len(with_kdj20), without=len(without)))
    out_path = _PROJ / 'opt_study' / 'kdj_factor_study_result.json'
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"\n结果已落盘: {out_path}")


if __name__ == "__main__":
    main()
